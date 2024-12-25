use std::{str::FromStr, time::Duration};

use futures::TryFutureExt;
use gasket::{
    error::AsWorkError,
    runtime::{spawn_stage, WorkOutcome},
};

use mongodb::{
    bson::{doc, to_document, Bson, Document},
    options::{ClientOptions, UpdateOptions},
    Client, Collection,
};
use serde::Deserialize;
use tokio::runtime::Runtime;

use crate::{bootstrap, crosscut, model};

fn value_to_bson(value: impl Into<model::Value>) -> Bson {
    let value: model::Value = value.into();
    match value {
        model::Value::String(s) => Bson::String(s),
        model::Value::BigInt(n) => Bson::String(n.to_string()),
        model::Value::Cbor(b) => Bson::String(hex::encode(b)),
        model::Value::Json(j) => Bson::Document(doc! {
            "type": "json",
            "value": mongodb::bson::to_bson(&j).unwrap_or(Bson::Null)
        }),
    }
}

type InputPort = gasket::messaging::TwoPhaseInputPort<model::CRDTCommand>;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub connection_string: String,
    pub database_name: String,
    pub collection_name: String,
    pub cursor_key: Option<String>,
}

impl Config {
    pub fn bootstrapper(
        self,
        _chain: &crosscut::ChainWellKnownInfo,
        _intersect: &crosscut::IntersectConfig,
    ) -> Bootstrapper {
        Bootstrapper {
            config: self,
            input: Default::default(),
        }
    }

    pub fn cursor_key(&self) -> &str {
        self.cursor_key.as_deref().unwrap_or("_cursor")
    }
}

pub struct Bootstrapper {
    config: Config,
    input: InputPort,
}

impl Bootstrapper {
    pub fn borrow_input_port(&mut self) -> &'_ mut InputPort {
        &mut self.input
    }

    pub fn build_cursor(&self) -> Cursor {
        Cursor {
            config: self.config.clone(),
            runtime: Runtime::new().expect("Failed to create Tokio runtime"),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline) {
        let worker = Worker {
            config: self.config.clone(),
            client: None,
            collection: None,
            input: self.input,
            ops_count: Default::default(),
            runtime: Runtime::new().expect("Failed to create Tokio runtime"),
        };

        pipeline.register_stage(spawn_stage(
            worker,
            gasket::runtime::Policy {
                tick_timeout: Some(Duration::from_secs(6000)),
                bootstrap_retry: gasket::retries::Policy {
                    max_retries: 20,
                    backoff_unit: Duration::from_secs(1),
                    backoff_factor: 2,
                    max_backoff: Duration::from_secs(60),
                },
                ..Default::default()
            },
            Some("mongodb"),
        ));
    }
}

pub struct Cursor {
    config: Config,
    runtime: Runtime,
}

impl Cursor {
    pub fn last_point(&mut self) -> Result<Option<crosscut::PointArg>, crate::Error> {
        self.runtime.block_on(async {
            let client = Client::with_uri_str(&self.config.connection_string)
                .await
                .map_err(|e| crate::Error::StorageError(e.to_string()))?;
            
            let db = client.database(&self.config.database_name);
            let collection = db.collection::<mongodb::bson::Document>(&self.config.collection_name);

            let cursor_doc = collection
                .find_one(doc! { "_id": self.config.cursor_key() }, None)
                .await
                .map_err(|e| crate::Error::StorageError(e.to_string()))?;

            match cursor_doc {
                Some(doc) => {
                    let point_str = doc.get("point")
                        .and_then(|b| b.as_str())
                        .ok_or_else(|| crate::Error::StorageError("Invalid cursor format".into()))?;
                    Ok(Some(crosscut::PointArg::from_str(point_str)?))
                }
                None => Ok(None),
            }
        })
    }
}

pub struct Worker {
    config: Config,
    client: Option<Client>,
    collection: Option<Collection<mongodb::bson::Document>>,
    ops_count: gasket::metrics::Counter,
    input: InputPort,
    runtime: Runtime,
}

impl gasket::runtime::Worker for Worker {
    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Builder::new()
            .with_counter("storage_ops", &self.ops_count)
            .build()
    }

    fn work(&mut self) -> gasket::runtime::WorkResult {
        let msg = self.input.recv_or_idle()?;
        let collection = self.collection.as_ref().unwrap();

        self.runtime.block_on(async {
            match msg.payload {
            model::CRDTCommand::BlockStarting(_) => {
                // MongoDB transactions require replica sets, so we'll just proceed without transaction
                // for simplicity. In production, you might want to use transactions if running with replica sets.
            }
            model::CRDTCommand::GrowOnlySetAdd(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$addToSet": { "values": value_to_bson(value) }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::TwoPhaseSetAdd(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$addToSet": { "values": value_to_bson(value) }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::TwoPhaseSetRemove(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": format!("{}.ts", key) },
                        doc! { 
                            "$addToSet": { "tombstones": value_to_bson(value) }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::SetAdd(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$addToSet": { "values": value_to_bson(value) }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::SetRemove(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$pull": { "values": value_to_bson(value) }
                        },
                        None,
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::LastWriteWins(key, value, ts) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$set": { 
                                "value": value_to_bson(value),
                                "timestamp": (ts as i64),
                            }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::SortedSetAdd(key, value, delta) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$inc": { format!("scores.{}", value): delta }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::SortedSetRemove(key, value, delta) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$inc": { format!("scores.{}", value): delta }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::AnyWriteWins(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$set": { "value": value_to_bson(value) }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::PNCounter(key, value) => {
                collection
                    .update_one(
                        doc! { "_id": &key },
                        doc! { 
                            "$inc": { "counter": value }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            model::CRDTCommand::BlockFinished(point) => {
                let cursor_str = crosscut::PointArg::from(point).to_string();
                collection
                    .update_one(
                        doc! { "_id": self.config.cursor_key() },
                        doc! { 
                            "$set": { "point": cursor_str }
                        },
                        UpdateOptions::builder().upsert(true).build(),
                    )
                    .await
                    .map_err(|e| crate::Error::StorageError(e.to_string()))
                    .or_restart()?;
            }
            };

            self.ops_count.inc(1);
            self.input.commit();

            Ok(WorkOutcome::Partial)
        })
    }

    fn bootstrap(&mut self) -> Result<(), gasket::error::Error> {
        let client = self.runtime.block_on(async {
            Client::with_uri_str(&self.config.connection_string)
                .await
                .or_retry()
        })?;
        
        let db = client.database(&self.config.database_name);
        let collection = db.collection(&self.config.collection_name);

        self.collection = Some(collection);
        self.client = Some(client);

        Ok(())
    }

    fn teardown(&mut self) -> Result<(), gasket::error::Error> {
        Ok(())
    }
}
