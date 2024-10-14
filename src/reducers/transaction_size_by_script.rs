// CRFA
// used by crfa in prod

use pallas_traverse::MultiEraOutput;
use pallas_traverse::{MultiEraBlock, OutputRef};
use serde::Deserialize;

use crate::crosscut::epochs::block_epoch;
use crate::{crosscut, model, prelude::*};

use std::collections::HashSet;

#[derive(Deserialize, Copy, Clone)]
pub enum Projection {
    Individual,
    Total,
}

#[derive(Deserialize, Copy, Clone, PartialEq)]
pub enum AddrType {
    Hex,
}

#[derive(Deserialize, Copy, Clone, PartialEq)]
pub enum AggrType {
    Epoch,
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub filter: Option<crosscut::filters::Predicate>,
    pub aggr_by: Option<AggrType>,
    pub key_addr_type: Option<AddrType>,
    pub projection: Projection,
}

pub struct Reducer {
    config: Config,
    policy: crosscut::policies::RuntimePolicy,
    chain: crosscut::ChainWellKnownInfo,
}

impl Reducer {

    fn config_key(&self, address: String, epoch_no: u64) -> String {
        let def_key_prefix = "trx_size_by_script";

        match &self.config.aggr_by {
            Some(aggr_type) if matches!(aggr_type, AggrType::Epoch) => {
                return match &self.config.key_prefix {
                    Some(prefix) => format!("{}.{}.{}", prefix, address, epoch_no),
                    None => format!("{}.{}", def_key_prefix.to_string(), address),
                };
            },
            _ => {
                return match &self.config.key_prefix {
                    Some(prefix) => format!("{}.{}", prefix, address),
                    None => format!("{}.{}", def_key_prefix.to_string(), address),
                };
            }
        };
    }

    fn process_inbound_txo(
        &mut self,
        ctx: &model::BlockContext,
        seen: &mut HashSet<String>,
        input: &OutputRef,
    ) -> Result<(), gasket::error::Error> {

        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;

        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(())
        };

        let is_script_address = utxo.address().map_or(false, |addr| addr.has_script());

        if !is_script_address {
            return Ok(());
        }

        let address = utxo.address()
        .map(|addr| {
            match &self.config.key_addr_type {
                Some(addr_typ) if matches!(addr_typ, AddrType::Hex) => addr.to_hex(),
                _ => addr.to_string()
            }
        })
        .or_panic()?;

        seen.insert(address);

        Ok(())
    }

    fn process_outbound_txo(
        &mut self,
        seen: &mut HashSet<String>,
        tx_output: &MultiEraOutput,
    ) -> Result<(), gasket::error::Error> {
        let is_script_address = tx_output.address().map_or(false, |addr| addr.has_script());

        if !is_script_address {
            return Ok(());
        }

        let address = tx_output.address()
        .map(|addr| {
            match &self.config.key_addr_type {
                Some(addr_typ) if matches!(addr_typ, AddrType::Hex) => addr.to_hex(),
                _ => addr.to_string()
            }
        })
        .or_panic()?;

        seen.insert(address);

        Ok(())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {

        for tx in block.txs().into_iter() {
            if filter_matches!(self, block, &tx, ctx) {
                let epoch_no = block_epoch(&self.chain, block);
                let mut seen = HashSet::new();

                for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                    self.process_inbound_txo(&ctx, &mut seen, &consumed)?;
                }

                for (_, produced) in tx.produces() {
                    self.process_outbound_txo(&mut seen, &produced)?;
                }

                let tx_len = tx.encode().len();

                if tx_len == 0 {
                    return Ok(());
                }

                for addr in seen.iter() {
                    let key = self.config_key(addr.to_string(), epoch_no);

                    let crdt = match &self.config.projection {
                        Projection::Individual => model::CRDTCommand::GrowOnlySetAdd(key, format!("{}", tx_len)),
                        Projection::Total => model::CRDTCommand::PNCounter(key, tx_len as i64),
                    };

                    output.send(gasket::messaging::Message::from(crdt))?;
                }
            }
        }

        Ok(())
    }
}

impl Config {
    pub fn plugin(self, 
        chain: &crosscut::ChainWellKnownInfo,
        policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy: policy.clone(),
            chain: chain.clone(),
        };

        super::Reducer::TransactionSizeByScript(reducer)
    }
}