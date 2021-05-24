use crate::{tx::*, tx_manager::*, Error, Result};
use std::{collections::VecDeque, sync::Mutex, usize};

pub struct Database {
    // fixed table for now
    data: Vec<Versions>,
    pub(crate) tx_manager: TxManager,
}

impl Database {
    pub fn new() -> Self {
        let mut data = Vec::new();
        data.resize_with(10, Default::default);
        Database {
            data,
            tx_manager: TxManager::default(),
        }
    }

    pub(crate) fn put(&self, tx: &Transaction, key: usize, value: usize) -> Result<()> {
        let is_committed = |txid| self.tx_manager.get_status(txid) == TxStatus::Committed;
        self.data[key].put(tx.id(), value, is_committed)
    }

    pub(crate) fn get(&self, tx: &Transaction, key: usize) -> Result<usize> {
        // 表示该事务可以看到哪些事务版本的值
        // self.tx_manager.get_status()
        self.data[key].get(|txid| tx.snapshot().can_see(txid), |txid| self.tx_manager.get_status(txid))
    }
}

#[derive(Default)]
struct Versions {
    /// Old-to-new version list.
    records: Mutex<VecDeque<Record>>,
}

#[derive(Debug)]
struct Record {
    tmin: TxId,
    tmax: TxId,
    data: usize,
    // redo_log中备份的data, 用于rollback回滚时使用
    redo_log_data: usize,
}

impl Versions {
    // 1. 当对一个key进行多次put修改时 需要根据TxId 来找到上次修改的Record
    fn put(&self, txid: TxId, data: usize, is_committed: impl Fn(TxId) -> bool) -> Result<()> {
        trace!("put: txid={:?}, data={:?}", txid, data);
        let mut records = self.records.lock().unwrap();
        let mut find_rec: Option<&mut Record> = None;
        for item in records.iter_mut() {
            if item.tmin == txid {
                find_rec = Some(item);
            }
        }
        if let Some(rec) = find_rec {
            rec.data = data;
        } else {
            let record = Record {
                tmin: txid,
                tmax: TxId::MAX,
                data,
                redo_log_data: data,
            };
            records.push_back(record);
        }
        Ok(())
    }

    fn get(&self, can_see: impl Fn(TxId) -> bool, get_status: impl Fn(TxId) -> TxStatus) -> Result<usize> {
        let records = self.records.lock().unwrap();
        let mut max_tx_id = 0;
        let mut data  = 0;
        let mut status = false;
        for record in records.iter().rev() {
            // if tx is rollback, ignore it
            if can_see(record.tmin) && record.tmin >= max_tx_id && get_status(record.tmin) == TxStatus::Committed{
                status = true;
                max_tx_id = record.tmin;
                data = record.data
            }
            
        }
        if status {
            return Ok(data);
        }
        Err(Error::NotFound)
    }

    fn put_v2(&self, txid: TxId, data: usize, is_committed: impl Fn(TxId) -> bool) -> Result<()> {
        trace!("put: txid={:?}, data={:?}", txid, data);
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.back_mut() {
            // WARNING 多个Transaction::begin 的顺序 和put函数的顺序可以是不一样的
            // 这个时候record.tmin >= txid 可能不满足
            if record.tmin >= txid {
                println!("abriotttttttt");
                return Err(Error::Abort);
            // 如果上一个事务已经执行完了， 上一个事务的maxid 就等于当前事务的id
            } else if is_committed(record.tmin) {
                record.tmax = txid;
            } else {
                todo!("stall until commit");
            }
        }
        records.push_back(Record {
            tmin: txid,
            tmax: TxId::max_value(),
            redo_log_data: data,
            data,
        });
        Ok(())
    }

    fn get_v2(&self, can_see: impl Fn(TxId) -> bool) -> Result<usize> {
        trace!("get:");
        let records = self.records.lock().unwrap();
        for record in records.iter().rev() {
            if can_see(record.tmin) {
                return Ok(record.data);
            }
        }
        Err(Error::NotFound)
    }
}
