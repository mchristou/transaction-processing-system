use serde::{Serialize, Serializer};
use std::collections::{HashMap, HashSet};

use crate::records::{Record, TxType};

pub type ClientId = u16;
pub type TxId = u32;

#[derive(Debug, Serialize, PartialEq, Default)]
pub struct AccountRecord {
    pub client: u16,
    #[serde(serialize_with = "serialize_f32_4dp")]
    pub available: f32,
    #[serde(serialize_with = "serialize_f32_4dp")]
    pub held: f32,
    #[serde(serialize_with = "serialize_f32_4dp")]
    pub total: f32,
    pub locked: bool,
}

pub fn process_records(records: Vec<Record>) -> HashMap<ClientId, AccountRecord> {
    let mut result: HashMap<ClientId, AccountRecord> = HashMap::new();
    let mut processed_records: HashMap<(ClientId, TxId), Record> = HashMap::new();
    let mut disputes: HashMap<ClientId, HashSet<TxId>> = HashMap::new();

    for record in records {
        if matches!(record.r#type, TxType::Deposit | TxType::Withdrawal)
            && processed_records
                .keys()
                .any(|&(_, tx_id)| tx_id == record.tx)
        {
            continue;
        }

        match record.r#type {
            TxType::Deposit => {
                deposit(&mut result, &record);
                processed_records.insert((record.client, record.tx), record);
            }
            TxType::Withdrawal => {
                withdraw(&mut result, &record);
                processed_records.insert((record.client, record.tx), record);
            }
            TxType::Dispute => dispute(&mut result, &mut disputes, &processed_records, &record),
            TxType::Resolve => resolve(&mut result, &mut disputes, &processed_records, &record),
            TxType::Chargeback => {
                chargeback(&mut result, &mut disputes, &processed_records, &record)
            }
        }
    }

    result
}

pub fn deposit(result: &mut HashMap<ClientId, AccountRecord>, record: &Record) {
    if let Some(amount) = record.amount {
        if amount <= 0 as f32 {
            return;
        }

        result
            .entry(record.client)
            .and_modify(|r| {
                if !r.locked {
                    r.available += amount;
                    r.total = r.available + r.held;
                }
            })
            .or_insert_with(|| AccountRecord {
                client: record.client,
                available: amount,
                total: amount,
                held: 0.0,
                locked: false,
            });
    }
}

pub fn withdraw(result: &mut HashMap<ClientId, AccountRecord>, record: &Record) {
    // In the case that the client does not exist or the client does not have enough available
    // to withdraw, this operation will not do anything.
    if let Some(amount) = record.amount {
        if let Some(account_record) = result.get_mut(&record.client) {
            if account_record.locked {
                return;
            }

            if account_record.available >= amount {
                account_record.available -= amount;
                account_record.total = account_record.available + account_record.held;
            }
        }
    }
}

pub fn dispute(
    result: &mut HashMap<ClientId, AccountRecord>,
    disputes: &mut HashMap<ClientId, HashSet<TxId>>,
    processed_records: &HashMap<(ClientId, TxId), Record>,
    record: &Record,
) {
    if processed_records.is_empty() {
        return;
    }

    let Some(out_record) = result.get_mut(&record.client) else {
        return;
    };

    if out_record.locked {
        return;
    }

    let client_disputes = disputes.entry(record.client).or_default();

    if client_disputes.contains(&record.tx) {
        // Transaction already disputed
        return;
    }

    if let Some(processed_record) = processed_records.get(&(record.client, record.tx)) {
        if let Some(amount) = processed_record.amount {
            match processed_record.r#type {
                TxType::Deposit | TxType::Withdrawal => {
                    out_record.available -= amount;
                    out_record.held += amount;
                    out_record.total = out_record.available + out_record.held;
                    client_disputes.insert(record.tx);
                }
                _ => {}
            }
        }
    }
}

pub fn resolve(
    result: &mut HashMap<ClientId, AccountRecord>,
    disputes: &mut HashMap<ClientId, HashSet<TxId>>,
    processed_records: &HashMap<(ClientId, TxId), Record>,
    record: &Record,
) {
    let Some(client_disputes) = disputes.get_mut(&record.client) else {
        return;
    };

    if !client_disputes.contains(&record.tx) {
        // Assume there is an error on the partner's side.
        return;
    }

    let Some(out_record) = result.get_mut(&record.client) else {
        return;
    };

    if out_record.locked {
        return;
    }

    if let Some(processed_record) = processed_records.get(&(record.client, record.tx)) {
        if let Some(amount) = processed_record.amount {
            out_record.available += amount;
            out_record.held -= amount;
            out_record.total = out_record.available + out_record.held;

            client_disputes.remove(&record.tx);
        }
    }
}

pub fn chargeback(
    result: &mut HashMap<ClientId, AccountRecord>,
    disputes: &mut HashMap<ClientId, HashSet<TxId>>,
    processed_records: &HashMap<(ClientId, TxId), Record>,
    record: &Record,
) {
    let Some(client_disputes) = disputes.get_mut(&record.client) else {
        return;
    };

    if !client_disputes.contains(&record.tx) {
        // Assume there is an error on the partner's side.
        return;
    }

    let Some(out_record) = result.get_mut(&record.client) else {
        return;
    };

    if out_record.locked {
        return;
    }

    if let Some(processed_record) = processed_records.get(&(record.client, record.tx)) {
        if let Some(amount) = processed_record.amount {
            if out_record.held >= amount {
                out_record.held -= amount;
                out_record.total = out_record.available + out_record.held;
            }

            client_disputes.remove(&record.tx);
            out_record.locked = true;
        }
    }
}

fn serialize_f32_4dp<S>(value: &f32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let rounded = (value * 10_000.0).round() / 10_000.0;
    serializer.serialize_str(&format!("{:.4}", rounded))
}

#[cfg(test)]
mod tests {
    use crate::records::{read_csv, TxType};

    use super::*;
    use std::{collections::HashMap, collections::HashSet};

    #[test]
    fn deposit_existing_client() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(1, AccountRecord::default());
        let record = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(100.0),
        };

        deposit(&mut result, &record);

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].total, 100.0);
    }

    #[test]
    fn deposit_new_client() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        let record = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(100.0),
        };

        deposit(&mut result, &record);

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].total, 100.0);
    }

    #[test]
    fn deposit_zero_amount() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        let record = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(0.0),
        };

        deposit(&mut result, &record);

        assert_eq!(result.get(&1), None);
    }

    #[test]
    fn deposit_negative_amount() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        let record_positive_amount = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(100.0),
        };

        deposit(&mut result, &record_positive_amount);
        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].total, 100.0);

        let record_negative_amount = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(-100.0),
        };

        deposit(&mut result, &record_negative_amount);
        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].total, 100.0);
    }

    #[test]
    fn multiple_transactions_same_id() {
        let records = vec![
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 1,
                amount: Some(100.0),
            },
            Record {
                r#type: TxType::Withdrawal,
                client: 1,
                tx: 1,
                amount: Some(50.0),
            },
        ];

        let processed_records = process_records(records);

        // The available amount and the total should be 100.0 since the second (Withdrawal) record
        // will not be processed because other record with same tx id already processed.
        assert_eq!(processed_records[&1].available, 100.0);
        assert_eq!(processed_records[&1].total, 100.0);
    }

    #[test]
    fn withdraw_sufficient_funds() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 100.0,
                held: 0.0,
                total: 100.0,
                locked: false,
            },
        );
        let record = Record {
            r#type: TxType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(50.0),
        };

        withdraw(&mut result, &record);

        assert_eq!(result[&1].available, 50.0);
        assert_eq!(result[&1].total, 50.0);
    }

    #[test]
    fn withdraw_insufficient_funds() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 100.0,
                held: 0.0,
                total: 100.0,
                locked: false,
            },
        );

        let record = Record {
            r#type: TxType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(150.0),
        };

        withdraw(&mut result, &record);

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].total, 100.0);
    }

    #[test]
    fn dispute_existing_transaction() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 100.0,
                held: 0.0,
                total: 100.0,
                locked: false,
            },
        );

        let mut disputes: HashMap<u16, HashSet<u32>> = HashMap::new();
        let mut processed_records = HashMap::new();
        processed_records.insert(
            (1, 1),
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 1,
                amount: Some(50.0),
            },
        );
        processed_records.insert(
            (1, 123),
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 123,
                amount: Some(50.0),
            },
        );

        let record = Record {
            r#type: TxType::Dispute,
            client: 1,
            tx: 123,
            amount: None,
        };

        dispute(&mut result, &mut disputes, &processed_records, &record);

        assert_eq!(result[&1].available, 50.0);
        assert_eq!(result[&1].held, 50.0);
        assert_eq!(result[&1].total, 100.0);
        assert!(disputes[&1].contains(&123));
    }

    #[test]
    fn dispute_non_existing_transaction() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 100.0,
                held: 0.0,
                total: 100.0,
                locked: false,
            },
        );

        let mut disputes: HashMap<u16, HashSet<u32>> = HashMap::new();
        let processed_records = HashMap::new();

        let record = Record {
            r#type: TxType::Dispute,
            client: 1,
            tx: 123,
            amount: None,
        };

        dispute(&mut result, &mut disputes, &processed_records, &record);

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].held, 0.0);
        assert_eq!(result[&1].total, 100.0);
        assert!(!disputes.contains_key(&1));
    }

    #[test]
    fn resolve_existing_dispute() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 50.0,
                held: 50.0,
                total: 100.0,
                locked: false,
            },
        );

        let mut disputes: HashMap<u16, HashSet<u32>> = HashMap::new();
        let mut tx_disputed = HashSet::new();
        tx_disputed.insert(123);
        disputes.insert(1, tx_disputed);

        let processed_records = vec![
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 1,
                amount: Some(50.0),
            },
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 123,
                amount: Some(50.0),
            },
        ]
        .into_iter()
        .map(|r| ((r.client, r.tx), r))
        .collect();

        let record = Record {
            r#type: TxType::Resolve,
            client: 1,
            tx: 123,
            amount: None,
        };

        resolve(&mut result, &mut disputes, &processed_records, &record);

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].held, 0.0);
        assert_eq!(result[&1].total, 100.0);
        assert!(!disputes[&1].contains(&123));
    }

    #[test]
    fn resolve_without_dispute() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        let mut disputes: HashMap<u16, HashSet<u32>> = HashMap::new();
        let mut processed_records = HashMap::new();

        let deposit_record = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(100.0),
        };

        deposit(&mut result, &deposit_record);
        processed_records.insert((deposit_record.client, deposit_record.tx), deposit_record);

        resolve(
            &mut result,
            &mut disputes,
            &processed_records,
            &Record {
                r#type: TxType::Resolve,
                client: 1,
                tx: 1,
                amount: None,
            },
        );

        assert_eq!(result[&1].available, 100.0);
        assert_eq!(result[&1].held, 0.0);
        assert_eq!(result[&1].total, 100.0);
    }

    #[test]
    fn chargeback_existing_dispute() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 50.0,
                held: 50.0,
                total: 100.0,
                locked: false,
            },
        );

        let mut disputes: HashMap<u16, HashSet<u32>> = HashMap::new();
        let mut tx_disputed = HashSet::new();
        tx_disputed.insert(123);
        disputes.insert(1, tx_disputed);

        let processed_records = vec![
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 1,
                amount: Some(50.0),
            },
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 123,
                amount: Some(50.0),
            },
        ]
        .into_iter()
        .map(|r| ((r.client, r.tx), r))
        .collect();

        let record = Record {
            r#type: TxType::Chargeback,
            client: 1,
            tx: 123,
            amount: None,
        };

        chargeback(&mut result, &mut disputes, &processed_records, &record);

        assert_eq!(result[&1].available, 50.0);
        assert_eq!(result[&1].held, 0.0);
        assert_eq!(result[&1].total, 50.0);
        assert!(result[&1].locked);
        assert!(!disputes[&1].contains(&123));
    }

    #[test]
    fn transactions_on_locked_account() {
        let mut result: HashMap<u16, AccountRecord> = HashMap::new();
        result.insert(
            1,
            AccountRecord {
                client: 1,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: true,
            },
        );

        let record = Record {
            r#type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(100.0),
        };

        deposit(&mut result, &record);

        assert_eq!(result[&1].available, 0.0);
        assert_eq!(result[&1].total, 0.0);
    }

    #[test]
    fn test_process_records() {
        let records = read_csv("test-inputs/test_input_full.csv").unwrap();

        let processed_records = process_records(records);
        let mut expected_processed_records = HashMap::new();

        expected_processed_records.insert(
            1,
            AccountRecord {
                client: 1,
                available: 200.0,
                held: 0.0,
                total: 200.0,
                locked: false,
            },
        );
        expected_processed_records.insert(
            2,
            AccountRecord {
                client: 2,
                available: 250.0,
                held: 0.0,
                total: 250.0,
                locked: true,
            },
        );

        assert_eq!(processed_records[&1], expected_processed_records[&1]);
        assert_eq!(processed_records[&2], expected_processed_records[&2]);
    }
}
