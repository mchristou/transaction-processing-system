use serde::Deserialize;
use std::{error::Error, fs::File, path::Path};

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct Record {
    #[serde(deserialize_with = "trim_and_parse_tx_type")]
    pub r#type: TxType,
    #[serde(deserialize_with = "trim_and_parse_u16")]
    pub client: u16,
    #[serde(deserialize_with = "trim_and_parse_u32")]
    pub tx: u32,
    #[serde(deserialize_with = "trim_and_parse_f32_4dp")]
    pub amount: Option<f32>,
}

pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Vec<Record>, Box<dyn Error>> {
    let file = File::open(path)?;
    // The CSV reader is buffered automatically, so it does not needed to
    // wrap rdr in a buffered reader like io::BufReader
    let mut rdr = csv::Reader::from_reader(file);

    let records: Result<Vec<_>, _> = rdr.deserialize::<Record>().collect::<Result<Vec<_>, _>>();

    Ok(records?)
}

fn trim_and_parse_tx_type<'de, D>(deserializer: D) -> Result<TxType, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    let trimmed = s.trim();
    match trimmed.to_lowercase().as_str() {
        "deposit" => Ok(TxType::Deposit),
        "withdrawal" => Ok(TxType::Withdrawal),
        "dispute" => Ok(TxType::Dispute),
        "resolve" => Ok(TxType::Resolve),
        "chargeback" => Ok(TxType::Chargeback),
        _ => Err(serde::de::Error::unknown_variant(
            trimmed,
            &["deposit", "withdrawal", "dispute", "resolve", "chargeback"],
        )),
    }
}

fn trim_and_parse_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    let trimmed = s.trim();
    trimmed.parse::<u32>().map_err(serde::de::Error::custom)
}

fn trim_and_parse_u16<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    let trimmed = s.trim();
    trimmed.parse::<u16>().map_err(serde::de::Error::custom)
}

fn trim_and_parse_f32_4dp<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        let value: f32 = trimmed.parse().map_err(serde::de::Error::custom)?;
        let rounded = (value * 10_000.0).round() / 10_000.0;
        Ok(Some(rounded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_csv() {
        let records = read_csv("test-inputs/test_input.csv").unwrap();
        let expected_records = vec![
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 1,
                amount: Some(1.0),
            },
            Record {
                r#type: TxType::Deposit,
                client: 2,
                tx: 2,
                amount: Some(2.0),
            },
            Record {
                r#type: TxType::Deposit,
                client: 1,
                tx: 3,
                amount: Some(2.0),
            },
            Record {
                r#type: TxType::Withdrawal,
                client: 1,
                tx: 4,
                amount: Some(1.5),
            },
            Record {
                r#type: TxType::Withdrawal,
                client: 2,
                tx: 5,
                amount: Some(3.0),
            },
        ];

        assert_eq!(records, expected_records);
    }
}
