use records::read_csv;
use std::{env, error::Error};

mod records;
mod transaction;

use transaction::process_records;

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = get_file_path_from_args()?;
    let records = read_csv(file_path)?;
    let processed_records = process_records(records);

    let mut wtr = csv::WriterBuilder::new().from_writer(std::io::stdout());
    for record in processed_records {
        wtr.serialize(record.1)?;
    }

    wtr.flush()?;

    Ok(())
}

fn get_file_path_from_args() -> Result<String, Box<dyn Error>> {
    const CSV_EXTENSION: &str = ".csv";

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <file.csv>", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];
    if !file_path.ends_with(CSV_EXTENSION) {
        eprintln!("Error: The file must have a .csv extension");
        std::process::exit(1);
    }

    Ok(file_path.to_owned())
}
