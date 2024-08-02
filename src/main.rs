use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use clap::Parser;
use csv::{StringRecord};
use elasticsearch::{BulkOperation, BulkParts, Elasticsearch};
use elasticsearch::http::transport::Transport;
use serde_json::Value;

#[derive(Parser, Debug)]
struct Args {
    filename: String,
}

fn record_to_hashmap(headers: &StringRecord, record: &StringRecord) -> HashMap<String, String> {
    headers.iter().zip(record.iter())
        .map(|(header, field)| (header.to_string(), field.to_string()))
        .collect()
}

async fn drain(
    client: &Elasticsearch,
    buffer: &mut Vec<HashMap<String, String>>,
    index_name: &String,
) {
    let body: Vec<BulkOperation<Value>> = buffer.drain(..).map(|r| -> BulkOperation<Value> {
        // println!("{:?}",r);
        BulkOperation::index(serde_json::to_value(r).unwrap()).index(index_name).into()
    }).collect();
    let response = client
        .bulk(BulkParts::Index(index_name))
        .body(body)
        .send()
        .await;
    println!("{:?}", response);
}

fn extract_index_name(file_name: &String) -> String {
    let path = Path::new(file_name);
    let file_stem = path.file_stem().unwrap().to_str().unwrap();
    file_stem.to_string()
}

async fn read_file(filename: &String, batch_size: usize) -> Result<(), Box<dyn Error>> {
    log::info!("Starting read_file");
    let transport = Transport::single_node("http://127.0.0.1:9200")?;
    let client = Elasticsearch::new(transport);
    let file = File::open(filename)?;
    let index_name = extract_index_name(filename);
    let mut reader = csv::Reader::from_reader(file);
    let mut record: StringRecord;
    let mut buffer: Vec<HashMap<String, String>> = Vec::with_capacity(batch_size);

    let mut idx = 0;

    let headers = reader.headers()?.clone();

    for result in reader.records() {
        record = result?;
        buffer.push(record_to_hashmap(&headers, &record));
        idx += 1;
        if idx > batch_size {
            idx = 0;
            drain(&client, &mut buffer, &index_name).await;
            // break;
        }
    }

    Ok(())
}
#[tokio::main]
async fn main() {
    femme::start();
    let args = Args::parse();
    println!("{:?}", args);
    let _ = read_file(&args.filename, 10).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_index_name() {
        let file_name = "/home/test/data.csv".to_string();
        let index_name = extract_index_name(&file_name);
        assert_eq!(index_name, "data");
    }
}
