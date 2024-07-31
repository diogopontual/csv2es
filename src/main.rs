use clap::{Parser};
use csv::{ByteRecord, StringRecord};
use elasticsearch::{BulkOperation, BulkParts, Elasticsearch};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
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
) {
    let body:  Vec<BulkOperation<Value>> =  buffer.drain(..).map(|r| -> BulkOperation<Value> {
         println!("{:?}",r);
        BulkOperation::index(serde_json::to_value(r).unwrap()).index("teste").into()
     }).collect();
    let response = client
         .bulk(BulkParts::Index("test"))
         .body(body)
         .send()
         .await;
    println!("{:?}",response);
}

async fn read_file<P: AsRef<Path>>(filename: P, batch_size: usize) -> Result<(), Box<dyn Error>> {
    log::info!("Starting read_fil1e");
    let client = Elasticsearch::default();
    let file = File::open(filename)?;
    let mut reader = csv::Reader::from_reader(file);
    let mut record : StringRecord;
    let mut buffer: Vec<HashMap<String,String>> = Vec::with_capacity(batch_size);

    let mut idx = 0;

    let headers=  reader.headers()?.clone();

    for result in reader.records() {
        record = result?;
        buffer.push(record_to_hashmap(&headers, &record));
        idx += 1;
        if idx > batch_size {
            idx = 0;
            drain(&client,&mut buffer).await;
            break;
        }
    }

    Ok(())
}
#[tokio::main]
async fn main() {
    femme::start();
    let args = Args::parse();
    println!("{:?}", args);
    let _ = read_file(args.filename, 10).await;
}
