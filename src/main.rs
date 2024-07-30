use clap::{Parser, Subcommand};
use csv::{ByteRecord, StringRecord};
use elasticsearch::{BulkOperation, BulkParts, Elasticsearch};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;


#[derive(Parser, Debug)]
struct Args {
    filename: String,
}

async fn drain(
    client: &Elasticsearch,
    data: &Vec<HashMap<String, String>>,
    count: usize,
) {
    log::info!("Starting drain");
    let body: Vec<BulkOperation<_>> = data
        .iter()
        .map(|p| BulkOperation::index(p).into())
        .collect();
    let response = client
        .bulk(BulkParts::Index("test"))
        .body(body)
        .send()
        .await;
}

async fn read_file<P: AsRef<Path>>(filename: P, batch_size: usize) -> Result<(), Box<dyn Error>> {
    log::info!("Starting read_file");
    let client = Elasticsearch::default();
    let file = File::open(filename)?;
    let mut record: StringRecord;
    let mut reader = csv::Reader::from_reader(file);
    let mut buffer: Vec<HashMap<String, String>> = Vec::new();
    let mut idx = 0;
    let r = reader.headers();
    r.
    let headers: Vec<String> = reader.headers().into_iter().map(|el| -> String{
        String::from(el.as_slice())
    }).collect();
    log::info!("{:?}",headers);
    for result in reader.records() {

        record = result.unwrap();
        let map = record
            .iter()
            .fold(HashMap::new(),
                |mut map: HashMap<String, String>, e| -> HashMap<String, String> {
                    map.insert(String::from(headers.get(map.keys().len()).unwrap()), String::from(e));
                    map
                },
            );
        buffer.push(map);
        idx += 0;
        if idx == batch_size {
            drain(&client, &buffer, batch_size).await;
            idx = 0;
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() {
    femme::start();
    let args = Args::parse();
    println!("{:?}", args);
    let _ = read_file(args.filename, 1000).await;
}
