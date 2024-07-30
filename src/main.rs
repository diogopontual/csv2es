use std::error::Error;
use std::fs::File;
use std::path::Path;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[derive(Debug)]
struct Args{
    filename: String,
}

fn read_file<P: AsRef<Path>>(filename: P) -> Result<(), Box<dyn Error>>{
    let file = File::open(filename)?;
    let mut reader = csv::Reader::from_reader(file);

    for result in reader.records(){
        let record = result?;
        println!("{:?}",record);
    }
    Ok(())
}
fn main() {
    let args = Args::parse();
    println!("{:?}",args);
}
