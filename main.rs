#[macro_use]
extern crate serde_derive;

extern crate csv;
extern crate serde;

use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::process;

use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct SimilarityRecord {
    prb_id1: u32,
    prb_id2: u32,
    msm_count1: u16,
    msm_count2: u16,
    combined_msm_count: u16,
    combined_msm_count_pruned: u16,
    pct25_similarity: f32,
    pct50_similarity: f32,
    pct75_similarity: f32,
}

fn run() -> Result<(), Box<Error>> {
    let mut similarity_map = HashMap::new();

    let prb_id = get_second_arg()?
        .into_string()
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b' ')
        // .double_quote(false)
        // .escape(Some(b'\\'))
        // .flexible(true)
        // .comment(Some(b'#'))
        .from_reader(file);
    for result in rdr.deserialize() {
        let record: SimilarityRecord = result?;
        let prb_entry = similarity_map.entry(record.prb_id1).or_insert(vec![]);
        prb_entry.push((record.prb_id2, record.pct50_similarity));

    }

    let simils: Vec<&(u32, f32)> = similarity_map.get(&prb_id).unwrap().into_iter().filter(|ps| ps.1 > 0.5).collect();
    println!("{:?}", simils.len());
    println!("{:?}", simils);
    Ok(())
}

fn get_first_arg() -> Result<OsString, Box<Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 2 arguments, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

fn get_second_arg() -> Result<OsString, Box<Error>> {
    match env::args_os().nth(2) {
        None => Err(From::from("expected 2 arguments, but got one")),
        Some(prb_id) => Ok(prb_id),
    }
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}
