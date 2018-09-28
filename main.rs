#[macro_use]
extern crate serde_derive;

extern crate actix;
extern crate actix_web;
extern crate flate2;
extern crate serde;

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::process;

use actix_web::{http, server, App, HttpRequest, HttpResponse, Path, State};
use flate2::read::GzDecoder;

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

struct AppState {
    similarity_map: BTreeMap<u32, Vec<(u32, f32)>>,
}

#[derive(Deserialize)]
struct Info {
    prb_id: String,
}

fn run<'a>(
    mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>>,
) -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let prb_id = get_second_arg()?
        .into_string()
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let data = GzDecoder::new(file);

    for line in io::BufReader::new(data).lines().skip(1) {
        let line = line.unwrap();
        let b: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        let prb_id1 = b[0].parse::<u32>()?;
        let prb_id2 = b[1].parse::<u32>()?;
        let pct50_similarity = b[7].parse::<f32>()?;
        let prb_entry = similarity_map.entry(prb_id1).or_insert(vec![]);
        prb_entry.push((prb_id2, pct50_similarity));
    }

    println!("Done building state. now searching");
    let simils: Vec<&(u32, f32)> = similarity_map
        .get(&prb_id)
        .unwrap()
        .into_iter()
        .filter(|ps| ps.1 > 0.5)
        .collect();
    println!("{:?}", simils.len());
    println!("{:?}", simils);

    Ok(similarity_map)
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
fn index(data: (State<AppState>, Path<Info>)) -> String {
    // fn index(req: &HttpRequest<AppState>) -> HttpResponse {
    let (state, path) = data;
    let prb_id: u32 = path.prb_id.parse::<u32>().unwrap();
    let ss: Vec<&(u32, f32)> = state
        .similarity_map
        .get(&prb_id)
        .unwrap()
        .into_iter()
        .filter(|ps| ps.1 > 0.5)
        .collect();
    format!("{:?}", ss)
}

fn main() {
    let sys = actix::System::new("probe-similarity");

    server::new(|| {
        let similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
        let similarity_map = run(similarity_map).unwrap();

        App::with_state(AppState {
            similarity_map: similarity_map,
        }).prefix("/probe-similarity")
        .resource("/{prb_id}", |r| r.method(http::Method::GET).with(index))
    }).bind("127.0.0.1:8100")
    .unwrap()
    .start();

    println!("Started http server: 127.0.0.1:8100");
    let _ = sys.run();
}
