#[macro_use]
extern crate serde_derive;

extern crate actix;
extern crate actix_web;
extern crate csv;
extern crate flate2;
extern crate memmap;
extern crate quickersort;
extern crate rayon;
extern crate serde;
extern crate time;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead};
use std::str;
use std::sync::Arc;

use actix_web::{http, server, App, HttpRequest, Json, Path, Query, Result, State};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use time::SteadyTime;

fn load_with_bufreader_multithreaded() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    let data = GzDecoder::new(file);

    let buffer = io::BufReader::new(data);
    let start_time = SteadyTime::now();

    let bufbuf: Vec<String> = buffer.lines().skip(1).map(|l| l.unwrap()).collect();

    println!("file in memory, now processing in parallel");
    let similarity_map = bufbuf
        .par_iter()
        .enumerate()
        .fold(
            || BTreeMap::new(),
            |mut part_map, (line_num, line)| {
                let b: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();

                let prb_id1 = b[0].parse::<u32>().unwrap();
                let prb_id2 = b[1].parse::<u32>().unwrap();
                let pct50_similarity = b[7].parse::<f32>().unwrap();
                let prb_entry_1 = part_map.entry(prb_id1).or_insert(vec![]);
                prb_entry_1.push((prb_id2, pct50_similarity));
                let prb_entry_2 = part_map.entry(prb_id2).or_insert(vec![]);
                prb_entry_2.push((prb_id1, pct50_similarity));

                if line_num % 1_000_000 == 0 || line_num == 1 {
                    println!("{:?}", b);
                    println!("{:?}mil", line_num / 1_000_000);
                    println!("{:?}", SteadyTime::now() - start_time);
                };

                part_map
            },
        ).reduce(
            || BTreeMap::new(),
            |mut total_map, next_chunk_map| {
                next_chunk_map.into_iter().for_each(|kv| {
                    let mut kkv = kv.1.clone();
                    let existing_entry = total_map.entry(kv.0).or_insert(vec![]);
                    existing_entry.append(&mut kkv);
                });
                total_map
            },
        );

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);
    Ok(similarity_map)
}

fn load_with_cursor_single_thread() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let unzipdata = GzDecoder::new(file);
    let mut data = io::BufReader::new(unzipdata);
    let mut line = vec![];
    let start_time = SteadyTime::now();
    let mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
    let mut line_num: u64 = 0;

    println!("Start processing in one thread");
    'line: loop {
        let num_bytes = data.read_until(b'\n', &mut line).unwrap();
        if num_bytes == 0 {
            break;
        }
        // let b: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        let b: Vec<String> = line
            .split(|c| c == &b' ')
            .map(|s| str::from_utf8(s).unwrap().to_string())
            .collect();

        let prb_id1;
        match b[0].parse::<u32>() {
            Ok(prb_id) => {
                prb_id1 = prb_id;
            }
            Err(_) => {
                println!("header");
                line.clear();
                continue 'line;
            }
        };

        let prb_id2 = b[1].parse::<u32>()?;
        let pct50_similarity = b[7].parse::<f32>()?;
        let prb_entry_1 = similarity_map.entry(prb_id1).or_insert(vec![]);
        prb_entry_1.push((prb_id2, pct50_similarity));
        let prb_entry_2 = similarity_map.entry(prb_id2).or_insert(vec![]);
        prb_entry_2.push((prb_id1, pct50_similarity));

        if line_num % 1_000_000 == 0 || line_num == 1 {
            println!("{:?}", SteadyTime::now() - start_time);
            println!("{:?}", b);
            println!("{:?}mil", line_num / 1_000_000);
        };

        line_num += 1;
        line.clear();
    }

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);
    Ok(similarity_map)
}

fn get_first_arg() -> Result<OsString, Box<Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from(
            "Expected at least 1 argument with the gzipped csv file, but got none",
        )),
        Some(file_path) => Ok(file_path),
    }
}

fn get_second_arg() -> Result<OsString, Box<Error>> {
    match env::args_os().nth(2) {
        None => Err(From::from("skipping second argument")),
        Some(bind_string) => Ok(bind_string),
    }
}

struct AppState {
    similarity_map: Arc<BTreeMap<u32, Vec<(u32, f32)>>>,
}

#[derive(Deserialize)]
struct Info {
    prb_id: String,
}

#[derive(Deserialize)]
struct QueryInfo {
    cutoff: f32,
    mode: String,
}

#[derive(Serialize, Clone)]
struct SimilarProbe {
    prb_id: u32,
    similarity: f32,
}

#[derive(Serialize)]
struct JsonResult {
    count: usize,
    result: Vec<SimilarProbe>,
}

// struct PrbSimPair(u32, f32);

impl PartialEq for SimilarProbe {
    // use only the prb_id in this tuple
    // for matching them as the same element
    fn eq(&self, other: &SimilarProbe) -> bool {
        self.prb_id == other.prb_id
    }
}

impl Eq for SimilarProbe {}

impl Hash for SimilarProbe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prb_id.hash(state);
    }
}

fn index(data: (State<AppState>, Path<Info>, Query<QueryInfo>)) -> Result<Json<JsonResult>> {
    let (state, path, query) = data;
    println!("{:?}", path.prb_id);
    println!("{:?}", query.cutoff);
    println!("{:?}", query.mode);

    let hs: HashSet<SimilarProbe> = path
        .prb_id
        .split(",")
        .map(|prb_id_str| {
            let prb_id = &prb_id_str.parse::<u32>().unwrap();
            let mut probes_set: HashSet<SimilarProbe> = HashSet::new();

            state
                .similarity_map
                .get(prb_id)
                .unwrap()
                .into_iter()
                .filter(|ps| {
                    if query.mode == "dissimilar" {
                        ps.1 <= query.cutoff
                    } else {
                        ps.1 >= query.cutoff
                    }
                }).for_each(|v| {
                    probes_set.insert(SimilarProbe {
                        prb_id: v.0,
                        similarity: v.1,
                    });
                });
            // probes_set.iter().for_each(|p| println!("{:?}", p.prb_id));
            probes_set
        }).fold(
            HashSet::new(),
            |intersect_probes: HashSet<SimilarProbe>, probes_set: HashSet<SimilarProbe>| {
                if intersect_probes.len() != 0 {
                    intersect_probes
                        .intersection(&probes_set)
                        .cloned()
                        .collect()
                } else {
                    probes_set
                }
            },
        );

    let mut v = hs
        .into_iter()
        .fold(Vec::new(), |mut vec: Vec<SimilarProbe>, p: SimilarProbe| {
            vec.push(p);
            vec
        });

    v.sort_by(|a, b| {
        b.similarity
            .partial_cmp(&a.similarity)
            .unwrap_or(Ordering::Equal)
    });

    Ok(Json(JsonResult {
        count: v.len(),
        result: v,
    }))
}

fn nadir<'a>(state: (State<AppState>)) -> Result<Json<String>> {
    let nadir_probe = &state.similarity_map.iter().max_by(
        |a: &(&u32, &Vec<(u32, f32)>), b: &(&u32, &Vec<(u32, f32)>)| {
            let a_max: f32 =
                a.1.iter()
                    .map(|v: &(u32, f32)| v.1)
                    .filter(|s| *s > 0.3)
                    .sum::<f32>()
                    .floor();
            let b_max: f32 =
                b.1.iter()
                    .map(|v: &(u32, f32)| v.1)
                    .filter(|s| *s > 0.3)
                    .sum::<f32>()
                    .floor();

            let a_max_int = a_max as u32;
            let b_max_int = b_max as u32;

            a_max_int.cmp(&b_max_int)
        },
    );

    let prb_id: String = match nadir_probe {
        Some(np) => np.0.to_string(),
        _ => "no nadir probe".to_string(),
    };

    Ok(Json(prb_id))
}

fn p404(req: &HttpRequest) -> Result<Json<String>> {
    Ok(Json(("not found").to_string()))
}

fn main() {
    let sys = actix::System::new("probe-similarity");
    let similarity_map = match load_with_cursor_single_thread() {
        Ok(sm) => Arc::new(sm),
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };

    let bind_address = match get_second_arg() {
        Ok(ba) => ba.into_string().unwrap(),
        Err(_) => "127.0.0.1:8100".to_string(),
    };

    server::new(move || {
        vec![
            App::with_state(AppState {
                similarity_map: Arc::clone(&similarity_map),
            }).prefix("/probe-similarity")
            .resource("/nadir", |r| r.method(http::Method::GET).with(nadir))
            .resource("/{prb_id}", |r| r.method(http::Method::GET).with(index))
            .boxed(),
            App::new()
                .default_resource(|r| r.method(http::Method::GET).f(&p404))
                .boxed(),
        ]
    }).bind(&bind_address)
    .unwrap()
    .start();

    println!("Started http server on {}", &bind_address);
    let _ = sys.run();
}
