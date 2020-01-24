#![feature(impl_trait_in_bindings)]

#[macro_use]
extern crate serde_derive;

use std::{
    io::{stdin, BufReader, Read},
    // path::Path,
    prelude::*,
    task,
};

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{read_dir, File};
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead};
use std::iter::FromIterator;
use std::str;
use std::sync::{Arc, RwLock};
use std::thread;

use actix_web::{http, web, App, HttpRequest, HttpServer};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use regex::Regex;
use time::SteadyTime;

use avro_rs::Reader;

// for write test
use avro_rs::types::{Record, Value};
use avro_rs::Schema;
use avro_rs::Writer;

use rand::prelude::*;

const BQ_DATA_PATH: &str = "./bq_data";
const BQ_FILE_REGEX: &str = "jaccard_similarity_ipv4";
const BQ_REGEX: &str = "_2019-12-16_";
// const BQ_DATE_REGEX: &str = r"_^\d{4}-\d{2}-\d{2}_";

fn load_avro_from_stdin() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<dyn Error>> {
    let start_time = SteadyTime::now();
    let mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
    let mut tot_rec_num: usize = 0;

    let schema = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "prbId1", "type": "long"},
            {"name": "prbId2", "type": "long"},
            {"name": "median25", "type": "double"},
            {"name": "median5", "type": "double"},
            {"name": "median75", "type": "double"}
        ]
    }"#;

    let reader_schema = Schema::parse_str(schema).unwrap();
    let stdin = std::io::stdin();
    let stdin = stdin.lock();
    // let ss = BufReader::new(stdin);
    let source = Reader::with_schema(&reader_schema, stdin).unwrap();

    source.for_each(|chunk: Result<Value, _>| match chunk.unwrap() {
        Value::Record(n) => {
            let mut nn = n.into_iter();
            let prb_id1 =
                avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId1").unwrap().1).unwrap();
            let prb_id2 =
                avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId2").unwrap().1).unwrap();
            let pct50_similarity =
                avro_rs::from_value::<f32>(&nn.find(|kv| kv.0 == "median5").unwrap().1).unwrap();

            let prb_entry1 = similarity_map.entry(prb_id1).or_insert(vec![]);
            prb_entry1.push((prb_id2, pct50_similarity));

            let prb_entry2 = similarity_map.entry(prb_id2).or_insert(vec![]);
            prb_entry2.push((prb_id1, pct50_similarity));

            if tot_rec_num % 1_000_000 == 0 || tot_rec_num < 1 {
                println!(
                    "{:?}mil records loaded in {:?}s...",
                    tot_rec_num / 1_000_000,
                    (SteadyTime::now() - start_time).num_seconds(),
                );
            };
            tot_rec_num += 1;
        }
        err => {
            println!("some err");
            println!("{:?}", err);
        }
    });
    println!("total records processed: {}", tot_rec_num);
    println!("total records in map: {}", &similarity_map.len());

    Ok(similarity_map)
}

fn load_avro_from_file() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<dyn Error>> {
    let start_time = SteadyTime::now();
    let mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
    let mut tot_rec_num: usize = 0;

    let schema = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "prbId1", "type": "long"},
            {"name": "prbId2", "type": "long"},
            {"name": "median25", "type": "double"},
            {"name": "median5", "type": "double"},
            {"name": "median75", "type": "double"}
        ]
    }"#;

    let reader_schema = Schema::parse_str(schema).unwrap();

    let data_dir = read_dir(BQ_DATA_PATH)?;

    data_dir.into_iter().for_each(|file| {
        let file_path = file.unwrap().path();
        if &file_path.to_str().unwrap()[..BQ_FILE_REGEX.len()] != BQ_FILE_REGEX {
            ()
        }

        let file = File::open(&file_path).unwrap();
        let source = Reader::with_schema(&reader_schema, file).unwrap();

        source.for_each(|chunk: Result<Value, _>| match chunk.unwrap() {
            Value::Record(n) => {
                let mut nn = n.into_iter();
                let prb_id1 =
                    avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId1").unwrap().1).unwrap();
                let prb_id2 =
                    avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId2").unwrap().1).unwrap();
                let pct50_similarity =
                    avro_rs::from_value::<f32>(&nn.find(|kv| kv.0 == "median5").unwrap().1)
                        .unwrap();
                let prb_entry1 = similarity_map.entry(prb_id1).or_insert(vec![]);
                prb_entry1.push((prb_id2, pct50_similarity));

                let prb_entry2 = similarity_map.entry(prb_id2).or_insert(vec![]);
                prb_entry2.push((prb_id1, pct50_similarity));

                if tot_rec_num % 1_000_000 == 0 || tot_rec_num < 1 {
                    println!(
                        "{:?}mil records loaded in {:?}s from file {:?}..",
                        tot_rec_num / 1_000_000,
                        (SteadyTime::now() - start_time).num_seconds(),
                        &file_path
                    );
                };
                tot_rec_num += 1;
            }
            err => {
                println!("some err");
                println!("{:?}", err);
            }
        });
        println!("file: {:?}", file_path);
        println!("total records processed: {}", tot_rec_num);
        println!("total records in map: {}", &similarity_map.len());
    });

    Ok(similarity_map)
}

fn load_avro_from_file_multithreaded(
    src_files: Vec<std::path::PathBuf>,
) -> Result<(BTreeMap<u32, Vec<(u32, f32)>>, String), Box<dyn Error>> {
    let start_time = SteadyTime::now();

    let schema = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "prbId1", "type": "long"},
            {"name": "prbId2", "type": "long"},
            {"name": "median25", "type": "double"},
            {"name": "median5", "type": "double"},
            {"name": "median75", "type": "double"}
        ]
    }"#;

    let reader_schema = Schema::parse_str(schema).unwrap();

    // extract the base name from the files vector,
    // will be returned from this function so that
    // it can be used to compare to another run of select_files
    // to see if new files are available.
    let base_name: String;
    match src_files.get(0) {
        Some(b_n) => {
            base_name = b_n.file_name().unwrap().to_str().unwrap()
                [..(BQ_FILE_REGEX.len() + BQ_REGEX.len())]
                .to_string();
        }
        None => {
            return Err(From::from(format!(
                "No suitable files found in {}.",
                BQ_DATA_PATH
            )));
        }
    };

    let similarity_map = src_files
        .par_iter()
        .fold(
            || (0, BTreeMap::new()),
            |mut part_map: (usize, BTreeMap<u32, Vec<(u32, f32)>>), file_path| {
                let f = File::open(file_path).unwrap();
                let source = Reader::with_schema(&reader_schema, f).unwrap();
                source.for_each(|chunk: Result<Value, _>| match chunk.unwrap() {
                    Value::Record(n) => {
                        let mut nn = n.into_iter();
                        let prb_id1 =
                            avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId1").unwrap().1)
                                .unwrap();
                        let prb_id2 =
                            avro_rs::from_value::<u32>(&nn.find(|kv| kv.0 == "prbId2").unwrap().1)
                                .unwrap();
                        let pct50_similarity =
                            avro_rs::from_value::<f32>(&nn.find(|kv| kv.0 == "median5").unwrap().1)
                                .unwrap();
                        let prb_entry1 = part_map.1.entry(prb_id1).or_insert(vec![]);
                        prb_entry1.push((prb_id2, pct50_similarity));

                        let prb_entry2 = part_map.1.entry(prb_id2).or_insert(vec![]);
                        prb_entry2.push((prb_id1, pct50_similarity));

                        let dur = SteadyTime::now() - start_time;
                        if part_map.0 % 1_000_000 == 0 || part_map.0 < 1 {
                            println!(
                                "{:?}mil records loaded in {}s from file {:?}...",
                                part_map.0 / 1_000_000,
                                dur.num_milliseconds() as f32 / 1000.0 as f32,
                                file_path
                            );
                        };
                        part_map.0 += 1;
                    }
                    err => {
                        println!("some err");
                        println!("{:?}", err);
                    }
                });
                println!(
                    "total records processed in file {:?}: {}",
                    &file_path, &part_map.0
                );
                println!(
                    "total probe records in map in file {:?}: {}",
                    &*file_path,
                    &part_map.1.len()
                );

                part_map
            },
        )
        .reduce(
            || (0, BTreeMap::new()),
            |mut total_map, next_chunk_map| {
                next_chunk_map.1.into_iter().for_each(|mut kv| {
                    let existing_entry = total_map.1.entry(kv.0).or_insert(vec![]);
                    existing_entry.append(&mut kv.1);
                });
                total_map.0 += next_chunk_map.0;
                total_map
            },
        );

    let dur = SteadyTime::now() - start_time;
    println!(
        "grand total of {} records loaded in {}s",
        similarity_map.0,
        dur.num_milliseconds() as f32 / 1000.0
    );
    println!("grand total of {} probes in map", similarity_map.1.len());
    Ok((similarity_map.1, base_name))
}

fn check_updated_files(
    base_name: &String,
) -> Result<Option<(Vec<std::path::PathBuf>, String)>, Box<dyn Error>> {
    let updated_files: Option<(Vec<std::path::PathBuf>, String)>;
    let new_base_name: Option<String> = None;
    match select_latest_files(BQ_DATA_PATH) {
        Ok(files) => {
            if files.get(0).is_some()
                && &files[0].file_name().unwrap().to_string_lossy().to_string()
                    [..(BQ_FILE_REGEX.len() + BQ_REGEX.len())]
                    != base_name
            {
                println!("new files found...");
                println!("current base name: {}", base_name);
                let new_base_name = files[0].file_name().unwrap().to_string_lossy()
                    [..(BQ_FILE_REGEX.len() + BQ_REGEX.len())]
                    .to_string();
                println!("new base name: {}", new_base_name);
                updated_files = Some((files, new_base_name));
            } else {
                updated_files = None;
            };
        }
        Err(e) => {
            return Err(From::from(
                "Error occurred while checking for updated files...",
            ))
        }
    };
    Ok(updated_files)
}

fn load_with_bufreader_multithreaded() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<dyn Error>> {
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
            |mut part_map, (line_num, line)| -> BTreeMap<u32, Vec<(u32, f32)>> {
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
        )
        .reduce(
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

fn load_with_cursor_single_thread() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<dyn Error>> {
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

fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from(
            "Expected at least 1 argument with the gzipped csv file, but got none",
        )),
        Some(file_path) => Ok(file_path),
    }
}

fn get_second_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(2) {
        None => Err(From::from("skipping second argument")),
        Some(bind_string) => Ok(bind_string),
    }
}

struct AppState {
    similarity_map: Arc<RwLock<BTreeMap<u32, Vec<(u32, f32)>>>>,
}

#[derive(Deserialize)]
struct Info {
    prb_id: String,
}

#[derive(Deserialize)]
struct DualProbesInfo {
    prb_id1: u32,
    prb_id2: u32,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Similar,
    Dissimilar,
}

#[derive(Deserialize)]
struct QueryInfo {
    cutoff: Option<f32>,
    limit: Option<u32>,
    mode: Option<String>,
    band: Option<String>,
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

#[derive(Clone)]
struct SimProbVec(Vec<(u32, f32)>);

// struct PrbSimPair(u32, f32);
impl SimilarProbe {
    fn new_vec_of(similar_probes: Vec<(u32, f32)>) -> Vec<SimilarProbe> {
        similar_probes
            .iter()
            .map(|v| SimilarProbe {
                prb_id: v.0,
                similarity: v.1,
            })
            .collect()
    }
}

impl std::fmt::Debug for SimilarProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "prb_id: {}, s: {}", self.prb_id, self.similarity)
    }
}

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

async fn similarities_for_prb_id(
    data: (web::Data<AppState>, web::Path<Info>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, path, query) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    println!("{:?}", path.prb_id);
    println!("{:?}", query.cutoff);
    println!("{:?}", query.mode);
    let mut cutoff: f32 = 0.0;
    let mut limit: usize = 0;
    let mut query_err: Vec<&str> = vec![];

    if let Some(co) = query.cutoff {
        cutoff = co;
    } else {
        match query.limit {
            Some(l) => {
                limit = l as usize;
            }
            None => {
                query_err.push("cutoff");
                query_err.push("limit");
            }
        }
    };

    let mode = if let Some(mode) = &query.mode {
        mode
    } else {
        "similar"
    };

    if query_err.len() > 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "queryparameter{}: `{}` required",
                if query_err.len() == 1 { "" } else { "s" },
                query_err.join("`,`")
            ),
        ));
    };

    let hs: HashSet<SimilarProbe> = path
        .prb_id
        .split(",")
        .map(|prb_id_str| {
            let prb_id = &prb_id_str.parse::<u32>().unwrap();
            let mut probes_set: HashSet<SimilarProbe> = HashSet::new();

            similarity_map
                .get(prb_id)
                .unwrap()
                .into_iter()
                .filter(|ps| {
                    if mode == "dissimilar" {
                        ps.1 <= cutoff
                    } else {
                        ps.1 >= cutoff
                    }
                })
                .for_each(|v| {
                    probes_set.insert(SimilarProbe {
                        prb_id: v.0,
                        similarity: v.1,
                    });
                });
            // probes_set.iter().for_each(|p| println!("{:?}", p.prb_id));
            probes_set
        })
        // .fold(
        //     HashSet::new(),
        //     |intersect_probes: HashSet<SimilarProbe>, probes_set: HashSet<SimilarProbe>| {
        //         if intersect_probes.len() != 0 {
        //             println!("this cannot happen");
        //             intersect_probes
        //                 .intersection(&probes_set)
        //                 .cloned()
        //                 .collect()
        //         } else {
        //             probes_set
        //         }
        //     },
        // );

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

    Ok(web::Json(JsonResult {
        count: v.len(),
        result: if limit == 0 { v } else { v[..limit].to_vec() },
    }))
}

async fn nadir(
    data: (web::Data<AppState>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, query) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    println!("{:?} nadir", query.mode);
    let mut mode: Mode = Mode::Similar;
    let mut query_err: Vec<&str> = vec![];

    if let Some(m) = &query.mode {
        match m.as_str() {
            "similar" => {
                mode = Mode::Similar;
            }
            "dissimilar" => {
                mode = Mode::Dissimilar;
            }
            _ => {
                query_err.push("mode");
            }
        }
    };

    let cutoff = if let Some(co) = query.cutoff {
        co
    } else {
        match mode {
            // kick out low similarities
            Mode::Similar => 0.3,
            // kick out probes that are basically the same out (by default)
            Mode::Dissimilar => 0.99,
        }
    };

    if query_err.len() > 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "queryparameter{}: `{}` invalid",
                if query_err.len() == 1 { "" } else { "s" },
                query_err.join("`,`")
            ),
        ));
    };

    // let sim_iter = state.similarity_map.iter();
    let mut sim_sums: Vec<(u32, f32)> = Vec::from_iter(similarity_map.iter().map(|kv| {
        (
            *kv.0,
            kv.1.iter()
                .map(|v| v.1)
                .filter(|v| match mode {
                    Mode::Similar => v > &cutoff,
                    Mode::Dissimilar => v < &cutoff,
                })
                .sum(),
        )
    }));

    match mode {
        Mode::Similar => {
            sim_sums.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        }
        Mode::Dissimilar => {
            sim_sums.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        }
    }

    let nadir_probes = sim_sums[..25].to_vec();

    Ok(web::Json(JsonResult {
        count: nadir_probes.len(),
        result: SimilarProbe::new_vec_of(nadir_probes),
    }))
}

fn _get_recursive_dissim_probes<'a>(
    sim_map: &BTreeMap<u32, Vec<(u32, f32)>>,
    // prb_id: u32,
    acc_prbs_vec: &'a mut Vec<SimilarProbe>,
    max_count: usize,
    band: &(f32, f32),
) {
    let prb_id = acc_prbs_vec.last().unwrap().prb_id;
    let dissim_prb_vec: &Vec<(u32, f32)> = sim_map.get(&prb_id).unwrap();
    dissim_prb_vec
        .to_owned()
        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let filtered_iter = dissim_prb_vec
        .into_iter()
        .filter(|p| acc_prbs_vec.into_iter().all(|ps| ps.prb_id != p.0))
        .filter(|ps| ps.1 >= band.0 && ps.1 <= band.1)
        .collect::<Vec<_>>();

    let len = filtered_iter.len();

    if len == 0 {
        //retreat and rerun!
        let p = acc_prbs_vec.pop();
        println!(
            "retreat {}, count {}",
            p.unwrap().prb_id,
            acc_prbs_vec.len()
        );
        return _get_recursive_dissim_probes(sim_map, acc_prbs_vec, max_count, band);
    }

    let mut rng = thread_rng();
    let i = if len > 1 {
        rng.sample(rand::distributions::Uniform::new_inclusive(0, len - 1))
    } else {
        0
    };

    println!(
        "size {}, selected i {}, count {}",
        len,
        i,
        acc_prbs_vec.len()
    );
    match filtered_iter.into_iter().nth(i) {
        Some(prb) => {
            println!("{:?}", prb);
            acc_prbs_vec.push(SimilarProbe {
                prb_id: prb.0,
                similarity: prb.1,
            });
        }
        None => {
            println!("size {}, none selected, count {}", len, acc_prbs_vec.len());
            ()
        }
    };

    // if len == 1 {
    //     return ();
    // }

    // .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    // let dissim_prb = dissim_prb_vec.last().unwrap();
    if max_count >= acc_prbs_vec.len() {
        return _get_recursive_dissim_probes(sim_map, acc_prbs_vec, max_count, band);
    }
}

async fn recursive_dissimilarities_for_prb_id(
    data: (web::Data<AppState>, web::Path<Info>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, path, query) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    let mut query_err: Vec<&str> = vec![];
    let limit: usize;
    let mut band: (f32, f32) = (0.0, 1.0);
    let prb_id = &(path.prb_id.parse::<u32>().unwrap());

    match query.limit {
        Some(l) => {
            limit = l as usize;
        }
        None => {
            limit = 10;
        }
    };

    match &query.band {
        Some(b) => {
            let v: Vec<f32> = b
                .split(",")
                .map(|s: &str| s.parse::<f32>().unwrap())
                .collect::<Vec<_>>();
            band.0 = *v.first().unwrap();
            band.1 = *v.last().unwrap();
        }
        None => band = (0.3, 0.5),
    }

    let mode = if let Some(m) = &query.mode {
        match m.as_str() {
            "similar" => Mode::Similar,
            "dissimilar" => Mode::Dissimilar,
            _ => {
                query_err.push("mode");
                // This last line just to satisfy the type
                // it isn't actually used, because we're going to throw.
                Mode::Dissimilar
            }
        }
    } else {
        Mode::Dissimilar
    };

    let results = &mut Vec::<SimilarProbe>::new();
    results.push(SimilarProbe {
        prb_id: *prb_id,
        similarity: 1.0,
    });
    _get_recursive_dissim_probes(&similarity_map, results, limit, &band);
    println!("--- end of query ---");
    Ok(web::Json(JsonResult {
        count: results.len(),
        result: results.to_owned(),
    }))
}

fn _get_aggregated_recursive_dissim_probes<'a>(
    sim_map: &BTreeMap<u32, Vec<(u32, f32)>>,
    mode: Mode,
    acc_prbs_sum_vecs: &'a mut Vec<(u32, f32)>,
    max_count: usize,
    mut recurse_count: usize,
    band: &(f32, f32),
) {
    let last_acc_prbs_vec: &(u32, f32) = acc_prbs_sum_vecs.last().unwrap();
    let last_prb_id = last_acc_prbs_vec.0;

    let vv = acc_prbs_sum_vecs.to_owned();
    let vv1 = acc_prbs_sum_vecs.to_owned();
    let mut sums_for_prb_id: Vec<(u32, f32)> = sim_map
        // get all similarities for the last probe in the vector holding all the sums,
        // that were passed in to this function.
        .get(&last_prb_id)
        .unwrap()
        .iter()
        // add all the sums that are already in the vec of probes and sums,
        // or fill in the blanks.
        .map(move |(prb_id, sum)| {
            let exist_sum = vv1.iter().find(|p| &p.0 == prb_id);
            match exist_sum {
                Some(s) => {
                    println!(
                        "adding for prb_id {}: {} + {} = {}",
                        prb_id,
                        sum,
                        s.1,
                        sum + s.1
                    );
                    (*prb_id, sum + s.1)
                }
                None => (*prb_id, *sum),
            }
        })
        .collect::<Vec<_>>();

    match mode {
        // sort by the lowest sum if dissimilar was chosen by the user (the default also)
        Mode::Dissimilar => {
            sums_for_prb_id.sort_by(|a: &(u32, f32), b: &(u32, f32)| a.1.partial_cmp(&b.1).unwrap())
        }
        // sort by the highest sum if similar was chosen by the user.
        Mode::Similar => {
            sums_for_prb_id.sort_by(|a: &(u32, f32), b: &(u32, f32)| b.1.partial_cmp(&a.1).unwrap())
        }
    };

    let sums_for_prb_id_clone = sums_for_prb_id.clone();

    let f_sums = sums_for_prb_id
        .into_iter()
        .filter(|(prb_id, ps)| {
            ps >= &band.0 && ps <= &band.1 && vv.iter().find(|p| &p.0 == prb_id).is_none()
        })
        .collect::<Vec<_>>();

    // select a random probe from the set that is consists of the
    // first 10 probes of the probes with (summes) similarities within the band.
    let len = if f_sums.len() < 10 { f_sums.len() } else { 10 };
    println!("length of filtered sums set:  {:?}", len);

    let mut rng = thread_rng();
    let i = if len > 1 {
        rng.sample(rand::distributions::Uniform::new_inclusive(0, len - 1))
    } else {
        0
    };

    match f_sums.get(i) {
        Some(prb) => {
            assert_eq!(
                true,
                acc_prbs_sum_vecs
                    .iter()
                    .all(|(prb_id, _)| { prb_id != &prb.0 })
            );
            acc_prbs_sum_vecs.push(*prb);

            println!(
                "Added prb {:?} to the selection set with a value of {} in round {}.",
                prb.0, prb.1, recurse_count
            );
        }
        None => {
            recurse_count += 1;
            println!("Empty new probe set in round {}", recurse_count);
        }
    };

    // change all the occurences to hold the (updated) intermediary sum
    acc_prbs_sum_vecs.clone().iter().for_each(|(prb_id, _)| {
        match sums_for_prb_id_clone.iter().find(|p| &p.0 == prb_id) {
            Some(p) => {
                acc_prbs_sum_vecs.retain(|p| &p.0 != prb_id);
                // only restore the probe if the ps value is within the set band,
                // otherwise update the recursion_count to make sure we do not
                // keep on doing this forever.
                // Note that this way we may return less than the number of probes set
                // by the `limit` query pararmeter.
                if p.1 <= band.1 && p.1 >= band.0 {
                    acc_prbs_sum_vecs.push(*p);
                } else {
                    recurse_count += 1;

                    println!(
                        "No new {:?} probe within the band found for probe {} in round {}",
                        mode, prb_id, recurse_count
                    );
                }
            }
            None => {
                println!(
                    "No {:?} probe found for probe {} in round {}",
                    mode, prb_id, recurse_count
                );
            }
        };
    });

    if &max_count >= &acc_prbs_sum_vecs.len() && recurse_count <= max_count * 2 {
        return _get_aggregated_recursive_dissim_probes(
            sim_map,
            mode,
            acc_prbs_sum_vecs,
            max_count,
            recurse_count,
            band,
        );
    }
}

async fn aggregated_recursive_dissimilarities_for_prb_id(
    data: (web::Data<AppState>, web::Path<Info>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, path, query) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    let mut query_err: Vec<&str> = vec![];
    // let mut limit: usize = 10;
    // let mut band: (f32, f32) = (0.0, 1.0);
    let prb_id = &(path.prb_id.parse::<u32>().unwrap());

    let mode = if let Some(m) = &query.mode {
        match m.as_str() {
            "similar" => Mode::Similar,
            "dissimilar" => Mode::Dissimilar,
            _ => {
                query_err.push("mode");
                Mode::Dissimilar
            }
        }
    } else {
        Mode::Dissimilar
    };

    let band = if let Some(b) = &query.band {
        let v: Vec<f32> = b
            .split(",")
            .map(|s: &str| s.parse::<f32>().unwrap())
            .collect::<Vec<_>>();
        (*v.first().unwrap(), *v.last().unwrap())
    } else {
        (0.0, 1.0)
    };

    let limit: usize = if let Some(l) = query.limit {
        l as usize
    } else {
        10
    };

    if query_err.len() > 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "queryparameter{}: `{}` invalid",
                if query_err.len() == 1 { "" } else { "s" },
                query_err.join("`,`")
            ),
        ));
    };

    let probe_vecs = &mut vec![(prb_id.to_owned(), 1.0)];
    _get_aggregated_recursive_dissim_probes(&similarity_map, mode, probe_vecs, limit, 0, &band);
    println!("result: {:?}", probe_vecs);
    println!("--- end of query ---");
    Ok(web::Json(JsonResult {
        count: probe_vecs.len(),
        result: SimilarProbe::new_vec_of(probe_vecs.to_owned()),
    }))
}

async fn similarity_for_prb_id_prb_id(
    data: (
        web::Data<AppState>,
        web::Path<DualProbesInfo>,
        web::Query<QueryInfo>,
    ),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, path, query) = data;
    let similarity_map = state.similarity_map.read().unwrap();

    Ok(web::Json(JsonResult {
        count: 1,
        result: vec![SimilarProbe {
            prb_id: path.prb_id2,
            similarity: similarity_map
                .get(&path.prb_id1)
                .unwrap()
                .into_iter()
                .find(|(prb_id, _)| prb_id == &path.prb_id2)
                .unwrap()
                .1,
        }],
    }))
}

async fn similarity_for_prb_ids(
    data: (
        web::Data<AppState>,
        web::Query<QueryInfo>,
        web::Json<ProbesPayload>,
    ),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, query, payload) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    println!("post request w/ probe set: {:?}", &payload.prb_ids);

    let mut query_err: Vec<&str> = vec![];
    let mode = if let Some(m) = &query.mode {
        match m.as_str() {
            "similar" => Mode::Similar,
            "dissimilar" => Mode::Dissimilar,
            _ => {
                query_err.push("mode");
                Mode::Dissimilar
            }
        }
    } else {
        Mode::Dissimilar
    };

    let band = if let Some(b) = &query.band {
        let v: Vec<f32> = b
            .split(",")
            .map(|s: &str| s.parse::<f32>().unwrap())
            .collect::<Vec<_>>();
        (*v.first().unwrap(), *v.last().unwrap())
    } else {
        (0.0, 1.0)
    };

    let limit: usize = if let Some(l) = query.limit {
        l as usize
    } else {
        10
    };

    if query_err.len() > 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "queryparameter{}: `{}` invalid",
                if query_err.len() == 1 { "" } else { "s" },
                query_err.join("`,`")
            ),
        ));
    };

    // Ok(web::Json(JsonResult {
    //     count: 1,
    //     result: vec![SimilarProbe {
    //         prb_id: path.prb_id2,
    //         similarity: similarity_map
    //             .get(&path.prb_id1)
    //             .unwrap()
    //             .into_iter()
    //             .find(|(prb_id, _)| prb_id == &path.prb_id2)
    //             .unwrap()
    //             .1,
    //     }],
    // }))

    return Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "not implemented",
    ));
}

#[derive(Deserialize)]
struct ProbesPayload {
    prb_ids: Vec<u32>,
}

async fn similarity_for_prb_set(
    // info: web::Json<ProbesPayload>,
    data: (
        web::Data<AppState>,
        web::Query<QueryInfo>,
        web::Json<ProbesPayload>,
    ),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, query, payload) = data;
    let similarity_map = state.similarity_map.read().unwrap();
    println!("post request w/ probe set: {:?}", &payload.prb_ids);
    let mut query_err: Vec<&str> = vec![];

    let mode = if let Some(m) = &query.mode {
        match m.as_str() {
            "similar" => Mode::Similar,
            "dissimilar" => Mode::Dissimilar,
            _ => {
                query_err.push("mode");
                Mode::Dissimilar
            }
        }
    } else {
        Mode::Dissimilar
    };

    let band = if let Some(b) = &query.band {
        let v: Vec<f32> = b
            .split(",")
            .map(|s: &str| s.parse::<f32>().unwrap())
            .collect::<Vec<_>>();
        (*v.first().unwrap(), *v.last().unwrap())
    } else {
        (0.0, 1.0)
    };

    let limit: usize = if let Some(l) = query.limit {
        l as usize
    } else {
        10
    };

    if query_err.len() > 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "queryparameter{}: `{}` invalid",
                if query_err.len() == 1 { "" } else { "s" },
                query_err.join("`,`")
            ),
        ));
    };

    // let probe_vecs = &mut vec![(prb_id.to_owned(), 1.0)];
    let mut probe_vecs: Vec<(u32, f32)> = payload
        .prb_ids
        .iter()
        .map(|prb_id| (*prb_id, 0.0))
        .collect();

    _get_aggregated_recursive_dissim_probes(
        &similarity_map,
        mode,
        &mut probe_vecs,
        limit,
        0,
        &band,
    );
    println!("result: {:?}", probe_vecs);
    println!("--- end of query ---");
    Ok(web::Json(JsonResult {
        count: probe_vecs.len(),
        result: SimilarProbe::new_vec_of(probe_vecs.to_owned()),
    }))
}

fn p404(req: &HttpRequest) -> std::io::Result<web::Json<String>> {
    Ok(web::Json(("not found").to_string()))
}

// Try to locate the latest files from BQ by
// reading the dirname passed into the function and
// then making vectors of files that have similar names,
// basically the first (BQ_FILE_REGEX + BQ_REGEX) characters are
// the same within a batch. Then take the batch with most recent creation
// timestamps.
fn select_latest_files(data_path: &str) -> Result<Vec<std::path::PathBuf>, std::io::Error> {
    let data_dir = read_dir(data_path)?;
    let mut newest_created: Option<std::time::SystemTime> = None;

    let ordered_paths: BTreeMap<String, (Vec<std::path::PathBuf>, std::time::SystemTime)> =
        data_dir.fold(
            BTreeMap::new(),
            |mut f_a: BTreeMap<String, (Vec<std::path::PathBuf>, std::time::SystemTime)>,
             f: Result<std::fs::DirEntry, _>| {
                if let Ok(ff) = f {
                    let c = &ff.metadata().unwrap().created().unwrap();
                    let f_n = ff.file_name().to_str().unwrap().to_owned();
                    let n_c = if let Some(n_c) = newest_created {
                        n_c
                    } else {
                        *c
                    };

                    if f_n[..BQ_FILE_REGEX.len()] == *BQ_FILE_REGEX {
                        let f_entry = f_a
                            .entry(f_n[..(BQ_FILE_REGEX.len() + BQ_REGEX.len())].to_string())
                            .or_insert((vec![], *c));
                        f_entry.0.push(ff.path());

                        if n_c <= *c {
                            newest_created = Some(*c);
                        }
                    }
                }
                f_a
            },
        );

    let mut select_paths = ordered_paths.values().collect::<Vec<_>>();
    select_paths.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    Ok(select_paths.first().unwrap().0.to_vec())
}

fn clear_sim_map(sim_map_lock: &Arc<RwLock<BTreeMap<u32, Vec<(u32, f32)>>>>) {
    println!("clearing simularity map...");
    *sim_map_lock.write().unwrap() = BTreeMap::new();
}

fn load_new_sim_map_and_replace(
    sim_map_lock: &Arc<RwLock<BTreeMap<u32, Vec<(u32, f32)>>>>,
    files: Vec<std::path::PathBuf>,
) {
    let similarity_map = match load_avro_from_file_multithreaded(files) {
        Ok((sm, _)) => sm,
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };
    *sim_map_lock.write().unwrap() = similarity_map;
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let src_files = select_latest_files(BQ_DATA_PATH).unwrap();

    let (similarity_map, mut base_name) = match load_avro_from_file_multithreaded(src_files) {
        Ok((sm, bn)) => (Arc::new(RwLock::new(sm)), bn),
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };

    let bind_address = match get_second_arg() {
        Ok(ba) => ba.into_string().unwrap(),
        Err(_) => "127.0.0.1:8100".to_string(),
    };
    println!("Started http server on {}", &bind_address);

    // Fire up a thread that will look for available updates,
    // load and swap it out it with the current state
    let sim_map_clone = similarity_map.clone();
    thread::spawn(move || loop {
        thread::sleep(std::time::Duration::from_secs(70));
        println!("checking for new files...");
        let new_files = check_updated_files(&base_name).unwrap();
        match new_files {
            Some(files) => {
                println!("found new files, loading...");
                load_new_sim_map_and_replace(&sim_map_clone, files.0);
                base_name = files.1;
            }
            None => {
                println!("no new files found...");
            }
        }
    });

    HttpServer::new(move || {
        App::new()
            .data(AppState {
                similarity_map: Arc::clone(&similarity_map),
            })
            .service(
                web::scope("/probe-similarity")
                    .route("/nadir", web::get().to(nadir))
                    .route("/probe-set", web::post().to(similarity_for_prb_set))
                    .route(
                        "/recursive/{prb_id}",
                        web::get().to(recursive_dissimilarities_for_prb_id),
                    )
                    .route(
                        "/aggregated-recursive/{prb_id}",
                        web::get().to(aggregated_recursive_dissimilarities_for_prb_id),
                    )
                    .route(
                        "/probes/{prb_id1}/{prb_id2}",
                        web::get().to(similarity_for_prb_id_prb_id),
                    )
                    .route("/probe/{prb_id}", web::get().to(similarities_for_prb_id))
                    .route("/probes/", web::post().to(similarity_for_prb_ids)),
            )
    })
    .bind("127.0.0.1:8100")?
    .run()
    .await
}
