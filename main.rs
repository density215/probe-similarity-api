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
use std::sync::Arc;

use actix_web::{http, web, App, HttpRequest, HttpServer};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use time::SteadyTime;

use avro_rs::Reader;

// for write test
use avro_rs::types::{Record, Value};
use avro_rs::Schema;
use avro_rs::Writer;

const BQ_DATA_PATH: &str = "./bq_data";
const BQ_FILE_REGEX: &str = "jaccard_similarity_ipv4_2019-12-16_";

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

fn load_avro_from_file() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
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

fn load_avro_from_file_multithreaded() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
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

    let data_dir = read_dir(BQ_DATA_PATH)?;

    let f_map: Vec<std::path::PathBuf> = data_dir
        .map(|f| -> Option<std::path::PathBuf> {
            let file = f.unwrap();
            let file_name = &file.file_name();
            if &file_name.to_str().unwrap()[..BQ_FILE_REGEX.len()] == BQ_FILE_REGEX {
                let file_path = file.path();
                println!("selected {:?}...", file_path);
                Some(file_path)
            } else {
                None
            }
        })
        .filter_map(|f| f)
        .collect();

    let similarity_map = f_map
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
    Ok(similarity_map.1)
}

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

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Similar,
    Dissimilar,
}

#[derive(Deserialize)]
struct QueryInfo {
    cutoff: Option<f32>,
    mode: Option<String>,
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

#[derive(Serialize)]
struct NadirResult(Vec<(u32, f32)>);

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

async fn index(
    data: (web::Data<AppState>, web::Path<Info>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<JsonResult>> {
    let (state, path, query) = data;
    println!("{:?}", path.prb_id);
    println!("{:?}", query.cutoff);
    println!("{:?}", query.mode);
    let mut cutoff: f32 = 0.0;
    let mut query_err: Vec<&str> = vec![];

    if let Some(co) = query.cutoff {
        cutoff = co;
    } else {
        query_err.push("cutoff");
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

            state
                .similarity_map
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
        .fold(
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

    Ok(web::Json(JsonResult {
        count: v.len(),
        result: v,
    }))
}

async fn nadir(
    data: (web::Data<AppState>, web::Query<QueryInfo>),
) -> std::io::Result<web::Json<NadirResult>> {
    let (state, query) = data;
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
            // kick out probes that are basically the same out by default
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
    let mut sim_sums: Vec<(u32, f32)> = Vec::from_iter(state.similarity_map.iter().map(|kv| {
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

    Ok(web::Json(NadirResult(nadir_probes)))
}

fn p404(req: &HttpRequest) -> std::io::Result<web::Json<String>> {
    Ok(web::Json(("not found").to_string()))
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    // let sys = actix::System::new("probe-similarity");
    let similarity_map = match load_avro_from_file_multithreaded() {
        Ok(sm) => Arc::new(sm),
        Err(e) => {
            println!("{}", e);
            std::process::exit(1);
        }
    };

    // let similarity_map = match load_avro_from_file_multithreaded() {
    //     Ok(sm) => Arc::new(sm),
    //     Err(e) => {
    //         println!("{}", e);
    //         std::process::exit(1);
    //     }
    // };

    let bind_address = match get_second_arg() {
        Ok(ba) => ba.into_string().unwrap(),
        Err(_) => "127.0.0.1:8100".to_string(),
    };
    println!("Started http server on {}", &bind_address);

    HttpServer::new(move || {
        App::new()
            .data(AppState {
                similarity_map: Arc::clone(&similarity_map),
            })
            .service(
                web::scope("/probe-similarity")
                    .route("/nadir", web::get().to(nadir))
                    .route("/{prb_id}", web::get().to(index)),
            )
            // .prefix("/probe-similarity")
            .route("/", web::get().to(index))
    })
    .bind("127.0.0.1:8100")?
    .run()
    .await
}
