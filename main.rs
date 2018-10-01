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
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::str;
use std::sync::Arc;

use actix_web::{http, server, App, Json, Path, Query, Result, State};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use memmap::Mmap;
use rayon::prelude::*;
use time::{Duration, SteadyTime};

#[derive(Debug, Deserialize)]
struct SimilarityRecord {
    prb_id1: u32,
    prb_id2: u32,
    msm_set_size1: u16,
    msm_set_size2: u16,
    msm_set_size_overlap: u16,
    msm_set_size_overlap_usable: u16,
    metric_q1: f32,
    metric_q2: f32,
    metric_q3: f32,
}

fn load_with_mmap(
    mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>>,
) -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let mut file = File::open(file_path)?;

    // let mut buffer = Vec::new();
    let mut buffer = io::BufReader::new(file);
    // file.read_to_end(&mut buffer);
    // let mut line_num: u64 = 0;
    println!("done loading file");

    // csv reader
    // 8 sec for 1 mil rows (parsing into SimilarityStruct, not our data structure).
    // let mut rdr = ReaderBuilder::new()
    //     .has_headers(false)
    //     .delimiter(b' ')
    //     .from_reader(file);

    // let mut start_time = SteadyTime::now();
    // for result in rdr.deserialize() {
    //     let record: SimilarityRecord = result?;
    //     // println!("{:?}", record);

    //     line_num += 1;
    //     if line_num % 1_000_000 == 0 || line_num == 1 {
    //         println!("{:?}", record);
    //         println!("{:?}mil", line_num / 1_000_000);
    //         println!("{:?}", SteadyTime::now() - start_time);
    //         start_time = SteadyTime::now()
    //     };
    // }

    // mmap for loop
    // println!("done mapping file to memory");
    // for r in sim_mmap.split(|ch| ch == new_line_slice).skip(1) {
    //     // println!("{:?}", str::from_utf8(r).unwrap());
    //     let b: Vec<String> = r
    //         .split(|c| c == blank)
    //         .map(|s| str::from_utf8(s).unwrap().to_string())
    //         .collect();
    //     let prb_id1 = b[0].parse::<u32>()?;
    //     let prb_id2 = b[1].parse::<u32>()?;
    //     let pct50_similarity = b[7].parse::<f32>()?;
    //     let prb_entry = similarity_map.entry(prb_id1).or_insert(vec![]);
    //     prb_entry.push((prb_id2, pct50_similarity));

    //     line_num += 1;
    //     if line_num % 100_000 == 0 {
    //         println!("{:?}", line_num)
    //     };
    // }

    // BufReader
    // 12 seconds for 1 mil rows! (non compressed)
    // 14.3 - 15.5 seconds for 1 mil rows (gz compressed)

    // let mut start_time = SteadyTime::now();
    // // let data = GzDecoder::new(file);
    // // for line in io::BufReader::new(data).lines().skip(1) {
    // for line in buffer.lines().skip(1) {
    //     let line = line.unwrap();
    //     let b: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
    //     let prb_id1 = b[0].parse::<u32>()?;
    //     let prb_id2 = b[1].parse::<u32>()?;
    //     let pct50_similarity = b[7].parse::<f32>()?;
    //     let prb_entry = similarity_map.entry(prb_id1).or_insert(vec![]);
    //     prb_entry.push((prb_id2, pct50_similarity));

    //     line_num += 1;
    //     if line_num % 1_000_000 == 0 || line_num == 1 {
    //         println!("{:?}", b);
    //         println!("{:?}mil", line_num / 1_000_000);
    //         println!("{:?}", SteadyTime::now() - start_time);
    //         start_time = SteadyTime::now()
    //     };
    // }

    Ok(similarity_map)
}
// ---------------------------------------
// new parallel BufReader w/ fold

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

fn load_with_memmap_multithreaded(
    mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>>,
) -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let start_time = SteadyTime::now();
    let sim_mmap = unsafe { Mmap::map(&file)? };
    let simsim: Vec<&[u8]> = sim_mmap.split(|ch| ch == &b'\n').collect();

    println!("file in memory, now processing in parallel");
    similarity_map = simsim
        .par_iter()
        .enumerate()
        .fold(
            || BTreeMap::new(),
            |mut part_map, (line_num, line)| {
                let b: Vec<String> = line
                    .split(|c| c == &b' ')
                    .map(|s| str::from_utf8(s).unwrap().to_string())
                    .collect();

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
            |mut total_map, mut part_map| {
                total_map.append(&mut part_map);
                total_map
            },
        );

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);
    Ok(similarity_map)
}

fn load_with_bufreader_single_thread() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let data = GzDecoder::new(file);
    let buffer = io::BufReader::new(data);
    let start_time = SteadyTime::now();
    let similarity_map = buffer
        .lines()
        .skip(1)
        .map(|l| l.unwrap())
        .into_iter()
        .enumerate()
        .fold(BTreeMap::new(), |mut part_map, (line_num, line)| {
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
            };

            part_map
        });

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);

    Ok(similarity_map)
}

fn load_with_cursor_single_thread() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let unzipdata = GzDecoder::new(file);
    let mut data = io::BufReader::new(unzipdata);
    // let cursor = io::Cursor::new(data);
    let mut line = vec![];
    let start_time = SteadyTime::now();
    // .skip(1)
    // .map(|l| l.unwrap())
    let mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
    let mut line_num: u64 = 0;

    println!("Start processing in one thread");
    'line: loop {
        let num_bytes = data.read_until(b'\n', &mut line).unwrap();
        // println!("{:?}", num_bytes);
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

        // let prb_id1 = b[0].parse::<u32>()?;
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

fn load_with_bufreader_single_thread_for_loop() -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>>
{
    let file_path = get_first_arg()?;
    let mut file = File::open(file_path)?;
    let data = GzDecoder::new(file);

    let mut buffer = io::BufReader::new(data);
    let start_time = SteadyTime::now();
    let mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();

    let mut line_num: u64 = 0;

    let start_time = SteadyTime::now();

    for line in buffer.lines().skip(1) {
        line_num += 1;
        if line_num % 1_000_000 == 0 {
            println!("{:?}", line_num);
            println!("{:?}", SteadyTime::now() - start_time);
        };
        let line = line.unwrap();
        let b: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        let prb_id1 = b[0].parse::<u32>()?;
        let prb_id2 = b[1].parse::<u32>()?;
        let pct50_similarity = b[7].parse::<f32>()?;
        let prb_entry = similarity_map.entry(prb_id1).or_insert(vec![]);
        prb_entry.push((prb_id2, pct50_similarity));
    }

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);
    Ok(similarity_map)
}

fn load_with_memmap_single_thread(
    mut similarity_map: BTreeMap<u32, Vec<(u32, f32)>>,
) -> Result<BTreeMap<u32, Vec<(u32, f32)>>, Box<Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;

    let start_time = SteadyTime::now();
    let sim_mmap = unsafe { Mmap::map(&file)? };
    let simsim: Vec<&[u8]> = sim_mmap.split(|ch| ch == &b'\n').collect();

    println!("file in memory, now processing in single thread");
    similarity_map =
        simsim
            .iter()
            .enumerate()
            .fold(BTreeMap::new(), |mut part_map, (line_num, line)| {
                let b: Vec<String> = line
                    .split(|c| c == &b' ')
                    .map(|s| str::from_utf8(s).unwrap().to_string())
                    .collect();

                if b.len() == 9 {
                    let prb_id1 = b[0].parse::<u32>().unwrap();
                    let prb_id2 = b[1].parse::<u32>().unwrap();
                    let pct50_similarity = b[7].parse::<f32>().unwrap();
                    let prb_entry_1 = part_map.entry(prb_id1).or_insert(vec![]);
                    prb_entry_1.push((prb_id2, pct50_similarity));
                    let prb_entry_2 = part_map.entry(prb_id2).or_insert(vec![]);
                    prb_entry_2.push((prb_id1, pct50_similarity));
                }

                if line_num % 1_000_000 == 0 || line_num == 1 {
                    println!("{:?}", b);
                    println!("{:?}mil", line_num / 1_000_000);
                    println!("{:?}", SteadyTime::now() - start_time);
                };

                part_map
            });

    println!("completed BTreeMap");
    println!("{:?}", SteadyTime::now() - start_time);
    Ok(similarity_map)
}

// ----------------------------------

// 13 seconds for 1 mil rows

// let sim_mmap = unsafe { Mmap::map(&file)? };
// let mut line_num: i64 = 0;

// let mut start_time = SteadyTime::now();
// let sss: Vec<&[u8]> = sim_mmap
//     .split(|ch| ch == &b'\n')
//     .skip(1)
//     .collect();
// sss.into_iter().for_each(|r| -> () {
//     let mut line_num: i64  = if line_num > 0 { line_num } else { 1 };
//     let mut start_time = if line_num == 1 { start_time } else { SteadyTime::now() };
//     let b: Vec<String> = r
//         .split(|c| c == &b' ')
//         .map(|s| str::from_utf8(s).unwrap().to_string())
//         .collect();
//     // println!("{:?}", b);

//     if b.len() == 9 {
//         // let prb_id1 = match b[0].parse::<u32>() {
//         //     Ok(prb_id1) => prb_id1,
//         //     Err(_) => 0,
//         // };

//         // let prb_id2 = match b[0].parse::<u32>() {
//         //     Ok(prb_id2) => prb_id2,
//         //     Err(_) => 0,
//         // };

//         // let pct50_similarity = match b[7].parse::<f32>() {
//         //     Ok(pct) => pct,
//         //     Err(_) => 0.0,
//         // };

//         let prb_id1 = b[0].parse::<u32>().unwrap();
//         let prb_id2 = b[1].parse::<u32>().unwrap();
//         let pct50_similarity = b[7].parse::<f32>().unwrap();

//         // we want this table to symmetrical, so we push to both
//         // a record on key prb_1 and one with a key on prb_2
//         let prb_entry_1 = similarity_map.entry(prb_id1).or_insert(vec![]);
//         prb_entry_1.push((prb_id2, pct50_similarity));
//         let prb_entry_2 = similarity_map.entry(prb_id2).or_insert(vec![]);
//         prb_entry_2.push((prb_id1, pct50_similarity));
//     }

//     line_num = if line_num < 1_000_000 { line_num + 1 } else { 0 };
//     if line_num == 0 {
//         println!("{:?}", b);
//         println!("{:?}mil", line_num / 1_000_000);
//         println!("{:?}", SteadyTime::now() - start_time);
//         start_time = SteadyTime::now()
//     };
// });

// let bb: Vec<String> = buffer.lines().map(|l| l.unwrap()).skip(1).collect();
// let chunks: Vec<&[String]> = bb.chunks(1_000_000).collect();

// println!("done chunking");
// let similarity_map = chunks
//     .par_iter()
//     .map(|chunk| {
//         let mut start_time = SteadyTime::now();
//         let chunk = chunk.into_iter().fold(
//             BTreeMap::new(),
//             |mut part_map, r| {
//                 let b: Vec<&str> = r.split_whitespace().collect();

//                 let prb_id1 = b[0].parse::<u32>().unwrap();
//                 let prb_id2 = b[1].parse::<u32>().unwrap();
//                 let pct50_similarity = b[7].parse::<f32>().unwrap();
//                 let prb_entry_1 = part_map.entry(prb_id1).or_insert(vec![]);
//                 prb_entry_1.push((prb_id2, pct50_similarity));
//                 let prb_entry_2 = part_map.entry(prb_id2).or_insert(vec![]);
//                 prb_entry_2.push((prb_id1, pct50_similarity));

//                 // println!("{:?}", probe_similarity_map);
//                 // probe_similarity_map.append(&mut next_map);
//                 // probe_similarity_map
//                 // next_map.append(&mut probe_similarity_map);
//                 // next_map
//                 // println!("{:?}", part_map);
//                 part_map
//             },
//         );
//         println!("chunk");
//         println!("{:?}", SteadyTime::now() - start_time);

//         chunk
//     }).reduce(
//         || BTreeMap::new(),
//         |mut total_map, next_chunk_map| {
//             next_chunk_map.into_iter().for_each(|mut kv| {
//                 let mut kkv = kv.1.clone();
//                 let existing_entry = total_map.entry(kv.0).or_insert(vec![]);
//                 existing_entry.append(&mut kkv);
//                 // total_map().append(&mut map);
//                 // total_map()
//             });
//             total_map
//         },
//     );
//

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

    // let data = GzDecoder::new(file);
    let mut line_num: u64 = 0;
    for line in io::BufReader::new(file).lines().skip(1) {
        line_num += 1;
        if line_num % 100_000 == 0 {
            println!("{:?}", line_num)
        };
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
        .filter(|ps| ps.1 > 0.05)
        .collect();
    println!("{:?}", simils.len());
    println!("{:?}", simils);

    Ok(similarity_map)
}

fn get_first_arg() -> Result<OsString, Box<Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("Expected at least 1 argument with the gzipped csv file, but got none")),
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
    prb_id: u32,
}

#[derive(Deserialize)]
struct QueryInfo {
    cutoff: f32,
    mode: String,
}

#[derive(Serialize)]
struct SimilarProbe {
    prb_id: u32,
    similarity: f32,
}

#[derive(Serialize)]
struct JsonResult {
    count: usize,
    result: Vec<SimilarProbe>,
}

fn index(data: (State<AppState>, Path<Info>, Query<QueryInfo>)) -> Result<Json<JsonResult>> {
    let (state, path, query) = data;
    println!("{:?}", path.prb_id);
    println!("{:?}", query.cutoff);
    println!("{:?}", query.mode);
    let mut s: Vec<SimilarProbe> = state
        .similarity_map
        .get(&path.prb_id)
        .unwrap()
        .into_iter()
        .filter(|ps| {
            if query.mode == "dissimilar" {
                ps.1 <= query.cutoff
            } else {
                ps.1 >= query.cutoff
            }
        }).map(|v| SimilarProbe {
            prb_id: v.0,
            similarity: v.1,
        }).collect();

    s.sort_by(|a, b| {
        b.similarity
            .partial_cmp(&a.similarity)
            .unwrap_or(Ordering::Equal)
    });
    Ok(Json(JsonResult {
        count: s.len(),
        result: s,
    }))
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
        App::with_state(AppState {
            similarity_map: Arc::clone(&similarity_map),
        }).prefix("/probe-similarity")
        .resource("/{prb_id}", |r| r.method(http::Method::GET).with(index))
    }).bind(&bind_address)
    .unwrap()
    .start();

    println!("Started http server on {}", &bind_address);
    let _ = sys.run();
}
