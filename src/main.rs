#[deny(warnings)]
//use csv::Reader;
use clap::Parser;
use anyhow::Result;
use serde::{Deserialize,Deserializer};
use chrono::naive::NaiveDate;
use dotenv;
use num_traits::Float;
use std::collections::HashMap;
use prettytable::{Table,Row as PRow,Cell};

/// This is a program to provide Distribution of Returns Analysis of a
/// stock downloaded from Yahoo Finance. It expects .csvs in that format.
#[derive(Parser,Debug)]
#[command(name="pm")]
#[command(bin_name="pm")]
enum PmCli {
    Dor(DorArgs),
}
#[derive(clap::Args,Debug)]
#[command(author,version,about,long_about=None)]
struct DorArgs {
    /// Path to .csv file to analyze
    #[arg(long,value_name="DIR",value_hint=clap::ValueHint::DirPath)]
    file: std::path::PathBuf,
    /// The period of each datapoint. 1 = 1 day usually
    #[arg(long,default_value="1")]
    period: usize,
}

#[derive(Debug, Deserialize, PartialEq,Clone)]
struct Row {
    #[serde(rename = "Date", deserialize_with="str_to_naive")]
    date: NaiveDate,
    #[serde(rename = "Open")]
    open: f64,
    #[serde(rename = "High")]
    high: f64,
    #[serde(rename = "Low")]
    low: f64,
    #[serde(rename = "Close")]
    close: f64,
    #[serde(rename ="Adj Close")]
    adj_close: f64,
    #[serde(rename = "Volume")]
    volume: i64,
}

#[derive(Debug,Clone)]
struct DataPoint {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    /// the date attached to this point
    date: NaiveDate,
    /// This is the Close-to-Close return
    c2c_return: f64,
    c2c: f64,
    /// This is the High-to-Low return
    h2l_return: f64,
    h2l: f64,
    /// This is the Open-to-Close return
    o2c_return: f64,
    o2c: f64,
    true_range: f64,
    true_range_percentage: f64,
}
fn fmax(floats: &[f64]) -> f64 {
    let mut m = Float::min_value();
    for i in 0..floats.len() {
        if floats[i] > m {
            m = floats[i];
        }
    }
    m
}

fn rows_to_datapoints(args: &DorArgs, rows: &[Row]) -> Result<Vec<DataPoint>> {
    let end = rows.len()-1;
    let mut results = vec![];
    let mut idx = args.period;
    while idx < end {
        let crow = &rows[idx];
        let prow = &rows[idx - args.period];
        let h2l = crow.high - crow.low;
        let true_range = fmax(&vec![
            h2l,
            (crow.high - prow.close).abs(),
            (prow.close - crow.low).abs()
        ]);
        
        let true_range_percentage = true_range / crow.open;
        let dp = DataPoint {
            open: crow.open,
            high: crow.high,
            low: crow.low,
            close: crow.close,
            date: crow.date,
            c2c_return: (crow.close - prow.close) / crow.close, 
            c2c: crow.close - prow.close,
            h2l_return: (crow.high - crow.low) / crow.low, 
            h2l,
            o2c_return: (crow.close - crow.open) / crow.open, 
            o2c: crow.close - crow.open,
            true_range,
            true_range_percentage,
        };
        results.push(dp);
        idx = idx + args.period;
    }
    Ok(results)
}
fn str_to_naive<'de, D>(deserializer: D) -> Result<NaiveDate,D::Error>
where
    D: Deserializer<'de>
{
    let rs = String::deserialize(deserializer)?;
    let mut fmt = "%m/%d/%Y".to_string();
    let maybe_fmt = dotenv::var("DATE_FORMAT");
    if maybe_fmt.is_ok() {
        fmt = maybe_fmt.unwrap();
    }
    Ok(NaiveDate::parse_from_str(&rs,&fmt).unwrap()) 
}
fn get_rows(args: &DorArgs) -> Result<Vec<Row>> {
    let mut rdr = csv::Reader::from_path(args.file.clone())?;
    let mut results = vec![];
    for row in rdr.deserialize::<Row>() {
        results.push(row?);
    }
    Ok(results) 
}
fn get_atrp(args: &DorArgs, dps: &[DataPoint]) -> f64 {
    let mut count = 0.0;
    let mut total = 0.0;
    for dp in dps {
        total += dp.true_range_percentage;
        count += 1.0
    }
    total / count
}

fn group_daily_by_month(_args: &DorArgs, dps: &[DataPoint]) -> HashMap<String,Vec<DataPoint>> {
    let mut result: HashMap<String,Vec<DataPoint>> = HashMap::new();
    for dp in dps {
        let key = dp.date.format("%Y/%m").to_string();
        result.entry(key).and_modify(|e| e.push(dp.clone()) ).or_insert(vec![dp.clone()]);
    }
    result
}

fn group_daily_by_quarter(_args: &DorArgs, dps: &[DataPoint]) -> HashMap<String,Vec<DataPoint>> {
    let mut result: HashMap<String,Vec<DataPoint>> = HashMap::new();
    for dp in dps {
        let month = dp.date.format("%m").to_string();
        let year = dp.date.format("%Y").to_string();
        let qtr = match &month[..] {
            "01" | "02" | "03" => "First",
            "04" | "05" | "06" => "Second",
            "07" | "08" | "09" => "Third",
            "10" | "11" | "12" => "Fourth",
            _ => panic!("Cannot match month"),
        };
        let key = format!("{}/{}",year,qtr);
        result.entry(key).and_modify(|e| e.push(dp.clone()) ).or_insert(vec![dp.clone()]);
    }
    result
}

fn get_high(dps: &[DataPoint]) -> f64 {
    let mut m = Float::min_value();
    for d in dps {
        if d.high > m {
            m = d.high;
        }
    }
    m
}

fn get_low(dps: &[DataPoint]) -> f64 {
    let mut m = Float::max_value();
    for d in dps {
        if d.low < m {
            m = d.low;
        }
    }
    m
}

fn grouped_to_datapoints(_args: &DorArgs, map: &HashMap<String,Vec<DataPoint>>) -> Result<Vec<DataPoint>> {
   let mut results = vec![]; 
   let mut prow: Option<DataPoint> = None;
   for (key,odps) in map.iter() {
       let mut dps = odps.clone();
       dps.sort_by_key(|e| e.date );
        let high = get_high(&dps);
        let low = get_low(&dps);
        let open = dps[0].open;
        let close = dps[dps.len() - 1].close;
        let h2l = high - low;
        let true_range = match prow {
            Some(ref p) => fmax(&vec![
                h2l,
                (high - p.close).abs(),
                (p.close - low).abs()
            ]),
            None => 0.0,
        };
        let c2c = match prow {
            Some(ref p) => close - p.close,
            None => 0.0,
        };
        let c2c_return = match prow {
            Some(ref p) => c2c / close,
            None => 0.0,
        };
        let h2l_return = match prow {
            Some(ref p) => (high - low) / low,
            None => 0.0,
        };
        let year = dps[dps.len()-1].date.format("%Y").to_string();
        let month = dps[dps.len()-1].date.format("%m").to_string();
        let true_range_percentage = true_range / open;
        let dp = DataPoint {
            open: open,
            high: high,
            low: low,
            close: close,
            date: NaiveDate::parse_from_str(&format!("{}/{}/1",year,month),"%Y/%m/%d")?,
            c2c_return: c2c_return, 
            c2c: c2c,
            h2l_return: h2l, 
            h2l,
            o2c_return: (close - open) / open, 
            o2c: open - close,
            true_range,
            true_range_percentage,
        };
        results.push(dp.clone());
        prow = Some(dp.clone());
   }
   results.sort_by_key(|e| e.date );
   Ok(results)
}

impl DataPoint {
    fn to_row(&self) -> PRow {
        PRow::new(
            vec![
                Cell::new(&self.date.format("%Y/%m/%d").to_string()),
                Cell::new(&format!("{:.2}",self.open)),
                Cell::new(&format!("{:.2}",self.high)),
                Cell::new(&format!("{:.2}",self.low)),
                Cell::new(&format!("{:.2}",self.close)),
                Cell::new(&format!("{:.2}",self.true_range)),
                Cell::new(&format!("{:.2}",self.true_range_percentage)),
            ]
        )
    }
}
fn output_table(_args: &DorArgs, dps: &[DataPoint] ) -> Result<()> {
    let mut table = Table::new();
    table.add_row(
        PRow::new(
            vec![
                Cell::new("D"),
                Cell::new("O"),
                Cell::new("H"),
                Cell::new("L"),
                Cell::new("C"),
                Cell::new("TR"),
                Cell::new("TRP"),
            ]
        )
    );
    for dp in dps {
        table.add_row(dp.to_row());
    }
    table.printstd();
    Ok(())
}

fn output_positions(_args: &DorArgs, positions: &[Position] ) -> Result<()> {
    let mut table = Table::new();
    table.add_row(
        PRow::new(
            vec![
                Cell::new("ID"),
                Cell::new("OD"),
                Cell::new("I"),
                Cell::new("O"),
            ]
        )
    );
    for pos in positions {
        table.add_row(pos.to_row());
    }
    table.printstd();
    Ok(())
}

fn get_highest_high(dps: &[DataPoint]) -> DataPoint {
    let mut final_dp = dps[0];
    for dp in dps {
        if dp.high > final_dp.high {
            final_dp = dp.clone();
        }
    }
    return final_dp;
}

fn get_lowest_low(dps: &[DataPoint]) -> DataPoint {
    let mut final_dp = dps[0];
    for dp in dps {
        if dp.low < final_dp.low {
            final_dp = dp.clone();
        }
    }
    return final_dp;
}
enum PositionDirection {
    Long,
    Short,
}
struct Position {
    id: i64;
    entry: DataPoint,
    outry: Option<DataPoint>,
    direction: PositionDirection,
}

impl Position {
    fn to_row(&self) -> PRow {
        PRow::new(
            vec![
                Cell::new(&self.entry.date.format("%Y/%m/%d").to_string()),
                match self.outry {
                    Some(o) => Cell::new(&o.date.format("%Y/%m/%d").to_string()),
                    None => Cell::new("None"),
                },
                match self.direction {
                    PositionDirection::Long => {
                        Cell::new(&format!("{:.2}",self.entry.high))
                    },
                    PositionDirection::Short => {
                        Cell::new(&format!("{:.2}",self.entry.low))
                    },
                },
                match self.direction {
                    PositionDirection::Long => {
                        match self.outry {
                            Some(o) => Cell::new(&format!("{:.2}",o.low)),
                            None => Cell::new("None"),
                        }
                    },
                    PositionDirection::Short => {
                        match self.outry {
                            Some(o) => Cell::new(&format!("{:.2}",o.low)),
                            None => Cell::new("None"),
                        }
                    },
                },
            ]
        )
    }
}
/// Returns the positions (long and short) based on simple buying the 55 days high,
/// called a breakout
fn turtle(_args: &DorArgs, dps: &[DataPoint]) -> Result<Vec<Position>> {
    // The strategy is to buy at the 55 day high, or short at the 55 day low
    let mut idx = 0;
    let mut positions:Vec<Position> = vec![];
    let mut position: Option<Position>;
    for dp in dps {
        if idx < 55 {
            continue;
        }
        let cdp = dps[idx];
        let pdp = get_highest_high(dps[(idx - 55)..(idx - 1)]);
        let pdp2 = get_lowest_low(dps[(idx - 55)..(idx - 1)]);
        
        if let Some(p) = position {
            // if we have a position, then we need to choose:
            // 1) stay in
            // 2) Sell long
            // 3) Cover short
            match p.direction {
                PositionDirection::Long => {
                    let tenp = p.entry.high * 0.10;
                    let h2l = p.entry.high - cdp.low; 
                    // if the delta is negative, we are in profit, so keep it 
                    if h2l > 0.0 && h2l >= tenp {
                        // cut the position 
                        positions.push(Position {
                            id: p.id,
                            entry: p.entry.clone(),
                            outry: cdp.clone(),
                            direction: p.direction,
                        });
                        position = None;
                    }
                },
                PositionDirection::Short => {
                    let tenp = p.entry.low * 0.10;
                    let l2h = p.entry.low - cdp.high; 
                    if l2h < 0.0 && l2h.abs() >= tenp {
                        // cut the position
                        positions.push(Position {
                            id: p.id,
                            entry: p.entry.clone(),
                            outry: cdp.clone(),
                            direction: p.direction,
                        });
                        position = None;
                    }
                },
            }
        } else {
            if cdp.high > pdp.high {
                position = Some(Position { id: idx, entry: cdp.clone(), outry: None, direction: PositionDirection::Long });
            } else if cdp.low < pdp2.low {
                position = Some(Position { id: idx, entry: cdp.clone(), outry: None, direction: PositionDirection::Short });
            }
        }
        idx += 1;
    }
    return positions;
}

fn handle_dor(args: &DorArgs) -> Result<()> {
   let rows = get_rows(args)?; 
   let dps = rows_to_datapoints(args,&rows)?;
   let atrp = get_atrp(args,&dps);
   let byq = grouped_to_datapoints(args,&group_daily_by_quarter(args,&dps))?;
   output_table(args,&byq)?;
   println!("{:.2}",-1.034);
    Ok(())
}
fn main() {
    let args = PmCli::parse();
    let result = match args {
        PmCli::Dor(a) => handle_dor(&a),
    };
    if !result.is_ok() {
        println!("There was an error: {:?}",result);
    }
}
