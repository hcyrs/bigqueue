extern crate bigqueue;
extern crate time;

use time::PreciseTime;
use bigqueue::BigQueue;
use std::fs;
use std::path::PathBuf;

fn main() {
//    dequeue();
//    pop();
    peek();
}

fn pop() {
    fs::create_dir_all(PathBuf::from("/tmp/bigqueue")).expect("create dir error");
    let mut q = BigQueue::new(&"/tmp/bigqueue", true).unwrap();

    let start = PreciseTime::now();
    let total = 100000000;
    let data = b"1234567890abcdefghij";

    for _a in 0..total {
        q.push(data).expect("error");
    }
    let end = PreciseTime::now();
    println!("{} seconds for enqueue. {} ps", start.to(end), total*1000000/start.to(end).num_microseconds().unwrap());

    let mut count = 0;

    let start = PreciseTime::now();
    loop {
        let pop_data = q.pop();
        if  pop_data.is_ok() && pop_data.unwrap().len() == data.len() {
            count = count + 1;
        } else {
            println!("count {}", count);
            break;
        }
    }
    let end = PreciseTime::now();
    println!("{} seconds for dequeue.", start.to(end));

}

fn dequeue() {
    fs::create_dir_all(PathBuf::from("/tmp/bigqueue")).expect("create dir error");
    let mut q = BigQueue::new(&"/tmp/bigqueue", true).unwrap();

    let start = PreciseTime::now();
    let total = 100000000;
    for _a in 0..total {
        q.push(b"1234567890abcdefghij").expect("error");
    }
    let end = PreciseTime::now();
    println!("{} seconds for enqueue. {} ps", start.to(end), total*1000000/start.to(end).num_microseconds().unwrap());

    let mut count = 0;

    let start = PreciseTime::now();
    loop {
        if q.dequeue().is_ok() {
            count = count + 1;
        } else {
            println!("count {}", count);
            break;
        }
    }
    let end = PreciseTime::now();
    println!("{} seconds for dequeue.", start.to(end));
}

fn peek() {
    fs::create_dir_all(PathBuf::from("/tmp/bigqueue")).expect("create dir error");
    let mut q = BigQueue::new(&"/tmp/bigqueue", true).unwrap();
    let data = b"1234567890abcdefghij";
    q.push(data).expect("push error");

    let mut count = 0;

    loop {
        let pk = q.peek();
        if pk.is_ok() {
            let b = pk.unwrap();
            count = count + 1;

            if count == 5 {
                println!("peek {}", count);
                break;
            }
        }
    }
}