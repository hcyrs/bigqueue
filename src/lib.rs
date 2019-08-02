// Copyright 2019 bigqueue.rs Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

/*!
* A big, fast and persistent queue based on memory mapped file.
*
* ```rust
* use bigqueue::BigQueue;
*
* let mut q = BigQueue::new(&"/tmp/bigqueue", true).unwrap();
*
* let total = 10000;
* let data = b"1234567890abcdefghij";
*
* for _a in 0..total {
*     q.push(data).unwrap();
* }
*
* let mut count = 0;
* loop {
*     let pop_data = q.pop();
*     if  pop_data.is_ok() && pop_data.unwrap().len() == data.len() {
*         count = count + 1;
*     } else {
*         println!("count {}", count);
*         break;
*     }
* }
*
* ```
*
*
* ```rust
* use std::{fs, thread};
* use std::time::Duration;
*
* fs::create_dir_all(PathBuf::from("/tmp/spsc")).expect("create dir error");
* if let Ok((mut tx, mut rx)) = bigqueue::channel("/tmp/spsc", true){
*     let v = b"1234567890abcdefghij";
*     let total = 100000000;
*     let t = thread::spawn(move|| {
*         for _i in 0..total {
*             tx.enqueue(v).unwrap();
*         }
*     });
*
*     let two_sec = Duration::from_secs(2);
*     thread::sleep(two_sec);
*
*     let start = PreciseTime::now();
*     let mut count = 0;
*     loop{
*         if rx.dequeue().is_ok() {
*             count = count + 1;
*
*         }
*         if count == total {
*             println!("count {}", count);
*             break;
*         }
*
*     }
*     let end = PreciseTime::now();
*     println!("{} seconds for enqueue and dequeue. {} ps", start.to(end), total*1000000/start.to(end).num_microseconds().unwrap());
*     t.join().unwrap();
* }
* ```
*/

extern crate failure;
#[macro_use]
extern crate failure_derive;

use std::cell::UnsafeCell;
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;

use lru::LruCache;
use memmap::MmapMut;

use crate::bigqueue::Index;

type Result<T> = std::result::Result<T, Error>;

mod bigqueue;

pub struct BigQueue {
    index: Index,
    config: Config,
    dir: PathBuf,

    head_aid: usize,
    head_offset: usize,
    tail_aid: usize,
    tail_offset: usize,
    q_head: Rc<UnsafeCell<bigqueue::Arena>>,
    q_tail: Rc<UnsafeCell<bigqueue::Arena>>,
    cache: LruCache<usize, Rc<UnsafeCell<bigqueue::Arena>>>,
}

pub fn channel(dir: &str, reset: bool) -> Result<(Sender, Receiver)> {
    let a = Rc::new(UnsafeCell::new(BigQueue::new(dir, reset)?));
    Ok((Sender::new(a.clone()), Receiver::new(a)))
}


pub struct Sender {
    inner: Rc<UnsafeCell<BigQueue>>,
}

unsafe impl Send for Sender {}

pub struct Receiver {
    inner: Rc<UnsafeCell<BigQueue>>,
}

unsafe impl Send for Receiver {}

impl Sender {
    fn new(inner: Rc<UnsafeCell<BigQueue>>) -> Sender {
        Sender { inner: inner }
    }

    pub fn enqueue(&mut self, elem: &[u8]) -> Result<()> {
        unsafe { *self.inner.get().push(elem) }
    }
}

impl Receiver {
    fn new(inner: Rc<UnsafeCell<BigQueue>>) -> Receiver {
        Receiver { inner }
    }

    pub fn dequeue(&mut self) -> Result<()> {
        unsafe { *self.inner.get().dequeue() }
    }
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "fail to write.")]
    Write,
    #[fail(display = "{} is not directory.", _0)]
    IsDir(String),
    #[fail(display = "{} is not writable.", _0)]
    CanWrite(String),
    #[fail(display = "{} not exist.", _0)]
    Exist(String),
    #[fail(display = "queue is empty.")]
    QueueEmpty,
    #[fail(display = "fail to open {} with length {}.", _0, _1)]
    OpenFileWithLength(String, usize),
    #[fail(display = "fail to read length.")]
    ReadLength,
    #[fail(display = "fail to read.")]
    Read,
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
}


const DEFAULT_ARENA_SIZE: usize = 128 * 1024 * 1024;
const MIN_ARENAS_MAX_IN_MEM: u8 = 3;

pub struct Config {
    pub arena_size: usize,
    pub max_arenas_in_mem: u8,
}

impl Config {
    pub fn new() -> Config {
        Config {
            arena_size: DEFAULT_ARENA_SIZE,
            max_arenas_in_mem: MIN_ARENAS_MAX_IN_MEM,
        }
    }
}

#[inline]
fn write_u64(mmap: &mut MmapMut, offset: usize, v: u64) -> Result<()> {
    let r: Range<usize> = offset..offset + 8;
    if let Some(area) = mmap.get_mut(r) {
        area.copy_from_slice(&transform_u64_to_array_of_u8(v));
        return Ok(());
    }
    Err(Error::Write)
}

#[inline]
fn write_bytes(mmap: &mut MmapMut, offset: usize, v: &[u8]) -> Result<()> {
    let bytes_length = v.len();
    let r: Range<usize> = offset..offset + bytes_length;
    if let Some(area) = mmap.get_mut(r) {
        area.copy_from_slice(v);
        return Ok(());
    }
    Err(Error::Write)
}

#[inline]
fn read_u64(mmap: &MmapMut, offset: usize) -> Option<u64> {
    let r = Range { start: offset, end: offset + 8 };
    if let Some(slice) = mmap.get(r) {
        return Some(transform_array_of_u8_to_u64(slice));
    }
    None
}

#[inline]
fn read_bytes(mmap: &MmapMut, offset: usize, length: u64) -> Option<Vec<u8>> {
    let r = Range { start: offset, end: offset + (length as usize) };
    if let Some(slice) = mmap.get(r) {
        return Some(slice.to_vec());
    }
    None
}

#[inline]
fn transform_u64_to_array_of_u8(x: u64) -> [u8; 8] {
    use byteorder::{ByteOrder, LittleEndian};
    let mut bytes: [u8; 8] = [0; 8];
    LittleEndian::write_u64(&mut bytes, x);
    bytes
}

#[inline]
fn transform_array_of_u8_to_u64(x: &[u8]) -> u64 {
    use byteorder::{ByteOrder, LittleEndian};
    LittleEndian::read_u64(x)
}