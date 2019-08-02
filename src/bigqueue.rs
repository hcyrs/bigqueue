// Copyright 2019 bigqueue.rs Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::{fmt, fs};
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::fs::ReadDir;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use lru::LruCache;
use memmap::MmapMut;

use crate::{BigQueue, read_u64, write_bytes, write_u64};
use crate::{Error, Result};
use crate::Config;

impl BigQueue {
    pub fn with_config(_dir: &str, reset: bool, conf: Config) -> Result<BigQueue> {
        match fs::metadata(_dir) {
            Ok(v) => {
                if !v.is_dir() {
                    return Err(Error::IsDir(_dir.to_string()));
                }
                if v.permissions().readonly() {
                    return Err(Error::IsDir(_dir.to_string()));
                }
            }
            Err(_) => {
                return Err(Error::Exist(_dir.to_string()));
            }
        }
        if reset {
            let read_dir = fs::read_dir(_dir)
                .expect(&format!("fail to read directory {}", _dir));
            delete_dir_contents(read_dir);
        }

        let q_index = match Index::new(_dir) {
            Ok(v) => { v }
            Err(err) => {
                return Err(err);
            }
        };
        let (h_aid, h_offset) = q_index.get_head_tuple().expect("read index error");
        let (t_aid, t_offset) = q_index.get_tail_tuple().expect("read index error");
        let num_arenas = t_aid + 1 - h_aid;


        let q_config = conf;
        let q_dir = PathBuf::from(_dir);

        let mut tail: Option<Rc<UnsafeCell<Arena>>> = None;
        let mut head: Option<Rc<UnsafeCell<Arena>>> = None;

        let t_arena = BigQueue::open_a_arena(_dir, &q_config, t_aid).expect(&format!("error to memmap data file {}", t_aid));
        let arc = Rc::new(UnsafeCell::new(t_arena));
        tail = Some(arc.clone());

        if h_aid != t_aid {
            let h_arena = BigQueue::open_a_arena(_dir, &q_config, h_aid).expect(&format!("error to memmap data file {}", h_aid));
            head = Some(Rc::new(UnsafeCell::new(h_arena)));
        } else {
            head = Some(arc);
        }

        let queue = BigQueue {
            index: q_index,
            config: q_config,
            dir: q_dir,
            head_aid: h_aid,
            head_offset: h_offset,
            tail_aid: t_aid,
            tail_offset: t_offset,
            q_head: head.expect("open file error"),
            q_tail: tail.expect("open file error"),
            cache: LruCache::new(3),
        };
        Ok(queue)
    }
    pub fn new(dir: &str, reset: bool) -> Result<BigQueue> {
        BigQueue::with_config(dir, reset, Config::new())
    }

    pub fn is_empty(&self) -> bool {
        self.head_aid == self.tail_aid && self.head_offset == self.tail_offset
    }

    pub fn peek(&mut self) -> Result<Vec<u8>> {
        if self.is_empty() {
            return Err(Error::QueueEmpty);
        }

        let old_aid: usize = self.head_aid;
        let old_offset: usize = self.head_offset;
        let head = self.q_head.clone();

        let read_length = self.read_length();
        if read_length.is_none() {
            return Err(Error::ReadLength);
        }
        let length = read_length.unwrap();
        let mut result: Vec<u8> = Vec::with_capacity(length);

        if let Some(data) = self.read_bytes(length) {
            result.extend(data);
        } else {
            return Err(Error::Read);
        }

        self.head_offset = old_offset;
        self.change_head(old_aid, head);
        return Ok(result);
    }

    pub fn pop(&mut self) -> Result<Vec<u8>> {
        if self.is_empty() {
            return Err(Error::QueueEmpty);
        }

        let read_length = self.read_length();
        if read_length.is_none() {
            return Err(Error::ReadLength);
        }
        let length = read_length.unwrap();
        let mut result: Vec<u8> = Vec::with_capacity(length);

        if let Some(data) = self.read_bytes(length) {
            result.extend(data);
            self.set_head_index(self.head_aid, self.head_offset);
        } else {
            return Err(Error::Read);
        }

        return Ok(result);
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<()> {
        let length = bytes.len();
        let n_offset = self.write_length(self.tail_offset, length as u64);
        self.write_bytes(n_offset, bytes).expect("fail to write when push");
        self.set_tail_index(self.tail_aid, self.tail_offset);
        Ok(())
    }

    pub fn dequeue(&mut self) -> Result<()> {
        if self.is_empty() {
            return Err(Error::QueueEmpty);
        }
        if let Some(length) = self.read_length() {
            let mut head_aid = self.head_aid;
            let mut head_offset = self.head_offset;

            if (head_offset + length) >= self.config.arena_size {
                head_aid = head_aid + 1;
                self.flip_head_page_to(head_aid).expect("fail to flip next page");
                head_offset = (head_offset + length - self.config.arena_size) % self.config.arena_size;
            } else {
                head_offset = (head_offset + length) % self.config.arena_size;
            }
            self.set_head_index(head_aid, head_offset);
        } else {
            return Err(Error::ReadLength);
        }
        Ok(())
    }

    pub fn shrink(&mut self) {
        let read_dir = fs::read_dir(&self.dir);
        if read_dir.is_err() {
            return;
        }
        let read_dir_res = read_dir.unwrap();
        for entry in read_dir_res {
            if let Ok(entry) = entry {
                let path = entry.path();
                let ext = path.clone().into_os_string().into_string().unwrap();
                if ext.ends_with(".dat") {
                    let part: Vec<&str> = ext.split("_").collect();
                    if part.len() != 2 {
                        continue;
                    }
                    let part: Vec<&str> = part[1].split(".").collect();
                    let index_usize = part[0].parse::<usize>().unwrap();
                    if (
                        self.tail_aid >= self.head_aid && index_usize < self.head_aid)
                        ||
                        (
                            self.tail_aid < self.head_aid
                                && (index_usize > self.tail_aid && index_usize < self.head_aid
                            )
                        ) {
                        if let Ok(_) = fs::remove_file(path) {}
                    }
                }
            };
        }
    }
}

impl Drop for BigQueue {
    fn drop(&mut self) {
        self.cache.clear();
        self.shrink();
    }
}

impl BigQueue {
    fn open_a_arena(_dir: &str, config: &Config, aid: usize) -> Result<Arena> {
        let dir = PathBuf::from(_dir);
        let data_path = dir.join(format!("arena_{}.dat", aid));
        return Arena::new(data_path, config.arena_size);
    }

    fn set_head_index(&mut self, aid: usize, offset: usize) {
        self.index.set_head(aid, offset);
        self.head_aid = aid;
        self.head_offset = offset;
    }

    fn set_tail_index(&mut self, aid: usize, offset: usize) {
        self.index.set_tail(aid, offset);
        self.tail_aid = aid;
        self.tail_offset = offset;
    }

    fn get_head_map(&mut self) -> &mut memmap::MmapMut {
        let m = unsafe { (&mut (*self.q_head.get()).mmap) };
        return m;
    }

    fn get_tail(&mut self) -> &mut Arena {
        let m = unsafe { (&mut (*self.q_tail.get())) };
        return m;
    }

    #[inline]
    fn set_tail(&mut self, id: usize, a: Arena) {
        self.q_tail = Rc::new(UnsafeCell::new(a));
        self.tail_aid = id;
    }

    #[inline]
    fn set_head(&mut self, id: usize, a: Arena) {
        self.q_head = Rc::new(UnsafeCell::new(a));
        self.head_aid = id;
    }

    #[inline]
    fn change_head(&mut self, id: usize, a: Rc<UnsafeCell<Arena>>) {
        self.head_aid = id;
        self.q_head = a;
    }

    #[inline]
    fn flip_head_page_to(&mut self, aid: usize) -> Result<()> {
        if let Some(a) = self.cache.pop(&aid) {
            if let Ok(aa) = Rc::try_unwrap(a) {
                self.set_head(aid, aa.into_inner());
                return Ok(());
            }
        }
        self.set_head(aid, self.load_arena(aid)?);
        Ok(())
    }

    #[inline]
    fn flip_tail_page_forward(&mut self) {
        let old_aid = self.tail_aid;
        {
            let z = self.q_tail.clone();
            self.cache.put(old_aid, z);
        }

        let aid = 1 + old_aid;
        self.set_tail(aid, Arena {
            mmap: self.open_arena(aid).expect("load arena error").mmap,
        });
    }

    #[inline]
    fn open_arena(&self, aid: usize) -> Result<Arena> {
        let data_path = self.dir.join(format!("arena_{}.dat", aid));
        return Arena::new(data_path, self.config.arena_size);
    }

    #[inline]
    fn load_arena(&self, aid: usize) -> Result<Arena> {
        let data_path = self.dir.join(format!("arena_{}.dat", aid));
        if !data_path.exists() {
            return Err(Error::Exist(data_path.to_string_lossy().to_string()));
        }
        return Arena::new(data_path, self.config.arena_size);
    }

    fn delete_arena_backed_file(&self, aid: usize) -> bool {
        let file_path = self.dir.join(format!("arena_{}.dat", aid));
        if let Err(_) = fs::remove_file(file_path) {
            return false;
        }
        true
    }

    #[inline]
    fn write_length(&mut self, offset: usize, length: u64) -> usize {
        let mut i_offset = offset;
        if i_offset + 8 > self.config.arena_size {
            self.flip_tail_page_forward();
            i_offset = 0;
        }
        self.get_tail().write_u64_at(i_offset, length);
        i_offset = i_offset + 8;
        if i_offset == self.config.arena_size {
            self.flip_tail_page_forward();
            i_offset = 0;
        }
        i_offset
    }

    #[inline]
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        let length = bytes.len();
        let mut i_offset = offset;
        let mut i_length = bytes.len();
        let mut count = 0;
        loop {
            if i_offset + i_length > self.config.arena_size {
                let range = Range { start: count, end: count + self.config.arena_size - i_offset };
                let write_in = bytes.get(range).unwrap();
                self.get_tail().write_bytes_at(i_offset, write_in)?;
                count = count + write_in.len();
                i_length = length - count;
                self.flip_tail_page_forward();
                i_offset = 0;
            } else {
                let range = Range { start: count, end: length };
                let write_in = bytes.get(range).unwrap();
                self.get_tail().write_bytes_at(i_offset, write_in)?;
                if i_offset + i_length == self.config.arena_size {
                    self.flip_tail_page_forward();
                    self.tail_offset = 0;
                } else {
                    self.tail_offset = i_offset + i_length;
                }
                return Ok(());
            }
        }
    }

    #[inline]
    fn read_length(&mut self) -> Option<(usize)> {
        let mut offset = self.head_offset;
        let mut next_offset = offset + 8;
        if next_offset > self.config.arena_size {
            self.flip_head_page_to(self.head_aid + 1);
            offset = 0;
            next_offset = 8;
        }
        if let Some(length) = read_u64(self.get_head_map(), offset) {
            if next_offset == self.config.arena_size {
                self.flip_head_page_to(self.head_aid + 1);
                self.head_offset = 0;
            } else {
                self.head_offset = next_offset;
            }
            Some(length as usize)
        } else {
            None
        }
    }

    #[inline]
    fn read_bytes(&mut self, length: usize) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::with_capacity(length);
        let mut i_offset = self.head_offset;
        let mut i_length = length;

        loop {
            if i_offset + i_length > self.config.arena_size {
                let range = Range { start: i_offset, end: self.config.arena_size };
                if let Some(slice) = self.get_head_map().get(range) {
                    result.extend_from_slice(slice);
                    i_length = i_length - slice.len();
                    i_offset = 0;
                    self.flip_head_page_to(self.head_aid + 1).expect("fail to flip page when read");
                } else {
                    return None;
                }
            } else {
                let range = Range { start: i_offset, end: i_length + i_offset };
                if let Some(slice) = self.get_head_map().get(range) {
                    result.extend_from_slice(slice);
                    if i_offset + i_length == self.config.arena_size {
                        self.flip_head_page_to(self.head_aid + 1).expect("fail to flip page when read");
                        self.head_offset = 0;
                    } else {
                        self.head_offset = i_offset + i_length;
                    }
                    return Some(result);
                } else {
                    return None;
                }
            }
        }
    }
}

fn delete_dir_contents(read_dir_res: ReadDir) {
    for entry in read_dir_res {
        if let Ok(entry) = entry {
            let path = entry.path();
            let ext = path.clone().into_os_string().into_string().unwrap();
            if ext.ends_with(".dat") {
                fs::remove_file(path).expect("Failed to remove a file");
            }
        };
    }
}


const INDEX_FILE: &str = "index.dat";
const INDEX_FILE_SIZE: usize = 4 * 8;

pub struct Index {
    file: String,
    arena: Arena,
}

impl Index {
    fn new(dir: &str) -> Result<Index> {
        let base = Path::new(&dir);
        let index_path = base.join(INDEX_FILE);

        Ok(Index {
            file: index_path.as_path().to_string_lossy().into_owned(),
            arena: Arena::new(index_path, INDEX_FILE_SIZE)?,
        })
    }

    pub fn get_head_tuple(&self) -> Option<(usize, usize)> {
        Some((
            self.arena.read_u64_at_windows(0)? as usize,
            self.arena.read_u64_at_windows(1)? as usize,
        ))
    }

    fn set_head(&mut self, aid: usize, offset: usize) -> Result<()> {
        self.arena.write_u64_at_windows(0, aid as u64)?;
        self.arena.write_u64_at_windows(1, offset as u64)?;
        Ok(())
    }

    pub fn get_tail_tuple(&self) -> Option<(usize, usize)> {
        Some((
            self.arena.read_u64_at_windows(2)? as usize,
            self.arena.read_u64_at_windows(3)? as usize,
        ))
    }

    fn set_tail(&mut self, aid: usize, offset: usize) -> Result<()> {
        self.arena.write_u64_at_windows(2, aid as u64)?;
        self.arena.write_u64_at_windows(3, offset as u64)?;
        Ok(())
    }
}

pub struct Arena {
    mmap: MmapMut,
}

impl Arena {
    pub fn new(path: PathBuf, size: usize) -> Result<Arena> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path) {
            Err(e) => {
                return Err(Error::Io(e));
            }
            Ok(file) => {
                if file.set_len(size as u64).is_err() {
                    return Err(Error::OpenFileWithLength(path.to_string_lossy().to_string(), size));
                }
                Ok(Arena {
                    mmap: unsafe { MmapMut::map_mut(&file).expect("mmap index.dat error") }
                })
            }
        }
    }

    pub fn read_u64_at(&self, offsize: usize) -> Option<u64> {
        read_u64(&self.mmap, offsize)
    }

    pub fn read_u64_at_windows(&self, win: usize) -> Option<u64> {
        self.read_u64_at(win * 8)
    }

    pub fn write_u64_at(&mut self, offsize: usize, v: u64) -> Result<()> {
        write_u64(&mut self.mmap, offsize, v)
    }

    pub fn write_bytes_at(&mut self, offsize: usize, bytes: &[u8]) -> Result<()> {
        write_bytes(&mut self.mmap, offsize, bytes)
    }

    pub fn write_u64_at_windows(&mut self, win: usize, v: u64) -> Result<()> {
        self.write_u64_at(win * 8, v)
    }

    pub fn flush(&mut self) -> Result<()> {
        match self.mmap.flush() {
            Ok(v) => v,
            Err(_) => {
                ()
            }
        }
        Ok(())
    }
}

impl fmt::Display for Arena {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self.mmap[..])
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[test]
    fn test_index() {
        use crate::bigqueue::Index;
        use std::fs;

        fs::create_dir_all(PathBuf::from("/tmp/oo0o0o")).expect("failed to create dir");

        let mut qi = Index::new("/tmp/oo0o0o").expect("failed to open the 1");
        qi.set_head(1, 3);
        qi.set_tail(1, 4);

        let (head_aid, head_offset) = qi.get_head_tuple().expect("failed to open the 1");
        let (tail_aid, tail_offset) = qi.get_tail_tuple().expect("failed to open the 1");

        assert_eq!(head_aid, 1);
        assert_eq!(tail_aid, 1);
        assert_eq!(head_offset, 3);
        assert_eq!(tail_offset, 4);
    }

    #[test]
    fn test_arena() {
        use crate::bigqueue::Arena;
        use std::path::PathBuf;
        let mut t = match Arena::new(PathBuf::from("/tmp/11.dat"), 5 * 8) {
            Ok(t) => t,
            _ => {
                panic!("")
            }
        };
        t.write_u64_at(0, 100u64);
        t.write_u64_at(8, 10u64);
        t.flush().unwrap();
//        println!("{:?}",t.read_u64_at_windows(0));
//        println!("{}",t.read_u64_at(0));
        println!("{}", t);
//        assert_eq!(content, &mmap[..])
    }
}
