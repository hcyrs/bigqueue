extern crate bigqueue;

use time::PreciseTime;
use std::fs;
use std::time::Duration;
use std::path::PathBuf;

fn main() {
    use std::thread;
    use bigqueue;
    fs::create_dir_all(PathBuf::from("/tmp/spsc")).expect("create dir error");
    if let Ok((mut tx, mut rx)) = bigqueue::channel("/tmp/spsc", false){
        let v = b"1234567890abcdefghij";
        let total = 5000000;
        let t = thread::spawn(move|| {
            for _i in 0..total {
                tx.enqueue(v).unwrap();
            }
        });

        let two_sec = Duration::from_secs(2);
        thread::sleep(two_sec);

        let start = PreciseTime::now();
        let mut count = 0;
        loop{
            if rx.dequeue().is_ok() {
                count = count + 1;

            }
            if count == total {
                println!("count {}", count);
                break;
            }

        }
        let end = PreciseTime::now();
        println!("{} seconds for enqueue and dequeue. {} ps", start.to(end), total*1000000/start.to(end).num_microseconds().unwrap());
        t.join().unwrap();
    }
}
