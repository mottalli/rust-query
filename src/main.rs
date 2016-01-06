extern crate rand;
extern crate memmap;
extern crate time;

use std::io;
use std::io::{Write, BufWriter};
use std::mem::size_of;
use std::path::{Path};
use std::fs;
use std::fs::{File};
use rand::Rng;
use memmap::{Mmap, Protection};

fn benchmark<F, R>(description: &str, mut f: F) -> R
    where F: FnMut() -> R 
{
    println!("{}...", description);
    let tic = time::now();
    let res = f();
    let toc = time::now();
    
    let delta = toc - tic;
    println!("Elapsed: {} ms.", delta.num_milliseconds());
    res
}

fn raw_bytes<T: Sized>(v: &T) -> &[u8] {
    let ptr = v as *const T;
    unsafe { std::slice::from_raw_parts(ptr as *const u8, size_of::<T>()) }
}

trait Nullable {
    fn null_value() -> Self;
}

impl Nullable for i32 {
    fn null_value() -> i32 { std::i32::MIN }
}

impl Nullable for i64 {
    fn null_value() -> i64 { std::i64::MIN }
}

trait ValueGenerator<T>
    where T: Sized + Nullable
{
    fn generate(&self, n: usize, rng: &mut Rng, writer: &mut Write) {
        let null_probability = self.null_probability();

        for _ in 0..n {
            let val: T = if rng.next_f32() < null_probability {
                T::null_value()
            } else {
                self.generate_next(rng)
            };

            writer.write(raw_bytes(&val)).expect("Could not write random values!");
        }
    }

    fn generate_next(&self, rng: &mut Rng) -> T;
    fn null_probability(&self) -> f32 { 0.9f32 }
}

struct Int32Generator;

impl Int32Generator {
    fn new() -> Int32Generator { Int32Generator }
}

impl ValueGenerator<i32> for Int32Generator {
    fn generate_next(&self, rng: &mut Rng) -> i32 {
        (rng.next_u32() as i32) % 1000
    }
}

struct Int64Generator;

impl Int64Generator {
    fn new() -> Int64Generator { Int64Generator }
}

impl ValueGenerator<i64> for Int64Generator {
    fn generate_next(&self, rng: &mut Rng) -> i64 {
        (rng.next_u64() as i64) % 10000
    }
}

fn generate_random_values_into<T: Nullable, P: AsRef<Path>>(n: usize, path: P, generator: &mut ValueGenerator<T>) -> io::Result<()> {
    let path_buf = path.as_ref().to_path_buf();

    {
        let basedir = path_buf.parent().unwrap();

        if let Err(_) = basedir.metadata() {
            try!(fs::create_dir_all(basedir));
        }
    }

    {
        if let Ok(_) = path_buf.metadata() {
            println!("File {} already exists. Skipping.", path_buf.display());
            return Ok(());
        }
    }

    println!("Creating file {}...", path_buf.display());
    let mut handler = BufWriter::new(try!(File::create(path_buf)));

    let mut rng = rand::thread_rng();
    generator.generate(n, &mut rng, &mut handler);

    Ok(())
}

fn convert_map_to_slice<'a, T: Sized>(map: &'a Mmap) -> &'a [T] {
    unsafe {
        let ptr = map.ptr();
        let size = map.len() / std::mem::size_of::<T>();
        std::slice::from_raw_parts(ptr as *const T, size)
    }
}

struct Table<'a> {
    int32col: &'a [i32],
    int64col: &'a [i64]
}

impl<'a> Table<'a> {
    fn query1(&self) {
        // SELECT COUNT(*) WHERE int32col IS NOT NULL AND int64col IS NOT NULL
        let n = self.int32col.len();

        let mut cnt: i64 = 0;

        for i in 0..n {
            let int32 = self.int32col[i];
            let int64 = self.int64col[i];

            if int32 == i32::null_value() {
                continue;
            } else if int64 == i64::null_value() {
                continue;
            }

            cnt += 1;
        }

        println!("Result: {}", cnt);
    }

    fn query2(&self) {
        // SELECT COUNT(*) WHERE int32col IS NOT NULL AND int64col > 100
        let cnt = self.int32col.iter().zip(self.int64col.iter())
            .filter(|&v| *v.0 != i32::null_value() && *v.1 > 100)
            .count();

        println!("Result: {}", cnt);
    }
}

fn main() {
    let n: usize = 150_000_000;
    let int32filename = "/tmp/rust-query-table/int32.bin";
    let int64filename = "/tmp/rust-query-table/int64.bin";

    generate_random_values_into(n, int32filename, &mut Int32Generator::new()).expect("Could not generate i32 file");
    generate_random_values_into(n, int64filename, &mut Int64Generator::new()).expect("Could not generate i64 file");

    let mmap_int32 = Mmap::open_path(int32filename, Protection::Read).expect("Could not map i32 file");
    let mmap_int64 = Mmap::open_path(int64filename, Protection::Read).expect("Could not map i64 file");


    let int32col: &[i32] = convert_map_to_slice(&mmap_int32);
    let int64col: &[i64] = convert_map_to_slice(&mmap_int64);

    benchmark("Warmup", || {
        let v = int32col.iter().filter(|&x| *x != i32::null_value()).count();
        println!("Value 1: {}", v);
        let v = int64col.iter().filter(|&x| *x != i64::null_value()).count();
        println!("Value 2: {}", v);
    });

    let table = Table {
        int32col: int32col,
        int64col: int64col
    };

    benchmark("Query 1", || table.query1());
    benchmark("Query 2", || table.query2());
}

/*#[macro_use] extern crate log;
extern crate env_logger;
extern crate rand;
extern crate memmap;
extern crate time;

use std::path::Path;
use std::fs;
use std::io;
use std::io::{Write, BufWriter};
use std::marker;
use rand::Rng;

fn benchmark<F, R>(mut f: F) -> R
    where F: FnMut() -> R 
{
    
    let tic = time::now();
    let res = f();
    let toc = time::now();
    
    let delta = toc - tic;
    println!("Elapsed: {} ms.", delta.num_milliseconds());
    res
}

trait Nullable {
    fn null_value() -> Self;
}

impl Nullable for i32 {
    fn null_value() -> i32 { std::i32::MIN }
}

impl Nullable for i64 {
    fn null_value() -> i64 { std::i64::MIN }
}

trait ShortRandGenerable {
    fn gen_short_random(rng: &mut rand::Rng) -> Self;
}

impl ShortRandGenerable for i32 {
    fn gen_short_random(rng: &mut rand::Rng) -> i32 { (rng.next_u32() as i32) % 100 }
}

impl ShortRandGenerable for i64 {
    fn gen_short_random(rng: &mut rand::Rng) -> i64 { (rng.next_u64() as i64) % 10000 }
}

struct ColumnGenerator<T>
{
    writer: Box<io::Write>,
    //writer: BufWriter<fs::File>,
    rng: rand::ThreadRng,
    null_probabilty: f32,
 
    _phantom: marker::PhantomData<T>,
}

fn raw_bytes<T: Sized>(v: &T) -> &[u8] {
    let ptr = v as *const T;
    unsafe { std::slice::from_raw_parts(ptr as *const u8, std::mem::size_of::<T>()) }
}

impl<T> ColumnGenerator<T> 
    where T: rand::Rand + Nullable + Copy + ShortRandGenerable
{
    fn new<P: AsRef<Path>>(dest_file: P, null_probabilty: f32) -> ColumnGenerator<T> {
        ColumnGenerator {
            writer: Box::new(BufWriter::new(fs::File::create(dest_file).expect("Could not create file"))),
            rng: rand::thread_rng(),
            null_probabilty: null_probabilty,
            _phantom: marker::PhantomData
        }
    }

    fn gen_next(&mut self) {
        let val = 
            if self.rng.gen::<f32>() > self.null_probabilty {
                T::gen_short_random(&mut self.rng)
            } else {
                T::null_value()
            };

        let raw: &[u8] = raw_bytes(&val);
        self.writer.write(raw).expect("Could not write bytes");
    }
}

fn create_table_files(dest_dir: &Path) -> io::Result<()> {
    match dest_dir.metadata() {
        Ok(_) => info!("El directorio ya existe"),
        Err(_) => {
            info!("El directorio no existe. Creando archivos...");
            try!(fs::create_dir_all(dest_dir));

            let mut path_buf = dest_dir.to_path_buf();

            path_buf.push("int32col1.bin");
            let mut gen32 = ColumnGenerator::<i32>::new(&path_buf, 0.9);

            path_buf.pop();
            path_buf.push("int64col1.bin");
            let mut gen64 = ColumnGenerator::<i64>::new(&path_buf, 0.9);

            for _ in 0..150_000_000 {
                gen32.gen_next();
                gen64.gen_next();
            }

        }
    }
    
    Ok(())

}

fn convert_map_to_slice<'a, T: Sized>(map: &'a memmap::Mmap) -> &'a [T] {
    unsafe {
        let ptr = map.ptr();
        let size = map.len() / std::mem::size_of::<T>();
        std::slice::from_raw_parts(ptr as *const T, size)
    }
}

fn query1(int32col: &[i32], int64col: &[i64]) {
    // SELECT COUNT(*) WHERE int32col IS NOT NULL AND int64col > 100
    let n = int32col.len();

    let mut cnt: i64 = 0;

    for i in 0..n {
        let int32 = int32col[i];
        let int64 = int64col[i];

        if int32 == i32::null_value() {
            continue;
        } else if int64 <= 100 {
            continue;
        }

        cnt += 1;
    }

    println!("Result: {}", cnt);
}

fn query2(int32col: &[i32], int64col: &[i64]) {
    // SELECT COUNT(*) WHERE int32col IS NOT NULL AND int64col > 100
    
    let cnt = int32col.iter().zip(int64col.iter())
        .filter(|&v| *v.0 != i32::null_value() && *v.1 > 100)
        .count();

    println!("Result: {}", cnt);
}

fn query3(int32col: &[i32], int64col: &[i64]) {
    // SELECT int32col, SUM(int64col) WHERE int32col IS NOT NULL AND int64col > 100
    let mut buffer = int32col.iter().zip(int64col.iter())
        .filter(|&v| *v.0 != i32::null_value() && *v.1 > 100)
        .map(|v| (*v.0, *v.1))
        .collect::<Vec<(i32, i64)>>();

    buffer.sort_by(|&v1, &v2| v1.0.cmp(&v2.0));
}

fn main() {
    env_logger::init().unwrap();

    let dest_dir = Path::new("/tmp/rust-query-table");
    create_table_files(dest_dir).expect("Could not create table");

    info!("Mapeando archivos...");
    let mmap_int32col = memmap::Mmap::open_path("/tmp/rust-query-table/int32col1.bin", memmap::Protection::Read).unwrap();
    let mmap_int64col = memmap::Mmap::open_path("/tmp/rust-query-table/int64col1.bin", memmap::Protection::Read).unwrap();
    let int32col: &[i32] = convert_map_to_slice(&mmap_int32col);
    let int64col: &[i64] = convert_map_to_slice(&mmap_int64col);

    // Warm up the cache
    let s = int32col.iter().filter(|&x| *x != i32::null_value()).count();
    info!("Warmup 1: {}", s);
    let s = int64col.iter().filter(|&x| *x != i64::null_value()).count();
    info!("Warmup 2: {}", s);

    info!("Query 1");
    benchmark(|| query1(int32col, int64col));

    info!("Query 2");
    benchmark(|| query2(int32col, int64col));

    info!("Query 3");
    benchmark(|| query3(int32col, int64col));
}
*/
