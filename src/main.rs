extern crate rand;
extern crate memmap;
extern crate time;
extern crate snappy_framed;

use std::io;
use std::io::{Write, BufWriter, BufRead, BufReader, Cursor, Bytes, Read};
use std::mem::{size_of, transmute};
use std::path::{Path, PathBuf};
use std::fs;
use std::fs::{File};
use rand::Rng;
use memmap::{Mmap, Protection};
use snappy_framed::write::SnappyFramedEncoder;
use snappy_framed::read::{SnappyFramedDecoder, CrcMode};
use std::marker::PhantomData;

// -----------------------------------------------------------------------------------------------
trait RandomGenerator<T> {
    fn generate_next(&mut self) -> T;
}

trait NewRandomGenerator<T> {
    fn new_random_generator() -> Box<RandomGenerator<T>>;
}

struct ThreadRngRandomGenerator {
    rng: rand::ThreadRng
}

impl ThreadRngRandomGenerator {
    fn new() -> ThreadRngRandomGenerator {
        ThreadRngRandomGenerator {
            rng: rand::thread_rng()
        }
    }
}

impl RandomGenerator<i32> for ThreadRngRandomGenerator {
    fn generate_next(&mut self) -> i32 {
        (self.rng.next_u32() as i32) % 1_000
    }
}

impl RandomGenerator<i64> for ThreadRngRandomGenerator {
    fn generate_next(&mut self) -> i64 {
        (self.rng.next_u64() as i64) % 1_000_000
    }
}

impl NewRandomGenerator<i32> for i32 {
    fn new_random_generator() -> Box<RandomGenerator<i32>> {
        Box::new(ThreadRngRandomGenerator::new())
    }
}

impl NewRandomGenerator<i64> for i64 {
    fn new_random_generator() -> Box<RandomGenerator<i64>> {
        Box::new(ThreadRngRandomGenerator::new())
    }
}

/*
struct RawValuesIterator<'a, R, T>
    where R: Read + 'a
{
    reader: BufReader<R>,
    _phantom: PhantomData<T>
}

impl<'a, R, T> RawValuesIterator<'a, R, T>
    where R: Read + 'a
{
    fn new(reader: &'a R) -> RawValuesIterator<'a, R, T> {
        RawValuesIterator {
            reader: BufReader::new(reader),
            _phantom: PhantomData
        }
    }
}
*/

// -----------------------------------------------------------------------------------------------
/*
struct CompressedValuesIterator<'a, T> {
    reader: SnappyFramedDecoder<Cursor<&'a [u8]>>,
    remaining_elements: usize,
    _phantom: PhantomData<T>
}

impl<'a, T> CompressedValuesIterator<'a, T> {
    fn new(compressed_data: &'a [u8]) -> CompressedValuesIterator<T> {
        CompressedValuesIterator {
            reader: SnappyFramedDecoder::new(Cursor::new(compressed_data), CrcMode::Ignore),
            remaining_elements: 0,
            _phantom: PhantomData
        }
    }
}

impl<'a, T> Iterator for CompressedValuesIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
    }
}
*/

// -----------------------------------------------------------------------------------------------
struct Column<T> {
    raw_mmap: Mmap,
    compressed_mmap: Mmap,
    _phantom: PhantomData<T>,
}

impl<T> Column<T> {
    fn new<P: AsRef<Path>>(raw_file: P, compressed_file: P) -> Column<T> {
        Column {
            raw_mmap: Mmap::open_path(raw_file, Protection::Read).expect("Could not map raw file"),
            compressed_mmap: Mmap::open_path(compressed_file, Protection::Read).expect("Could not map compressed file"),
            _phantom: PhantomData
        }
    }

    fn len(&self) -> usize {
        self.raw_mmap.len() / std::mem::size_of::<T>()
    }

    fn raw_values(&self) -> &[T] {
        unsafe {
            let ptr = self.raw_mmap.ptr();
            std::slice::from_raw_parts(ptr as *const T, self.len())
        }
    }

    /*fn compressed_values_iterator(&self) {
        let mut cursor = unsafe { Cursor::new(self.compressed_mmap.as_slice()) };
        let mut decoder = SnappyFramedDecoder::new(&mut cursor, CrcMode::Ignore);
    }*/
}

// -----------------------------------------------------------------------------------------------
struct ColumnGenerator<T> {
    name: String,
    dir: PathBuf,

    _phantom: PhantomData<T>
}

impl<T> ColumnGenerator<T> 
    where T: Nullable + NewRandomGenerator<T>
{
    fn new<P: AsRef<Path>>(name: &str, dir: P) -> ColumnGenerator<T> {
        ColumnGenerator {
            name: String::from(name),
            dir: dir.as_ref().to_owned(),
            _phantom: PhantomData
        }
    }

    fn filename(&self) -> PathBuf {
        let mut result = self.dir.clone();
        result.push(&self.name);
        result.set_extension("bin");
        result
    }

    fn compressed_filename(&self) -> PathBuf {
        let mut result = self.filename();
        result.set_extension("bin.snappy");
        result
    }

    fn generate_random_column(&self, n: usize, null_probability: f32) -> io::Result<Column<T>> {
        let filename = self.filename();
        println!("Generating {} random values into {}...", n, filename.display());

        {
            let mut writer = BufWriter::new(try!(File::create(filename)));
            let mut rng = rand::thread_rng();
            let mut generator: Box<RandomGenerator<T>> = T::new_random_generator();

            for _ in 0..n {
                let val: T = if rng.next_f32() < null_probability {
                    T::null_value()
                } else {
                    generator.generate_next()
                };

                writer.write(raw_bytes(&val)).expect("Could not write random values to file");
            }
        }
        
        try!(self.compress_values());

        Ok(Column::new(self.filename(), self.compressed_filename()))
    }

    fn compress_values(&self) -> io::Result<()> {
        let src_filename = self.filename();
        let dest_filename = self.compressed_filename();

        let mut src_reader = try!(File::open(&src_filename));
        let mut dest_writer = try!(SnappyFramedEncoder::new(try!(File::create(&dest_filename))));

        println!("Compresing values into {}...", dest_filename.display());
        try!(io::copy(&mut src_reader, &mut dest_writer));
        Ok(())
    }
}

// -----------------------------------------------------------------------------------------------
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

// -----------------------------------------------------------------------------------------------
fn raw_bytes<T: Sized>(v: &T) -> &[u8] {
    let ptr = v as *const T;
    unsafe { std::slice::from_raw_parts(ptr as *const u8, size_of::<T>()) }
}

// -----------------------------------------------------------------------------------------------
trait Nullable {
    fn null_value() -> Self;
}

impl Nullable for i32 {
    fn null_value() -> i32 { std::i32::MIN }
}

impl Nullable for i64 {
    fn null_value() -> i64 { std::i64::MIN }
}

// -----------------------------------------------------------------------------------------------
struct Table {
    int32_column: Column<i32>,
    int64_column: Column<i64>
}

impl Table {
    fn len(&self) -> usize {
        self.int32_column.len()
    }

    fn query1(&self) {
        // SELECT COUNT(*) WHERE int32_col IS NOT NULL AND int64_col IS NOT NULL
        let n = self.len();
        let mut cnt: i64 = 0;

        let int32_values: &[i32] = self.int32_column.raw_values();
        let int64_values: &[i64] = self.int64_column.raw_values();

        for i in 0..n {
            let int32 = int32_values[i];
            let int64 = int64_values[i];

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
        // SELECT COUNT(*) WHERE int32_col IS NOT NULL AND int64_col > 100
        let cnt = self.int32_column.raw_values().iter().zip(self.int64_column.raw_values().iter())
            .filter(|&v| *v.0 != i32::null_value() && *v.1 != i64::null_value())
            .count();

        println!("Result: {}", cnt);
    }

    fn test_compress(&self) {
        // let mut cursor = unsafe { Cursor::new(self.int32_column.compressed_mmap.as_slice()) };
        let mut cursor = unsafe { Cursor::new(self.int32_column.compressed_mmap.as_slice()) };
        let mut decoder = SnappyFramedDecoder::new(&mut cursor, CrcMode::Ignore);
        let mut reader = BufReader::new(decoder);
        let buffer: &[u8] = reader.fill_buf().unwrap();

        println!("Read {} bytes", buffer.len());

        let casted_buffer: &[i32] = unsafe {
            let bytes = buffer.as_ptr();
            let size = buffer.len() / size_of::<i32>();
            std::slice::from_raw_parts(bytes as *const i32, size)
        };

        println!("Got a total of {} int32s", casted_buffer.len());
    }
}

// -----------------------------------------------------------------------------------------------

fn main() {
    let dest_dir = "/tmp/rust-query-table";
    let n = 1_000_000;

    let int32_column = ColumnGenerator::<i32>::new("int32_column", dest_dir).generate_random_column(n, 0.95).unwrap();
    let int64_column = ColumnGenerator::<i64>::new("int64_column", dest_dir).generate_random_column(n, 0.95).unwrap();

    let table = Table {
        int32_column: int32_column,
        int64_column: int64_column
    };

    println!("Warmup...");
    table.query1();
    table.query2();

    benchmark("Query 1: Raw access", || table.query1());
    benchmark("Query 2: Raw access w/iterators", || table.query2());

    table.test_compress();
}
