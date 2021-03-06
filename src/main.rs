extern crate rand;
extern crate memmap;
extern crate time;
extern crate snappy_framed;

use std::io;
use std::io::{Write, BufWriter, BufRead, BufReader, Cursor, Read};
use std::mem;
use std::path::{Path, PathBuf};
use std::fs::{File};
use rand::Rng;
use memmap::{Mmap, Protection};
use snappy_framed::write::SnappyFramedEncoder;
use snappy_framed::read::{SnappyFramedDecoder, CrcMode};
use std::marker::PhantomData;

// -----------------------------------------------------------------------------------------------
fn cast_slice<'a, T>(s: &'a [u8]) -> &'a [T] {
    let size = s.len() / mem::size_of::<T>();
    let ptr = s.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, size) }
}

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

// -----------------------------------------------------------------------------------------------
trait BlockReader {
    fn next_block<'a>(&'a mut self) -> Option<&'a [u8]>;
}

struct BufBlockReader<R>
    where R: Read
{
    reader: BufReader<R>,
    bytes_to_consume: usize
}


impl<R> BufBlockReader<R> 
    where R: Read
{
    fn new(reader: R) -> BufBlockReader<R> {
        BufBlockReader {
            reader: BufReader::new(reader),
            bytes_to_consume: 0
        }
    }
}

impl<R> BlockReader for BufBlockReader<R>
    where R: Read
{
    fn next_block<'a>(&'a mut self) -> Option<&'a [u8]> {
        self.reader.consume(self.bytes_to_consume);
        let buffer = self.reader.fill_buf().unwrap();
        self.bytes_to_consume = buffer.len();
        match self.bytes_to_consume {
            0 => None,
            _ => Some(buffer)
        }
    }
}

// -----------------------------------------------------------------------------------------------
trait SliceGenerator<T> {
    fn next_slice<'a>(&'a mut self) -> Option<&'a [T]>;
}

impl<R, T> SliceGenerator<T> for BufBlockReader<R>
    where R: Read
{
    fn next_slice<'a>(&'a mut self) -> Option<&'a [T]> {
        match self.next_block() {
            None => None,
            Some(s) => Some(cast_slice(s))
        }
    }
}

// -----------------------------------------------------------------------------------------------
struct BlockValuesIterator<'a, R, T>
    where R: Read,
          T: 'a
{
    reader: BufBlockReader<R>,
    next_value_ptr: *const T,
    remaining_values: usize,
    _phantom: PhantomData<&'a T>
}

impl<'a, R, T> BlockValuesIterator<'a, R, T>
    where R: Read,
          T: 'a
{
    fn new(reader: R) -> BlockValuesIterator<'a, R, T> {
        BlockValuesIterator {
            reader: BufBlockReader::new(reader),
            next_value_ptr: std::ptr::null(),
            remaining_values: 0,
            _phantom: PhantomData
        }
    }
}

impl<'a, R, T> Iterator for BlockValuesIterator<'a, R, T>
    where R: Read,
          T: 'a
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_values == 0 {
            match self.reader.next_slice() {
                None => return None,
                Some(s) => {
                    self.next_value_ptr = s.as_ptr();
                    self.remaining_values = s.len();
                }
            }
        } else {
            self.next_value_ptr = unsafe { self.next_value_ptr.offset(1) };
            self.remaining_values -= 1;
        }

        unsafe { Some(&*self.next_value_ptr) }
    }
}

// -----------------------------------------------------------------------------------------------
struct Column<T> {
    raw_mmap: Mmap,
    compressed_mmap: Mmap,
    _phantom: PhantomData<T>,
}

impl<T> Column<T>
    where T: Copy
{
    fn new<P: AsRef<Path>>(raw_file: P, compressed_file: P) -> Column<T> {
        Column {
            raw_mmap: Mmap::open_path(raw_file, Protection::Read).expect("Could not map raw file"),
            compressed_mmap: Mmap::open_path(compressed_file, Protection::Read).expect("Could not map compressed file"),
            _phantom: PhantomData
        }
    }

    fn len(&self) -> usize {
        self.raw_mmap.len() / mem::size_of::<T>()
    }

    fn raw_values(&self) -> &[T] {
        unsafe {
            cast_slice(self.raw_mmap.as_slice())
        }
    }


    fn raw_values_block_iterator<'a>(&'a self) -> Box<Iterator<Item=&T> + 'a> {
        let cursor = unsafe { Cursor::new(self.raw_mmap.as_slice()) };
        let it = BlockValuesIterator::new(cursor);
        Box::new(it)
    }

    fn compressed_values_block_iterator<'a>(&'a self) -> Box<Iterator<Item=&T> + 'a> {
        let cursor = unsafe { Cursor::new(self.compressed_mmap.as_slice()) };
        let decoder = SnappyFramedDecoder::new(cursor, CrcMode::Ignore);
        let it = BlockValuesIterator::new(decoder);
        Box::new(it)
    }
}

// -----------------------------------------------------------------------------------------------
struct ColumnGenerator<T> {
    name: String,
    dir: PathBuf,

    _phantom: PhantomData<T>
}

impl<T> ColumnGenerator<T> 
    where T: Nullable + NewRandomGenerator<T> + Copy
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

        match filename.metadata() {
            Ok(_) => println!("File {} already exists. Skipping.", filename.display()),
            Err(_) => {
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
            }
        }

        try!(self.compress_values());

        Ok(Column::new(self.filename(), self.compressed_filename()))
    }

    fn compress_values(&self) -> io::Result<()> {
        let src_filename = self.filename();
        let dest_filename = self.compressed_filename();

        match dest_filename.metadata() {
            Ok(_) => println!("File {} already exists. Skipping.", dest_filename.display()),
            Err(_) => {
                let mut src_reader = try!(File::open(&src_filename));
                let mut dest_writer = try!(SnappyFramedEncoder::new(try!(File::create(&dest_filename))));

                println!("Compresing values into {}...", dest_filename.display());
                try!(io::copy(&mut src_reader, &mut dest_writer));
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------------------------
fn benchmark<F>(description: &str, mut f: F)
    where F: FnMut()
{
    let n = 2;

    println!("{}...", description);

    // Warmup call
    f();

    let mut total_time = 0;

    for _ in 0..n {
        let tic = time::now();
        f();
        let toc = time::now();
        let delta = toc-tic;
        total_time += delta.num_microseconds().unwrap();
    }

    println!("Avg. elapsed: {} µs.", total_time/n);
}

// -----------------------------------------------------------------------------------------------
fn raw_bytes<T: Sized>(v: &T) -> &[u8] {
    let ptr = v as *const T;
    unsafe { std::slice::from_raw_parts(ptr as *const u8, mem::size_of::<T>()) }
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

            if int32 == i32::null_value() {
                continue;
            } else {
                let int64 = int64_values[i];

                if int64 == i64::null_value() {
                    continue;
                }
            }

            cnt += 1;
        }
    }

    fn query2(&self) {
        // SELECT COUNT(*) WHERE int32_col IS NOT NULL AND int64_col > 100
        let cnt = self.int32_column.raw_values().iter().zip(self.int64_column.raw_values().iter())
            .filter(|&v| *v.0 != i32::null_value() && *v.1 != i64::null_value())
            .count();
    }

    fn query3(&self) {
        let cnt = self.int32_column.raw_values_block_iterator().zip(self.int64_column.raw_values_block_iterator())
            .filter(|&v| *v.0 != i32::null_value() && *v.1 != i64::null_value())
            .count();
    }

    fn query4(&self) {
        let cnt = self.int32_column.compressed_values_block_iterator().zip(self.int64_column.compressed_values_block_iterator())
            .filter(|&v| *v.0 != i32::null_value() && *v.1 != i64::null_value())
            .count();
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

    benchmark("Query 1: Raw access", || table.query1());
    benchmark("Query 2: Raw access w/iterators", || table.query2());
    benchmark("Query 3: Block access w/iterators", || table.query3());
    benchmark("Query 4: Block access w/iterators", || table.query4());
}
