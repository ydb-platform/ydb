use std::{
    env,
    fs::File,
    io::{self, prelude::*},
};

#[cfg(feature = "std")]
use log::info;

fn main() {
    let options = zopfli::Options::default();
    let output_type = zopfli::Format::Gzip;

    // TODO: CLI arguments
    // TODO: Allow specifying output to STDOUT

    let extension = match output_type {
        zopfli::Format::Gzip => ".gz",
        zopfli::Format::Zlib => ".zlib",
        zopfli::Format::Deflate => ".deflate",
    };

    for filename in env::args().skip(1) {
        let file =
            File::open(&filename).unwrap_or_else(|why| panic!("couldn't open {filename}: {why}"));
        let filesize = file.metadata().map(|x| x.len()).unwrap() as usize;

        let out_filename = format!("{filename}{extension}");

        // Attempt to create the output file, panic if the output file could not be opened
        let out_file = File::create(&out_filename)
            .unwrap_or_else(|why| panic!("couldn't create output file {out_filename}: {why}"));
        let mut out_file = WriteStatistics::new(out_file);

        zopfli::compress(options, output_type, &file, &mut out_file)
            .unwrap_or_else(|why| panic!("couldn't write to output file {out_filename}: {why}"));

        let out_size = out_file.count;
        info!(
            "Original Size: {}, Compressed: {}, Compression: {}% Removed",
            filesize,
            out_size,
            100.0 * (filesize - out_size) as f64 / filesize as f64
        );
    }
}

struct WriteStatistics<W> {
    inner: W,
    count: usize,
}

impl<W> WriteStatistics<W> {
    const fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W: Write> Write for WriteStatistics<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf);
        if let Ok(size) = res {
            self.count += size;
        }
        res
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
