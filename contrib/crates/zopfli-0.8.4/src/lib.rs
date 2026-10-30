#![deny(trivial_casts, trivial_numeric_casts, missing_docs)]

//! A reimplementation of the [Zopfli](https://github.com/google/zopfli) compression library in Rust.
//!
//! Zopfli is a state of the art DEFLATE compressor that heavily prioritizes compression over speed.
//! It usually compresses much better than other DEFLATE compressors, generating standard DEFLATE
//! streams that can be decompressed with any DEFLATE decompressor, at the cost of being
//! significantly slower.
//!
//! # Features
//!
//! This crate exposes the following features. You can enable or disable them in your `Cargo.toml`
//! as needed.
//!
//! - `gzip` (enabled by default): enables support for compression in the gzip format.
//! - `zlib` (enabled by default): enables support for compression in the Zlib format.
//! - `std` (enabled by default): enables linking against the Rust standard library. When not enabled,
//!   the crate is built with the `#![no_std]` attribute and can be used in any environment where
//!   [`alloc`](https://doc.rust-lang.org/alloc/) (i.e., a memory allocator) is available. In addition,
//!   the crate exposes minimalist versions of the `std` I/O traits it needs to function, allowing users
//!   to implement them.
//! - `nightly`: enables code constructs and features specific to the nightly Rust toolchain. Currently,
//!   this feature improves rustdoc generation and enables the namesake feature on `crc32fast`, but this
//!   may change in the future. This feature also used to enable `simd-adler32`'s namesake feature, but
//!   it no longer does as the latest `simd-adler32` release does not build with the latest nightlies
//!   (as of 2024-05-18) when that feature is enabled.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(feature = "nightly", feature(doc_cfg))]

// No-op log implementation for no-std targets
#[cfg(not(feature = "std"))]
macro_rules! debug {
    ( $( $_:expr ),* ) => {};
}
#[cfg(not(feature = "std"))]
macro_rules! trace {
    ( $( $_:expr ),* ) => {};
}
#[cfg(not(feature = "std"))]
macro_rules! log_enabled {
    ( $( $_:expr ),* ) => {
        false
    };
}

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate alloc;

pub use deflate::{BlockType, DeflateEncoder};
#[cfg(feature = "gzip")]
pub use gzip::GzipEncoder;
#[cfg(all(test, feature = "std"))]
use proptest::prelude::*;
#[cfg(feature = "zlib")]
pub use zlib::ZlibEncoder;

mod blocksplitter;
mod cache;
mod deflate;
#[cfg(feature = "gzip")]
mod gzip;
mod hash;
#[cfg(any(doc, not(feature = "std")))]
mod io;
mod iter;
mod katajainen;
mod lz77;
#[cfg(not(feature = "std"))]
mod math;
mod squeeze;
mod symbols;
mod tree;
mod util;
#[cfg(feature = "zlib")]
mod zlib;

use core::num::NonZeroU64;
#[cfg(all(not(doc), feature = "std"))]
use std::io::{Error, Write};

#[cfg(any(doc, not(feature = "std")))]
pub use io::{Error, ErrorKind, Write};

/// Options for the Zopfli compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(all(test, feature = "std"), derive(proptest_derive::Arbitrary))]
pub struct Options {
    /// Maximum amount of times to rerun forward and backward pass to optimize LZ77
    /// compression cost.
    /// Good values: 10, 15 for small files, 5 for files over several MB in size or
    /// it will be too slow.
    ///
    /// Default value: 15.
    #[cfg_attr(
        all(test, feature = "std"),
        proptest(
            strategy = "(1..=10u64).prop_map(|iteration_count| NonZeroU64::new(iteration_count).unwrap())"
        )
    )]
    pub iteration_count: NonZeroU64,
    /// Stop after rerunning forward and backward pass this many times without finding
    /// a smaller representation of the block.
    ///
    /// Default value: practically infinite (maximum `u64` value)
    pub iterations_without_improvement: NonZeroU64,
    /// Maximum amount of blocks to split into (0 for unlimited, but this can give
    /// extreme results that hurt compression on some files).
    ///
    /// Default value: 15.
    pub maximum_block_splits: u16,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            iteration_count: NonZeroU64::new(15).unwrap(),
            iterations_without_improvement: NonZeroU64::new(u64::MAX).unwrap(),
            maximum_block_splits: 15,
        }
    }
}

/// The output file format to use to store data compressed with Zopfli.
#[derive(Debug, Copy, Clone)]
#[cfg(feature = "std")]
pub enum Format {
    /// The gzip file format, as defined in
    /// [RFC 1952](https://datatracker.ietf.org/doc/html/rfc1952).
    ///
    /// This file format can be easily decompressed with the gzip
    /// program.
    #[cfg(feature = "gzip")]
    Gzip,
    /// The zlib file format, as defined in
    /// [RFC 1950](https://datatracker.ietf.org/doc/html/rfc1950).
    ///
    /// The zlib format has less header overhead than gzip, but it
    /// stores less metadata.
    #[cfg(feature = "zlib")]
    Zlib,
    /// The raw DEFLATE stream format, as defined in
    /// [RFC 1951](https://datatracker.ietf.org/doc/html/rfc1951).
    ///
    /// Raw DEFLATE streams are not meant to be stored as-is because
    /// they lack error detection and correction metadata. They
    /// are usually embedded in other file formats, such as gzip
    /// and zlib.
    Deflate,
}

/// Compresses data from a source with the Zopfli algorithm, using the specified
/// options, and writes the result to a sink in the defined output format.
#[cfg(feature = "std")]
pub fn compress<R: std::io::Read, W: Write>(
    options: Options,
    output_format: Format,
    mut in_data: R,
    out: W,
) -> Result<(), Error> {
    match output_format {
        #[cfg(feature = "gzip")]
        Format::Gzip => {
            let mut gzip_encoder = GzipEncoder::new_buffered(options, BlockType::Dynamic, out)?;
            std::io::copy(&mut in_data, &mut gzip_encoder)?;
            gzip_encoder.into_inner()?.finish().map(|_| ())
        }
        #[cfg(feature = "zlib")]
        Format::Zlib => {
            let mut zlib_encoder = ZlibEncoder::new_buffered(options, BlockType::Dynamic, out)?;
            std::io::copy(&mut in_data, &mut zlib_encoder)?;
            zlib_encoder.into_inner()?.finish().map(|_| ())
        }
        Format::Deflate => {
            let mut deflate_encoder =
                DeflateEncoder::new_buffered(options, BlockType::Dynamic, out);
            std::io::copy(&mut in_data, &mut deflate_encoder)?;
            deflate_encoder.into_inner()?.finish().map(|_| ())
        }
    }
}

#[doc(hidden)]
#[deprecated(
    since = "0.8.2",
    note = "Object pools are no longer used. This function is now a no-op and will be removed in version 0.9.0."
)]
#[cfg(feature = "std")] // TODO remove for 0.9.0
pub fn prewarm_object_pools() {}

#[cfg(all(test, feature = "std"))]
mod test {
    use std::io;

    use miniz_oxide::inflate;
    use proptest::proptest;

    use super::*;

    proptest! {
        #[test]
        fn deflating_is_reversible(
            options: Options,
            btype: BlockType,
            data in prop::collection::vec(any::<u8>(), 0..64 * 1024)
        ) {
            let mut compressed_data = Vec::with_capacity(data.len());

            let mut encoder = DeflateEncoder::new(options, btype, &mut compressed_data);
            io::copy(&mut &*data, &mut encoder).unwrap();
            encoder.finish().unwrap();

            let decompressed_data = inflate::decompress_to_vec(&compressed_data).expect("Could not inflate compressed stream");
            prop_assert_eq!(data, decompressed_data, "Decompressed data should match input data");
        }
    }
}
