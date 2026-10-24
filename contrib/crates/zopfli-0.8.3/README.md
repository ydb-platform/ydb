<div align="center">
<img src="https://avatars.githubusercontent.com/u/100982154?s=200&v=4" alt="Zopfli in Rust logo">
<h1>Zopfli in Rust</h1>

<a href="https://crates.io/crates/zopfli"><img alt="crates.io latest version" src="https://img.shields.io/crates/v/zopfli"></a>
<a href="https://docs.rs/zopfli"><img alt="docs.rs status" src="https://img.shields.io/docsrs/zopfli?label=docs.rs"></a>
</div>

This is a reimplementation of the [Zopfli](https://github.com/google/zopfli) compression tool in Rust.

Carol Nichols started the Rust implementation as an experiment in incrementally rewriting a C library in Rust, keeping the project compiling at every step. For more information about that experiment, see [the slides for a talk she gave about it](https://github.com/carols10cents/rust-out-your-c-talk) and [the repo as it was for the experiment](https://github.com/carols10cents/zopfli).

The minimum supported Rust version (MSRV) for this crate is 1.73. Bumping this version is not considered a breaking change for semantic versioning purposes. We will try to do it only when we estimate that such a bump would not cause widespread inconvenience or breakage.

## How to build

To build the code, run:

```
$ cargo build --release
```

and the executable will be in `target/release/zopfli`.

This should work on stable or beta Rust.

## Running the tests

There are some unit tests, mostly around the boundary package merge algorithm implementation in katajainen.rs, and a property-based test for compression reversibility. These tests can be run with:

```
$ cargo test
```

Golden master tests, to check that compressed files are exactly the same as the C implementation would generate, can be run using:

```
$ ./test/run.sh
```
