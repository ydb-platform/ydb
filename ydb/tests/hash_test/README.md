# Some hash functions tests

## Performance test with hyper-threading and cache hierarchy

It is a no-brainer that SSE and AVX versions of hash functions are faster than "legacy". However, in case of hyper-threading two cores share all execution units and integer units are fast, while vector units take multiple clock cycles to finish and completely flood the execution ports. This leaves the second thread stalled, which is why hyper-threading helps integer workloads much more than vector workloads.

In this test we want to compare "RAW" performance (single core, different physical cores) and hyper-threading performance.

Another interesting thing to measure is to compare Integer/SSE/AVX when access various cache levels and RAM. Our hunch is that in case of RAM difference between SSE/AVX might be much smaller. By default we use `L1 = 48 KiB`, `L2 = 1280 KiB`, `L3 = 48 MiB`.

To run perf test:
```
./hash_test --perf [--l1 48] [--l2 1280] [--l3 49152] [--block-size  --left-cores=1,2,3 --right-cores=32,33,34
```

The test will run:
* pure memory access
* chacha encryption (integer)
* chacha encryption (SSE)
* chacha encryption (AVX512)
* XXH3_64bits (AVX2)
* XXH3_64bits (SSE2)
* CityHash64

## Equvalency of different hash / encryption implementations

--functional mode. Here number of threads is specified via --threads

I want to do two things:
1. Make sure that integer/sse/avx* produce the same. I.e. chacha family and xxh3 family independent on implementation do the same.
2. Compare on different machines. If file size is specified --output then output 4K hashes -> their blocks, 4K hashes -> their blocks.
--output-size controls amount of data in gigabytes. Preallocate the file (properly count number of needed hash blocks) and spread the file between the threads. For simplicity use memory mapped file. If --input option is specified – read the data, compute hash (using all variants), compare with the persisted one. Again, --threads and memory mapped file is suggested for simplicity.

```
./hash_test --functional [--threads N] [--seed S] [--duration-s D]
./hash_test --functional --output FILE --output-size G [--threads N] [--seed S]
./hash_test --functional --input FILE [--threads N] [--verify-data]
```

With neither `--output` nor `--input`, `--functional` runs an in-memory equivalence
sweep for `--duration-s` seconds (default 10), fuzzing lengths, buffer offsets, and
call-splitting patterns across every ChaCha/XXH3 implementation available on this
machine. Exit code 0 means every implementation agreed; 1 means a mismatch (or an
I/O error in file mode) was found, printed with enough detail (seed, thread, length,
offset, counter) to reproduce it.

Implementation note: enciphering a buffer as a sequence of streamed segments on one
cipher instance only reproduces the single-call result when every segment except
possibly the last is a multiple of 64 bytes — the classic partial-block-state
constraint of the vectorized ChaCha implementations. This matches how the `--perf`
kernels drive the ciphers, so it isn't a real-world limitation, but the equivalence
sweep constrains its randomly generated split points accordingly.

### ARM64 / Apple Silicon

`--functional` (both the in-memory sweep and the file write/verify modes) also runs on
ARM64, including macOS on Apple Silicon; `--perf` remains Linux/x86-64-only. On ARM64,
XXH3 gets a pinned NEON kernel (`xxh3-neon`, alongside the always-available
`xxh3-scalar`) in place of `xxh3-sse2`/`xxh3-avx2`/`xxh3-dispatch`, and only
`chacha-int` is available (the vectorized ChaCha implementation isn't built for ARM64
in this repo). This makes the cross-machine workflow from goal 2 above concrete:

```
# on an x86-64 machine
./hash_test --functional --output hashes.bin --output-size 1

# copy hashes.bin to an Apple Silicon Mac, then:
./hash_test --functional --input hashes.bin --verify-data
```

A clean run means XXH3-scalar/NEON and CityHash64 on the Mac agree byte-for-byte with
whatever XXH3-scalar/SSE2/AVX2 variant produced the file on x86-64.