[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Code style](https://github.com/ariya/fastlz/workflows/Code%20style/badge.svg)](https://github.com/ariya/fastlz/actions)
[![Address Sanitizer](https://github.com/ariya/fastlz/workflows/Address%20Sanitizer/badge.svg)](https://github.com/ariya/fastlz/actions)

## Overview

FastLZ (MIT license) is an ANSI C/C90 implementation of [Lempel-Ziv 77 algorithm](https://en.wikipedia.org/wiki/LZ77_and_LZ78#LZ77) (LZ77) of lossless data compression. It is suitable to compress series of text/paragraphs, sequences of raw pixel data, or any other blocks of data with lots of repetition. It is not intended to be used on images, videos, and other formats of data typically already in an optimal compressed form.

The focus for FastLZ is a very fast compression and decompression, doing that at the cost of the compression ratio. As an illustration, the comparison with zlib when compressing [enwik8](http://www.mattmahoney.net/dc/textdata.html) (also in [more details](https://github.com/inikep/lzbench)):

||Ratio|Compression|Decompression
|--|--|--|--|
|FastLZ |54.2%|159 MB/s|305 MB/s|
|zlib -1|42.3%|50 MB/s|184 MB/s|
|zlib -9|36.5%|11 MB/s|185 MB/s|

FastLZ is used by many software products, from a number of games (such as [Death Stranding](https://en.wikipedia.org/wiki/Death_Stranding)) to various open-source projects ([Godot Engine](https://godotengine.org/), [Facebook HHVM](https://hhvm.com/), [Apache Traffic Server](https://trafficserver.apache.org/), [Calligra Office](https://www.calligra.org/), [OSv](http://osv.io/), [Netty](https://netty.io/), etc). It even serves as the basis for other compression projects like [BLOSC](https://blosc.org/).

For other implementations of byte-aligned LZ77, take a look at [LZ4](https://lz4.github.io/lz4/), [Snappy](http://google.github.io/snappy/), [Density](https://github.com/centaurean/density), [LZO](http://www.oberhumer.com/opensource/lzo/), [LZF](http://oldhome.schmorp.de/marc/liblzf.html), [LZJB](https://en.wikipedia.org/wiki/LZJB), [LZRW](http://www.ross.net/compression/lzrw1.html), etc.

## Usage

FastLZ can be used directly in any C/C++ applications. For other programming languages/environments, use the corresponding binding:

* [Rust](https://crates.io/crates/fastlz), available on Crates: `cargo install fastlz`
* [Python](https://pypi.org/project/fastlz/), available on PyPi: `pip install fastlz`
* [JavaScript](https://www.npmjs.com/package/fastlz), available on npm: `npm install fastlz`
* [Ruby](https://rubygems.org/gems/fastlz), available on Rubygems: `gem install fastlz`
* Lua via [github.com/oneoo/lua-fastlz](https://github.com/oneoo/lua-fastlz)

FastLZ consists of only two files: `fastlz.h` and `fastlz.c`. Just add these files to your project in order to use FastLZ. For the detailed information on the API to perform compression and decompression, see `fastlz.h`.

For [Vcpkg](https://github.com/microsoft/vcpkg) users, FastLZ is [already available](https://github.com/microsoft/vcpkg): `vcpkg install fastlz`.

A simple file compressor called `6pack` is included as an example on how to use FastLZ. The corresponding decompressor is `6unpack`.

FastLZ supports any standard-conforming ANSI C/C90 compiler, including the popular ones such as [GCC](https://gcc.gnu.org/), [Clang](https://clang.llvm.org/), [Visual Studio](https://visualstudio.microsoft.com/vs/features/cplusplus/), and even [Tiny CC](https://bellard.org/tcc/). FastLZ works well on a number of architectures (32-bit and 64-bit, big endian and little endian), from Intel/AMD, PowerPC, System z, ARM, MIPS, and RISC-V.

The continuous integration system runs an extensive set of compression-decompression round trips on the following systems:


For more details, check the corresponding [GitHub Actions build logs](https://github.com/ariya/FastLZ/actions).

|                      |                                                                                                         |                                                                                                   |                                                                                             |
|----------------------|--------------------------------------------------------------------------------------------------------:|--------------------------------------------------------------------------------------------------:|--------------------------------------------------------------------------------------------:|
| **amd64**            | **Linux**                                                                                               | **Windows**                                                                                       | **macOS**                                                                                   |
|                  GCC | ![amd64_linux_gcc](https://github.com/ariya/FastLZ/workflows/amd64_linux_gcc/badge.svg)                 | ![amd64_windows_gcc](https://github.com/ariya/FastLZ/workflows/amd64_windows_gcc/badge.svg)       | ![amd64_macos_gcc](https://github.com/ariya/FastLZ/workflows/amd64_macos_gcc/badge.svg)     |
|                Clang | ![amd64_linux_clang](https://github.com/ariya/FastLZ/workflows/amd64_linux_clang/badge.svg)             | ![amd64_windows_clang](https://github.com/ariya/FastLZ/workflows/amd64_windows_clang/badge.svg)   | ![amd64_macos_clang](https://github.com/ariya/FastLZ/workflows/amd64_macos_clang/badge.svg) |
|               TinyCC | ![amd64_linux_tcc](https://github.com/ariya/FastLZ/workflows/amd64_linux_tcc/badge.svg)                 | ![amd64_windows_tcc](https://github.com/ariya/FastLZ/workflows/amd64_windows_tcc/badge.svg)       |                                                                                             |
|              VS 2019 |                                                                                                         | ![amd64_windows_vs2019](https://github.com/ariya/FastLZ/workflows/amd64_windows_vs2019/badge.svg) |                                                                                             |
| **i686**             | **Linux**                                                                                               | **Windows**                                                                                       | **macOS**                                                                                   |
|                  GCC | ![i686_linux_gcc](https://github.com/ariya/FastLZ/workflows/i686_linux_gcc/badge.svg)                   |                                                                                                   |                                                                                             |
|                Clang | ![i686_linux_clang](https://github.com/ariya/FastLZ/workflows/i686_linux_clang/badge.svg)               |                                                                                                   |                                                                                             |
|               TinyCC |                                                                                                         | ![i686_windows_tcc](https://github.com/ariya/FastLZ/workflows/i686_windows_tcc/badge.svg)         |                                                                                             |
|              VS 2019 |                                                                                                         | ![i686_windows_vs2019](https://github.com/ariya/FastLZ/workflows/i686_windows_vs2019/badge.svg)   |                                                                                             |
| **i586**             | **Linux**                                                                                               | **DOS**                                                                                           |                                                                                             |
|                  GCC |                                                                                                         | ![i586_dos_gcc_cross](https://github.com/ariya/FastLZ/workflows/i586_dos_gcc_cross/badge.svg)     |                                                                                             |
|                      | **Linux**                                                                                               |                                                                                                   |                                                                                             |
| **powerpc**          |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![powerpc_linux_gcc](https://github.com/ariya/FastLZ/workflows/powerpc_linux_gcc/badge.svg)            |                                                                                                   |                                                                                             |
| **ppc64(le)**        |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![ppc64_linux_gcc](https://github.com/ariya/FastLZ/workflows/ppc64_linux_gcc/badge.svg)                |                                                                                                   |                                                                                             |
|                  GCC |  ![ppc64le_linux_gcc](https://github.com/ariya/FastLZ/workflows/ppc64le_linux_gcc/badge.svg)            |                                                                                                   |                                                                                             |
| **s390x**            |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![s390x_linux_gcc](https://github.com/ariya/FastLZ/workflows/s390x_linux_gcc/badge.svg)                |                                                                                                   |                                                                                             |
| **armhf**            |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![armhf_linux_gcc](https://github.com/ariya/FastLZ/workflows/armhf_linux_gcc/badge.svg)                |                                                                                                   |                                                                                             |
| **arm64**            |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![arm64_linux_gcc](https://github.com/ariya/FastLZ/workflows/arm64_linux_gcc/badge.svg)                |                                                                                                   |                                                                                             |
| **mips(el)**         |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![mipsel_linux_gcc](https://github.com/ariya/FastLZ/workflows/mipsel_linux_gcc/badge.svg)              |                                                                                                   |                                                                                             |
|                  GCC |  ![mips_linux_gcc](https://github.com/ariya/FastLZ/workflows/mips_linux_gcc/badge.svg)                  |                                                                                                   |                                                                                             |
| **mips64(el)**       |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![mips64el_linux_gcc](https://github.com/ariya/FastLZ/workflows/mips64el_linux_gcc/badge.svg)          |                                                                                                   |                                                                                             |
|                  GCC |  ![mips64_linux_gcc](https://github.com/ariya/FastLZ/workflows/mips64_linux_gcc/badge.svg)              |                                                                                                   |                                                                                             |
|  **riscv**           |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![riscv_linux_gcc](https://github.com/ariya/FastLZ/workflows/riscv_linux_gcc/badge.svg)                |                                                                                                   |                                                                                             |
| **riscv64**          |                                                                                                         |                                                                                                   |                                                                                             |
|                  GCC |  ![riscv64_linux_gcc](https://github.com/ariya/FastLZ/workflows/riscv64_linux_gcc/badge.svg)            |                                                                                                   |                                                                                             |



## Block Format

Let us assume that FastLZ compresses an array of bytes, called the _uncompressed block_, into another array of bytes, called the _compressed block_. To understand what will be stored in the compressed block, it is illustrative to demonstrate how FastLZ will _decompress_ the block to retrieve the original uncompressed block.

The first 3-bit of the block, i.e. the 3 most-significant bits of the first byte, is the **block tag**. Currently the block tag determines the compression level used to produce the compressed block.

|Block tag|Compression level|
|---------|-----------------|
|   0     |    Level 1      |
|   1     |    Level 2      |

The content of the block will vary depending on the compression level.

### Block Format for Level 1

FastLZ Level 1 implements LZ77 compression algorithm with 8 KB sliding window and up to 264 bytes of match length.

The compressed block consists of one or more **instructions**.
Each instruction starts with a 1-byte opcode, 2-byte opcode, or 3-byte opcode.

| Instruction type | Opcode[0] | Opcode[1] | Opcode[2]
|-----------|------------------|--------------------|--|
| Literal run | `000`, L&#x2085;-L&#x2080; | -|- |
| Short match | M&#x2082;-M&#x2080;, R&#x2081;&#x2082;-R&#x2088; | R&#x2087;-R&#x2080; | - |
| Long match | `111`, R&#x2081;&#x2082;-R&#x2088; | M&#x2087;-M&#x2080; | R&#x2087;-R&#x2080; |

Note that the _very first_ instruction in a compressed block is always a literal run.

#### Literal run instruction

For the literal run instruction, there is one or more bytes following the code. This is called the literal run.

The 5 least-significant bits of `opcode[0]`, _L_, determines the **number of literals** following the opcode. The value of 0 indicates a 1-byte literal run, 1 indicates a 2-byte literal run, and so on. The minimum literal run is 1 and the maximum literal run is 32.

The decompressor copies (_L + 1_) bytes of literal run, starting from the first one right after opcode.

_Example_: If the compressed block is a 4-byte array of `[0x02, 0x41, 0x42, 0x43]`, then the opcode is `0x02` and that means a literal run of 3 bytes. The decompressor will then copy the subsequent 3 bytes, `[0x41, 0x42, 0x43]`, to the output buffer. The output buffer now represents the (original) uncompressed block, `[0x41, 0x42, 0x43]`.

#### Short match instruction

The 3 most-significant bits of `opcode[0]`, _M_, determines the **match length**. The value of 1 indicates a 3-byte match, 2 indicates a 4-byte match and so on. The minimum match length is 3 and the maximum match length is 8.

The 5 least-significant bits of `opcode[0]` combined with the 8 bits of the `opcode[1]`, _R_, determines the **reference offset**. Since the offset is encoded in 13 bits, the minimum is 0 and the maximum is 8191.

The following C code retrieves the match length and reference offset:

```c
M = opcode[0] >> 5;
R = 256 * (opcode[0] << 5) + opcode[1];
```

The decompressor copies _(M+2)_ bytes, starting from the location offsetted by _R_ in the output buffer. Note that _R_ is a *back reference*, i.e. the value of 0 corresponds the last byte in the output buffer, 1 is the second to last byte, and so forth.

_Example 1_: If the compressed block is a 7-byte array of `[0x03, 0x41, 0x42, 0x43, 0x44, 0x20, 0x02]`, then there are two instructions in the there. The first instruction is the literal run of 4 bytes (due to _L = 3_). Thus, the decompressor copies 4 bytes to the output buffer, resulting in `[0x41, 0x42, 0x43, 0x44]`. The second instruction is the short match of 3 bytes (from _M = 1_, i.e `0x20 >> 5`) and the offset of 2. Therefore, the compressor goes back 2 bytes from the last position, copies 3 bytes (`[0x42, 0x43, 0x44]`), and appends them to the output buffer. The output buffer now represents the complete uncompressed data, `[0x41, 0x42, 0x43, 0x44, 0x42, 0x43, 0x44]`.

_Example 2_: If the compressed block is a 4-byte array of `[0x00, 0x61, 0x40, 0x00]`, then there are two instructions in there. The first instruction is the literal run of just 1 byte (_L = 0_). Thus, the decompressor copies the byte (`0x61`) to the output buffer. The output buffer now becomes `[0x61]`. The second instruction is the short match of 4 bytes (from _M = 2_, i.e. `0x40 >> 5`) and the offset of 0. Therefore, the decompressor copies 4 bytes starting using the back reference of 0 (i.e. the position of `0x61`). The output buffer now represents the complete uncompressed data, `[0x61, 0x61, 0x61, 0x61, 0x61]`.

#### Long match instruction

The value of `opcode[1]`, _M_, determines the **match length**. The value of 0 indicates a 9-byte match, 1 indicates a 10-byte match and so on. The minimum match length is 9 and the maximum match length is 264.

The 5 least-significant bits of `opcode[0]` combined with the 8 bits of `opcode[2]`, _R_, determines the **reference offset**. Since the offset is encoded in 13 bits, the minimum is 0 and the maximum is 8191.

The following C code retrieves the match length and reference offset:

```c
M = opcode[1];
R = 256 * (opcode[0] << 5) + opcode[2];
```
The decompressor copies _(M+9)_ bytes, starting from the location offsetted by _R_ in the output buffer. Note that _R_ is a *back reference*, i.e. the value of 0 corresponds to the last byte in the output buffer, 1 is for the second to last byte, and so forth.

_Example_:  If the compressed block is a 4-byte array of `[0x01, 0x44, 0x45, 0xE0, 0x01, 0x01]`, then there are two instructions in there. The first instruction is the literal run with the length of 2 (due to _L = 1_). Thus, the decompressor copies the 2-byte literal run (`[0x44, 0x45]`) to the output buffer. The second instruction is the long match with the match length of 10 (from _M = 1_) and the offset of 1. Therefore, the decompressor copies 10 bytes starting using the back reference of 1 (i.e. the position of `0x44`). The output buffer now represents the complete uncompressed data, `[0x44, 0x45, 0x44, 0x45, 0x44, 0x45, 0x44, 0x45, 0x44, 0x45, 0x44, 0x45]`.

#### Decompressor Reference Implementation

The following 40-line C function implements a fully-functional decompressor for the above block format. Note that it is intended to be educational, e.g. no bound check is implemented, and therefore it is absolutely **unsafe** for production.

```c
void fastlz_level1_decompress(const uint8_t* input, int length, uint8_t* output) {
  int src = 0;
  int dest = 0;
  while (src < length) {
    int type = input[src] >> 5;
    if (type == 0) {
      /* literal run */
      int run = 1 + input[src];
      src = src + 1;
      while (run > 0) {
        output[dest] = input[src];
        src = src + 1;
        dest = dest + 1;
        run = run - 1;
      }
    } else if (type < 7) {
      /* short match */
      int ofs = 256 * (input[src] & 31) + input[src + 1];
      int len = 2 + (input[src] >> 5);
      src = src + 2;
      int ref = dest - ofs - 1;
      while (len > 0) {
        output[dest] = output[ref];
        ref = ref + 1;
        dest = dest + 1;
        len = len - 1;
      }
    } else {
      /* long match */
      int ofs = 256 * (input[src] & 31) + input[src + 2];
      int len = 9 + input[src + 1];
      src = src + 3;
      int ref = dest - ofs - 1;
      while (len > 0) {
        output[dest] = output[ref];
        ref = ref + 1;
        dest = dest + 1;
        len = len - 1;
      }
    }
  }
}
```

### Block Format for Level 2

(To be written)
