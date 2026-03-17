[![Build Status](https://travis-ci.org/litespeedtech/ls-qpack.svg?branch=master)](https://travis-ci.org/litespeedtech/ls-qpack)
[![Build status](https://ci.appveyor.com/api/projects/status/kat31lt42ds0rmom?svg=true)](https://ci.appveyor.com/project/litespeedtech/ls-qpack)
[![Build Status](https://api.cirrus-ci.com/github/litespeedtech/ls-qpack.svg)](https://cirrus-ci.com/github/litespeedtech/ls-qpack)

# ls-qpack
QPACK compression library for use with HTTP/3

## Introduction

QPACK is the compression mechanism used by
[HTTP/3](https://en.wikipedia.org/wiki/HTTP/3) to compress HTTP headers.
It is in the process of being standardazed by the QUIC Working Group.  The
[QPACK Internet-Draft](https://tools.ietf.org/html/draft-ietf-quic-qpack-11)
is has been stable for some time and we don't expect functional changes to
it before the final RFC is released.

## Functionality

ls-qpack is a full-featured, tested, and fast QPACK library.  The QPACK encoder
produces excellent compression results based on an [innovative mnemonic technique](https://blog.litespeedtech.com/2021/04/05/qpack-mnemonic-technique/).  It boasts the fastest Huffman
[encoder](https://blog.litespeedtech.com/2019/10/03/fast-huffman-encoder/) and
[decoder](https://blog.litespeedtech.com/2019/09/16/fast-huffman-decoder/).

The library is production quality.  It is used in
[OpenLiteSpeed](https://openlitespeed.org/),
LiteSpeed [Web Server](https://www.litespeedtech.com/products#lsws),
and LiteSpeed [Web ADC](https://www.litespeedtech.com/products#wadc).

The library is robust:
1. The encoder does not assume anything about usual HTTP headers such as `Server`
   or `User-Agent`.  Instead, it uses its mnemonic compression technique to
   achieve good compression results for any input.
1. The decoder uses modulo arithmetic to track dynamic table insertions.  This is
   in contrast to all other QPACK implementations, which use an integer counter,
   meaning that at some point, the decoder will break.
1. The decoder processes input in streaming fashion.  The caller does not have to
   buffer the contents of HTTP/3 `HEADERS` frame.  Instead, the decoder can be
   supplied input byte-by-byte.

## Other Languages

The ls-qpack library is implemented in vanilla C99.  It makes it a good candidate
for wrapping into a library for a higher-level language.  As of this writing, we
know of the following wrappers:
- Go: [ls-qpack-go](https://github.com/mpiraux/ls-qpack-go)
- Python: [pylsqpack](https://github.com/aiortc/pylsqpack)
- Rust: [ls-qpack-rs](https://github.com/BiagioFesta/ls-qpack-rs)
- TypeScript: [quicker](https://github.com/rmarx/quicker/tree/draft-20/lib/ls-qpack)
- Ruby: [lsqpack-ruby](https://github.com/unasuke/lsqpack-ruby)

## Versioning

Before the QPACK RFC is released, the three parts of the version are:
- MAJOR: set to zero;
- MINOR: set to the number of QPACK Internet-Draft the lirbary supports; and
- PATCH: set to the patch number

Once the RFC is released, MAJOR will be set to 1 and the version will follow
the usual MAJOR.MINOR.PATCH pattern.

## API

The API is documented in the header file, [lsqpack.h](lsqpack.h).
One example how it is used in real code can be seen in
[lsquic](https://github.com/litespeedtech/lsquic), a QUIC and HTTP/3 library
developed by LiteSpeed Technologies.

A different API, without the use of `struct lsxpack_header`, is available
on branch-v1.
