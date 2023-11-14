Turbo Base64:Fastest Base64 SIMD/Neon[![Build Status](https://travis-ci.org/powturbo/Turbo-Base64.svg?branch=master)](https://travis-ci.org/powturbo/Turbo-Base64)
===================================

###### **Fastest Base64 SIMD** Encoding library
 * 100% C (C++ headers), as simple as memcpy. 
 * No other base64 library encode or decode faster
 * :sparkles: **Scalar** can be faster than other SSE or ARM Neon based base64 libraries
 * :new: (2019.12) Turbo Base64 **SSE** faster than other SSE/AVX/AVX2! base64 library
 * :new: (2019.12) Fastest **AVX2** implementation, damn near to memcpy 
 * TurboBase64 AVX2 decoding is ~2x faster than other AVX2 libs.
 * :new: (2020.1) For short string, TurboBase64 is 3-4 times faster than other libs.
 * :new: (2019.1) Fastest **ARM Neon** base64
 * :+1: Dynamic CPU detection and **JIT scalar/sse/avx/avx2** switching
 * Base64 robust **error checking**, optimzed for **long+short** strings
 * Portable library, 32/64 bits, **SSE/AVX/AVX2**, **ARM Neon**, **Power9 Altivec**
 * OS:Linux amd64, arm64, Power9, MacOs, s390x. Windows: Mingw, visual c++
 * Big+Little endian
 * Ready and simple to use library, no armada of files, no hassles dependencies
<p>

------------------------------------------------------------------------

## Benchmark incl. the best SIMD Base64 libs:
- with [TurboBench](https://github.com/powturbo/TurboBench)
- Single thread
- Including base64 error checking
- Small file + realistic and practical (no PURE cache) benchmark with large binary game assets corpus pd3d.tar (20 MB)
- Unlike other benchmarks, the best of the best scalar+simd libraries are included

#### Benchmark Intel CPU: Skylake i7-6700 3.4GHz gcc 9.2
|E Size|ratio%|E MB/s|D MB/s|Name|1MB binary 2019.12 |
|--------:|-----:|--------:|--------:|----------------|----------------|
|1333336|133.3|**16329**|**26032**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|1333336|133.3|**9920**|**16207**|[**TB64avx**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx**|
|1333336|133.3|**8891**|**12132**|[**TB64sse**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 sse**|
|1333336|133.3|12973|13986|[fb64avx2](https://github.com/lemire/fastbase64)|Fastbase64 avx2|
|1333336|133.3|12962|13970|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|
|1333336|133.3|8392|8717|[b64avx](https://github.com/aklomp/base64)|Base64 avx|
|1333336|133.3|6899|7477|[b64sse](https://github.com/aklomp/base64)|Base64 sse41|
|1000000|100.0|29934|29992|memcpy||

|E Size|ratio%|E MB/s|D MB/s|Name| 20MB binary 2019.12|
|--------:|-----:|--------:|--------:|----------------|----------------|
|26666668|133.3|**8920**|**12706**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|26666668|133.3|**8466**|**12401**|[**TB64avx**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx**|
|26666668|133.3|**8103**|**11291**|[**TB64sse**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 sse**|
|26666668|133.3|7795|10452|[fb64avx2](https://github.com/lemire/fastbase64)|Fastbase64 avx2|
|26666668|133.3|7809|10381|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|
|26666668|133.3|7161|8172|[b64avx](https://github.com/aklomp/base64)|Base64 avx|
|26666668|133.3|6420|7042|[b64sse](https://github.com/aklomp/base64)|Base64 sse41|
|||||||
|26666668|133.3|**3925**|**4281**|[**TB64x**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 scalar**|
|26666668|133.3|1840|3320|[b64plain](https://github.com/aklomp/base64)|Base64 plain|
|26666668|133.3|1908|2752|[TB64s](https://github.com/powturbo/TurboBase64)|**Turbo Base64 scalar**|
|26666668|133.3|1522|3198|[chrome](https://github.com/lemire/fastbase64)|Google Chrome base64|
|26666668|133.3|1871|1612|[fb64plain](https://github.com/lemire/fastbase64)|FastBase64 plain|
|26666668|133.3|1122|816|[quicktime](https://github.com/lemire/fastbase64)|Apple Quicktime base64|
|27083334|135.4|1100|178|[linux](https://github.com/lemire/fastbase64)|Linux base64|
|20000000|100.0|14432|14464|memcpy||

<a name="short"></a> TurboBase64 vs. Base64 for short strings (incl. checking)
|String length|E MB/s|D MB/s|Name|1MB short strings 2020.01 |
|------------:|--------:|--------:|----------------|----------------|
| 4 - 16      |**1682**|**1843**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|             |559|622|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|
| 8 - 32      |**2835**|**2965**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|             |838|818|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|
| 16 - 64     |**4623**|**4893**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|             |1555|1229|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|
| 32 - 128    |**7218**|**7533**|[**TB64avx2**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 avx2**|
|             |2955|2365|[b64avx2](https://github.com/aklomp/base64)|Base64 avx2|

###### Benchmark ARM Neon: ARMv8 A73-ODROID-N2 1.8GHz (clang 6.0)
|E Size|ratio%|E MB/s|D MB/s|Name|30MB binary 2019.12|
|--------:|-----:|--------:|--------:|----------------|----------------|
|40000000|133.3|**2026**|**1650**|[**TB64neon**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 Neon**|
|40000000|133.3|1795|1285|[b64neon64](https://github.com/aklomp/base64)|Base64 Neon|
|40000000|133.3|**1270**|**1095**|[**TB64x**](https://github.com/powturbo/TurboBase64)|**Turbo Base64 scalar**|
|40000000|133.3|695|965|[TB64s](https://github.com/powturbo/TurboBase64)|**Turbo Base64 scalar**|
|40000000|133.3|512|782|[fb64neon](https://github.com/lemire/fastbase64)|Fastbase64 SIMD Neon|
|40000000|133.3|565|460|[Chrome](https://github.com/lemire/fastbase64)|Google Chrome base64|
|40000000|133.3|642|614|[b64plain](https://github.com/aklomp/base64)|Base64 plain|
|40000000|133.3|506|548|[fb64plain](https://github.com/lemire/fastbase64)|Fastbase64 plain|
|40500000|135.4|314|91|[Linux](https://github.com/lemire/fastbase64)|Linux base64|
|30000000|100.0|3820|3834|memcpy||

(**bold** = pareto in category)  MB=1.000.000<br />
(E/D) : Encode/Decode


<p>

## Compile: (Download or clone Turbo Base64 SIMD)
        git clone git://github.com/powturbo/TurboBase64.git
        make

## Usage: (Benchmark App)

        ./tb64app file 
        or  
        ./tb64app

## Function usage:

>**static inline unsigned turbob64len(unsigned n)**<br />
	Base64 output length after encoding

>**unsigned tb64enc(const unsigned char *in, unsigned inlen, unsigned char *out)**<br />
	Encode binary input 'in' buffer into base64 string 'out'<br />
	with automatic cpu detection for simd and switch (sse/avx2/scalar<br />
	**in**          : Input buffer to encode<br />
	**inlen**       : Length in bytes of input buffer<br />
	**out**         : Output buffer<br />
	**return value**: Length of output buffer<br />
	**Remark**      : byte 'zero' is not written to end of output stream<br />
    	         	  Caller must add 0 (out[outlen] = 0) for a null terminated string<br />


>**unsigned tb64dec(const unsigned char *in, unsigned inlen, unsigned char *out)**<br />
	Decode base64 input 'in' buffer into binary buffer 'out' <br />
	**in**          : input buffer to decode<br />
	**inlen**       : length in bytes of input buffer <br />
	**out**         : output buffer<br />
	**return value**: >0 output buffer length<br />
                      0 Error (invalid base64 input or input length = 0)<br />

### Environment:

###### OS/Compiler (32 + 64 bits):
- Windows: Visual C++ (2017)
- Windows: MinGW-w64 makefile
- Linux amd/intel: GNU GCC (>=4.6)
- Linux amd/intel: Clang (>=3.2) 
- Linux arm: aarch64 ARMv8 Neon: gcc (>=6.3) 
- Linux arm: aarch64 ARMv8 Neon: clang (>=6.0) 
- MaxOS: XCode (>=9)
- PowerPC ppc64le: gcc (>=8.0) incl. SIMD Altivec

###### References:
- [fastbase v2019.12](https://github.com/lemire/fastbase64)
- [base64 v.0.4.0 2019.12](https://github.com/aklomp/base64)
- [base64simd](https://github.com/WojciechMula/base64simd)

###### * **SIMD Base64 publications:**
  * :green_book:[Faster Base64 Encoding and Decoding Using AVX2 Instructions](https://arxiv.org/abs/1704.00605)
  * :green_book:[RFC 4648:The Base16, Base32, and Base64 Data Encodings](https://tools.ietf.org/html/rfc4648)

Last update: 03 MAR 2020

