No hash function is perfect, but some are useful.
====

wyhash and wyrand are the ideal 64-bit hash function and PRNG respectively: 

**solid**:  wyhash passed SMHasher, wyrand passed BigCrush, practrand.

**portable**: 64-bit/32-bit system, big/little endian.
  
**fastest**:  Efficient on 64-bit machines, especially for short keys.
  
**simplest**: In the sense of code size.

**salted**: We use dynamic secret to avoid intended attack.

wyhash is the default hashing algorithm of the great [Zig](https://ziglang.org), [V](https://vlang.io), [Nim](https://nim-lang.org) and [Go (since 1.17)](https://golang.org/src/runtime/hash64.go) language. One milestone is that wyhash has deployed by Microsoft on [Windows Terminal] (https://github.com/microsoft/terminal/pull/13686).

**Simple Example:**
```
#include  "wyhash.h"
uint64_t _wyp[4];
make_secret(time(NULL),_wyp);
string  s="fcdskhfjs";
uint64_t h=wyhash(s.c_str(),s.size(),0,_wyp);
```

----------------------------------------

g++-9 benchmark.cpp t1ha/src/t1ha2.c -o benchmark -Ofast -s  -Wall -march=native

/usr/share/dict/words

|hash function  |short hash/us  |bulk_256B GB/s |bulk_64KB GB/s |
|----           |----           |----           |----           |
|**wyhash_final3** |419.63         |20.97          |25.10          |
|wyhash_final2  |204.08         |21.12          |25.10          |
|wyhash_final1  |196.33         |15.56          |17.92          |
|wyhash_gamma (dangerous) |399.92         |19.16          |25.80          |
|wyhash_beta    |180.51         |18.20          |17.37          |
|wyhash_alpha   |204.50         |20.54          |25.92          |
|wyhash_v6 (avx2) |164.08         |12.08          |41.35          |
|wyhash_v5      |221.59         |15.40          |16.78          |
|wyhash_v4      |153.03         |13.89          |16.81          |
|wyhash_v3      |191.32         |13.55          |15.72          |
|wyhash_v2      |94.28          |11.91          |11.22          |
|wyhash_v1      |91.66          |16.03          |18.95          |
|wyhash32 (32 bit) |149.78         |5.45           |4.89           |
|xxh3_scalar    |152.47         |8.39           |13.05          |
|xxh3_avx2      |144.62         |9.85           |44.82          |

----------------------------------------

**C#**  https://github.com/cocowalla/wyhash-dotnet

**C++**  https://github.com/tommyettinger/waterhash

**C++** https://github.com/alainesp/wy

**GO**  https://github.com/dgryski/go-wyhash

**GO**  https://github.com/orisano/wyhash

**GO** https://github.com/littleli/go-wyhash16

**GO** https://github.com/zeebo/wyhash

**GO** https://github.com/lonewolf3739/wyhash-go

**GO** https://github.com/zhangyunhao116/wyhash (final version 1 && 3)

**Java** https://github.com/OpenHFT/Zero-Allocation-Hashing

**Java** https://github.com/dynatrace-oss/hash4j (final version 3)

**Kotlin Multiplatform** https://github.com/appmattus/crypto/tree/main/cryptohash

**Nim** https://github.com/nim-lang/Nim/blob/devel/lib/pure/hashes.nim

**Nim** https://github.com/jackhftang/wyhash.nim

**Nim** https://github.com/littleli/nim-wyhash16

**Rust**  https://github.com/eldruin/wyhash-rs

**Swift** https://github.com/lemire/SwiftWyhash

**Swift**  https://github.com/lemire/SwiftWyhashBenchmark

**Swift**  https://github.com/jeudesprits/PSWyhash

**V** https://github.com/vlang/v/tree/master/vlib/hash/wyhash (v4)

**Zig** https://github.com/ManDeJan/zig-wyhash

**absl hashmap** https://github.com/abseil/abseil-cpp/blob/master/absl/hash/internal/wyhash.cc

----------------------------------------

I thank these names:

Reini Urban

Dietrich Epp

Joshua Haberman

Tommy Ettinger

Daniel Lemire

Otmar Ertl

cocowalla

leo-yuriev

Diego Barrios Romero

paulie-g 

dumblob

Yann Collet

ivte-ms

hyb

James Z.M. Gao

easyaspi314 (Devin)

TheOneric

