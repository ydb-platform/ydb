wyhash has evolved into [rapidhash](https://github.com/Nicoshev/rapidhash) !  
With improved speed, quality and compatibility.
====

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

**Limitations:**

It is known now that wyhash/wyrand have their limitations:

Both of them are not 64 bit collision resistant, but is about 62 bits (flyingmutant/Cyan4973/vigna)

When test on longer dataset (32TB, 23 days), wyrand will fail practrand (vigna)

And there may be more flaws detected in the future. 

User should make their own decision based the advantage and the flaws of wyhash/wyrand as no one is perfect.

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

**Java** https://github.com/dynatrace-oss/hash4j (final version 3 and 4)

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

**absl hashmap** https://github.com/abseil/abseil-cpp/blob/master/absl/hash/internal/low_level_hash.h

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

flyingmutant

vigna

tansy
