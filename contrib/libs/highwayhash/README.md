Strong (well-distributed and unpredictable) hashes:

*   Portable implementation of
    [SipHash](https://www.131002.net/siphash/siphash.pdf)
*   HighwayHash, a 5x faster SIMD hash with [security
    claims](https://arxiv.org/abs/1612.06257)

## Quick Start

To build on a Linux or Mac platform, simply run `make`. For Windows, we provide
a Visual Studio 2015 project in the `msvc` subdirectory.

Run `benchmark` for speed measurements. `sip_hash_test` and `highwayhash_test`
ensure the implementations return known-good values for a given set of inputs.

64-bit SipHash for any CPU:

    #include "highwayhash/sip_hash.h"
    using namespace highwayhash;
    const HH_U64 key2[2] HH_ALIGNAS(16) = {1234, 5678};
    char in[8] = {1};
    return SipHash(key2, in, 8);

64, 128 or 256 bit HighwayHash for the CPU determined by compiler flags:

    #include "highwayhash/highwayhash.h"
    using namespace highwayhash;
    const HHKey key HH_ALIGNAS(32) = {1, 2, 3, 4};
    char in[8] = {1};
    HHResult64 result;  // or HHResult128 or HHResult256
    HHStateT<HH_TARGET> state(key);
    HighwayHashT(&state, in, 8, &result);

64, 128 or 256 bit HighwayHash for the CPU on which we're currently running:

    #include "highwayhash/highwayhash_target.h"
    #include "highwayhash/instruction_sets.h"
    using namespace highwayhash;
    const HHKey key HH_ALIGNAS(32) = {1, 2, 3, 4};
    char in[8] = {1};
    HHResult64 result;  // or HHResult128 or HHResult256
    InstructionSets::Run<HighwayHash>(key, in, 8, &result);

C-callable 64-bit HighwayHash for the CPU on which we're currently running:

    #include "highwayhash/c_bindings.h"
    const uint64_t key[4] = {1, 2, 3, 4};
    char in[8] = {1};
    return HighwayHash64(key, in, 8);

## Introduction

Hash functions are widely used, so it is desirable to increase their speed and
security. This package provides two 'strong' (well-distributed and
unpredictable) hash functions: a faster version of SipHash, and an even faster
algorithm we call HighwayHash.

SipHash is a fast but 'cryptographically strong' pseudo-random function by
Aumasson and Bernstein [https://www.131002.net/siphash/siphash.pdf].

HighwayHash is a new way of mixing inputs which may inspire new
cryptographically strong hashes. Large inputs are processed at a rate of 0.24
cycles per byte, and latency remains low even for small inputs. HighwayHash is
faster than SipHash for all input sizes, with 5 times higher throughput at 1
KiB. We discuss design choices and provide statistical analysis and preliminary
cryptanalysis in https://arxiv.org/abs/1612.06257.

## Applications

Unlike prior strong hashes, these functions are fast enough to be recommended
as safer replacements for weak hashes in many applications. The additional CPU
cost appears affordable, based on profiling data indicating C++ hash functions
account for less than 0.25% of CPU usage.

Hash-based selection of random subsets is useful for A/B experiments and similar
applications. Such random generators are idempotent (repeatable and
deterministic), which is helpful for parallel algorithms and testing. To avoid
bias, it is important that the hash function be unpredictable and
indistinguishable from a uniform random generator. We have verified the bit
distribution and avalanche properties of SipHash and HighwayHash.

64-bit hashes are also useful for authenticating short-lived messages such as
network/RPC packets. This requires that the hash function withstand
differential, length extension and other attacks. We have published a formal
security analysis for HighwayHash. New cryptanalysis tools may still need to be
developed for further analysis.

Strong hashes are also important parts of methods for protecting hash tables
against unacceptable worst-case behavior and denial of service attacks
(see "hash flooding" below).

## SipHash

Our SipHash implementation is a fast and portable drop-in replacement for
the reference C code. Outputs are identical for the given test cases (messages
between 0 and 63 bytes).

Interestingly, it is about twice as fast as a SIMD implementation using SSE4.1
(https://goo.gl/80GBSD). This is presumably due to the lack of SIMD bit rotate
instructions.

SipHash13 is a faster but weaker variant with one mixing round per update and
three during finalization.

We also provide a data-parallel 'tree hash' variant that enables efficient SIMD
while retaining safety guarantees. This is about twice as fast as SipHash, but
does not return the same results.

## HighwayHash

We have devised a new way of mixing inputs with AVX2 multiply and permute
instructions. The multiplications are 32x32 -> 64 bits and therefore infeasible
to reverse. Permuting equalizes the distribution of the resulting bytes.

The internal state occupies four 256-bit AVX2 registers. Due to limitations of
the instruction set, the registers are partitioned into two 512-bit halves that
remain independent until the reduce phase. The algorithm outputs 64 bit digests
or up to 256 bits at no extra cost.

In addition to high throughput, the algorithm is designed for low finalization
cost. The result is more than twice as fast as SipTreeHash.

For older CPUs, we also provide an SSE4.1 version (80% as fast for large inputs
and 95% as fast for short inputs) and a portable version (10% as fast).

Statistical analyses and preliminary cryptanalysis are given in
https://arxiv.org/abs/1612.06257.

## Versioning and stability

SipHash and HighwayHash 1.0 are 'fingerprint functions' whose input -> hash
mapping will not change. This is important for applications that write hashes to
persistent storage.

HighwayHash has not yet reached 1.0 and may still change in the near future. We
will announce when it is frozen.

## Speed measurements

To measure the CPU cost of a hash function, we can either create an artificial
'microbenchmark' (easier to control, but probably not representative of the
actual runtime), or insert instrumentation directly into an application (risks
influencing the results through observer overhead). We provide novel variants of
both approaches that mitigate their respective disadvantages.

profiler.h uses software write-combining to stream program traces to memory
with minimal overhead. These can be analyzed offline, or when memory is full,
to learn how much time was spent in each (possibly nested) zone.

nanobenchmark.h enables cycle-accurate measurements of very short functions.
It uses CPU fences and robust statistics to minimize variability, and also
avoids unrealistic branch prediction effects.

We compile the C++ implementations with a patched GCC 4.9 and run on a single
core of a Xeon E5-2690 v3 clocked at 2.6 GHz. CPU cost is measured as cycles per
byte for various input sizes:

Algorithm        | 8     | 31   | 32   | 63   | 64   | 1024
---------------- | ----- | ---- | ---- | ---- | ---- | ----
HighwayHashAVX2  | 7.34  | 1.81 | 1.71 | 1.04 | 0.95 | 0.24
HighwayHashSSE41 | 8.00  | 2.11 | 1.75 | 1.13 | 0.96 | 0.30
SipTreeHash      | 16.51 | 4.57 | 4.09 | 2.22 | 2.29 | 0.57
SipTreeHash13    | 12.33 | 3.47 | 3.06 | 1.68 | 1.63 | 0.33
SipHash          | 8.13  | 2.58 | 2.73 | 1.87 | 1.93 | 1.26
SipHash13        | 6.96  | 2.09 | 2.12 | 1.32 | 1.33 | 0.68

SipTreeHash is slower than SipHash for small inputs because it processes blocks
of 32 bytes. AVX2 and SSE4.1 HighwayHash are faster than SipHash for all input
sizes due to their highly optimized handling of partial vectors.

Note that previous measurements included the initialization of their input,
which dramatically increased timings especially for small inputs.

## CPU requirements

SipTreeHash[13] requires an AVX2-capable CPU (e.g. Haswell). HighwayHash
includes a dispatcher that chooses the best available (AVX2, SSE4.1 or portable)
implementation at runtime, as well as a directly callable function template that
can only run on the CPU for which it was built. SipHash[13] and
ScalarSipTreeHash[13] have no particular CPU requirements.

Our implementations use custom AVX2 vector classes with overloaded operators
(e.g. `const V4x64U a = b + c`) for type-safety and improved readability vs.
compiler intrinsics (e.g. `const __m256i a = _mm256_add_epi64(b, c)`).

We intend to port HighwayHash to other SIMD-capable platforms, especially ARM.

Our instruction_sets dispatcher avoids running newer instructions on older CPUs
that do not support them. However, intrinsics, and therefore also any vector
classes that use them, require a compiler flag that also enables the compiler to
generate code for that CPU. This means the intrinsics must be placed in separate
translation units that are compiled with the required flags. It is important
that these source files and their headers not define any inline functions,
because that might break the one definition rule and cause crashes.

To minimize dispatch overhead when hashes are computed often (e.g. in a loop),
we can inline the hash function into its caller using templates. The dispatch
overhead will only be paid once (e.g. before the loop). The template mechanism
also avoids duplicating code in each CPU-specific implementation.

## Defending against hash flooding

To mitigate hash flooding attacks, we need to take both the hash function and
the data structure into account.

We wish to defend (web) services that utilize hash sets/maps against
denial-of-service attacks. Such data structures assign attacker-controlled
input messages `m` to a hash table bin `b` by computing the hash `H(s, m)`
using a hash function `H` seeded by `s`, and mapping it to a bin with some
narrowing function `b = R(h)`, discussed below.

Attackers may attempt to trigger 'flooding' (excessive work in insertions or
lookups) by finding multiple `m` that map to the same bin. If the attacker has
local access, they can do far worse, so we assume the attacker can only issue
remote requests. If the attacker is able to send large numbers of requests,
they can already deny service, so we need only ensure the attacker's cost is
sufficiently large compared to the service's provisioning.

If the hash function is 'weak', attackers can easily generate 'hash collisions'
(inputs mapping to the same hash values) that are independent of the seed. In
other words, certain input messages will cause collisions regardless of the seed
value. The author of SipHash has published C++ programs to generate such
'universal (key-independent) multicollisions' for CityHash and Murmur. Similar
'differential' attacks are likely possible for any hash function consisting only
of reversible operations (e.g. addition/multiplication/rotation) with a constant
operand. `n` requests with such inputs cause `n^2` work for an unprotected hash
table, which is unacceptable.

By contrast, 'strong' hashes such as SipHash or HighwayHash require infeasible
attacker effort to find a hash collision (an expected 2^32 guesses of `m` per
the birthday paradox) or recover the seed (2^63 requests). These security claims
assume the seed is secret. It is reasonable to suppose `s` is initially unknown
to attackers, e.g. generated on startup or even per-connection. A timing attack
by Wool/Bar-Yosef recovers 13-bit seeds by testing all 8K possibilities using
millions of requests, which takes several days (even assuming unrealistic 150 us
round-trip times). It appears infeasible to recover 64-bit seeds in this way.

However, attackers are only looking for multiple `m` mapping to the same bin
rather than identical hash values. We assume they know or are able to discover
the hash table size `p`. It is common to choose `p = 2^i` to enable an efficient
`R(h) := h & (p - 1)`, which simply retains the lower hash bits. It may be
easier for attackers to compute partial collisions where only the lower `i` bits
match. This can be prevented by choosing a prime `p` so that `R(h) := h % p`
incorporates all hash bits. The costly modulo operation can be avoided by
multiplying with the inverse (https://goo.gl/l7ASm8). An interesting alternative
suggested by Kyoung Jae Seo chooses a random subset of the `h` bits. Such an `R`
function can be computed in just 3 cycles using PEXT from the BMI2 instruction
set. This is expected to defend against SAT-solver attacks on the hash bits at a
slightly lower cost than the multiplicative inverse method, and still allows
power-of-two table sizes.

Summary thus far: given a strong hash function and secret seed, it appears
infeasible for attackers to generate hash collisions because `s` and/or `R` are
unknown. However, they can still observe the timings of data structure
operations for various `m`. With typical table sizes of 2^10 to 2^17 entries,
attackers can detect some 'bin collisions' (inputs mapping to the same bin).
Although this will be costly for the attacker, they can then send many instances
of such inputs, so we need to limit the resulting work for our data structure.

Hash tables with separate chaining typically store bin entries in a linked list,
so worst-case inputs lead to unacceptable linear-time lookup cost. We instead
seek optimal asymptotic worst-case complexity for each operation (insertion,
deletion and lookups), which is a constant factor times the logarithm of the
data structure size. This naturally leads to a tree-like data structure for each
bin. The Java8 HashMap only replaces its linked list with trees when needed.
This leads to additional cost and complexity for deciding whether a bin is a
list or tree.

Our first proposal (suggested by Github user funny-falcon) avoids this overhead
by always storing one tree per bin. It may also be worthwhile to store the first
entry directly in the bin, which avoids allocating any tree nodes in the common
case where bins are sparsely populated. What kind of tree should be used?
Scapegoat and splay trees only offer amortized complexity guarantees, whereas
treaps require an entropy source and have higher constant factors in practice.
Self-balancing structures such as 2-3 or red-black trees require additional
bookkeeping information. We can hope to reduce rebalancing cost by realizing
that the output bits of strong `H` functions are uniformly distributed. When
using them as keys instead of the original message `m`, recent relaxed balancing
schemes such as left-leaning red-black or weak AVL trees may require fewer tree
rotations to maintain their invariants. Note that `H` already determines the
bin, so we should only use the remaining bits. 64-bit hashes are likely
sufficient for this purpose, and HighwayHash generates up to 256 bits. It seems
unlikely that attackers can craft inputs resulting in worst cases for both the
bin index and tree key without being able to generate hash collisions, which
would contradict the security claims of strong hashes. Even if they succeed, the
relaxed tree balancing still guarantees an upper bound on height and therefore
the worst-case operation cost. For the AVL variant, the constant factors are
slightly lower than for red-black trees.

The second proposed approach uses augmented/de-amortized cuckoo hash tables
(https://goo.gl/PFwwkx). These guarantee worst-case `log n` bounds for all
operations, but only if the hash function is 'indistinguishable from random'
(uniformly distributed regardless of the input distribution), which is claimed
for SipHash and HighwayHash but certainly not for weak hashes.

Both alternatives retain good average case performance and defend against
flooding by limiting the amount of extra work an attacker can cause. The first
approach guarantees an upper bound of `log n` additional work even if the hash
function is compromised.

In summary, a strong hash function is not, by itself, sufficient to protect a
chained hash table from flooding attacks. However, strong hash functions are
important parts of two schemes for preventing denial of service. Using weak hash
functions can slightly accelerate the best-case and average-case performance of
a service, but at the risk of greatly reduced attack costs and worst-case
performance.

## Third-party implementations / bindings

Thanks to Damian Gryski for making us aware of these third-party
implementations or bindings. Please feel free to get in touch or
raise an issue and we'll add yours as well.

By | Language | URL
--- | --- | ---
Damian Gryski | Go and SSE | https://github.com/dgryski/go-highway/
Lovell Fuller | node.js bindings | https://github.com/lovell/highwayhash
Vinzent Steinberg | Rust bindings | https://github.com/vks/highwayhash-rs

## Modules

### Hashes

*   c_bindings.h declares C-callable versions of SipHash/HighwayHash.
*   sip_hash.cc is the compatible implementation of SipHash, and also provides
    the final reduction for sip_tree_hash.
*   sip_tree_hash.cc is the faster but incompatible SIMD j-lanes tree hash.
*   scalar_sip_tree_hash.cc is a non-SIMD version.
*   state_helpers.h simplifies the implementation of the SipHash variants.
*   highwayhash.h is our new, fast hash function.
*   hh_avx2.h, hh_sse41.h and hh_portable.h are its various implementations.
*   highwayhash_target.h chooses the best available implementation at runtime.

### Infrastructure

*   arch_specific.h offers byte swapping and CPUID detection.
*   compiler_specific.h defines some compiler-dependent language extensions.
*   data_parallel.h provides a C++11 ThreadPool and PerThread (similar to
    OpenMP).
*   instruction_sets.h and targets.h enable efficient CPU-specific dispatching.
*   nanobenchmark.h measures elapsed times with < 1 cycle variability.
*   os_specific.h sets thread affinity and priority for benchmarking.
*   profiler.h is a low-overhead, deterministic hierarchical profiler.
*   tsc_timer.h obtains high-resolution timestamps without CPU reordering.
*   vector256.h and vector128.h contain wrapper classes for AVX2 and SSE4.1.

By Jan Wassenberg <jan.wassenberg@gmail.com> and Jyrki Alakuijala
<jyrki.alakuijala@gmail.com>, updated 2017-02-07

This is not an official Google product.
