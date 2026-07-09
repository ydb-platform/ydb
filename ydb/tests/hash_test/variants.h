#pragma once

#include <util/system/types.h>

// Variant tables for --functional (ai_func.md §4.3): unlike the perf
// TKernelDesc registry, these operate on arbitrary lengths and never mutate
// hidden state across calls, so they are safe to use for correctness
// comparisons (equivalence sweep, file write/verify).

struct THashVariant {
    const char* Name;               // e.g. "xxh3-sse2"
    bool (*IsSupported)();          // CPU feature gate; nullptr => always supported
    ui64 (*Hash)(const void* data, size_t len);
};

// XXH3 family, reference (scalar) first, so callers can compare every other
// entry against Variants[0]. CityHash64 has a single implementation and
// nothing to cross-check, so it is not part of this table (ai_func.md §4.3).
const THashVariant* GetXxh3Variants(size_t* count);

struct TCipherVariant {
    const char* Name;               // e.g. "chacha-sse"
    bool (*IsSupported)();          // CPU feature gate; nullptr => always supported
    // Some vectorized implementations hard-abort on plaintext pointers with
    // the wrong alignment (e.g. ChaChaVec requires 0 or 8 mod 16 -- see
    // chacha_vec.cpp). nullptr => any alignment is fine. Callers that probe
    // misaligned buffers (the functional equivalence sweep) must check this
    // before invoking Encipher with a given pointer.
    bool (*IsAlignmentOk)(const void* plaintext);
    // Fresh cipher per call: SetKey(key, 32); SetIV(iv, counter64 as an
    // 8-byte little-endian block index); Encipher(src, dst, len).
    // Deterministic and position-independent -- safe to call concurrently
    // from many threads, since no state is shared across calls.
    void (*Encipher)(const ui8 key[32], const ui8 iv[8], ui64 counter64,
                     const ui8* src, ui8* dst, size_t len);
};

const TCipherVariant* GetChaChaVariants(size_t* count);

// One-shot exports of the pinned-ISA XXH3 implementations (ai_func.md §4.1):
// the symbols inside those kernel TUs are `static` (XXH_INLINE_ALL), so
// these trivial wrappers -- defined at the bottom of the respective kernel
// TUs -- are the only way to reach them from outside. Only compiled on the
// same platforms as the kernels themselves.
#if !defined(_win_)
ui64 XxhScalarHashOneShot(const void* data, size_t len);
#endif
#if !(defined(_win_) || defined(_arm64_))
ui64 XxhSse2HashOneShot(const void* data, size_t len);
ui64 XxhAvx2HashOneShot(const void* data, size_t len);
#endif
#if defined(_arm64_) && !defined(_win_)
ui64 XxhNeonHashOneShot(const void* data, size_t len);
#endif
