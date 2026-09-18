#pragma once

#include <util/system/types.h>

#include <cstring>

#if defined(__linux__) && defined(__x86_64__)
#include <contrib/libs/isa-l/include/erasure_code.h>

extern "C" {
void ec_encode_data_avx512(int, int, int, unsigned char*, unsigned char**, unsigned char**) __attribute__((weak));
void ec_encode_data_avx2_gfni(int, int, int, unsigned char*, unsigned char**, unsigned char**) __attribute__((weak));
void ec_encode_data_avx512_gfni(int, int, int, unsigned char*, unsigned char**, unsigned char**) __attribute__((weak));
}
#endif

namespace NKikimr {

// A read-only diagnostic of vendored ISA-L 2.31's Linux x86 dispatcher. The
// public entry is ENDBR64 (optional) followed by JMP [RIP + displacement].
// Resolve the already-initialized pointer and compare actual function addresses;
// do not infer dispatch from CPUID. A changed stub is explicitly unobserved.
// This never invokes, selects or modifies a codec implementation.
inline const char* ObservedIsaLEncodeDispatcher() {
#if defined(__linux__) && defined(__x86_64__)
    const auto* code = reinterpret_cast<const ui8*>(&ec_encode_data);
    if (code[0] == 0xf3 && code[1] == 0x0f && code[2] == 0x1e && code[3] == 0xfa) code += 4;
    if (code[0] != 0xff || code[1] != 0x25) return "unobserved: unknown dispatcher stub";
    i32 displacement;
    memcpy(&displacement, code + 2, sizeof(displacement));
    using TFunction = decltype(&ec_encode_data);
    TFunction target;
    memcpy(&target, code + 6 + displacement, sizeof(target));
#define MATCH_IMPL(name) if (target == &name) return #name
    MATCH_IMPL(ec_encode_data_base);
    MATCH_IMPL(ec_encode_data_sse);
    MATCH_IMPL(ec_encode_data_avx);
    MATCH_IMPL(ec_encode_data_avx2);
    MATCH_IMPL(ec_encode_data_avx512);
    MATCH_IMPL(ec_encode_data_avx2_gfni);
    MATCH_IMPL(ec_encode_data_avx512_gfni);
#undef MATCH_IMPL
    return "unobserved: unknown dispatcher target";
#else
    return "unobserved: platform dispatcher not instrumented";
#endif
}

} // namespace NKikimr
