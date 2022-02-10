//
// Created by Evgeny Sidorov on 12/04/17.
//

#include "proxy_avx2.h"
#include <library/cpp/digest/argonish/internal/argon2/argon2_base.h>
#include <library/cpp/digest/argonish/internal/argon2/argon2_avx2.h>
#include <library/cpp/digest/argonish/internal/blake2b/blake2b.h>
#include <library/cpp/digest/argonish/internal/blake2b/blake2b_avx2.h>

#define ZEROUPPER _mm256_zeroupper();

namespace NArgonish {
    ARGON2_PROXY_CLASS_IMPL(AVX2)
    BLAKE2B_PROXY_CLASS_IMPL(AVX2)
}

#undef ZEROUPPER
