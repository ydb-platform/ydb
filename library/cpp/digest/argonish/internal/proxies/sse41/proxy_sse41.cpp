//
// Created by Evgeny Sidorov on 12/04/17.
//

#include "proxy_sse41.h"
#include <library/cpp/digest/argonish/internal/argon2/argon2_base.h>
#include <library/cpp/digest/argonish/internal/argon2/argon2_sse41.h>
#include <library/cpp/digest/argonish/internal/blake2b/blake2b.h>
#include <library/cpp/digest/argonish/internal/blake2b/blake2b_sse41.h>

#define ZEROUPPER ;

namespace NArgonish {
    ARGON2_PROXY_CLASS_IMPL(SSE41)
    BLAKE2B_PROXY_CLASS_IMPL(SSE41)
}

#undef ZEROUPPER
