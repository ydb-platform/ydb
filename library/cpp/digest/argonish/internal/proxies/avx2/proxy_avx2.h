#pragma once

#include <util/generic/yexception.h>
#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/digest/argonish/blake2b.h>
#include <library/cpp/digest/argonish/internal/proxies/macro/proxy_macros.h>

namespace NArgonish {
    ARGON2_PROXY_CLASS_DECL(AVX2)
    BLAKE2B_PROXY_CLASS_DECL(AVX2)
}
