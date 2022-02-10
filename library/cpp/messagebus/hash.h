#pragma once

#include <util/str_stl.h>
#include <util/digest/numeric.h>

namespace NBus {
    namespace NPrivate {
        template <typename T>
        size_t Hash(const T& val) {
            return THash<T>()(val);
        }

        template <typename T, typename U>
        size_t HashValues(const T& a, const U& b) {
            return CombineHashes(Hash(a), Hash(b));
        }

    }
}
