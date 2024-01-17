#pragma once

#include <algorithm>
#include <util/generic/vector.h>

namespace NClickHouse {
    template <typename T>
    TVector<T> SliceVector(const TVector<T>& vec, size_t begin, size_t len) {
        TVector<T> result;

        if (begin < vec.size()) {
            len = std::min(len, vec.size() - begin);
            result.assign(vec.begin() + begin, vec.begin() + (begin + len));
        }

        return result;
    }

}
