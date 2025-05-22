#pragma once
#include "yql_panic.h"

namespace NYql {

template<class T, class F>
[[nodiscard]]
inline T EnsureDynamicCast(F from) {
    YQL_ENSURE(from, "source should not be null");
    T result = dynamic_cast<T>(from);
    YQL_ENSURE(result, "dynamic_cast failed");
    return result;
}

} // namespace NYql
