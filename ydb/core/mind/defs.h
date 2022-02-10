#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/mind/defs.h
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>

namespace NKikimr {

    // ensure that the type of passed variable is the same as given one
    template<typename T, typename U>
    inline T EnsureType(U &&value) {
        static_assert(std::is_same<T, U>::value, "unexpected returned value type");
        return std::move(value);
    }

}
