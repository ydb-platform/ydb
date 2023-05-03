#pragma once

#include "util/system/types.h"

namespace NKikimr {

enum EPercentileLevel {
    EPL_50 /* "50" */,
    EPL_95 /* "95" */,
    EPL_99 /* "99" */,
    EPL_100 /* "100" */,
    EPL_COUNT /* "EPL_COUNT" */
};

constexpr ui32 EPL_COUNT_NUM = static_cast<ui32>(EPL_COUNT);

}  // namespace NKikimr
