#pragma once

#include <util/system/types.h>

namespace NKikimr::NSsa {
// Problem: rolling update of services based on SSA runtime (example: YDB)
// requires careful management of SSA runtime versions. A SSA program
// that was built with a YDB stable-23-1 must work correctly at YDB stable-22-4
// (previous stable) if YDB rollback happens.
//
// Solution: we support SSA runtime version. Every incompatible change to
// SSA runtime increments SSA_RUNTIME_VERSION. A user of SSA runtime
// (YDB for example) manually chooses 'RuntimeVersion' to provide major/minor
// releases compatibility. For instance, if YDB stable-22-4 support version X,
// the owner of YDB stable-23-1 sets RuntimeVersion to version X to allow
// graceful rolling update. When there is not chance to rollback to previous
// version, for instance stable-22-4, the YDB owner can switch to the new
// version Y (Y > X).

// Bump this version every time incompatible runtime functions are introduced.
#ifndef SSA_RUNTIME_VERSION
#define SSA_RUNTIME_VERSION 5U
#endif

// History:
// v1 is the version supported by kikimr-22-4. Supports filter and cast(timestamp to uint64) pushdowns.
// v2 is the version supported by kikimr-23-1. Supports LIKE filter for Utf8 type, COUNT(col), COUNT(*), SUM(), MIN(), MAX(), AVG(), SOME() aggregations.
// v3 is the version supported by kikimr-23-3. Supports LIKE filter for String type, JSON_VALUE and JSON_EXISTS functions in filters
// v4 is the version supported by kikimr-24-1. Supports any comparsions and arithmetics on YQL kernels.
constexpr ui32 RuntimeVersion = SSA_RUNTIME_VERSION;

}
