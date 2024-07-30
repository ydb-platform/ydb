#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

// Problem: rolling update of services based on minikql runtime (example: YDB)
// requires careful management of minikql runtime versions. A minikql program
// that was built with a YDB stable-19-6 must work correctly at YDB stable-19-4
// (previous stable) if YDB rollback happens.
//
// Solution: we support minikql runtime version. Every incompatible change to
// minikql runtime increments MKQL_RUNTIME_VERSION. A user of minikql runtime
// (YDB for example) manually chooses 'RuntimeVersion' to provide major/minor
// releases compatibility. For instance, if YDB stable-19-4 support version X,
// the owner of YDB stable-19-6 sets RuntimeVersion to version X to allow
// graceful rolling update. When there is not chance to rollback to previous
// version, for instance stable-19-4, the YDB owner can switch to the new
// version Y (Y > X).
//
// Details: https://wiki.yandex-team.ru/yql/runtime/

// 1. Bump this version every time incompatible runtime nodes are introduced.
// 2. Make sure you provide runtime node generation for previous runtime versions.
#ifndef MKQL_RUNTIME_VERSION
#define MKQL_RUNTIME_VERSION 47U
#endif

// History:
// v4  is the version supported by kikimr-19-6
// v14 is the version supported by kikimr-20-2
// v14 is the version supported by kikimr-20-2
// v21 is the version supported by kikimr-20-4
// v21 is the version supported by kikimr-20-6
// v23 is the version supported by kikimr-21-2
// v24 is the version supported by kikimr-21-4
// v29 is the version supported by kikimr-22-2
// v30 is the version supported by kikimr-22-4
// v32 is the version supported by kikimr-23-1
// v39 is the version supported by kikimr-23-3
// v47 is the version supported by kikimr-24-1
constexpr ui32 RuntimeVersion = MKQL_RUNTIME_VERSION;

}
}
