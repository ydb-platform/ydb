#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <compare>

namespace NKikimr::NMiniKQL {

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
    #define MKQL_RUNTIME_VERSION 77U
#endif

class TRuntimeVersion {
public:
    constexpr explicit TRuntimeVersion(ui32 version)
        : Version_(version)
    {
    }

    constexpr ui32 Value() const {
        return Version_;
    }

    constexpr std::strong_ordering operator<=>(ui32 other) const {
        if (other < MinSupportedRuntimeVersion) {
            throw yexception() << "Runtime version must be >= " << MinSupportedRuntimeVersion << ", but got " << other;
        }
        return Version_ <=> other;
    }

    // History:
    // releases/ydb/stable-25-4: 67U
    // releases/ydb/stable-26-1: 67U
    static constexpr ui32 MinSupportedRuntimeVersion = 67U;

private:
    ui32 Version_;
};

inline constexpr TRuntimeVersion RuntimeVersion{MKQL_RUNTIME_VERSION};

static_assert(RuntimeVersion >= TRuntimeVersion::MinSupportedRuntimeVersion);

} // namespace NKikimr::NMiniKQL
