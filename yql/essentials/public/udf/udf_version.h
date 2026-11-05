#pragma once

#include <yql/essentials/public/udf/abi_version_check/abi_version_check.h>

#include <util/generic/fwd.h>
#include <util/system/defaults.h>
#include <util/system/types.h>

namespace NYql::NUdf {

// The ABI of this revision: a trunk host and everything linked into it.
#define CURRENT_UDF_ABI_VERSION_MAJOR 2
#define CURRENT_UDF_ABI_VERSION_MINOR 48
#define CURRENT_UDF_ABI_VERSION_PATCH 0

// The ABI every deployed host provides, and the only one a shared udf may rely on.
#define STABLE_UDF_ABI_VERSION_MAJOR 2
#define STABLE_UDF_ABI_VERSION_MINOR 47
#define STABLE_UDF_ABI_VERSION_PATCH 0

#ifdef USE_CURRENT_UDF_ABI_VERSION
    #define UDF_ABI_VERSION_MAJOR CURRENT_UDF_ABI_VERSION_MAJOR
    #define UDF_ABI_VERSION_MINOR CURRENT_UDF_ABI_VERSION_MINOR
    #define UDF_ABI_VERSION_PATCH CURRENT_UDF_ABI_VERSION_PATCH
#elif defined(USE_STABLE_UDF_ABI_VERSION)
    #define UDF_ABI_VERSION_MAJOR STABLE_UDF_ABI_VERSION_MAJOR
    #define UDF_ABI_VERSION_MINOR STABLE_UDF_ABI_VERSION_MINOR
    #define UDF_ABI_VERSION_PATCH STABLE_UDF_ABI_VERSION_PATCH
#else
    #if !defined(UDF_ABI_VERSION_MAJOR) || !defined(UDF_ABI_VERSION_MINOR) || !defined(UDF_ABI_VERSION_PATCH)
        #error Please use UDF_ABI_VERSION macro to define ABI version
    #endif
#endif

inline const char* CurrentAbiVersionStr()
{
#define STR(s) #s
#define XSTR(s) STR(s)

    return XSTR(UDF_ABI_VERSION_MAJOR) "." XSTR(UDF_ABI_VERSION_MINOR) "." XSTR(UDF_ABI_VERSION_PATCH);

#undef STR
#undef XSTR
}

#define UDF_ABI_COMPATIBILITY_VERSION(MAJOR, MINOR) ((MAJOR) * 100 + (MINOR))
#define UDF_ABI_COMPATIBILITY_VERSION_CURRENT UDF_ABI_COMPATIBILITY_VERSION(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR)

static_assert(UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8),
              "UDF ABI versions below 2.8 are no longer supported");

// NOLINTNEXTLINE(misc-redundant-expression)
static_assert(UDF_ABI_COMPATIBILITY_VERSION_CURRENT <=
                  UDF_ABI_COMPATIBILITY_VERSION(CURRENT_UDF_ABI_VERSION_MAJOR, CURRENT_UDF_ABI_VERSION_MINOR),
              "UDF ABI version " Y_STRINGIZE(UDF_ABI_VERSION_MAJOR) "." Y_STRINGIZE(UDF_ABI_VERSION_MINOR) " is above " Y_STRINGIZE(CURRENT_UDF_ABI_VERSION_MAJOR) "." Y_STRINGIZE(CURRENT_UDF_ABI_VERSION_MINOR));

constexpr ui32 MakeAbiVersion(ui8 major, ui8 minor, ui8 patch)
{
    return major * 10000 + minor * 100 + patch;
}

static_assert(MakeAbiVersion(STABLE_UDF_ABI_VERSION_MAJOR, STABLE_UDF_ABI_VERSION_MINOR, STABLE_UDF_ABI_VERSION_PATCH) <
                  MakeAbiVersion(CURRENT_UDF_ABI_VERSION_MAJOR, CURRENT_UDF_ABI_VERSION_MINOR, CURRENT_UDF_ABI_VERSION_PATCH),
              "The stable baseline must stay below the ABI of this revision");

constexpr ui16 MakeAbiCompatibilityVersion(ui8 major, ui8 minor)
{
    return major * 100 + minor;
}

constexpr ui32 CurrentAbiVersion()
{
    return MakeAbiVersion(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR, UDF_ABI_VERSION_PATCH);
}

// What YQL_ABI_VERSION() asks of the ABI the module is built at.
#if defined(MIN_UDF_ABI_VERSION_MAJOR) && defined(MIN_UDF_ABI_VERSION_MINOR) && defined(MIN_UDF_ABI_VERSION_PATCH)
static_assert(MakeAbiVersion(MIN_UDF_ABI_VERSION_MAJOR, MIN_UDF_ABI_VERSION_MINOR, MIN_UDF_ABI_VERSION_PATCH) <= CurrentAbiVersion(),
              "The module asks for a UDF ABI newer than the one it is built at");
#endif

constexpr ui32 CurrentCompatibilityAbiVersion()
{
    return MakeAbiCompatibilityVersion(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR);
}

constexpr bool IsAbiCompatible(ui32 version)
{
    // backward compatibility in greater minor versions of host
    return version / 10000 == UDF_ABI_VERSION_MAJOR &&
           (version / 100) % 100 <= UDF_ABI_VERSION_MINOR;
}

// The load aborts when two translation units in a binary hold different ABIs, which would have
// one version's code walk the other version's layout. The configure-time check sees modules only.
//
// Left out of bitcode: the JIT resolves an external call only against the symbols the host maps
// for it, and a host deployed before this check maps none, so the call would fail its load.
#ifndef LLVM_BC

// One per linked module, an inline variable being merged: a udf gets its own, the host exporting
// nothing for it to bind to. Whichever unit runs first leaves its version.
inline ui32 LoadedAbiVersion = 0;

// Internal to the unit, or the constructor would be merged into one copy carrying one version,
// with nothing to compare. A static object because MSVC has no constructor attribute.
namespace {
const struct TCheckAbiVersion {
    TCheckAbiVersion() {
        constexpr ui32 built = CurrentAbiVersion();

        LoadedAbiVersion = YqlCheckAbiVersion(LoadedAbiVersion, built);
    }
} CheckAbiOnLoad;
} // namespace

#endif

} // namespace NYql::NUdf

namespace NKikimr {
namespace NUdf = ::NYql::NUdf;
} // namespace NKikimr
