#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NYql::NUdf {

#define CURRENT_UDF_ABI_VERSION_MAJOR 2
#define CURRENT_UDF_ABI_VERSION_MINOR 45
#define CURRENT_UDF_ABI_VERSION_PATCH 0

#ifdef USE_CURRENT_UDF_ABI_VERSION
    #define UDF_ABI_VERSION_MAJOR CURRENT_UDF_ABI_VERSION_MAJOR
    #define UDF_ABI_VERSION_MINOR CURRENT_UDF_ABI_VERSION_MINOR
    #define UDF_ABI_VERSION_PATCH CURRENT_UDF_ABI_VERSION_PATCH
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

// NOLINTNEXTLINE(misc-redundant-expression)
static_assert(UDF_ABI_COMPATIBILITY_VERSION_CURRENT <=
                  UDF_ABI_COMPATIBILITY_VERSION(CURRENT_UDF_ABI_VERSION_MAJOR, CURRENT_UDF_ABI_VERSION_MINOR),
              "UDF ABI version " Y_STRINGIZE(UDF_ABI_VERSION_MAJOR) "." Y_STRINGIZE(UDF_ABI_VERSION_MINOR) " is above " Y_STRINGIZE(CURRENT_UDF_ABI_VERSION_MAJOR) "." Y_STRINGIZE(CURRENT_UDF_ABI_VERSION_MINOR));

constexpr ui32 MakeAbiVersion(ui8 major, ui8 minor, ui8 patch)
{
    return major * 10000 + minor * 100 + patch;
}

constexpr ui16 MakeAbiCompatibilityVersion(ui8 major, ui8 minor)
{
    return major * 100 + minor;
}

constexpr ui32 CurrentAbiVersion()
{
    return MakeAbiVersion(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR, UDF_ABI_VERSION_PATCH);
}

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

TString AbiVersionToStr(ui32 version);

} // namespace NYql::NUdf

namespace NKikimr {
namespace NUdf = ::NYql::NUdf;
} // namespace NKikimr
