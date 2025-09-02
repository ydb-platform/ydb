#pragma once
#include <util/generic/strbuf.h>
#include <util/system/types.h>

#include <array>

namespace NYql {

using TLangVersion = ui32;

constexpr TLangVersion UnknownLangVersion = 0;

constexpr inline TLangVersion MakeLangVersion(ui32 year, ui32 minor) {
    return year * 100u + minor;
}

constexpr inline ui32 GetYearFromLangVersion(TLangVersion ver) {
    return ver / 100u;
}

constexpr inline ui32 GetMinorFromLangVersion(TLangVersion ver) {
    return ver % 100u;
}

constexpr inline bool IsAvailableLangVersion(TLangVersion ver, TLangVersion max) {
    if (ver == UnknownLangVersion || max == UnknownLangVersion) {
        return true;
    }

    return ver <= max;
}

constexpr inline bool IsDeprecatedLangVersion(TLangVersion ver, TLangVersion max) {
    if (ver == UnknownLangVersion || max == UnknownLangVersion) {
        return false;
    }

    return GetYearFromLangVersion(ver) == GetYearFromLangVersion(max) - 2;
}

constexpr inline bool IsUnsupportedLangVersion(TLangVersion ver, TLangVersion max) {
    if (ver == UnknownLangVersion || max == UnknownLangVersion) {
        return false;
    }

    return GetYearFromLangVersion(ver) <= GetYearFromLangVersion(max) - 3;
}

constexpr TLangVersion MinLangVersion = MakeLangVersion(2025, 1);

TLangVersion GetMaxReleasedLangVersion();
TLangVersion GetMaxLangVersion();

constexpr ui32 LangVersionBufferSize = 4 + 1 + 2 + 1; // year.minor\0
using TLangVersionBuffer = std::array<char, LangVersionBufferSize>;

bool IsValidLangVersion(TLangVersion ver);
bool ParseLangVersion(TStringBuf str, TLangVersion& result);
bool FormatLangVersion(TLangVersion ver, TLangVersionBuffer& buffer, TStringBuf& result);

enum class EBackportCompatibleFeaturesMode {
    None,
    Released,
    All
};

bool IsBackwardCompatibleFeatureAvailable(TLangVersion currentVer, TLangVersion featureVer,
    EBackportCompatibleFeaturesMode mode);

}
