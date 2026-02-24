#include "yql_langver.h"

#include <util/string/cast.h>

#include <vector>
#include <utility>

namespace NYql {

namespace {

constexpr TLangVersion MaxReleasedLangVersion = MakeLangVersion(2025, 4);

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
const std::pair<ui32, ui32> Versions[] = {
#include "yql_langver_list.inc"
};

} // namespace

bool IsValidLangVersion(TLangVersion ver) {
    for (size_t i = 0; i < Y_ARRAY_SIZE(Versions); ++i) {
        if (ver == MakeLangVersion(Versions[i].first, Versions[i].second)) {
            return true;
        }
    }

    return false;
}

bool ParseLangVersion(TStringBuf str, TLangVersion& result) {
    result = UnknownLangVersion;
    if (str.size() != 7 || str[4] != '.') {
        return false;
    }

    ui32 year, minor;
    if (!TryFromString(str.SubString(0, 4), year)) {
        return false;
    }

    if (!TryFromString(str.SubString(5, 2), minor)) {
        return false;
    }

    result = MakeLangVersion(year, minor);
    return true;
}

bool FormatLangVersion(TLangVersion ver, TLangVersionBuffer& buffer, TStringBuf& result) {
    ui32 year = GetYearFromLangVersion(ver);
    if (year > 9999) {
        return false;
    }

    ui32 minor = GetMinorFromLangVersion(ver);
    Y_ASSERT(minor < 100);
    if (ToString(year, buffer.data() + 0, 4) != 4) {
        return false;
    }

    buffer[4] = '.';
    if (ToString(minor, buffer.data() + 5, 2) == 1) {
        buffer[6] = buffer[5];
        buffer[5] = '0';
    }

    buffer[7] = 0;
    result = TStringBuf(buffer.data(), buffer.size() - 1);
    return true;
}

TMaybe<TString> FormatLangVersion(TLangVersion ver) {
    TLangVersionBuffer buffer;
    TStringBuf result;
    if (!FormatLangVersion(ver, buffer, result)) {
        return Nothing();
    }

    return TString(result);
}

TLangVersion GetMaxReleasedLangVersion() {
    return MaxReleasedLangVersion;
}

TLangVersion GetMaxLangVersion() {
    TLangVersion max = 0;
    for (size_t i = 0; i < Y_ARRAY_SIZE(Versions); ++i) {
        auto v = MakeLangVersion(Versions[i].first, Versions[i].second);
        max = Max(max, v);
    }

    return max;
}

bool IsBackwardCompatibleFeatureAvailable(TLangVersion currentVer, TLangVersion featureVer,
                                          EBackportCompatibleFeaturesMode mode) {
    switch (mode) {
        case EBackportCompatibleFeaturesMode::All:
            return true;
        case EBackportCompatibleFeaturesMode::Released:
            return IsAvailableLangVersion(featureVer, GetMaxReleasedLangVersion());
        case EBackportCompatibleFeaturesMode::None:
            return IsAvailableLangVersion(featureVer, currentVer);
    }
}

void EnumerateLangVersions(const std::function<void(TLangVersion)>& callback) {
    for (size_t i = 0; i < Y_ARRAY_SIZE(Versions); ++i) {
        callback(MakeLangVersion(Versions[i].first, Versions[i].second));
    }
}

} // namespace NYql
