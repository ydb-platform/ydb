#pragma once

#include <yql/essentials/public/langver/yql_langver.h>

#include <expected>

namespace NYql {

struct TFeature {
    const TStringBuf Name;
    const TStringBuf Description;
    const TLangVersion MinLangVer = UnknownLangVersion;
    const TLangVersion MaxLangVer = UnknownLangVersion;

    static consteval TFeature Checked(TFeature f) {
        if (f.MinLangVer != UnknownLangVersion && !IsValidLangVersion(f.MinLangVer)) {
            throw std::invalid_argument("Bad MinLangVer");
        }

        if (f.MaxLangVer != UnknownLangVersion && !IsValidLangVersion(f.MaxLangVer)) {
            throw std::invalid_argument("Bad MaxLangVer");
        }

        return f;
    }
};

bool IsAvailableOn(
    TLangVersion current,
    EBackportCompatibleFeaturesMode mode,
    const TFeature& feature);

std::expected<std::monostate, TString> EnsureIsAvailableOn(
    TLangVersion current,
    EBackportCompatibleFeaturesMode mode,
    const TFeature& feature);

} // namespace NYql
