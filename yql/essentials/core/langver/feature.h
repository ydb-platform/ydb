#pragma once

#include <yql/essentials/public/langver/yql_langver.h>

#include <expected>

namespace NYql {

struct TFeature {
    TString Name;
    TString Description;
    TLangVersion MinLangVer = UnknownLangVersion;
    TLangVersion MaxLangVer = UnknownLangVersion;
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
