#include "feature.h"

#include <util/string/builder.h>

namespace NYql {

namespace {

template <typename TError>
std::expected<std::monostate, TError> GetAvailability(
    TLangVersion current,
    EBackportCompatibleFeaturesMode mode,
    const TFeature& feature)
{
    if (auto v = feature.MinLangVer; !IsBackwardCompatibleFeatureAvailable(current, v, mode)) {
        if constexpr (std::is_same_v<TError, TString>) {
            return std::unexpected(
                TStringBuilder()
                << feature.Description << " is not available before language version "
                << NYql::FormatLangVersion(v));
        } else if constexpr (std::is_same_v<TError, std::monostate>) {
            return std::unexpected(std::monostate());
        } else {
            static_assert(false, "Bad TError");
        }
    }

    if (auto v = feature.MaxLangVer; !IsAvailableLangVersion(current, v)) {
        if constexpr (std::is_same_v<TError, TString>) {
            return std::unexpected(
                TStringBuilder()
                << feature.Description << " is not available after language version "
                << NYql::FormatLangVersion(v));
        } else if constexpr (std::is_same_v<TError, std::monostate>) {
            return std::unexpected(std::monostate());
        } else {
            static_assert(false, "Bad TError");
        }
    }

    return std::monostate();
}

} // namespace

bool IsAvailableOn(
    TLangVersion current,
    EBackportCompatibleFeaturesMode mode,
    const TFeature& feature)
{
    return GetAvailability<std::monostate>(current, mode, feature).has_value();
}

std::expected<std::monostate, TString> EnsureIsAvailableOn(
    TLangVersion current,
    EBackportCompatibleFeaturesMode mode,
    const TFeature& feature)
{
    return GetAvailability<TString>(current, mode, feature);
}

} // namespace NYql
