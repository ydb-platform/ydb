#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <optional>
#include <unordered_set>

namespace NYql {

class TFeaturesExtractor : TMoveOnly {
public:
    struct TFeature {
        TFeature(TString&& value);

        TFeature(const TString& value);

        TFeature(TStringBuf value);

        TFeature(THashMap<TString, TFeature>&& value);

        std::variant<TString, THashMap<TString, TFeature>> Value;
    };

    using TFeatures = THashMap<TString, TFeature>;

private:
    TFeatures Features;
    std::unordered_set<TString> ResetFeatures;

public:
    TFeaturesExtractor(THashMap<TString, TString>&& features, std::unordered_set<TString>&& resetFeatures);

    TFeaturesExtractor(const THashMap<TString, TString>& features, const std::unordered_set<TString>& resetFeatures);

    TFeaturesExtractor(TFeatures&& features, std::unordered_set<TString>&& resetFeatures);

    TFeaturesExtractor(const TFeatures& features, const std::unordered_set<TString>& resetFeatures);

    bool IsFinished() const;

    TString GetRemainedParamsString() const;

    // Returns error description
    [[nodiscard]] std::optional<TString> ValidateResetFeatures() const;

    template <class T>
    std::optional<T> Extract(const TString& featureId, std::optional<T> noExistsValue = {}) {
        const auto it = Features.find(featureId);
        if (it == Features.end() || !std::holds_alternative<TString>(it->second.Value)) {
            return noExistsValue;
        }

        if (T result; TryFromString(std::get<TString>(it->second.Value), result)) {
            Features.erase(it);
            return result;
        }

        return {};
    }

    std::optional<TString> Extract(const TString& featureId);

    std::optional<std::variant<TString, TFeaturesExtractor>> ExtractNested(const TString& featureId);

    bool ExtractResetFeature(const TString& featureId);

private:
    static void GetRemainedFeatures(const TFeatures& features, const TString& prefix, std::unordered_set<TString>& result);
};

} // namespace NYql
