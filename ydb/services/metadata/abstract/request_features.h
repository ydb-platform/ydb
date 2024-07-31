#pragma once
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <map>
#include <optional>
#include <unordered_set>

namespace NYql {
class TFeaturesExtractor: TNonCopyable {
private:
    THashMap<TString, TString> Features;
    std::unordered_set<TString> ResetFeatures;

public:
    TFeaturesExtractor(const THashMap<TString, TString>& features, const std::unordered_set<TString>& resetFeatures)
        : Features(features)
        , ResetFeatures(resetFeatures)
    {}

    TFeaturesExtractor(THashMap<TString, TString>&& features, std::unordered_set<TString>&& resetFeatures)
        : Features(std::move(features))
        , ResetFeatures(std::move(resetFeatures))
    {}

    bool IsFinished() const {
        return Features.empty() && ResetFeatures.empty();
    }

    TString GetRemainedParamsString() const;

    // Throws exception on failure
    void ValidateResetFeatures() const;

    template <class T>
    std::optional<T> Extract(const TString& featureId, const std::optional<T> noExistsValue = {}) {
        T result;
        auto it = Features.find(featureId);
        if (it == Features.end()) {
            return noExistsValue;
        } else if (TryFromString(it->second, result)) {
            Features.erase(it);
            return result;
        } else {
            return {};
        }
    }

    std::optional<TString> Extract(const TString& paramName);

    bool ExtractResetFeature(const TString& paramName);
};
}
