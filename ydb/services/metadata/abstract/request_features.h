#pragma once
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <map>
#include <optional>

namespace NYql {
class TFeaturesExtractor: TNonCopyable {
private:
    THashMap<TString, TString> Features;
public:
    explicit TFeaturesExtractor(const THashMap<TString, TString>& features)
        : Features(features) {

    }

    explicit TFeaturesExtractor(THashMap<TString, TString>&& features)
        : Features(std::move(features)) {

    }

    bool IsFinished() const {
        return Features.empty();
    }

    TString GetRemainedParamsString() const;

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
};
}
