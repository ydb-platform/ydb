#include "request_features.h"
#include <util/string/join.h>
#include <set>

namespace NYql {
TString TFeaturesExtractor::GetRemainedParamsString() const {
    std::set<TString> features;
    for (auto&& i : Features) {
        features.emplace(i.first);
    }
    for (auto&& feature : ResetFeatures) {
        features.emplace(feature);
    }
    return JoinSeq(", ", features);
}

void TFeaturesExtractor::ValidateResetFeatures() const {
    std::set<TString> duplicateFeatures;
    for (auto&& feature : ResetFeatures) {
        if (Features.contains(feature)) {
            duplicateFeatures.emplace(feature);
        }
    }
    if (!duplicateFeatures.empty()) {
        throw yexception() << "Duplicate reset feature: " << JoinSeq(", ", duplicateFeatures);
    }
}

std::optional<TString> TFeaturesExtractor::Extract(const TString& paramName) {
    auto it = Features.find(paramName);
    if (it == Features.end()) {
        return {};
    } else {
        const TString result = it->second;
        Features.erase(it);
        return result;
    }
}

bool TFeaturesExtractor::ExtractResetFeature(const TString& paramName) {
    auto it = ResetFeatures.find(paramName);
    if (it == ResetFeatures.end()) {
        return false;
    }
    ResetFeatures.erase(it);
    return true;
}

}
