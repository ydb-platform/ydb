#include "request_features.h"
#include <util/string/join.h>
#include <set>

namespace NYql {
TString TFeaturesExtractor::GetRemainedParamsString() const {
    std::set<TString> features;
    for (auto&& i : Features) {
        features.emplace(i.first);
    }
    return JoinSeq(",", features);
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

}
