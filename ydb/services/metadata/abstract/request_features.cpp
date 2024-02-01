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

}
