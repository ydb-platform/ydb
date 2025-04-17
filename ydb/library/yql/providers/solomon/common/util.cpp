#include "util.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYql::NSo {

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType) {
    switch (clusterType) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            return NSo::NProto::ESolomonClusterType::CT_SOLOMON;
        case TSolomonClusterConfig::SCT_MONITORING:
            return NSo::NProto::ESolomonClusterType::CT_MONITORING;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterType));
    }
}

std::map<TString, TString> ExtractSelectorValues(const TString& selectors) {
    std::map<TString, TString> result;

    auto selectorValues = StringSplitter(selectors.substr(1, selectors.size() - 2)).Split(',').SkipEmpty().ToList<TString>();
    for (const auto& selectorValue : selectorValues) {
        size_t eqPos = selectorValue.find("=");
        TString key = StripString(selectorValue.substr(0, eqPos));
        TString value = StripString(selectorValue.substr(eqPos + 1, selectorValue.size() - eqPos - 1));
        result[key] = value.substr(1, value.size() - 2);
    }

    return result;
}

} // namespace NYql::NSo