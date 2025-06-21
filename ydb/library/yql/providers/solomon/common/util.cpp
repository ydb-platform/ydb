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
    YQL_ENSURE(selectors.size() >= 2, "Selectors should be at least 2 characters long");
    std::map<TString, TString> result;

    auto selectorValues = StringSplitter(selectors.substr(1, selectors.size() - 2)).Split(',').SkipEmpty().ToList<TString>();
    for (const auto& selectorValue : selectorValues) {
        size_t eqPos = selectorValue.find("=");
        YQL_ENSURE(eqPos <= selectorValue.size());

        TString key = StripString(selectorValue.substr(0, eqPos));
        TString value = StripString(selectorValue.substr(eqPos + 1, selectorValue.size() - eqPos - 1));
        YQL_ENSURE(!key.empty());
        YQL_ENSURE(value.size() >= 2);

        result[key] = value.substr(1, value.size() - 2);
    }

    return result;
}

NProto::TDqSolomonSource FillSolomonSource(const TSolomonClusterConfig* config, const TString& project) {
    NSo::NProto::TDqSolomonSource source;

    source.SetClusterType(NSo::MapClusterType(config->GetClusterType()));
    source.SetUseSsl(config->GetUseSsl());
    
    if (source.GetClusterType() == NSo::NProto::CT_MONITORING) {
        source.SetProject(config->GetPath().GetProject());
        source.SetCluster(config->GetPath().GetCluster());
    } else {
        source.SetProject(project);
    }
    
    source.SetEndpoint(config->GetCluster()); // Backward compatibility
    source.SetHttpEndpoint(config->GetCluster());
    source.SetGrpcEndpoint(config->GetCluster());
    for (const auto& attr : config->settings()) {
        if (attr.name() == "grpc_location"sv) {
            source.SetGrpcEndpoint(attr.value());
        }
    }

    return source;
}

} // namespace NYql::NSo
