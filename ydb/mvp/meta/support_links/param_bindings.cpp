#include "param_bindings.h"

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {
namespace {

TResolvedParamBindings ResolveConfiguredParamMappings(const TSupportLinkEntryConfig& config) {
    TResolvedParamBindings paramBindings;
    paramBindings.RequestMappings.reserve(config.LinkParameterMappingsSize());
    paramBindings.ClusterInfoMappings.reserve(config.LinkParameterMappingsSize());
    paramBindings.StaticMappings.reserve(config.LinkParameterMappingsSize());

    const int linkParameterMappingsSize = config.LinkParameterMappingsSize();
    for (int i = 0; i < linkParameterMappingsSize; ++i) {
        const auto& mapping = config.GetLinkParameterMappings(i);
        if (!mapping.HasParameter() || mapping.GetParameter().empty()) {
            ythrow yexception() << "link_parameter_mappings.parameter is required for source=" << config.GetSource();
        }

        switch (mapping.GetSourceValueCase()) {
            case TSupportLinkEntryConfig::TLinkParameterMapping::kFromRequest:
                paramBindings.RequestMappings.emplace_back(mapping.GetFromRequest(), mapping.GetParameter());
                break;

            case TSupportLinkEntryConfig::TLinkParameterMapping::kFromClusterInfo:
                paramBindings.ClusterInfoMappings.emplace_back(mapping.GetFromClusterInfo(), mapping.GetParameter());
                break;

            case TSupportLinkEntryConfig::TLinkParameterMapping::kStaticValue:
                paramBindings.StaticMappings.emplace_back(mapping.GetStaticValue(), mapping.GetParameter());
                break;

            case TSupportLinkEntryConfig::TLinkParameterMapping::SOURCEVALUE_NOT_SET:
                ythrow yexception()
                    << "link_parameter_mappings.parameter=" << mapping.GetParameter()
                    << " must set one of from_request, from_cluster_info or static_value for source="
                    << config.GetSource();
        }
    }

    return paramBindings;
}

} // namespace

TResolvedParamBindings ResolveParamBindings(const TSupportLinkEntryConfig& config, const TResolvedParamBindings& defaultParamBindings) {
    if (config.LinkParameterMappingsSize() != 0) {
        return ResolveConfiguredParamMappings(config);
    }

    return defaultParamBindings;
}

void ValidateParamsAreUnique(const TResolvedParamBindings& paramBindings, const TSupportLinkEntryConfig& config) {
    THashSet<TString> labels;

    for (const auto& mapping : paramBindings.RequestMappings) {
        const TString& targetLabel = mapping.second;
        if (!labels.insert(targetLabel).second) {
            ythrow yexception()
                << "duplicate target label '" << targetLabel
                << "' in link_parameter_mappings for source=" << config.GetSource();
        }
    }

    for (const auto& mapping : paramBindings.ClusterInfoMappings) {
        const TString& targetLabel = mapping.second;
        if (!labels.insert(targetLabel).second) {
            ythrow yexception()
                << "duplicate target label '" << targetLabel
                << "' in link_parameter_mappings for source=" << config.GetSource();
        }
    }

    for (const auto& mapping : paramBindings.StaticMappings) {
        const TString& targetLabel = mapping.second;
        if (!labels.insert(targetLabel).second) {
            ythrow yexception()
                << "duplicate target label '" << targetLabel
                << "' in link_parameter_mappings for source=" << config.GetSource();
        }
    }
}

TVector<std::pair<TString, TString>> BuildRequestParamValues(
    const TCgiParameters& requestParameters,
    const TVector<std::pair<TString, TString>>& requestMappings)
{
    TVector<std::pair<TString, TString>> paramValues;

    for (const auto& [requestParamName, targetLabel] : requestMappings) {
        const TString value = requestParameters.Get(requestParamName);
        if (!value.empty()) {
            paramValues.emplace_back(targetLabel, value);
        }
    }

    return paramValues;
}

TVector<std::pair<TString, TString>> BuildNonIdentityRequestParamValues(const TCgiParameters& requestParameters) {
    TVector<std::pair<TString, TString>> paramValues;

    for (const auto& [name, value] : requestParameters) {
        if (!IsIdentityRequestParameter(name) && !value.empty()) {
            paramValues.emplace_back(name, value);
        }
    }

    return paramValues;
}

TVector<std::pair<TString, TString>> BuildClusterInfoParamValues(
    const THashMap<TString, TString>& clusterInfo,
    const TVector<std::pair<TString, TString>>& clusterInfoMappings)
{
    TVector<std::pair<TString, TString>> paramValues;

    for (const auto& [clusterInfoField, targetLabel] : clusterInfoMappings) {
        const auto it = clusterInfo.find(clusterInfoField);
        if (it != clusterInfo.end() && !it->second.empty()) {
            paramValues.emplace_back(targetLabel, it->second);
        }
    }

    return paramValues;
}

TVector<std::pair<TString, TString>> BuildStaticParamValues(const TVector<std::pair<TString, TString>>& staticMappings) {
    TVector<std::pair<TString, TString>> paramValues;
    paramValues.reserve(staticMappings.size());

    for (const auto& [staticValue, targetLabel] : staticMappings) {
        paramValues.emplace_back(targetLabel, staticValue);
    }

    return paramValues;
}

} // namespace NMVP::NSupportLinks
