#pragma once

#include "source.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

struct TAdditionalParamBinding {
    enum class EValueSource {
        ClusterInfo,
        Value,
    };

    TString Label;
    EValueSource ValueSource = EValueSource::ClusterInfo;
    TString SourceValue;
};

struct TResolvedParamBindings {
    TVector<std::pair<TString, TString>> RequestParams;
    TVector<TAdditionalParamBinding> AdditionalParams;
};

inline TStringBuf GetSupportLinksEntityName(EEntityType entityType) {
    switch (entityType) {
        case EEntityType::Cluster:
            return "cluster";
        case EEntityType::Database:
            return "database";
        case EEntityType::Node:
            return "node";
        case EEntityType::Host:
            return "host";
    }
    ythrow yexception() << "unsupported support_links entity type";
}

inline TVector<TString> BuildDefaultRequestParamNames(EEntityType entityType) {
    TVector<TString> requestParamNames;
    requestParamNames.push_back("cluster");
    if (entityType == EEntityType::Database) {
        requestParamNames.push_back("database");
    } else if (entityType == EEntityType::Node) {
        requestParamNames.push_back("node");
    } else if (entityType == EEntityType::Host) {
        requestParamNames.push_back("host");
    }
    return requestParamNames;
}

inline THashSet<TString> BuildAllowedRequestParamNames(EEntityType entityType) {
    THashSet<TString> allowedRequestParamNames;
    for (const auto& requestParamName : BuildDefaultRequestParamNames(entityType)) {
        allowedRequestParamNames.insert(requestParamName);
    }
    return allowedRequestParamNames;
}

inline TVector<std::pair<TString, TString>> ResolveRequestParamBindings(
    const TSupportLinkEntryConfig&,
    EEntityType entityType)
{
    TVector<std::pair<TString, TString>> requestParams;
    for (const auto& requestParamName : BuildDefaultRequestParamNames(entityType)) {
        requestParams.emplace_back(requestParamName, requestParamName);
    }
    return requestParams;
}

inline TResolvedParamBindings ResolveConfiguredParamMappings(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType)
{
    const THashSet<TString> allowedRequestParamNames = BuildAllowedRequestParamNames(entityType);
    TResolvedParamBindings paramBindings;
    paramBindings.RequestParams.reserve(config.LinkParameterMappingsSize());
    paramBindings.AdditionalParams.reserve(config.LinkParameterMappingsSize());

    const int linkParameterMappingsSize = config.LinkParameterMappingsSize();
    for (int i = 0; i < linkParameterMappingsSize; ++i) {
        const auto& mapping = config.GetLinkParameterMappings(i);
        if (mapping.GetParameter().empty()) {
            ythrow yexception() << "link_parameter_mappings.parameter is required for source=" << config.GetSource();
        }

        const bool hasFromRequest = !mapping.GetFromRequest().empty();
        const bool hasFromClusterInfo = !mapping.GetFromClusterInfo().empty();
        const bool hasStaticValue = !mapping.GetStaticValue().empty();
        const size_t sourceCount = hasFromRequest + hasFromClusterInfo + hasStaticValue;
        if (sourceCount != 1) {
            ythrow yexception()
                << "link_parameter_mappings.parameter=" << mapping.GetParameter()
                << " must set exactly one of from_request, from_cluster_info or static_value for source="
                << config.GetSource();
        }

        if (hasFromRequest) {
            if (!allowedRequestParamNames.contains(mapping.GetFromRequest())) {
                ythrow yexception()
                    << "link_parameter_mappings.from_request=" << mapping.GetFromRequest()
                    << " is not supported for entity=" << GetSupportLinksEntityName(entityType)
                    << " and source=" << config.GetSource();
            }
            paramBindings.RequestParams.emplace_back(mapping.GetFromRequest(), mapping.GetParameter());
            continue;
        }

        TAdditionalParamBinding binding;
        binding.Label = mapping.GetParameter();
        if (hasFromClusterInfo) {
            binding.ValueSource = TAdditionalParamBinding::EValueSource::ClusterInfo;
            binding.SourceValue = mapping.GetFromClusterInfo();
        } else {
            binding.ValueSource = TAdditionalParamBinding::EValueSource::Value;
            binding.SourceValue = mapping.GetStaticValue();
        }
        paramBindings.AdditionalParams.push_back(std::move(binding));
    }

    return paramBindings;
}

inline TResolvedParamBindings ResolveParamBindings(
    const TSupportLinkEntryConfig& config,
    EEntityType entityType,
    const TVector<TAdditionalParamBinding>& defaultAdditionalParams)
{
    if (config.LinkParameterMappingsSize() != 0) {
        return ResolveConfiguredParamMappings(config, entityType);
    }

    return TResolvedParamBindings{
        .RequestParams = ResolveRequestParamBindings(config, entityType),
        .AdditionalParams = defaultAdditionalParams,
    };
}

inline void ValidateResolvedParamBindings(
    const TResolvedParamBindings& paramBindings,
    const TSupportLinkEntryConfig& config)
{
    THashSet<TString> labels;

    for (const auto& [requestParamName, targetLabel] : paramBindings.RequestParams) {
        Y_UNUSED(requestParamName);
        if (!labels.insert(targetLabel).second) {
            ythrow yexception()
                << "duplicate target label '" << targetLabel
                << "' in link_parameter_mappings for source=" << config.GetSource();
        }
    }

    for (const auto& additionalParam : paramBindings.AdditionalParams) {
        if (!labels.insert(additionalParam.Label).second) {
            ythrow yexception()
                << "duplicate target label '" << additionalParam.Label
                << "' in link_parameter_mappings for source=" << config.GetSource();
        }
    }
}

inline TVector<std::pair<TString, TString>> BuildRequestParamValues(
    const TCgiParameters& requestParameters,
    const TVector<std::pair<TString, TString>>& requestParamBindings)
{
    TVector<std::pair<TString, TString>> paramValues;

    for (const auto& [requestParamName, targetLabel] : requestParamBindings) {
        const TString value = requestParameters.Get(requestParamName);
        if (!value.empty()) {
            paramValues.emplace_back(targetLabel, value);
        }
    }

    return paramValues;
}

inline TVector<std::pair<TString, TString>> BuildAdditionalParamValues(
    const THashMap<TString, TString>& clusterInfo,
    const TVector<TAdditionalParamBinding>& additionalParamBindings)
{
    TVector<std::pair<TString, TString>> paramValues;

    for (const auto& additionalParam : additionalParamBindings) {
        if (additionalParam.ValueSource == TAdditionalParamBinding::EValueSource::Value) {
            paramValues.emplace_back(additionalParam.Label, additionalParam.SourceValue);
            continue;
        }

        const auto it = clusterInfo.find(additionalParam.SourceValue);
        if (it != clusterInfo.end() && !it->second.empty()) {
            paramValues.emplace_back(additionalParam.Label, it->second);
        }
    }

    return paramValues;
}

} // namespace NMVP::NSupportLinks
