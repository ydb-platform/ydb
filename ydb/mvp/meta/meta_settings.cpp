#include "meta_settings.h"

#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/core/utils.h>

#include <util/generic/yexception.h>

namespace NMVP {

namespace {

void CopyEffectiveEntityLinks(
    google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>* destination,
    const google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>& preferredLinks,
    const google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>& legacyLinks)
{
    const auto& sourceLinks = preferredLinks.empty() ? legacyLinks : preferredLinks;
    for (const auto& link : sourceLinks) {
        *destination->Add() = link;
    }
}

void CopyEntityLinks(
    google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>* destination,
    const google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>& sourceLinks)
{
    for (const auto& link : sourceLinks) {
        *destination->Add() = link;
    }
}

void CopyResolvedEntityLinks(
    TVector<TSupportLinkEntryConfig>& destination,
    const google::protobuf::RepeatedPtrField<TSupportLinkEntryConfig>& sourceLinks)
{
    destination.reserve(sourceLinks.size());
    for (const auto& link : sourceLinks) {
        destination.push_back(link);
    }
}

TSupportLinksConfig BuildEffectiveSupportLinksConfig(const TSupportLinksConfig& rawConfig) {
    TSupportLinksConfig effectiveConfig;

    const auto* byEntity = rawConfig.HasByEntity() ? &rawConfig.GetByEntity() : nullptr;

    CopyEffectiveEntityLinks(
        effectiveConfig.MutableCluster(),
        byEntity ? byEntity->GetCluster() : rawConfig.GetCluster(),
        rawConfig.GetCluster());
    CopyEffectiveEntityLinks(
        effectiveConfig.MutableDatabase(),
        byEntity ? byEntity->GetDatabase() : rawConfig.GetDatabase(),
        rawConfig.GetDatabase());
    CopyEffectiveEntityLinks(
        effectiveConfig.MutableNode(),
        byEntity ? byEntity->GetNode() : rawConfig.GetNode(),
        rawConfig.GetNode());
    CopyEffectiveEntityLinks(
        effectiveConfig.MutableHost(),
        byEntity ? byEntity->GetHost() : rawConfig.GetHost(),
        rawConfig.GetHost());

    return effectiveConfig;
}

} // namespace

TMetaSettings BuildMetaSettings(const NMvp::NMeta::TMetaConfig& config, NMvp::EAccessServiceType accessServiceType) {
    TMetaSettings settings;
    settings.MetaApiEndpoint = config.GetMetaApiEndpoint();
    settings.MetaDatabase = config.GetMetaDatabase();
    settings.AccessServiceType = accessServiceType;

    if (settings.MetaApiEndpoint.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "meta.meta_api_endpoint must be specified";
    }
    if (settings.MetaDatabase.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "meta.meta_database must be specified";
    }

    if (config.HasGrafana()) {
        settings.SupportLinks.GrafanaEndpoint = config.GetGrafana().GetEndpoint();
    }

    if (config.HasSupportLinks()) {
        const TSupportLinksConfig supportLinks = BuildEffectiveSupportLinksConfig(config.GetSupportLinks());
        NSupportLinks::ValidateSupportLinksConfig(supportLinks, settings);
        CopyResolvedEntityLinks(settings.SupportLinks.ClusterLinks, supportLinks.GetCluster());
        CopyResolvedEntityLinks(settings.SupportLinks.DatabaseLinks, supportLinks.GetDatabase());
        CopyResolvedEntityLinks(settings.SupportLinks.NodeLinks, supportLinks.GetNode());
        CopyResolvedEntityLinks(settings.SupportLinks.HostLinks, supportLinks.GetHost());
    }

    return settings;
}

} // namespace NMVP
