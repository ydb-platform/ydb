#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

namespace NMVP {

using TSupportLinkEntryConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry;
using TSupportLinksConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig;

struct TSupportLinksSettings {
    TString GrafanaEndpoint;
    TVector<TSupportLinkEntryConfig> ClusterLinks;
    TVector<TSupportLinkEntryConfig> DatabaseLinks;
    TVector<TSupportLinkEntryConfig> NodeLinks;
    TVector<TSupportLinkEntryConfig> HostLinks;
};

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    TString ServiceEndpoint;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TSupportLinksSettings SupportLinks;
    struct THealthcheckSettings {
        bool Enabled = false;
        bool Collect = true;
        TDuration CheckPeriod = TDuration::Seconds(15);
        TDuration DiscoveryPeriod = TDuration::Seconds(60);
        TDuration StatusTtl = TDuration::Seconds(120);
        TDuration RequestTimeout = TDuration::Seconds(30);
    } Healthcheck;
};

TMetaSettings BuildMetaSettings(const NMvp::NMeta::TMetaConfig& config, NMvp::EAccessServiceType accessServiceType);

} // namespace NMVP
