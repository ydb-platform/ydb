#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP {

using TSupportLinkEntryConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry;
using TSupportLinksConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig;

struct TSupportLinksSettings {
    TString GrafanaEndpoint;
    TVector<TSupportLinkEntryConfig> ClusterLinks;
    TVector<TSupportLinkEntryConfig> DatabaseLinks;
};

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TSupportLinksSettings SupportLinks;
};

TMetaSettings BuildMetaSettings(const NMvp::NMeta::TMetaConfig& config, NMvp::EAccessServiceType accessServiceType);

} // namespace NMVP
