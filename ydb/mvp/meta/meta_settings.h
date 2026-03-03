#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP {

struct TGrafanaSupportConfig {
    TString Endpoint;
    TString SecretName;
};

inline constexpr TStringBuf GRAFANA_WORKSPACE_COLUMN = "workspace";
inline constexpr TStringBuf GRAFANA_DATASOURCE_COLUMN = "grafana_ds";
struct TSupportLinkEntryConfig {
    TString Source;
    TString Title;
    TString Url;
    TString Tag;
    TString Folder;
};

struct TSupportLinksConfig {
    TVector<TSupportLinkEntryConfig> Cluster;
    TVector<TSupportLinkEntryConfig> Database;
};

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    bool HasMetaConfigBlock = false;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TGrafanaSupportConfig GrafanaConfig;
    TSupportLinksConfig SupportLinksConfig;
};

void ValidateMetaBaseConfig(const TMetaSettings& settings);

} // namespace NMVP
