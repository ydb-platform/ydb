#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP {

struct TGrafanaSupportConfig {
    TString Endpoint;
    TString SecretName;
    TString WorkspaceColumn = "workspace";
    TString DatasourceColumn = "grafana_ds";
};

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
    TGrafanaSupportConfig GrafanaSupportConfig;
    TSupportLinksConfig SupportLinksConfig;
};

} // namespace NMVP
