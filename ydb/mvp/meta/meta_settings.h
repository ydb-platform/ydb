#pragma once

#include <cstddef>

#include <memory>

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/meta/protos/config.pb.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP {

class ILinkSource;

struct TGrafanaSupportConfig {
    TString Endpoint;
    TString SecretName;
};

using TSupportLinkEntry = NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry;

struct TSupportLinkSources {
    TSupportLinkSources();
    ~TSupportLinkSources();
    TSupportLinkSources(TSupportLinkSources&&) noexcept;
    TSupportLinkSources& operator=(TSupportLinkSources&&) noexcept;
    TSupportLinkSources(const TSupportLinkSources&) = delete;
    TSupportLinkSources& operator=(const TSupportLinkSources&) = delete;

    TVector<std::unique_ptr<ILinkSource>> Cluster;
    TVector<std::unique_ptr<ILinkSource>> Database;
};

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    bool HasMetaConfigBlock = false;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TGrafanaSupportConfig GrafanaConfig;
    TSupportLinkSources SupportLinksConfig;
};

void ValidateMetaBaseConfig(const TMetaSettings& settings);

} // namespace NMVP
