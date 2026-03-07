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

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TGrafanaSupportConfig GrafanaConfig;
    TVector<std::shared_ptr<ILinkSource>> ClusterLinkSources;
    TVector<std::shared_ptr<ILinkSource>> DatabaseLinkSources;
};

} // namespace NMVP
