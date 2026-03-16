#pragma once

#include <cstddef>

#include <memory>

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <memory>

namespace NMVP {

using TSupportLinkEntryConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig::TSupportLinkEntry;
using TSupportLinksConfig = NMvp::NMeta::TMetaConfig::TSupportLinksConfig;

class ILinkSource;

struct TMetaSettings {
    TString MetaApiEndpoint;
    TString MetaDatabase;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;
    TString GrafanaEndpoint;
    TString GrafanaSecretName;
    TVector<std::shared_ptr<ILinkSource>> ClusterLinkSources;
    TVector<std::shared_ptr<ILinkSource>> DatabaseLinkSources;
};

} // namespace NMVP
