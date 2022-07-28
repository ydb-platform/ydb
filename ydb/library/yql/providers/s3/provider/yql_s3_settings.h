#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {

struct TS3Settings {
    using TConstPtr = std::shared_ptr<const TS3Settings>;

    NCommon::TConfSetting<bool, false> SourceCoroActor;
    NCommon::TConfSetting<ui64, false> MaxOutputObjectSize;
    NCommon::TConfSetting<ui64, false> UniqueKeysCountLimit;
    NCommon::TConfSetting<ui64, false> BlockSizeMemoryLimit;
    NCommon::TConfSetting<ui64, false> SerializeMemoryLimit; // Total serialization memory limit for all current blocks for all patition keys. Reachable in case of many but small partitions.
};

struct TS3ClusterSettings {
    TString Url, Token;
};

struct TS3Configuration : public TS3Settings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TS3Configuration>;

    TS3Configuration();
    TS3Configuration(const TS3Configuration&) = delete;

    void Init(const TS3GatewayConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx);

    bool HasCluster(TStringBuf cluster) const;

    TS3Settings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    std::unordered_map<TString, TS3ClusterSettings> Clusters;

    ui64 FileSizeLimit;
    std::unordered_map<TString, ui64> FormatSizeLimits;
    ui64 MaxFilesPerQuery;
    ui64 MaxDiscoveryFilesPerQuery;
    ui64 MaxReadSizePerQuery;
};

} // NYql
