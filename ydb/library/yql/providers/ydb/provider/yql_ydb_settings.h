#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/db_resolver/db_async_resolver_with_meta.h>
#include <ydb/core/yq/libs/common/database_token_builder.h>


namespace NYql {

struct TYdbSettings {
    using TConstPtr = std::shared_ptr<const TYdbSettings>;
};

struct TClusterMainSettings {
    TString Endpoint;
    TString Database;
    TString DatabaseId;
    bool Secure = false;
    bool AddBearerToToken = false;

    TYdbClusterConfig Raw;
};

struct TYdbConfiguration : public TYdbSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TYdbConfiguration>;

    TYdbConfiguration();
    TYdbConfiguration(const TYdbConfiguration&) = delete;

    void Init(
        const TYdbGatewayConfig& config,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const std::shared_ptr<NYq::TDatabaseAsyncResolverWithMeta> dbResolver,
        THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TDatabaseAuth>& databaseIds
    );

    bool HasCluster(TStringBuf cluster) const;

    TYdbSettings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    std::unordered_map<TString, TClusterMainSettings> Clusters;
    THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
};

} // NYql
