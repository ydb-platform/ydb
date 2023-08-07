#pragma once

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NYql {

struct TPqSettings {
    using TConstPtr = std::shared_ptr<const TPqSettings>;

    NCommon::TConfSetting<TString, false> Consumer;
    NCommon::TConfSetting<TString, false> Database; // It is needed in case of Cloud.LB for external users, but can be taken from config for internal LB.
    NCommon::TConfSetting<TString, false> PqReadByRtmrCluster_;
};

struct TPqClusterConfigurationSettings {
    TString ClusterName;
    NYql::TPqClusterConfig::EClusterType ClusterType = NYql::TPqClusterConfig::CT_UNSPECIFIED;
    TString Endpoint;
    TString ConfigManagerEndpoint;
    bool UseSsl = false;
    TString Database;
    TString DatabaseId;
    ui32 TvmId = 0;
    TString AuthToken;
    bool AddBearerToToken = false;
};

struct TPqConfiguration : public TPqSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TPqConfiguration>;

    TPqConfiguration();
    TPqConfiguration(const TPqConfiguration&) = delete;

    void Init(
        const TPqGatewayConfig& config,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
        THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& databaseIds);

    TString GetDatabaseForTopic(const TString& cluster) const;

    TPqSettings::TConstPtr Snapshot() const;
    THashMap<TString, TPqClusterConfigurationSettings> ClustersConfigurationSettings;
    THashMap<TString, TString> Tokens;
    THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
};

} // NYql
