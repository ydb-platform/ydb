#pragma once

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NYql {

struct TPqSettings {
    using TConstPtr = std::shared_ptr<const TPqSettings>;
private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
public:
    NCommon::TConfSetting<TString, Static> Auth;
    NCommon::TConfSetting<TString, Static> Consumer;
    NCommon::TConfSetting<TString, Static> Database; // It is needed in case of Cloud.LB for external users, but can be taken from config for internal LB.
    NCommon::TConfSetting<TString, Static> PqReadByRtmrCluster_;
    NCommon::TConfSetting<TDuration, Static> MaxPartitionReadSkew;
    NCommon::TConfSetting<ui64, Static> ReadSessionBufferBytes;
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
    bool SharedReading = false;
    TString ReconnectPeriod;
    TString ReadGroup;
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

    void AddCluster(
        const NYql::TPqClusterConfig& cluster,
        THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& databaseIds,
        const TCredentials::TPtr& credentials,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver>& dbResolver,
        const THashMap<TString, TString>& properties);

    TPqSettings::TConstPtr Snapshot() const;
    THashMap<TString, TPqClusterConfigurationSettings> ClustersConfigurationSettings;
    THashMap<TString, TString> Tokens;
    THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
};

} // NYql
