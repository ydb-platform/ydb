#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {

    struct TGenericSettings {
        using TConstPtr = std::shared_ptr<const TGenericSettings>;

        NCommon::TConfSetting<bool, false> UsePredicatePushdown;
        NCommon::TConfSetting<TString, false> DateTimeFormat;

        struct TDefault {
            static constexpr bool UsePredicatePushdown = false;
            static const TString DateTimeFormat; // = "string"
        };
    };

    struct TGenericConfiguration: public TGenericSettings, public NCommon::TSettingDispatcher {
        using TPtr = TIntrusivePtr<TGenericConfiguration>;

        TGenericConfiguration();
        TGenericConfiguration(const TGenericConfiguration&) = delete;

        void Init(const NYql::TGenericGatewayConfig& gatewayConfig, const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                  NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth, const TCredentials::TPtr& credentials);

        void AddCluster(const TGenericClusterConfig& clusterConfig, const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                        NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth, const TCredentials::TPtr& credentials);

        TGenericSettings::TConstPtr Snapshot() const;
        bool HasCluster(TStringBuf cluster) const;

    private:
        TString MakeStructuredToken(const TGenericClusterConfig& clusterConfig, const TCredentials::TPtr& credentials) const;

    public:
        THashMap<TString, TString> Tokens;
        THashMap<TString, TGenericClusterConfig> ClusterNamesToClusterConfigs; // cluster name -> cluster config
        THashMap<TString, TVector<TString>> DatabaseIdsToClusterNames;         // database id -> cluster name
    };
} // namespace NYql
