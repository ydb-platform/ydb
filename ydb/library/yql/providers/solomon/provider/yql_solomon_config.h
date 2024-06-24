#pragma once

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {

struct TSolomonSettings {
    using TConstPtr = std::shared_ptr<const TSolomonSettings>;
    NCommon::TConfSetting<bool, false> _EnableReading;
};

struct TSolomonConfiguration
    : public TSolomonSettings
    , public NCommon::TSettingDispatcher
{
    using TPtr = TIntrusivePtr<TSolomonConfiguration>;

    TSolomonConfiguration();
    TSolomonConfiguration(const TSolomonConfiguration&) = delete;

    template <typename TProtoConfig>
    void Init(const TProtoConfig& config, TIntrusivePtr<TTypeAnnotationContext> typeCtx)
    {
        TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
        for (auto& cluster: config.GetClusterMapping()) {
            clusters.push_back(cluster.GetName());
            ClusterConfigs[cluster.GetName()] = cluster;

            const TString authToken = typeCtx->Credentials->FindCredentialContent("cluster:default_" + cluster.GetName(), "default_solomon", cluster.GetToken());
            Tokens[cluster.GetName()] = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken);
        }

        this->SetValidClusters(clusters);

        this->Dispatch(config.GetDefaultSettings());
        for (auto& cluster: config.GetClusterMapping()) {
            this->Dispatch(cluster.GetName(), cluster.GetSettings());
        }
        this->FreezeDefaults();
    }

    TSolomonSettings::TConstPtr Snapshot() const;

    THashMap<TString, TSolomonClusterConfig> ClusterConfigs;
    THashMap<TString, TString> Tokens;
};

} // NYql
