#pragma once

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {

struct TSolomonSettings {
    using TConstPtr = std::shared_ptr<const TSolomonSettings>;
private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
public:
    NCommon::TConfSetting<bool, Static> _EnableReading;
    NCommon::TConfSetting<bool, Static> _EnableRuntimeListing;
    NCommon::TConfSetting<ui64, Static> _TruePointsFindRange;
    NCommon::TConfSetting<ui64, Static> MetricsQueuePageSize;
    NCommon::TConfSetting<ui64, Static> MetricsQueuePrefetchSize;
    NCommon::TConfSetting<ui64, Static> MetricsQueueBatchCountLimit;
    NCommon::TConfSetting<TString, Static> SolomonClientDefaultReplica;
    NCommon::TConfSetting<ui64, Static> ComputeActorBatchSize;
    NCommon::TConfSetting<ui64, Static> MaxApiInflight;
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
