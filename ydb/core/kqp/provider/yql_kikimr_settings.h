#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/core/protos/config.pb.h>

namespace NYql {

enum EOptionalFlag {
    Disabled = 0,
    Enabled = 1,
    Auto = 2
};

struct TKikimrSettings {
    using TConstPtr = std::shared_ptr<const TKikimrSettings>;

    /* KQP */
    NCommon::TConfSetting<ui32, false> _KqpSessionIdleTimeoutSec;
    NCommon::TConfSetting<ui32, false> _KqpMaxActiveTxPerSession;
    NCommon::TConfSetting<ui32, false> _KqpTxIdleTimeoutSec;
    NCommon::TConfSetting<ui64, false> _KqpExprNodesAllocationLimit;
    NCommon::TConfSetting<ui64, false> _KqpExprStringsAllocationLimit;
    NCommon::TConfSetting<TString, false> _KqpTablePathPrefix;
    NCommon::TConfSetting<ui32, false> _KqpSlowLogWarningThresholdMs;
    NCommon::TConfSetting<ui32, false> _KqpSlowLogNoticeThresholdMs;
    NCommon::TConfSetting<ui32, false> _KqpSlowLogTraceThresholdMs;
    NCommon::TConfSetting<ui32, false> _KqpYqlSyntaxVersion;
    NCommon::TConfSetting<bool, false> _KqpAllowUnsafeCommit;
    NCommon::TConfSetting<ui32, false> _KqpMaxComputeActors;
    NCommon::TConfSetting<bool, false> _KqpEnableSpilling;
    NCommon::TConfSetting<bool, false> _KqpDisableLlvmForUdfStages;

    /* No op just to avoid errors in Cloud Logging until they remove this from their queries */
    NCommon::TConfSetting<bool, false> KqpPushOlapProcess;

    /* Compile time */
    NCommon::TConfSetting<ui64, false> _CommitPerShardKeysSizeLimitBytes;
    NCommon::TConfSetting<TString, false> _DefaultCluster;
    NCommon::TConfSetting<ui32, false> _ResultRowsLimit;
    NCommon::TConfSetting<bool, false> EnableSystemColumns;
    NCommon::TConfSetting<bool, false> UseLlvm;

    /* Disable optimizer rules */
    NCommon::TConfSetting<bool, false> OptDisableJoinRewrite;
    NCommon::TConfSetting<bool, false> OptDisableJoinTableLookup;
    NCommon::TConfSetting<bool, false> OptDisableJoinReverseTableLookup;
    NCommon::TConfSetting<bool, false> OptDisableJoinReverseTableLookupLeftSemi;
    NCommon::TConfSetting<bool, false> OptDisableTopSort;
    NCommon::TConfSetting<bool, false> OptDisableSqlInToJoin;
    NCommon::TConfSetting<bool, false> OptEnableInplaceUpdate;
    NCommon::TConfSetting<bool, false> OptEnablePredicateExtract;
    NCommon::TConfSetting<bool, false> OptEnableOlapPushdown;
    NCommon::TConfSetting<bool, false> OptUseFinalizeByKey;

    /* Runtime */
    NCommon::TConfSetting<bool, true> ScanQuery;

    /* Accessors */
    bool HasDefaultCluster() const;
    bool HasAllowKqpUnsafeCommit() const;
    bool SystemColumnsEnabled() const;
    bool SpillingEnabled() const;
    bool DisableLlvmForUdfStages() const;

    bool HasOptDisableJoinRewrite() const;
    bool HasOptDisableJoinTableLookup() const;
    bool HasOptDisableJoinReverseTableLookup() const;
    bool HasOptDisableJoinReverseTableLookupLeftSemi() const;
    bool HasOptDisableTopSort() const;
    bool HasOptDisableSqlInToJoin() const;
    bool HasOptEnableOlapPushdown() const;
    bool HasOptUseFinalizeByKey() const;
    EOptionalFlag GetOptPredicateExtract() const;
    EOptionalFlag GetUseLlvm() const;

    // WARNING: For testing purposes only, inplace update is not ready for production usage.
    bool HasOptEnableInplaceUpdate() const;
};

struct TKikimrConfiguration : public TKikimrSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TKikimrConfiguration>;

    TKikimrConfiguration();
    TKikimrConfiguration(const TKikimrConfiguration&) = delete;

    template <typename TProtoConfig>
    void Init(const TProtoConfig& config)
    {
        TMaybe<TString> defaultCluster;
        TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
        for (auto& cluster: config.GetClusterMapping()) {
            clusters.push_back(cluster.GetName());
            if (cluster.HasDefault() && cluster.GetDefault()) {
                defaultCluster = cluster.GetName();
            }
        }

        this->SetValidClusters(clusters);

        if (defaultCluster) {
            this->Dispatch(NCommon::ALL_CLUSTERS, "_DefaultCluster", *defaultCluster, EStage::CONFIG);
        }

        // Init settings from config
        this->Dispatch(config.GetDefaultSettings());
        for (auto& cluster: config.GetClusterMapping()) {
            this->Dispatch(cluster.GetName(), cluster.GetSettings());
        }
        this->FreezeDefaults();
    }

    template <typename TDefultSettingsContainer, typename TSettingsContainer>
    void Init(const TDefultSettingsContainer& defaultSettings, const TString& cluster,
        const TSettingsContainer& settings, bool freezeDefaults)
    {
        this->SetValidClusters(TVector<TString>{cluster});

        this->Dispatch(NCommon::ALL_CLUSTERS, "_DefaultCluster", cluster, EStage::CONFIG);
        this->Dispatch(defaultSettings);
        this->Dispatch(NCommon::ALL_CLUSTERS, settings);

        if (freezeDefaults) {
            this->FreezeDefaults();
        }
    }

    TKikimrSettings::TConstPtr Snapshot() const;

    NKikimrConfig::TFeatureFlags FeatureFlags;

    bool EnableKqpScanQuerySourceRead = false;
    bool EnableKqpDataQuerySourceRead = false;
    bool EnableKqpScanQueryStreamLookup = false;
    bool EnableKqpDataQueryStreamLookup = false;
    bool EnableKqpDataQueryStreamPointLookup = false;
    bool EnableKqpScanQueryStreamIdxLookupJoin = false;
    bool EnablePredicateExtractForScanQuery = true;
    bool EnablePredicateExtractForDataQuery = false;
};

}
