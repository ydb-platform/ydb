#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NKikimrConfig {
    enum TTableServiceConfig_EBlockChannelsMode : int;
}

namespace NYql {

enum EOptionalFlag {
    Disabled = 0,
    Enabled = 1,
    Auto = 2
};

struct TKikimrSettings {
    using TConstPtr = std::shared_ptr<const TKikimrSettings>;
private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
    static constexpr NCommon::EConfSettingType Dynamic = NCommon::EConfSettingType::Dynamic;
public:
    /* KQP */
    NCommon::TConfSetting<ui32, Static> _KqpSessionIdleTimeoutSec;
    NCommon::TConfSetting<ui32, Static> _KqpMaxActiveTxPerSession;
    NCommon::TConfSetting<ui32, Static> _KqpTxIdleTimeoutSec;
    NCommon::TConfSetting<ui64, Static> _KqpExprNodesAllocationLimit;
    NCommon::TConfSetting<ui64, Static> _KqpExprStringsAllocationLimit;
    NCommon::TConfSetting<TString, Static> _KqpTablePathPrefix;
    NCommon::TConfSetting<ui32, Static> _KqpSlowLogWarningThresholdMs;
    NCommon::TConfSetting<ui32, Static> _KqpSlowLogNoticeThresholdMs;
    NCommon::TConfSetting<ui32, Static> _KqpSlowLogTraceThresholdMs;
    NCommon::TConfSetting<ui32, Static> _KqpYqlSyntaxVersion;
    NCommon::TConfSetting<bool, Static> _KqpYqlAntlr4Parser;
    NCommon::TConfSetting<bool, Static> _KqpAllowUnsafeCommit;
    NCommon::TConfSetting<ui32, Static> _KqpMaxComputeActors;
    NCommon::TConfSetting<bool, Static> _KqpEnableSpilling;
    NCommon::TConfSetting<bool, Static> _KqpDisableLlvmForUdfStages;
    NCommon::TConfSetting<ui64, Static> _KqpYqlCombinerMemoryLimit;

    /* No op just to avoid errors in Cloud Logging until they remove this from their queries */
    NCommon::TConfSetting<bool, Static> KqpPushOlapProcess;

    NCommon::TConfSetting<bool, Static> KqpForceImmediateEffectsExecution;

    /* Compile time */
    NCommon::TConfSetting<ui64, Static> _CommitPerShardKeysSizeLimitBytes;
    NCommon::TConfSetting<TString, Static> _DefaultCluster;
    NCommon::TConfSetting<ui32, Static> _ResultRowsLimit;
    NCommon::TConfSetting<bool, Static> EnableSystemColumns;
    NCommon::TConfSetting<bool, Static> UseLlvm;
    NCommon::TConfSetting<bool, Static> EnableLlvm;
    NCommon::TConfSetting<NDq::EHashJoinMode, Static> HashJoinMode;
    NCommon::TConfSetting<ui64, Static> EnableSpillingNodes;
    NCommon::TConfSetting<TString, Static> OverridePlanner;
    NCommon::TConfSetting<bool, Static> UseGraceJoinCoreForMap;
    NCommon::TConfSetting<bool, Static> UseBlockHashJoin;
    NCommon::TConfSetting<bool, Static> EnableOrderPreservingLookupJoin;
    NCommon::TConfSetting<bool, Static> OptEnableParallelUnionAllConnectionsForExtend;

    NCommon::TConfSetting<bool, Static> UseDqHashCombine;

    NCommon::TConfSetting<TString, Static> OptOverrideStatistics;
    NCommon::TConfSetting<NYql::TOptimizerHints, Static> OptimizerHints;

    /* Disable optimizer rules */
    NCommon::TConfSetting<bool, Static> OptDisableTopSort;
    NCommon::TConfSetting<bool, Static> OptDisableSqlInToJoin;
    NCommon::TConfSetting<bool, Static> OptEnableInplaceUpdate;
    NCommon::TConfSetting<bool, Static> OptEnablePredicateExtract;
    NCommon::TConfSetting<bool, Static> OptEnableOlapPushdown;
    NCommon::TConfSetting<bool, Static> OptEnableOlapPushdownAggregate;
    NCommon::TConfSetting<bool, Static> OptEnableOlapPushdownProjections;
    NCommon::TConfSetting<bool, Static> OptEnableOlapProvideComputeSharding;
    NCommon::TConfSetting<bool, Static> OptUseFinalizeByKey;
    NCommon::TConfSetting<bool, Static> OptShuffleElimination;
    NCommon::TConfSetting<bool, Static> OptShuffleEliminationWithMap;
    NCommon::TConfSetting<bool, Static> OptShuffleEliminationForAggregation;
    NCommon::TConfSetting<ui32, Static> CostBasedOptimizationLevel;
    NCommon::TConfSetting<bool, Static> UseBlockReader;

    NCommon::TConfSetting<NDq::EHashShuffleFuncType , Static> HashShuffleFuncType;
    NCommon::TConfSetting<NDq::EHashShuffleFuncType , Static> ColumnShardHashShuffleFuncType;

    NCommon::TConfSetting<ui32, Static> MaxDPHypDPTableSize;

    NCommon::TConfSetting<ui32, Static> MaxTasksPerStage;
    NCommon::TConfSetting<ui64, Static> DataSizePerPartition;
    NCommon::TConfSetting<ui32, Static> MaxSequentialReadsInFlight;

    NCommon::TConfSetting<ui32, Static> KMeansTreeSearchTopSize;

    /* Runtime */
    NCommon::TConfSetting<bool, Dynamic> ScanQuery;

    /* Accessors */
    bool HasDefaultCluster() const;
    bool HasAllowKqpUnsafeCommit() const;
    bool SystemColumnsEnabled() const;
    bool SpillingEnabled() const;
    bool DisableLlvmForUdfStages() const;

    bool HasOptDisableTopSort() const;
    bool HasOptDisableSqlInToJoin() const;
    bool HasOptEnableOlapPushdown() const;
    bool HasOptEnableOlapPushdownAggregate() const;
    bool HasOptEnableOlapPushdownProjections() const;
    bool HasOptEnableOlapProvideComputeSharding() const;
    bool HasOptUseFinalizeByKey() const;
    bool HasMaxSequentialReadsInFlight() const;
    bool OrderPreservingLookupJoinEnabled() const;
    EOptionalFlag GetOptPredicateExtract() const;
    EOptionalFlag GetUseLlvm() const;
    NDq::EHashJoinMode GetHashJoinMode() const;

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
            this->Dispatch(NCommon::ALL_CLUSTERS, "_DefaultCluster", *defaultCluster, EStage::CONFIG, NCommon::TSettingDispatcher::GetDefaultErrorCallback());
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

        this->Dispatch(NCommon::ALL_CLUSTERS, "_DefaultCluster", cluster, EStage::CONFIG, NCommon::TSettingDispatcher::GetDefaultErrorCallback());
        this->Dispatch(defaultSettings);
        this->Dispatch(NCommon::ALL_CLUSTERS, settings);

        if (freezeDefaults) {
            this->FreezeDefaults();
        }
    }

    TKikimrSettings::TConstPtr Snapshot() const;

    NKikimrConfig::TFeatureFlags FeatureFlags;

    bool EnableKqpScanQuerySourceRead = false;
    bool EnableKqpScanQueryStreamIdxLookupJoin = false;
    bool EnableKqpDataQueryStreamIdxLookupJoin = false;
    NSQLTranslation::EBindingsMode BindingsMode = NSQLTranslation::EBindingsMode::ENABLED;
    bool EnableAstCache = false;
    bool EnablePgConstsToParams = false;
    ui64 ExtractPredicateRangesLimit = 0;
    bool EnablePerStatementQueryExecution = false;
    bool EnableCreateTableAs = false;
    ui64 IdxLookupJoinsPrefixPointLimit = 1;
    bool AllowOlapDataQuery = false;
    bool EnableOlapSink = false;
    bool EnableOltpSink = false;
    bool EnableHtapTx = false;
    bool EnableStreamWrite = false;
    NKikimrConfig::TTableServiceConfig_EBlockChannelsMode BlockChannelsMode;
    bool EnableSpilling = true;
    ui32 DefaultCostBasedOptimizationLevel = 4;
    bool EnableConstantFolding = true;
    bool EnableFoldUdfs = true;
    ui64 DefaultEnableSpillingNodes = 0;
    bool EnableAntlr4Parser = false;
    bool EnableSnapshotIsolationRW = false;
    bool AllowMultiBroadcasts = false;
    bool DefaultEnableShuffleElimination = false;
    bool DefaultEnableShuffleEliminationForAggregation = false;
    bool FilterPushdownOverJoinOptionalSide = false;
    THashSet<TString> YqlCoreOptimizerFlags;
    bool EnableNewRBO = false;
    bool EnableSpillingInHashJoinShuffleConnections = false;
    bool EnableOlapScalarApply = false;
    bool EnableOlapSubstringPushdown = false;
    bool EnableIndexStreamWrite = false;
    bool EnableOlapPushdownProjections = false;
    bool EnableParallelUnionAllConnectionsForExtend = false;

    ui32 LangVer = NYql::MinLangVersion;
    NYql::EBackportCompatibleFeaturesMode BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;

    NDq::EHashShuffleFuncType DefaultHashShuffleFuncType = NDq::EHashShuffleFuncType::HashV1;
    NDq::EHashShuffleFuncType DefaultColumnShardHashShuffleFuncType = NDq::EHashShuffleFuncType::ColumnShardHashV1;

    void SetDefaultEnabledSpillingNodes(const TString& node);
    ui64 GetEnabledSpillingNodes() const;
    bool GetEnableOlapPushdownProjections() const;
    bool GetEnableParallelUnionAllConnectionsForExtend() const;
};

}
