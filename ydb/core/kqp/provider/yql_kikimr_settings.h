#pragma once

#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <util/generic/size_literals.h>


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
    NCommon::TConfSetting<ui32, Static> DqChannelVersion;

    NCommon::TConfSetting<bool, Static> UseDqHashCombine;
    NCommon::TConfSetting<bool, Static> UseDqHashAggregate;
    NCommon::TConfSetting<bool, Static> DqHashOperatorsUseBlocks;

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
    NCommon::TConfSetting<bool, Static> OptDisallowFuseJoins;
    NCommon::TConfSetting<bool, Static> OptCreateStageForAggregation;

    // Use CostBasedOptimizationLevel for internal usage. This is a dummy flag that is mapped to the optimization level during parsing.
    NCommon::TConfSetting<TString, Static> CostBasedOptimization;

    NCommon::TConfSetting<bool, Static> UseBlockReader;

    NCommon::TConfSetting<NDq::EHashShuffleFuncType , Static> HashShuffleFuncType;
    NCommon::TConfSetting<NDq::EHashShuffleFuncType , Static> ColumnShardHashShuffleFuncType;

    NCommon::TConfSetting<ui32, Static> MaxDPHypDPTableSize;
    NCommon::TConfSetting<ui32, Static> ShuffleEliminationJoinNumCutoff;

    NCommon::TConfSetting<ui32, Static> MaxTasksPerStage;
    NCommon::TConfSetting<ui64, Static> DataSizePerPartition;
    NCommon::TConfSetting<ui32, Static> MaxSequentialReadsInFlight;

    NCommon::TConfSetting<ui32, Static> KMeansTreeSearchTopSize;
    NCommon::TConfSetting<bool, Static> DisableCheckpoints;

    NCommon::TConfSetting<NKqpProto::EIsolationLevel, Static> DefaultTxMode;

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

struct TKikimrConfiguration : public TKikimrSettings, public NCommon::TSettingDispatcher, public NKikimrConfig::TTableServiceConfig {
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

    void ApplyServiceConfig(const TTableServiceConfig& serviceConfig) {
        if (serviceConfig.GetQueryLimits().HasResultRowsLimit()) {
            _ResultRowsLimit = serviceConfig.GetQueryLimits().GetResultRowsLimit();
        }

        CopyFrom(serviceConfig);

        if (const auto limit = serviceConfig.GetResourceManager().GetMkqlHeavyProgramMemoryLimit()) {
            _KqpYqlCombinerMemoryLimit = std::max(1_GB, limit - (limit >> 2U));
        }
    }

    TKikimrSettings::TConstPtr Snapshot() const;

    NKikimrConfig::TFeatureFlags FeatureFlags;

    NYql::EBackportCompatibleFeaturesMode GetYqlBackportMode() const;
    NSQLTranslation::EBindingsMode GetYqlBindingsMode() const;
    NDq::EHashShuffleFuncType GetDqDefaultHashShuffleFuncType() const;

    ui64 GetEnabledSpillingNodes() const;
    bool GetEnableOlapPushdownProjections() const;
    bool GetEnableParallelUnionAllConnectionsForExtend() const;
    bool GetEnableOlapPushdownAggregate() const;
    bool GetUseDqHashCombine() const;
    bool GetUseDqHashAggregate() const;
    bool GetDqHashOperatorsUseBlocks() const;
};

}
