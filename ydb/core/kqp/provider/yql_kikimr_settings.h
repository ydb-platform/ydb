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
    NCommon::TConfSetting<ui32, Static> DqChannelVersion;

    NCommon::TConfSetting<bool, Static> UseDqHashCombine;
    NCommon::TConfSetting<bool, Static> UseDqHashAggregate;

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
        if (serviceConfig.HasSqlVersion()) {
            _KqpYqlSyntaxVersion = serviceConfig.GetSqlVersion();
        }
        if (serviceConfig.GetQueryLimits().HasResultRowsLimit()) {
            _ResultRowsLimit = serviceConfig.GetQueryLimits().GetResultRowsLimit();
        }

        CopyFrom(serviceConfig);

        EnableKqpScanQuerySourceRead = serviceConfig.GetEnableKqpScanQuerySourceRead();
        EnableKqpScanQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpScanQueryStreamIdxLookupJoin();
        EnableKqpDataQueryStreamIdxLookupJoin = serviceConfig.GetEnableKqpDataQueryStreamIdxLookupJoin();

        EnablePgConstsToParams = serviceConfig.GetEnablePgConstsToParams() && serviceConfig.GetEnableAstCache();
        ExtractPredicateRangesLimit = serviceConfig.GetExtractPredicateRangesLimit();
        EnablePerStatementQueryExecution = serviceConfig.GetEnablePerStatementQueryExecution();
        EnableDiscardSelect = serviceConfig.GetEnableDiscardSelect();
        EnableCreateTableAs = serviceConfig.GetEnableCreateTableAs();
        EnableDataShardCreateTableAs = serviceConfig.GetEnableDataShardCreateTableAs();
        AllowOlapDataQuery = serviceConfig.GetAllowOlapDataQuery();
        EnableOlapSink = serviceConfig.GetEnableOlapSink();
        EnableOltpSink = serviceConfig.GetEnableOltpSink();
        EnableHtapTx = serviceConfig.GetEnableHtapTx();
        EnableStreamWrite = serviceConfig.GetEnableStreamWrite();
        EnableBatchUpdates = serviceConfig.GetEnableBatchUpdates();
        BlockChannelsMode = serviceConfig.GetBlockChannelsMode();
        IdxLookupJoinsPrefixPointLimit = serviceConfig.GetIdxLookupJoinPointsLimit();
        DefaultCostBasedOptimizationLevel = serviceConfig.GetDefaultCostBasedOptimizationLevel();
        DefaultEnableShuffleElimination = serviceConfig.GetDefaultEnableShuffleElimination();
        DefaultDqChannelVersion = serviceConfig.GetDqChannelVersion();
        EnableConstantFolding = serviceConfig.GetEnableConstantFolding();
        EnableFoldUdfs = serviceConfig.GetEnableFoldUdfs();
        SetDefaultEnabledSpillingNodes(serviceConfig.GetEnableSpillingNodes());
        EnableSpilling = serviceConfig.GetEnableQueryServiceSpilling();
        EnableSnapshotIsolationRW = serviceConfig.GetEnableSnapshotIsolationRW();
        AllowMultiBroadcasts = serviceConfig.GetAllowMultiBroadcasts();
        EnableNewRBO = serviceConfig.GetEnableNewRBO();
        EnableSpillingInHashJoinShuffleConnections = serviceConfig.GetEnableSpillingInHashJoinShuffleConnections();
        EnableOlapScalarApply = serviceConfig.GetEnableOlapScalarApply();
        EnableOlapSubstringPushdown = serviceConfig.GetEnableOlapSubstringPushdown();
        EnableIndexStreamWrite = serviceConfig.GetEnableIndexStreamWrite();
        EnableOlapPushdownProjections = serviceConfig.GetEnableOlapPushdownProjections();
        LangVer = serviceConfig.GetDefaultLangVer();
        EnableParallelUnionAllConnectionsForExtend = serviceConfig.GetEnableParallelUnionAllConnectionsForExtend();
        EnableTempTablesForUser = serviceConfig.GetEnableTempTablesForUser();
        EnableSimpleProgramsSinglePartitionOptimization = serviceConfig.GetEnableSimpleProgramsSinglePartitionOptimization();
        EnableSimpleProgramsSinglePartitionOptimizationBroadPrograms = serviceConfig.GetEnableSimpleProgramsSinglePartitionOptimizationBroadPrograms();

        EnableOlapPushdownAggregate = serviceConfig.GetEnableOlapPushdownAggregate();
        EnableOrderOptimizaionFSM = serviceConfig.GetEnableOrderOptimizaionFSM();
        EnableTopSortSelectIndex = serviceConfig.GetEnableTopSortSelectIndex();
        EnablePointPredicateSortAutoSelectIndex = serviceConfig.GetEnablePointPredicateSortAutoSelectIndex();
        EnableDqHashCombineByDefault = serviceConfig.GetEnableDqHashCombineByDefault();
        EnableDqHashAggregateByDefault = serviceConfig.GetEnableDqHashAggregateByDefault();
        EnableWatermarks = serviceConfig.GetEnableWatermarks();
        EnableBuildAggregationResultStages = serviceConfig.GetEnableBuildAggregationResultStages();
        EnableFallbackToYqlOptimizer = serviceConfig.GetEnableFallbackToYqlOptimizer();

        if (const auto limit = serviceConfig.GetResourceManager().GetMkqlHeavyProgramMemoryLimit()) {
            _KqpYqlCombinerMemoryLimit = std::max(1_GB, limit - (limit >> 2U));
        }

        switch (serviceConfig.GetBindingsMode()) {
            case NKikimrConfig::TTableServiceConfig::BM_ENABLED:
                BindingsMode = NSQLTranslation::EBindingsMode::ENABLED;
                break;
            case NKikimrConfig::TTableServiceConfig::BM_DISABLED:
                BindingsMode = NSQLTranslation::EBindingsMode::DISABLED;
                break;
            case NKikimrConfig::TTableServiceConfig::BM_DROP_WITH_WARNING:
                BindingsMode = NSQLTranslation::EBindingsMode::DROP_WITH_WARNING;
                break;
            case NKikimrConfig::TTableServiceConfig::BM_DROP:
                BindingsMode = NSQLTranslation::EBindingsMode::DROP;
                break;
        }

        if (serviceConfig.GetFilterPushdownOverJoinOptionalSide()) {
            FilterPushdownOverJoinOptionalSide = true;
            YqlCoreOptimizerFlags.insert("fuseequijoinsinputmultilabels");
            YqlCoreOptimizerFlags.insert("pullupflatmapoverjoinmultiplelabels");
            YqlCoreOptimizerFlags.insert("sqlinwithnothingornull");
        }

        switch(serviceConfig.GetDefaultHashShuffleFuncType()) {
            case NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V1:
                DefaultHashShuffleFuncType = NYql::NDq::EHashShuffleFuncType::HashV1;
                break;
            case NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2:
                DefaultHashShuffleFuncType = NYql::NDq::EHashShuffleFuncType::HashV2;
                break;
        }

        switch(serviceConfig.GetBackportMode()) {
            case NKikimrConfig::TTableServiceConfig_EBackportMode_Released:
                BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;
                break;
            case NKikimrConfig::TTableServiceConfig_EBackportMode_All:
                BackportMode = NYql::EBackportCompatibleFeaturesMode::All;
                break;
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
    bool EnableDataShardCreateTableAs = false;
    ui64 IdxLookupJoinsPrefixPointLimit = 1;
    bool AllowOlapDataQuery = false;
    bool EnableOlapSink = false;
    bool EnableOltpSink = false;
    bool EnableHtapTx = false;
    bool EnableStreamWrite = false;
    bool EnableBatchUpdates = false;
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
    bool EnableTempTablesForUser = false;
    bool EnableOlapPushdownAggregate = false;
    bool EnableOrderOptimizaionFSM = false;
    bool EnableBuildAggregationResultStages = false;

    bool EnableTopSortSelectIndex = true;
    bool EnablePointPredicateSortAutoSelectIndex = true;
    bool EnableSimpleProgramsSinglePartitionOptimization = true;
    bool EnableSimpleProgramsSinglePartitionOptimizationBroadPrograms = true;
    bool EnableDqHashCombineByDefault = true;
    bool EnableDqHashAggregateByDefault = false;
    bool EnableWatermarks = false;
    ui32 DefaultDqChannelVersion = 1u;
    bool EnableDiscardSelect = false;

    bool Antlr4ParserIsAmbiguityError = false;

    bool EnableFallbackToYqlOptimizer = false;

    ui32 LangVer = NYql::MinLangVersion;
    NYql::EBackportCompatibleFeaturesMode BackportMode = NYql::EBackportCompatibleFeaturesMode::Released;

    NDq::EHashShuffleFuncType DefaultHashShuffleFuncType = NDq::EHashShuffleFuncType::HashV1;
    NDq::EHashShuffleFuncType DefaultColumnShardHashShuffleFuncType = NDq::EHashShuffleFuncType::ColumnShardHashV1;

    void SetDefaultEnabledSpillingNodes(const TString& node);
    ui64 GetEnabledSpillingNodes() const;
    bool GetEnableOlapPushdownProjections() const;
    bool GetEnableParallelUnionAllConnectionsForExtend() const;
    bool GetEnableOlapPushdownAggregate() const;
    bool GetUseDqHashCombine() const;
    bool GetUseDqHashAggregate() const;
};

}
