#pragma once

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>
#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <library/cpp/yson/node/node.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <unordered_map>

namespace NYql {

enum class EQueryCacheMode {
    Disable     /* "disable" */,
    Readonly    /* "readonly" */,
    Refresh     /* "refresh" */,
    Normal      /* "normal" */,
};

enum class EReleaseTempDataMode {
    Never       /* "never" */,
    Immediate   /* "immediate" */,
    Finish      /* "finish" */,
};

enum class ETableContentDeliveryMode {
    Native      /* "native" */,
    File        /* "file" */,
};

enum class EJoinCollectColumnarStatisticsMode {
    Disable     /* "disable" */,
    Sync        /* "sync" */,
    Async       /* "async" */,
};

enum class EUseColumnarStatisticsMode {
    Disable     /* "disable" */,
    Auto        /* "auto" */,
    Force       /* "force" */,
};

enum class EStoredConstraint : ui64 {
    None     = 0ULL /* "None" */,
    Sorted   = 1ULL /* "Sorted" */,
    Unique   = 2ULL /* "Unique" */,
    Distinct = 4ULL /* "Distinct" */,
};

enum class EInferSchemaMode {
    Sequential = 0ULL  /* "sequential" */,
    Parallel   = 1ULL  /* "parallel" */,
    RPC        = 2ULL  /* "rpc" */,
};

enum class EColumnGroupMode {
    Disable     /* "disable" */,
    Single      /* "single" */,
    PerUsage    /* "perusage", "per-usage" */,
};

enum class EBlockOutputMode {
    Disable  /* "disable" */,
    Auto     /* "auto" */,
    Force    /* "force" */,
};

enum class ERuntimeClusterSelectionMode {
    Disable  /* "disable" */,
    Auto     /* "auto" */,
    Force    /* "force" */,
};

enum class EConvertDynamicTablesToStatic {
    Disable         /* "disable" */,
    Join            /* "join" */,
    All             /* "all" */,
};

enum class EFuseMapToMapReduceMode {
    Disable  /* "disable" */,
    Normal   /* "normal" */,
    Late     /* "late" */,
};

struct TYtSettings {
private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
    static constexpr NCommon::EConfSettingType Dynamic = NCommon::EConfSettingType::Dynamic;
    static constexpr NCommon::EConfSettingType StaticPerCluster = NCommon::EConfSettingType::StaticPerCluster;
public:

    using TConstPtr = std::shared_ptr<const TYtSettings>;

    // should be static, because are used on earlier stages

    // static per-cluster
    NCommon::TConfSetting<TGUID, StaticPerCluster> ExternalTx;
    NCommon::TConfSetting<TString, StaticPerCluster> TmpFolder;
    NCommon::TConfSetting<TString, StaticPerCluster> TablesTmpFolder;
    NCommon::TConfSetting<TString, StaticPerCluster> BinaryTmpFolder;
    NCommon::TConfSetting<TString, StaticPerCluster> StaticPool;
    NCommon::TConfSetting<TString, StaticPerCluster> StaticNetworkProject;
    NCommon::TConfSetting<TString, StaticPerCluster> CoreDumpPath;
    NCommon::TConfSetting<bool, StaticPerCluster> JobBlockInput;
    NCommon::TConfSetting<bool, StaticPerCluster> JobBlockTableContent;
    NCommon::TConfSetting<TSet<TString>, StaticPerCluster> JobBlockInputSupportedTypes;
    NCommon::TConfSetting<TSet<NUdf::EDataSlot>, StaticPerCluster> JobBlockInputSupportedDataTypes;
    NCommon::TConfSetting<EBlockOutputMode, StaticPerCluster> JobBlockOutput;
    NCommon::TConfSetting<TSet<TString>, StaticPerCluster> JobBlockOutputSupportedTypes;
    NCommon::TConfSetting<TSet<NUdf::EDataSlot>, StaticPerCluster> JobBlockOutputSupportedDataTypes;
    NCommon::TConfSetting<bool, StaticPerCluster> ValidatePool;
    NCommon::TConfSetting<TString, StaticPerCluster> _QueryDumpFolder;
    NCommon::TConfSetting<TString, StaticPerCluster> _QueryDumpAccount;
    NCommon::TConfSetting<bool, StaticPerCluster> _EnableDynamicTablesWrite;

    // static global
    NCommon::TConfSetting<TString, Static> Auth;
    NCommon::TConfSetting<TDuration, Static> TempTablesTtl;
    NCommon::TConfSetting<bool, Static> KeepTempTables;
    NCommon::TConfSetting<ui32, Static> InflightTempTablesLimit;
    NCommon::TConfSetting<EReleaseTempDataMode, Static> ReleaseTempData;
    NCommon::TConfSetting<bool, Static> IgnoreYamrDsv;
    NCommon::TConfSetting<bool, Static> IgnoreWeakSchema;
    NCommon::TConfSetting<ui32, Static> InferSchema;
    NCommon::TConfSetting<ui32, Static> ForceInferSchema;
    NCommon::TConfSetting<ui32, Static> InferSchemaTableCountThreshold;
    NCommon::TConfSetting<NSize::TSize, Static> DefaultCalcMemoryLimit;
    NCommon::TConfSetting<ui32, Static> ParallelOperationsLimit;
    NCommon::TConfSetting<ui32, Static> LocalCalcLimit;
    NCommon::TConfSetting<EQueryCacheMode, Static> QueryCacheMode;
    NCommon::TConfSetting<bool, Static> QueryCacheIgnoreTableRevision;
    NCommon::TConfSetting<TString, Static> QueryCacheSalt;
    NCommon::TConfSetting<TDuration, Static> QueryCacheTtl;
    NCommon::TConfSetting<bool, Static> QueryCacheUseExpirationTimeout;
    NCommon::TConfSetting<bool, Static> QueryCacheUseForCalc;
    NCommon::TConfSetting<ui32, Static> DefaultMaxJobFails;
    NCommon::TConfSetting<TString, Static> DefaultCluster;
    NCommon::TConfSetting<TDuration, Static> BinaryExpirationInterval;
    NCommon::TConfSetting<bool, Static> IgnoreTypeV3;
    NCommon::TConfSetting<bool, Static> _UseMultisetAttributes;
    NCommon::TConfSetting<TDuration, Static> FileCacheTtl;
    NCommon::TConfSetting<TString, Static> _ImpersonationUser;
    NCommon::TConfSetting<EInferSchemaMode, Static> InferSchemaMode;
    NCommon::TConfSetting<ui32, Static> BatchListFolderConcurrency;
    NCommon::TConfSetting<bool, Static> ForceTmpSecurity;
    NCommon::TConfSetting<ERuntimeClusterSelectionMode, Static> RuntimeClusterSelection;
    NCommon::TConfSetting<TString, Static> DefaultRuntimeCluster;
    NCommon::TConfSetting<bool, Static> _ForbidSensitiveDataInOperationSpec;
    NCommon::TConfSetting<NSize::TSize, Static> _LocalTableContentLimit;
    NCommon::TConfSetting<bool, Static> EnableDynamicStoreReadInDQ;
    NCommon::TConfSetting<bool, Static> UseDefaultArrowAllocatorInJobs;
    NCommon::TConfSetting<bool, Static> UseNativeYtDefaultColumnOrder;
    NCommon::TConfSetting<bool, Static> EarlyPartitionPruning;
    NCommon::TConfSetting<bool, Static> ValidateClusters;
    NCommon::TConfSetting<NSize::TSize, Static> _QueryDumpTableSizeLimit;
    NCommon::TConfSetting<ui32, Static> _QueryDumpTableCountPerClusterLimit;
    NCommon::TConfSetting<ui32, Static> _QueryDumpFileCountPerOperationLimit;
    NCommon::TConfSetting<bool, Static> KeepWorldDepForFillOp;
    NCommon::TConfSetting<ui32, Static> CostBasedOptimizerPartial;

    // Job runtime
    NCommon::TConfSetting<TString, Dynamic> Pool;
    NCommon::TConfSetting<NSize::TSize, Dynamic> DefaultMemoryLimit;
    NCommon::TConfSetting<double, Dynamic> DefaultMemoryReserveFactor;
    NCommon::TConfSetting<double, Dynamic> DefaultMemoryDigestLowerBound;
    NCommon::TConfSetting<NSize::TSize, Dynamic> MaxRowWeight;
    NCommon::TConfSetting<NSize::TSize, Dynamic> MaxKeyWeight;
    NCommon::TConfSetting<ui32, Dynamic> BufferRowCount;
    NCommon::TConfSetting<NSize::TSize, Dynamic> DataSizePerJob;
    NCommon::TConfSetting<NSize::TSize, Dynamic> DataSizePerSortJob;
    NCommon::TConfSetting<NSize::TSize, Dynamic> DataSizePerMapJob;
    NCommon::TConfSetting<NSize::TSize, Dynamic> DataSizePerPartition;
    NCommon::TConfSetting<TDuration, Dynamic> DefaultLocalityTimeout;
    NCommon::TConfSetting<TDuration, Dynamic> MapLocalityTimeout;
    NCommon::TConfSetting<TDuration, Dynamic> ReduceLocalityTimeout;
    NCommon::TConfSetting<TDuration, Dynamic> SortLocalityTimeout;
    NCommon::TConfSetting<NSize::TSize, Dynamic> MinLocalityInputDataWeight;
    NCommon::TConfSetting<ui64, Dynamic> MaxJobCount;
    NCommon::TConfSetting<ui64, Dynamic> UserSlots;
    NCommon::TConfSetting<double, Dynamic> DefaultOperationWeight;
    NCommon::TConfSetting<double, Dynamic> DefaultMapSelectivityFactor;
    NCommon::TConfSetting<bool, Dynamic> NightlyCompress;
    NCommon::TConfSetting<TString, Dynamic> PublishedCompressionCodec;
    NCommon::TConfSetting<TString, Dynamic> TemporaryCompressionCodec;
    NCommon::TConfSetting<NYT::EErasureCodecAttr, Dynamic> PublishedErasureCodec;
    NCommon::TConfSetting<NYT::EErasureCodecAttr, Dynamic> TemporaryErasureCodec;
    NCommon::TConfSetting<TDuration, Dynamic> ClientMapTimeout; // TODO: yt_native
    NCommon::TConfSetting<bool, Dynamic> UseTmpfs;
    NCommon::TConfSetting<bool, Dynamic> SuspendIfAccountLimitExceeded;
    NCommon::TConfSetting<NSize::TSize, Dynamic> ExtraTmpfsSize;
    NCommon::TConfSetting<NYT::EOptimizeForAttr, Dynamic> OptimizeFor; // {scan, lookup}
    NCommon::TConfSetting<TInstant, Dynamic> ExpirationDeadline;
    NCommon::TConfSetting<TDuration, Dynamic> ExpirationInterval;
    NCommon::TConfSetting<double, Dynamic> ScriptCpu;
    NCommon::TConfSetting<double, Dynamic> PythonCpu;
    NCommon::TConfSetting<double, Dynamic> JavascriptCpu;
    NCommon::TConfSetting<double, Dynamic> ErasureCodecCpu;
    NCommon::TConfSetting<double, Dynamic> ErasureCodecCpuForDq;
    NCommon::TConfSetting<TSet<TString>, Dynamic> Owners;
    NCommon::TConfSetting<TSet<TString>, Dynamic> OperationReaders;
    NCommon::TConfSetting<TString, Dynamic> SchedulingTag;
    NCommon::TConfSetting<TString, Dynamic> SchedulingTagFilter;
    NCommon::TConfSetting<TSet<TString>, Dynamic> PoolTrees;
    NCommon::TConfSetting<TSet<TString>, Dynamic> TentativePoolTrees;
    NCommon::TConfSetting<ui32, Dynamic> TentativeTreeEligibilitySampleJobCount;
    NCommon::TConfSetting<double, Dynamic> TentativeTreeEligibilityMaxJobDurationRatio;
    NCommon::TConfSetting<ui32, Dynamic> TentativeTreeEligibilityMinJobDuration;
    NCommon::TConfSetting<bool, Dynamic> UseDefaultTentativePoolTrees;
    NCommon::TConfSetting<TString, Dynamic> IntermediateAccount;
    NCommon::TConfSetting<ui32, Dynamic> IntermediateReplicationFactor;
    NCommon::TConfSetting<ui32, Dynamic> PublishedReplicationFactor;
    NCommon::TConfSetting<ui32, Dynamic> TemporaryReplicationFactor;
    NCommon::TConfSetting<TString, Dynamic> AutoMerge; // {relaxed, economy, disabled}
    NCommon::TConfSetting<TString, Dynamic> PublishedAutoMerge;
    NCommon::TConfSetting<TString, Dynamic> TemporaryAutoMerge;
    NCommon::TConfSetting<TVector<TString>, Dynamic> LayerPaths;
    NCommon::TConfSetting<THashMap<TString, TVector<TString>>, StaticPerCluster> LayerCaches;
    NCommon::TConfSetting<TString, Dynamic> DockerImage;
    NCommon::TConfSetting<NYT::TNode, Dynamic> JobEnv;
    NCommon::TConfSetting<NYT::TNode, Dynamic> OperationSpec;
    NCommon::TConfSetting<NYT::TNode, Dynamic> FmrOperationSpec;
    NCommon::TConfSetting<NYT::TNode, Dynamic> Annotations;
    NCommon::TConfSetting<NYT::TNode, Dynamic> StartedBy;
    NCommon::TConfSetting<NYT::TNode, Dynamic> Description;
    NCommon::TConfSetting<bool, Dynamic> UseSkiff;
    NCommon::TConfSetting<ui32, Dynamic> TableContentCompressLevel;
    NCommon::TConfSetting<bool, Dynamic> DisableJobSplitting;
    NCommon::TConfSetting<EUseColumnarStatisticsMode, Dynamic> UseColumnarStatistics;
    NCommon::TConfSetting<ETableContentDeliveryMode, Dynamic> TableContentDeliveryMode;
    NCommon::TConfSetting<bool, Dynamic> TableContentUseSkiff;
    NCommon::TConfSetting<TString, Dynamic> TableContentTmpFolder;
    NCommon::TConfSetting<bool, Dynamic> TableContentColumnarStatistics;
    NCommon::TConfSetting<TString, Dynamic> GeobaseDownloadUrl;
    NCommon::TConfSetting<ui32, Dynamic> MaxSpeculativeJobCountPerTask;
    NCommon::TConfSetting<NSize::TSize, Dynamic> LLVMMemSize;
    NCommon::TConfSetting<NSize::TSize, Dynamic> LLVMPerNodeMemSize;
    NCommon::TConfSetting<ui64, Dynamic> LLVMNodeCountLimit;
    NCommon::TConfSetting<NSize::TSize, Dynamic> SamplingIoBlockSize;
    NCommon::TConfSetting<NYT::TNode, Dynamic> PublishedMedia;
    NCommon::TConfSetting<NYT::TNode, Dynamic> TemporaryMedia;
    NCommon::TConfSetting<TString, Dynamic> PublishedPrimaryMedium;
    NCommon::TConfSetting<TString, Dynamic> TemporaryPrimaryMedium;
    NCommon::TConfSetting<TString, Dynamic> IntermediateDataMedium;
    NCommon::TConfSetting<TString, Dynamic> PrimaryMedium;
    NCommon::TConfSetting<ui64, Dynamic> QueryCacheChunkLimit;
    NCommon::TConfSetting<ui64, Dynamic> NativeYtTypeCompatibility;
    NCommon::TConfSetting<bool, Dynamic> _UseKeyBoundApi;
    NCommon::TConfSetting<TString, Dynamic> NetworkProject;
    NCommon::TConfSetting<bool, Dynamic> _EnableYtPartitioning;
    NCommon::TConfSetting<bool, Dynamic> ForceJobSizeAdjuster;
    NCommon::TConfSetting<bool, Dynamic> EnforceJobUtc;
    NCommon::TConfSetting<ui64, Static> _EnforceRegexpProbabilityFail;
    NCommon::TConfSetting<bool, Dynamic> UseRPCReaderInDQ;
    NCommon::TConfSetting<size_t, Dynamic> DQRPCReaderInflight;
    NCommon::TConfSetting<TDuration, Dynamic> DQRPCReaderTimeout;
    NCommon::TConfSetting<TSet<TString>, Dynamic> BlockReaderSupportedTypes;
    NCommon::TConfSetting<TSet<NUdf::EDataSlot>, Dynamic> BlockReaderSupportedDataTypes;
    NCommon::TConfSetting<TString, Dynamic> _BinaryCacheFolder;
    NCommon::TConfSetting<TString, Dynamic> RuntimeCluster;
    NCommon::TConfSetting<bool, Dynamic> _AllowRemoteClusterInput;
    NCommon::TConfSetting<bool, Dynamic> _EnableDq;

    // Optimizers (static global)
    NCommon::TConfSetting<ui32, Static> ExtendTableLimit; // Deprecated. Use MaxInputTables instead
    NCommon::TConfSetting<NSize::TSize, Static> CommonJoinCoreLimit;
    NCommon::TConfSetting<NSize::TSize, Static> CombineCoreLimit;
    NCommon::TConfSetting<NSize::TSize, Static> SwitchLimit;
    NCommon::TConfSetting<ui64, Static> JoinMergeTablesLimit;
    NCommon::TConfSetting<bool, Static> JoinMergeUseSmallAsPrimary;
    NCommon::TConfSetting<NSize::TSize, Static> JoinMergeReduceJobMaxSize;
    NCommon::TConfSetting<double, Static> JoinMergeUnsortedFactor; // (>=0.0)
    NCommon::TConfSetting<bool, Static> JoinMergeForce;
    NCommon::TConfSetting<NSize::TSize, Static> MapJoinLimit;
    NCommon::TConfSetting<ui64, Static> MapJoinShardMinRows;
    NCommon::TConfSetting<ui64, Static> MapJoinShardCount; // [1-10]
    NCommon::TConfSetting<bool, Static> MapJoinUseFlow;
    NCommon::TConfSetting<bool, Static> BlockMapJoin;
    NCommon::TConfSetting<NSize::TSize, Static> LookupJoinLimit;
    NCommon::TConfSetting<ui64, Static> LookupJoinMaxRows;
    NCommon::TConfSetting<EConvertDynamicTablesToStatic, Static> ConvertDynamicTablesToStatic;
    NCommon::TConfSetting<bool, Static> KeepMergeWithDynamicInput;
    NCommon::TConfSetting<NSize::TSize, Static> EvaluationTableSizeLimit;
    NCommon::TConfSetting<TSet<TString>, Static> DisableOptimizers;
    NCommon::TConfSetting<ui32, Static> MaxInputTables;
    NCommon::TConfSetting<ui32, Static> MaxInputTablesForSortedMerge;
    NCommon::TConfSetting<ui32, Static> MaxOutputTables;
    NCommon::TConfSetting<bool, Static> DisableFuseOperations;
    NCommon::TConfSetting<bool, Static> EnableFuseMapToMapReduce; // Deprecated. Use FuseMapToMapReduceMode
    NCommon::TConfSetting<EFuseMapToMapReduceMode, Static> FuseMapToMapReduce;
    NCommon::TConfSetting<NSize::TSize, Static> MaxExtraJobMemoryToFuseOperations;
    NCommon::TConfSetting<double, Static> MaxReplicationFactorToFuseOperations;
    NCommon::TConfSetting<ui32, Static> MaxOperationFiles;
    NCommon::TConfSetting<NSize::TSize, Static> MinPublishedAvgChunkSize;
    NCommon::TConfSetting<NSize::TSize, Static> MinTempAvgChunkSize;
    NCommon::TConfSetting<ui32, Static> TopSortMaxLimit;
    NCommon::TConfSetting<NSize::TSize, Static> TopSortSizePerJob;
    NCommon::TConfSetting<ui32, Static> TopSortRowMultiplierPerJob;
    NCommon::TConfSetting<bool, Static> JoinUseColumnarStatistics; // Deprecated. Use JoinCollectColumnarStatistics instead
    NCommon::TConfSetting<EJoinCollectColumnarStatisticsMode, Static> JoinCollectColumnarStatistics;
    NCommon::TConfSetting<NYT::EColumnarStatisticsFetcherMode, Static> JoinColumnarStatisticsFetcherMode;
    NCommon::TConfSetting<bool, Static> JoinWaitAllInputs;
    NCommon::TConfSetting<bool, Static> JoinAllowColumnRenames;
    NCommon::TConfSetting<bool, Static> JoinMergeSetTopLevelFullSort;
    NCommon::TConfSetting<bool, Static> JoinEnableStarJoin;
    NCommon::TConfSetting<NSize::TSize, Static> FolderInlineDataLimit;
    NCommon::TConfSetting<ui32, Static> FolderInlineItemsLimit;
    NCommon::TConfSetting<NSize::TSize, Static> TableContentMinAvgChunkSize;
    NCommon::TConfSetting<ui32, Static> TableContentMaxInputTables;
    NCommon::TConfSetting<ui32, Static> TableContentMaxChunksForNativeDelivery;
    NCommon::TConfSetting<NSize::TSize, Static> TableContentLocalExecution;
    NCommon::TConfSetting<bool, Static> UseTypeV2;
    NCommon::TConfSetting<bool, Static> UseNativeYtTypes;
    NCommon::TConfSetting<bool, Static> UseNativeDescSort;
    NCommon::TConfSetting<bool, Static> UseIntermediateSchema;
    NCommon::TConfSetting<bool, Static> UseIntermediateStreams;
    NCommon::TConfSetting<bool, Static> PassSqlFlagsForViewTranslation;
    NCommon::TConfSetting<bool, Static> UseFlow;
    NCommon::TConfSetting<ui16, Static> WideFlowLimit;
    NCommon::TConfSetting<bool, Static> UseSystemColumns;
    NCommon::TConfSetting<bool, Static> HybridDqExecution;
    NCommon::TConfSetting<TDuration, Static> HybridDqTimeSpentLimit;
    NCommon::TConfSetting<NSize::TSize, Static> HybridDqDataSizeLimitForOrdered;
    NCommon::TConfSetting<NSize::TSize, Static> HybridDqDataSizeLimitForUnordered;
    NCommon::TConfSetting<bool, Static> HybridDqExecutionFallback;
    NCommon::TConfSetting<bool, Static> UseYqlRowSpecCompactForm;
    NCommon::TConfSetting<bool, Static> UseNewPredicateExtraction;
    NCommon::TConfSetting<bool, Static> PruneKeyFilterLambda;
    NCommon::TConfSetting<bool, Static> DqPruneKeyFilterLambda;
    NCommon::TConfSetting<bool, Static> UseQLFilter;
    NCommon::TConfSetting<bool, Static> PruneQLFilterLambda;
    NCommon::TConfSetting<bool, Static> MergeAdjacentPointRanges;
    NCommon::TConfSetting<bool, Static> KeyFilterForStartsWith;
    NCommon::TConfSetting<ui64, Static> MaxKeyRangeCount;
    NCommon::TConfSetting<ui64, Static> MaxChunksForDqRead;
    NCommon::TConfSetting<bool, Static> JoinCommonUseMapMultiOut;
    NCommon::TConfSetting<bool, Static> UseAggPhases;
    NCommon::TConfSetting<bool, Static> UsePartitionsByKeysForFinalAgg;
    NCommon::TConfSetting<double, Static> MaxCpuUsageToFuseMultiOuts;
    NCommon::TConfSetting<double, Static> MaxReplicationFactorToFuseMultiOuts;
    NCommon::TConfSetting<ui64, Static> ApplyStoredConstraints;
    NCommon::TConfSetting<bool, Static> ViewIsolation;
    NCommon::TConfSetting<bool, Static> PartitionByConstantKeysViaMap;
    NCommon::TConfSetting<EColumnGroupMode, Static> ColumnGroupMode;
    NCommon::TConfSetting<ui16, Static> MinColumnGroupSize;
    NCommon::TConfSetting<ui16, Static> MaxColumnGroups;
    NCommon::TConfSetting<ui64, Static> ExtendedStatsMaxChunkCount;
    NCommon::TConfSetting<bool, Static> _EnableYtDqProcessWriteConstraints;
    NCommon::TConfSetting<bool, Static> CompactForDistinct;
    NCommon::TConfSetting<bool, Static> DropUnusedKeysFromKeyFilter;
    NCommon::TConfSetting<bool, Static> ReportEquiJoinStats;
    NCommon::TConfSetting<bool, Static> UseColumnGroupsFromInputTables;
    NCommon::TConfSetting<bool, Static> UseNativeDynamicTableRead;
    NCommon::TConfSetting<bool, Static> DontForceTransformForInputTables;
    NCommon::TConfSetting<bool, Static> _RequestOnlyRequiredAttrs;
    NCommon::TConfSetting<bool, Static> _CacheSchemaBySchemaId;
};

EReleaseTempDataMode GetReleaseTempDataMode(const TYtSettings& settings);
EJoinCollectColumnarStatisticsMode GetJoinCollectColumnarStatisticsMode(const TYtSettings& settings);
inline TString GetTablesTmpFolder(const TYtSettings& settings, const TString& cluster) {
    return settings.TablesTmpFolder.Get(cluster).GetOrElse(settings.TmpFolder.Get(cluster).GetOrElse({}));
}

struct TYtConfiguration : public TYtSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TYtConfiguration>;

    TYtConfiguration(TTypeAnnotationContext& typeCtx, const TQContext& qContext = {});
    TYtConfiguration(const TYtConfiguration&) = delete;

    template <class TProtoConfig, typename TFilter>
    void Init(const TProtoConfig& config, const TFilter& filter, TTypeAnnotationContext& typeCtx) {
        TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
        for (auto& cluster: config.GetClusterMapping()) {
            clusters.push_back(cluster.GetName());
            Tokens[cluster.GetName()] = typeCtx.Credentials->FindCredentialContent("cluster:default_" + cluster.GetName(), "default_yt", cluster.GetYTToken());
        }

        auto impersonationUser = typeCtx.Credentials->FindCredential("impersonation_user_yt");
        if (impersonationUser) {
            _ImpersonationUser = impersonationUser->Content;
        }

        this->SetValidClusters(clusters);

        // Init settings from config
        this->Dispatch(config.GetDefaultSettings(), filter);
        for (auto& cluster: config.GetClusterMapping()) {
            this->Dispatch(cluster.GetName(), cluster.GetSettings(), filter);
        }
        this->FreezeDefaults();
    }

    TYtSettings::TConstPtr Snapshot() const;

    THashMap<TString, TString> Tokens;
};

class TYtVersionedConfiguration: public TYtConfiguration {
public:
    using TPtr = TIntrusivePtr<TYtVersionedConfiguration>;

    struct TState {
        TVector<TYtSettings::TConstPtr> FrozenSettings;
        std::unordered_map<ui64, size_t> NodeIdToVer;
        TYtSettings::TConstPtr Snapshot;
    };

    TYtVersionedConfiguration(TTypeAnnotationContext& types, const TQContext& qContext = {})
        : TYtConfiguration(types, qContext)
    {
    }

    ~TYtVersionedConfiguration() = default;

    size_t FindNodeVer(const TExprNode& node);
    void CopyNodeVer(const TExprNode& from, const TExprNode& to);
    void FreezeZeroVersion();
    void PromoteVersion(const TExprNode& node);
    size_t GetLastVersion() const {
        return FrozenSettings.empty() ? 0 : FrozenSettings.size() - 1;
    }
    TYtSettings::TConstPtr GetSettingsForNode(const TExprNode& node);
    TYtSettings::TConstPtr GetSettingsVer(size_t ver);
    void ClearVersions();

    TState GetState() const {
        return TState{FrozenSettings, NodeIdToVer, Snapshot()};
    }

    void RestoreState(TState&& state) {
        FrozenSettings = std::move(state.FrozenSettings);
        NodeIdToVer = std::move(state.NodeIdToVer);
        *((TYtSettings*)this) = *state.Snapshot;
    }

private:
    TVector<TYtSettings::TConstPtr> FrozenSettings;
    std::unordered_map<ui64, size_t> NodeIdToVer;
};

bool ValidateCompressionCodecValue(const TString& codec);
void MediaValidator(const NYT::TNode& value);

} // NYql
