#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/ast/yql_expr.h>

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


struct TYtSettings {
    using TConstPtr = std::shared_ptr<const TYtSettings>;

    // should be static, because are used on earlier stages
    NCommon::TConfSetting<TString, false> Auth;
    NCommon::TConfSetting<TGUID, false> ExternalTx;
    NCommon::TConfSetting<TString, false> TmpFolder;
    NCommon::TConfSetting<TString, false> TablesTmpFolder;
    NCommon::TConfSetting<TDuration, false> TempTablesTtl;
    NCommon::TConfSetting<bool, false> KeepTempTables;
    NCommon::TConfSetting<ui32, false> InflightTempTablesLimit;
    NCommon::TConfSetting<EReleaseTempDataMode, false> ReleaseTempData;
    NCommon::TConfSetting<bool, false> IgnoreYamrDsv;
    NCommon::TConfSetting<bool, false> IgnoreWeakSchema;
    NCommon::TConfSetting<ui32, false> InferSchema;
    NCommon::TConfSetting<ui32, false> ForceInferSchema;
    NCommon::TConfSetting<ui32, false> InferSchemaTableCountThreshold;
    NCommon::TConfSetting<NSize::TSize, false> DefaultCalcMemoryLimit;
    NCommon::TConfSetting<ui32, false> ParallelOperationsLimit;
    NCommon::TConfSetting<EQueryCacheMode, false> QueryCacheMode;
    NCommon::TConfSetting<bool, false> QueryCacheIgnoreTableRevision;
    NCommon::TConfSetting<TString, false> QueryCacheSalt;
    NCommon::TConfSetting<TDuration, false> QueryCacheTtl;
    NCommon::TConfSetting<bool, false> QueryCacheUseExpirationTimeout;
    NCommon::TConfSetting<bool, false> QueryCacheUseForCalc;
    NCommon::TConfSetting<ui32, false> DefaultMaxJobFails;
    NCommon::TConfSetting<TString, false> CoreDumpPath;
    NCommon::TConfSetting<TString, false> DefaultCluster;
    NCommon::TConfSetting<TString, false> StaticPool;
    NCommon::TConfSetting<TString, false> BinaryTmpFolder;
    NCommon::TConfSetting<TDuration, false> BinaryExpirationInterval;
    NCommon::TConfSetting<bool, false> IgnoreTypeV3;
    NCommon::TConfSetting<bool, false> _UseMultisetAttributes;
    NCommon::TConfSetting<TDuration, false> FileCacheTtl;
    NCommon::TConfSetting<TString, false> _ImpersonationUser;
    NCommon::TConfSetting<EInferSchemaMode, false> InferSchemaMode;
    NCommon::TConfSetting<ui32, false> BatchListFolderConcurrency;
    NCommon::TConfSetting<bool, false> ForceTmpSecurity;

    // Job runtime
    NCommon::TConfSetting<TString, true> Pool;
    NCommon::TConfSetting<NSize::TSize, true> DefaultMemoryLimit;
    NCommon::TConfSetting<double, true> DefaultMemoryReserveFactor;
    NCommon::TConfSetting<double, true> DefaultMemoryDigestLowerBound;
    NCommon::TConfSetting<NSize::TSize, true> MaxRowWeight;
    NCommon::TConfSetting<NSize::TSize, true> MaxKeyWeight;
    NCommon::TConfSetting<ui32, true> BufferRowCount;
    NCommon::TConfSetting<NSize::TSize, true> DataSizePerJob;
    NCommon::TConfSetting<NSize::TSize, true> DataSizePerSortJob;
    NCommon::TConfSetting<NSize::TSize, true> DataSizePerMapJob;
    NCommon::TConfSetting<NSize::TSize, true> DataSizePerPartition;
    NCommon::TConfSetting<TDuration, true> DefaultLocalityTimeout;
    NCommon::TConfSetting<TDuration, true> MapLocalityTimeout;
    NCommon::TConfSetting<TDuration, true> ReduceLocalityTimeout;
    NCommon::TConfSetting<TDuration, true> SortLocalityTimeout;
    NCommon::TConfSetting<NSize::TSize, true> MinLocalityInputDataWeight;
    NCommon::TConfSetting<ui64, true> MaxJobCount;
    NCommon::TConfSetting<ui64, true> UserSlots;
    NCommon::TConfSetting<double, true> DefaultOperationWeight;
    NCommon::TConfSetting<double, true> DefaultMapSelectivityFactor;
    NCommon::TConfSetting<bool, true> NightlyCompress;
    NCommon::TConfSetting<TString, true> PublishedCompressionCodec;
    NCommon::TConfSetting<TString, true> TemporaryCompressionCodec;
    NCommon::TConfSetting<NYT::EErasureCodecAttr, true> PublishedErasureCodec;
    NCommon::TConfSetting<NYT::EErasureCodecAttr, true> TemporaryErasureCodec;
    NCommon::TConfSetting<TDuration, true> ClientMapTimeout; // TODO: yt_native
    NCommon::TConfSetting<bool, true> UseTmpfs;
    NCommon::TConfSetting<bool, true> SuspendIfAccountLimitExceeded;
    NCommon::TConfSetting<NSize::TSize, true> ExtraTmpfsSize;
    NCommon::TConfSetting<NYT::EOptimizeForAttr, true> OptimizeFor; // {scan, lookup}
    NCommon::TConfSetting<TInstant, true> ExpirationDeadline;
    NCommon::TConfSetting<TDuration, true> ExpirationInterval;
    NCommon::TConfSetting<double, true> ScriptCpu;
    NCommon::TConfSetting<double, true> PythonCpu;
    NCommon::TConfSetting<double, true> JavascriptCpu;
    NCommon::TConfSetting<double, true> ErasureCodecCpu;
    NCommon::TConfSetting<double, true> ErasureCodecCpuForDq;
    NCommon::TConfSetting<TSet<TString>, true> Owners;
    NCommon::TConfSetting<TSet<TString>, true> OperationReaders;
    NCommon::TConfSetting<TString, true> SchedulingTag;
    NCommon::TConfSetting<TString, true> SchedulingTagFilter;
    NCommon::TConfSetting<TSet<TString>, true> PoolTrees;
    NCommon::TConfSetting<TSet<TString>, true> TentativePoolTrees;
    NCommon::TConfSetting<ui32, true> TentativeTreeEligibilitySampleJobCount;
    NCommon::TConfSetting<double, true> TentativeTreeEligibilityMaxJobDurationRatio;
    NCommon::TConfSetting<ui32, true> TentativeTreeEligibilityMinJobDuration;
    NCommon::TConfSetting<bool, true> UseDefaultTentativePoolTrees;
    NCommon::TConfSetting<TString, true> IntermediateAccount;
    NCommon::TConfSetting<ui32, true> IntermediateReplicationFactor;
    NCommon::TConfSetting<ui32, true> PublishedReplicationFactor;
    NCommon::TConfSetting<ui32, true> TemporaryReplicationFactor;
    NCommon::TConfSetting<TString, true> AutoMerge; // {relaxed, economy, disabled}
    NCommon::TConfSetting<TString, true> PublishedAutoMerge;
    NCommon::TConfSetting<TString, true> TemporaryAutoMerge;
    NCommon::TConfSetting<TVector<TString>, true> LayerPaths;
    NCommon::TConfSetting<TString, true> DockerImage;
    NCommon::TConfSetting<NYT::TNode, true> JobEnv;
    NCommon::TConfSetting<NYT::TNode, true> OperationSpec;
    NCommon::TConfSetting<NYT::TNode, true> Annotations;
    NCommon::TConfSetting<NYT::TNode, true> StartedBy;
    NCommon::TConfSetting<NYT::TNode, true> Description;
    NCommon::TConfSetting<bool, true> UseSkiff;
    NCommon::TConfSetting<ui32, true> TableContentCompressLevel;
    NCommon::TConfSetting<bool, true> DisableJobSplitting;
    NCommon::TConfSetting<EUseColumnarStatisticsMode, true> UseColumnarStatistics;
    NCommon::TConfSetting<ETableContentDeliveryMode, true> TableContentDeliveryMode;
    NCommon::TConfSetting<bool, true> TableContentUseSkiff;
    NCommon::TConfSetting<TString, true> TableContentTmpFolder;
    NCommon::TConfSetting<bool, true> TableContentColumnarStatistics;
    NCommon::TConfSetting<TString, true> GeobaseDownloadUrl;
    NCommon::TConfSetting<ui32, true> MaxSpeculativeJobCountPerTask;
    NCommon::TConfSetting<NSize::TSize, true> LLVMMemSize;
    NCommon::TConfSetting<NSize::TSize, true> LLVMPerNodeMemSize;
    NCommon::TConfSetting<ui64, true> LLVMNodeCountLimit;
    NCommon::TConfSetting<NSize::TSize, true> SamplingIoBlockSize;
    NCommon::TConfSetting<NYT::TNode, true> PublishedMedia;
    NCommon::TConfSetting<NYT::TNode, true> TemporaryMedia;
    NCommon::TConfSetting<TString, true> PublishedPrimaryMedium;
    NCommon::TConfSetting<TString, true> TemporaryPrimaryMedium;
    NCommon::TConfSetting<TString, true> IntermediateDataMedium;
    NCommon::TConfSetting<TString, true> PrimaryMedium;
    NCommon::TConfSetting<ui64, true> QueryCacheChunkLimit;
    NCommon::TConfSetting<ui64, true> NativeYtTypeCompatibility;
    NCommon::TConfSetting<bool, true> _UseKeyBoundApi;
    NCommon::TConfSetting<TString, true> NetworkProject;
    NCommon::TConfSetting<bool, true> _EnableYtPartitioning;
    NCommon::TConfSetting<bool, true> _ForceJobSizeAdjuster;
    NCommon::TConfSetting<bool, true> EnforceJobUtc;
    NCommon::TConfSetting<bool, true> UseRPCReaderInDQ;
    NCommon::TConfSetting<size_t, true> DQRPCReaderInflight;
    NCommon::TConfSetting<TDuration, true> DQRPCReaderTimeout;
    NCommon::TConfSetting<TSet<TString>, true> BlockReaderSupportedTypes;
    NCommon::TConfSetting<TSet<NUdf::EDataSlot>, true> BlockReaderSupportedDataTypes;

    // Optimizers
    NCommon::TConfSetting<bool, true> _EnableDq;
    NCommon::TConfSetting<ui32, false> ExtendTableLimit; // Deprecated. Use MaxInputTables instead
    NCommon::TConfSetting<NSize::TSize, false> CommonJoinCoreLimit;
    NCommon::TConfSetting<NSize::TSize, false> CombineCoreLimit;
    NCommon::TConfSetting<NSize::TSize, false> SwitchLimit;
    NCommon::TConfSetting<ui64, false> JoinMergeTablesLimit;
    NCommon::TConfSetting<bool, false> JoinMergeUseSmallAsPrimary;
    NCommon::TConfSetting<NSize::TSize, false> JoinMergeReduceJobMaxSize;
    NCommon::TConfSetting<double, false> JoinMergeUnsortedFactor; // (>=0.0)
    NCommon::TConfSetting<bool, false> JoinMergeForce;
    NCommon::TConfSetting<NSize::TSize, false> MapJoinLimit;
    NCommon::TConfSetting<ui64, false> MapJoinShardMinRows;
    NCommon::TConfSetting<ui64, false> MapJoinShardCount; // [1-10]
    NCommon::TConfSetting<bool, false> MapJoinUseFlow;
    NCommon::TConfSetting<NSize::TSize, false> LookupJoinLimit;
    NCommon::TConfSetting<ui64, false> LookupJoinMaxRows;
    NCommon::TConfSetting<NSize::TSize, false> EvaluationTableSizeLimit;
    NCommon::TConfSetting<TSet<TString>, false> DisableOptimizers;
    NCommon::TConfSetting<ui32, false> MaxInputTables;
    NCommon::TConfSetting<ui32, false> MaxInputTablesForSortedMerge;
    NCommon::TConfSetting<ui32, false> MaxOutputTables;
    NCommon::TConfSetting<NSize::TSize, false> MaxExtraJobMemoryToFuseOperations;
    NCommon::TConfSetting<double, false> MaxReplicationFactorToFuseOperations;
    NCommon::TConfSetting<ui32, false> MaxOperationFiles;
    NCommon::TConfSetting<NSize::TSize, false> MinPublishedAvgChunkSize;
    NCommon::TConfSetting<NSize::TSize, false> MinTempAvgChunkSize;
    NCommon::TConfSetting<ui32, false> TopSortMaxLimit;
    NCommon::TConfSetting<NSize::TSize, false> TopSortSizePerJob;
    NCommon::TConfSetting<ui32, false> TopSortRowMultiplierPerJob;
    NCommon::TConfSetting<bool, false> JoinUseColumnarStatistics; // Deprecated. Use JoinCollectColumnarStatistics instead
    NCommon::TConfSetting<EJoinCollectColumnarStatisticsMode, false> JoinCollectColumnarStatistics;
    NCommon::TConfSetting<NYT::EColumnarStatisticsFetcherMode, false> JoinColumnarStatisticsFetcherMode;
    NCommon::TConfSetting<bool, false> JoinWaitAllInputs;
    NCommon::TConfSetting<bool, false> JoinAllowColumnRenames;
    NCommon::TConfSetting<bool, false> JoinMergeSetTopLevelFullSort;
    NCommon::TConfSetting<bool, false> JoinEnableStarJoin;
    NCommon::TConfSetting<NSize::TSize, false> FolderInlineDataLimit;
    NCommon::TConfSetting<ui32, false> FolderInlineItemsLimit;
    NCommon::TConfSetting<NSize::TSize, false> TableContentMinAvgChunkSize;
    NCommon::TConfSetting<ui32, false> TableContentMaxInputTables;
    NCommon::TConfSetting<ui32, false> TableContentMaxChunksForNativeDelivery;
    NCommon::TConfSetting<bool, false> TableContentLocalExecution;
    NCommon::TConfSetting<bool, false> UseTypeV2;
    NCommon::TConfSetting<bool, false> UseNativeYtTypes;
    NCommon::TConfSetting<bool, false> UseNativeDescSort;
    NCommon::TConfSetting<bool, false> UseIntermediateSchema;
    NCommon::TConfSetting<bool, false> UseFlow;
    NCommon::TConfSetting<ui16, false> WideFlowLimit;
    NCommon::TConfSetting<bool, false> UseSystemColumns;
    NCommon::TConfSetting<bool, false> HybridDqExecution;
    NCommon::TConfSetting<TDuration, false> HybridDqTimeSpentLimit;
    NCommon::TConfSetting<NSize::TSize, false> HybridDqDataSizeLimitForOrdered;
    NCommon::TConfSetting<NSize::TSize, false> HybridDqDataSizeLimitForUnordered;
    NCommon::TConfSetting<bool, false> HybridDqExecutionFallback;
    NCommon::TConfSetting<bool, false> UseYqlRowSpecCompactForm;
    NCommon::TConfSetting<bool, false> UseNewPredicateExtraction;
    NCommon::TConfSetting<bool, false> PruneKeyFilterLambda;
    NCommon::TConfSetting<bool, false> DqPruneKeyFilterLambda;
    NCommon::TConfSetting<bool, false> MergeAdjacentPointRanges;
    NCommon::TConfSetting<bool, false> KeyFilterForStartsWith;
    NCommon::TConfSetting<ui64, false> MaxKeyRangeCount;
    NCommon::TConfSetting<ui64, false> MaxChunksForDqRead;
    NCommon::TConfSetting<bool, false> JoinCommonUseMapMultiOut;
    NCommon::TConfSetting<bool, false> UseAggPhases;
    NCommon::TConfSetting<bool, false> UsePartitionsByKeysForFinalAgg;
    NCommon::TConfSetting<bool, false> _EnableWriteReorder;
    NCommon::TConfSetting<double, false> MaxCpuUsageToFuseMultiOuts;
    NCommon::TConfSetting<double, false> MaxReplicationFactorToFuseMultiOuts;
    NCommon::TConfSetting<ui64, false> ApplyStoredConstraints;
    NCommon::TConfSetting<bool, false> ViewIsolation;
    NCommon::TConfSetting<bool, false> PartitionByConstantKeysViaMap;
};

EReleaseTempDataMode GetReleaseTempDataMode(const TYtSettings& settings);
EJoinCollectColumnarStatisticsMode GetJoinCollectColumnarStatisticsMode(const TYtSettings& settings);
inline TString GetTablesTmpFolder(const TYtSettings& settings) {
    return settings.TablesTmpFolder.Get().GetOrElse(settings.TmpFolder.Get().GetOrElse({}));
}

struct TYtConfiguration : public TYtSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TYtConfiguration>;

    TYtConfiguration();
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

    TYtVersionedConfiguration() = default;
    ~TYtVersionedConfiguration() = default;

    size_t FindNodeVer(const TExprNode& node);
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

bool ValidateCompressionCodecValue(const TStringBuf& codec);
void MediaValidator(const NYT::TNode& value);

} // NYql
