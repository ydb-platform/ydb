#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

namespace NYql {

struct TDqSettings {

    enum class ETaskRunnerStats {
        Disable,
        Basic,
        Full,
        Profile
    };

    enum class ESpillingEngine {
        Disable     /* "disable" */,
        File        /* "file" */,
    };

    enum class EEnabledSpillingNodes : ui64 {
        None        = 0ULL      /* None */,
        GraceJoin   = 1ULL      /* "GraceJoin" */,
        All         = ~0ULL     /* "All" */,
    };

    struct TDefault {
        static constexpr ui32 MaxTasksPerStage = 20U;
        static constexpr ui32 MaxTasksPerOperation = 70U;
        static constexpr bool EnablePorto = false;
        static constexpr ui64 DataSizePerJob = 128_MB;
        static constexpr ui64 MaxDataSizePerJob = 600_MB;
        static constexpr int MaxNetworkRetries = 5;
        static constexpr ui64 LiteralTimeout = 60000; // 1 minutes
        static constexpr ui64 TableTimeout = 600000; // 10 minutes
        static constexpr ui64 LongWorkersAllocationFailTimeout = TableTimeout;
        static constexpr ui64 LongWorkersAllocationWarnTimeout = 30000; // 30 seconds
        static constexpr ui32 CloudFunctionConcurrency = 10;
        static constexpr ui64 ChannelBufferSize = 2000_MB;
        static constexpr ui64 OutputChunkMaxSize = 4_MB;
        static constexpr ui64 ChunkSizeLimit = 128_MB;
        static constexpr bool EnableDqReplicate = false;
        static constexpr ui64 WatermarksGranularityMs = 1000;
        static constexpr ui64 WatermarksLateArrivalDelayMs = 5000;
        static constexpr ui64 ParallelOperationsLimit = 16;
        static constexpr double HashShuffleTasksRatio = 0.5;
        static constexpr ui32 HashShuffleMaxTasks = 24;
        static constexpr bool UseFastPickleTransport = false;
        static constexpr bool UseOOBTransport = false;
        static constexpr bool AggregateStatsByStage = true;
        static constexpr bool EnableChannelStats = false;
        static constexpr bool ExportStats = false;
        static constexpr ETaskRunnerStats TaskRunnerStats = ETaskRunnerStats::Basic;
        static constexpr ESpillingEngine SpillingEngine = ESpillingEngine::Disable;
        static constexpr ui32 CostBasedOptimizationLevel = 3;
        static constexpr ui32 MaxDPccpDPTableSize = 40000U;
        static constexpr ui64 MaxAttachmentsSize = 2_GB;
        static constexpr bool SplitStageOnDqReplicate = true;
        static constexpr ui64 EnableSpillingNodes = 0;
        static constexpr bool EnableSpillingInChannels = false;
    };

    using TPtr = std::shared_ptr<TDqSettings>;

    NCommon::TConfSetting<ui64, false> DataSizePerJob;
    NCommon::TConfSetting<ui64, false> MaxDataSizePerJob;
    NCommon::TConfSetting<ui32, false> MaxTasksPerStage;
    NCommon::TConfSetting<ui32, false> MaxTasksPerOperation;
    NCommon::TConfSetting<ui32, false> WorkersPerOperation;
    NCommon::TConfSetting<ui64, false> MaxDataSizePerQuery;
    NCommon::TConfSetting<bool, false> AnalyticsHopping;
    NCommon::TConfSetting<bool, false> AnalyzeQuery;
    NCommon::TConfSetting<int, false> _AnalyzeQueryPercentage;
    NCommon::TConfSetting<int, false> MaxRetries;
    NCommon::TConfSetting<int, false> MaxNetworkRetries;
    NCommon::TConfSetting<ui64, false> RetryBackoffMs;
    NCommon::TConfSetting<bool, false> CollectCoreDumps;
    NCommon::TConfSetting<EFallbackPolicy, false> FallbackPolicy;
    NCommon::TConfSetting<ui64, false> PullRequestTimeoutMs;
    NCommon::TConfSetting<ui64, false> PingTimeoutMs;
    NCommon::TConfSetting<bool, false> UseSimpleYtReader;
    NCommon::TConfSetting<TString, false> OptLLVM;
    NCommon::TConfSetting<ui64, false> ChannelBufferSize;
    NCommon::TConfSetting<ui64, false> OutputChunkMaxSize;
    NCommon::TConfSetting<ui64, false> ChunkSizeLimit;
    NCommon::TConfSetting<NSize::TSize, false> MemoryLimit;
    NCommon::TConfSetting<ui64, false> _LiteralTimeout;
    NCommon::TConfSetting<ui64, false> _TableTimeout;
    NCommon::TConfSetting<ui64, false> _LongWorkersAllocationWarnTimeout;
    NCommon::TConfSetting<ui64, false> _LongWorkersAllocationFailTimeout;
    NCommon::TConfSetting<bool, false> EnableInsert;
    NCommon::TConfSetting<ui64, false> _AllResultsBytesLimit;
    NCommon::TConfSetting<ui64, false> _RowsLimitPerWrite;
    NCommon::TConfSetting<bool, false> EnableStrip;
    NCommon::TConfSetting<bool, false> EnableComputeActor;
    NCommon::TConfSetting<TString, false> ComputeActorType;
    NCommon::TConfSetting<bool, false> _EnablePorto;
    NCommon::TConfSetting<ui64, false> _PortoMemoryLimit;
    NCommon::TConfSetting<bool, false> EnableFullResultWrite;
    NCommon::TConfSetting<bool, false> _OneGraphPerQuery;
    NCommon::TConfSetting<TString, false> _FallbackOnRuntimeErrors;
    NCommon::TConfSetting<bool, false> _EnablePrecompute;
    NCommon::TConfSetting<bool, false> UseFinalizeByKey;
    NCommon::TConfSetting<bool, false> EnableDqReplicate;
    NCommon::TConfSetting<TString, false> WatermarksMode;
    NCommon::TConfSetting<bool, false> WatermarksEnableIdlePartitions;
    NCommon::TConfSetting<ui64, false> WatermarksGranularityMs;
    NCommon::TConfSetting<ui64, false> WatermarksLateArrivalDelayMs;
    NCommon::TConfSetting<bool, false> UseAggPhases;
    NCommon::TConfSetting<ui64, false> ParallelOperationsLimit;

    NCommon::TConfSetting<TString, false> WorkerFilter;
    NCommon::TConfSetting<NDq::EHashJoinMode, false> HashJoinMode;
    NCommon::TConfSetting<double, false> HashShuffleTasksRatio;
    NCommon::TConfSetting<ui32, false> HashShuffleMaxTasks;

    NCommon::TConfSetting<bool, false> UseWideChannels;
    NCommon::TConfSetting<bool, false> UseWideBlockChannels;
    NCommon::TConfSetting<bool, false> UseFastPickleTransport;
    NCommon::TConfSetting<bool, false> UseOOBTransport;

    NCommon::TConfSetting<bool, false> AggregateStatsByStage;
    NCommon::TConfSetting<bool, false> EnableChannelStats;
    NCommon::TConfSetting<bool, false> ExportStats;
    NCommon::TConfSetting<ETaskRunnerStats, false> TaskRunnerStats;
    NCommon::TConfSetting<bool, false> _SkipRevisionCheck;
    NCommon::TConfSetting<bool, false> UseBlockReader;
    NCommon::TConfSetting<ESpillingEngine, false> SpillingEngine;
    NCommon::TConfSetting<bool, false> DisableLLVMForBlockStages;
    NCommon::TConfSetting<bool, false> SplitStageOnDqReplicate;

    NCommon::TConfSetting<ui64, false> EnableSpillingNodes;
    NCommon::TConfSetting<bool, false> EnableSpillingInChannels;

    NCommon::TConfSetting<ui64, false> _MaxAttachmentsSize;
    NCommon::TConfSetting<bool, false> DisableCheckpoints;

    // This options will be passed to executor_actor and worker_actor
    template <typename TProtoConfig>
    void Save(TProtoConfig& config) {
#define SAVE_SETTING(name) \
        if (this->name.Get()) { \
            auto* s = config.AddSettings(); \
            s->SetName(#name); \
            s->SetValue(ToString(*this->name.Get())); \
        }

        // The below pragmas are intended to be used in actors (like Compute Actor, Executer, Worker Managers ...) and TaskRunner only.
        // If your pragma is used only in graph transformer don't place it here.
        SAVE_SETTING(AnalyticsHopping);
        SAVE_SETTING(MaxRetries);
        SAVE_SETTING(MaxNetworkRetries);
        SAVE_SETTING(WorkersPerOperation);
        SAVE_SETTING(RetryBackoffMs);
        SAVE_SETTING(FallbackPolicy);
        SAVE_SETTING(CollectCoreDumps);
        SAVE_SETTING(PullRequestTimeoutMs);
        SAVE_SETTING(PingTimeoutMs);
        SAVE_SETTING(OptLLVM);
        SAVE_SETTING(ChannelBufferSize);
        SAVE_SETTING(OutputChunkMaxSize);
        SAVE_SETTING(MemoryLimit);
        SAVE_SETTING(_LiteralTimeout);
        SAVE_SETTING(_TableTimeout);
        SAVE_SETTING(_LongWorkersAllocationWarnTimeout);
        SAVE_SETTING(_LongWorkersAllocationFailTimeout);
        SAVE_SETTING(_AllResultsBytesLimit);
        SAVE_SETTING(_RowsLimitPerWrite);
        SAVE_SETTING(EnableComputeActor);
        SAVE_SETTING(_EnablePorto);
        SAVE_SETTING(_PortoMemoryLimit);
        SAVE_SETTING(EnableFullResultWrite);
        SAVE_SETTING(_FallbackOnRuntimeErrors);
        SAVE_SETTING(WorkerFilter);
        SAVE_SETTING(ComputeActorType);
        SAVE_SETTING(WatermarksMode);
        SAVE_SETTING(WatermarksEnableIdlePartitions);
        SAVE_SETTING(WatermarksGranularityMs);
        SAVE_SETTING(WatermarksLateArrivalDelayMs);
        SAVE_SETTING(UseWideChannels);
        SAVE_SETTING(UseWideBlockChannels);
        SAVE_SETTING(UseFastPickleTransport);
        SAVE_SETTING(UseOOBTransport);
        SAVE_SETTING(AggregateStatsByStage);
        SAVE_SETTING(EnableChannelStats);
        SAVE_SETTING(ExportStats);
        SAVE_SETTING(TaskRunnerStats);
        SAVE_SETTING(SpillingEngine);
        SAVE_SETTING(EnableSpillingInChannels);
        SAVE_SETTING(DisableCheckpoints);
#undef SAVE_SETTING
    }

    TDqSettings::TPtr WithFillSettings(const IDataProvider::TFillSettings& fillSettings) const {
        auto copy = std::make_shared<TDqSettings>(*this);
        if (fillSettings.RowsLimitPerWrite && !copy->_RowsLimitPerWrite.Get()) {
            copy->_RowsLimitPerWrite = *fillSettings.RowsLimitPerWrite;
        }
        if (fillSettings.AllResultsBytesLimit && !copy->_AllResultsBytesLimit.Get()) {
            copy->_AllResultsBytesLimit = *fillSettings.AllResultsBytesLimit;
        }

        return copy;
    }

    NDqProto::EDataTransportVersion GetDataTransportVersion() const {
        const bool fastPickle = UseFastPickleTransport.Get().GetOrElse(TDqSettings::TDefault::UseFastPickleTransport);
        const bool oob = UseOOBTransport.Get().GetOrElse(TDqSettings::TDefault::UseOOBTransport);
        if (oob) {
            return fastPickle ? NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0 : NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0;
        } else {
            return fastPickle ? NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0 : NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0;
        }
    }

    bool IsSpillingEngineEnabled() const {
        return SpillingEngine.Get().GetOrElse(TDqSettings::TDefault::SpillingEngine) != ESpillingEngine::Disable;
    }

    bool IsSpillingInChannelsEnabled() const {
        if (!IsSpillingEngineEnabled()) return false;
        return EnableSpillingInChannels.Get().GetOrElse(TDqSettings::TDefault::EnableSpillingInChannels) != false;
    }

    ui64 GetEnabledSpillingNodes() const {
        if (!IsSpillingEngineEnabled()) return 0;
        return EnableSpillingNodes.Get().GetOrElse(TDqSettings::TDefault::EnableSpillingNodes);
    }

    bool IsDqReplicateEnabled(const TTypeAnnotationContext& typesCtx) const {
        return EnableDqReplicate.Get().GetOrElse(
            typesCtx.BlockEngineMode != EBlockEngineMode::Disable || TDqSettings::TDefault::EnableDqReplicate);
    }
};

struct TDqConfiguration: public TDqSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TDqConfiguration>;

    TDqConfiguration();
    TDqConfiguration(const TDqConfiguration&) = delete;

    template <class TProtoConfig, typename TFilter>
    void Init(const TProtoConfig& config, const TFilter& filter) {
        // Init settings from config
        this->Dispatch(config.GetDefaultSettings(), filter);

        this->FreezeDefaults();
    }
};

} //namespace NYql
