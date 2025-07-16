#pragma once

#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>

#include <yql/essentials/core/yql_data_provider.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

namespace NYql {

struct TDqSettings {
    friend struct TDqConfiguration;

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
        static constexpr ui32 CostBasedOptimizationLevel = 4;
        static constexpr ui32 MaxDPHypDPTableSize = 95'000U;
        static constexpr ui64 MaxAttachmentsSize = 2_GB;
        static constexpr bool SplitStageOnDqReplicate = true;
        static constexpr ui64 EnableSpillingNodes = 0;
        static constexpr bool EnableSpillingInChannels = false;
    };

    using TPtr = std::shared_ptr<TDqSettings>;

private:
    static constexpr NCommon::EConfSettingType Static = NCommon::EConfSettingType::Static;
public:

    NCommon::TConfSetting<ui64, Static> DataSizePerJob;
    NCommon::TConfSetting<ui64, Static> MaxDataSizePerJob;
    NCommon::TConfSetting<ui32, Static> MaxTasksPerStage;
    NCommon::TConfSetting<ui32, Static> MaxTasksPerOperation;
    NCommon::TConfSetting<ui32, Static> WorkersPerOperation;
    NCommon::TConfSetting<ui64, Static> MaxDataSizePerQuery;
    NCommon::TConfSetting<bool, Static> AnalyticsHopping;
    NCommon::TConfSetting<bool, Static> AnalyzeQuery;
    NCommon::TConfSetting<int, Static> _AnalyzeQueryPercentage;
    NCommon::TConfSetting<int, Static> MaxRetries;
    NCommon::TConfSetting<int, Static> MaxNetworkRetries;
    NCommon::TConfSetting<ui64, Static> RetryBackoffMs;
    NCommon::TConfSetting<bool, Static> CollectCoreDumps;
    NCommon::TConfSetting<EFallbackPolicy, Static> FallbackPolicy;
    NCommon::TConfSetting<ui64, Static> PullRequestTimeoutMs;
    NCommon::TConfSetting<ui64, Static> PingTimeoutMs;
    NCommon::TConfSetting<bool, Static> UseSimpleYtReader;
    NCommon::TConfSetting<TString, Static> OptLLVM;
    NCommon::TConfSetting<ui64, Static> ChannelBufferSize;
    NCommon::TConfSetting<ui64, Static> OutputChunkMaxSize;
    NCommon::TConfSetting<ui64, Static> ChunkSizeLimit;
    NCommon::TConfSetting<NSize::TSize, Static> MemoryLimit;
    NCommon::TConfSetting<ui64, Static> _LiteralTimeout;
private:
    NCommon::TConfSetting<ui64, Static> _TableTimeout;
    NCommon::TConfSetting<ui64, Static> QueryTimeout; // less or equal than _TableTimeout
public:
    NCommon::TConfSetting<ui64, Static> _LongWorkersAllocationWarnTimeout;
    NCommon::TConfSetting<ui64, Static> _LongWorkersAllocationFailTimeout;
    NCommon::TConfSetting<bool, Static> EnableInsert;
    NCommon::TConfSetting<ui64, Static> _AllResultsBytesLimit;
    NCommon::TConfSetting<ui64, Static> _RowsLimitPerWrite;
    NCommon::TConfSetting<bool, Static> EnableStrip;
    NCommon::TConfSetting<bool, Static> EnableComputeActor;
    NCommon::TConfSetting<TString, Static> ComputeActorType;
    NCommon::TConfSetting<bool, Static> _EnablePorto;
    NCommon::TConfSetting<ui64, Static> _PortoMemoryLimit;
    NCommon::TConfSetting<bool, Static> EnableFullResultWrite;
    NCommon::TConfSetting<bool, Static> _OneGraphPerQuery;
    NCommon::TConfSetting<TString, Static> _FallbackOnRuntimeErrors;
    NCommon::TConfSetting<bool, Static> _EnablePrecompute;
    NCommon::TConfSetting<bool, Static> UseFinalizeByKey;
    NCommon::TConfSetting<bool, Static> EnableDqReplicate;
    NCommon::TConfSetting<TString, Static> WatermarksMode;
    NCommon::TConfSetting<bool, Static> WatermarksEnableIdlePartitions;
    NCommon::TConfSetting<ui64, Static> WatermarksGranularityMs;
    NCommon::TConfSetting<ui64, Static> WatermarksLateArrivalDelayMs;
    NCommon::TConfSetting<bool, Static> UseAggPhases;
    NCommon::TConfSetting<ui64, Static> ParallelOperationsLimit;

    NCommon::TConfSetting<TString, Static> WorkerFilter;
    NCommon::TConfSetting<NDq::EHashJoinMode, Static> HashJoinMode;
    NCommon::TConfSetting<double, Static> HashShuffleTasksRatio;
    NCommon::TConfSetting<ui32, Static> HashShuffleMaxTasks;

    NCommon::TConfSetting<bool, Static> UseWideChannels;
    NCommon::TConfSetting<bool, Static> UseWideBlockChannels;
    NCommon::TConfSetting<bool, Static> UseFastPickleTransport;
    NCommon::TConfSetting<bool, Static> UseOOBTransport;

    NCommon::TConfSetting<bool, Static> AggregateStatsByStage;
    NCommon::TConfSetting<bool, Static> EnableChannelStats;
    NCommon::TConfSetting<bool, Static> ExportStats;
    NCommon::TConfSetting<ETaskRunnerStats, Static> TaskRunnerStats;
    NCommon::TConfSetting<bool, Static> _SkipRevisionCheck;
    NCommon::TConfSetting<bool, Static> UseBlockReader;
    NCommon::TConfSetting<ESpillingEngine, Static> SpillingEngine;
    NCommon::TConfSetting<bool, Static> DisableLLVMForBlockStages;
    NCommon::TConfSetting<bool, Static> SplitStageOnDqReplicate;

    NCommon::TConfSetting<ui64, Static> EnableSpillingNodes;
    NCommon::TConfSetting<bool, Static> EnableSpillingInChannels;

    NCommon::TConfSetting<ui64, Static> _MaxAttachmentsSize;
    NCommon::TConfSetting<bool, Static> DisableCheckpoints;
    NCommon::TConfSetting<bool, Static> UseGraceJoinCoreForMap;
    NCommon::TConfSetting<TString, Static> Scheduler;

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
        SAVE_SETTING(QueryTimeout);
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
        SAVE_SETTING(Scheduler);
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

    ui64 GetQueryTimeout() const {
        auto upper = _TableTimeout.Get().GetOrElse(TDefault::TableTimeout);
        if (QueryTimeout.Get().Defined()) {
            return Min(*QueryTimeout.Get(), upper);
        }

        return upper;
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
