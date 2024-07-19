#include "yql_dq_settings.h"
#include <util/string/split.h>

namespace NYql {

TDqConfiguration::TDqConfiguration() {
    REGISTER_SETTING(*this, DataSizePerJob);
    REGISTER_SETTING(*this, MaxDataSizePerJob);
    REGISTER_SETTING(*this, MaxTasksPerStage);
    REGISTER_SETTING(*this, MaxTasksPerOperation);
    REGISTER_SETTING(*this, WorkersPerOperation);
    REGISTER_SETTING(*this, MaxDataSizePerQuery);
    REGISTER_SETTING(*this, AnalyticsHopping);
    REGISTER_SETTING(*this, AnalyzeQuery);
    REGISTER_SETTING(*this, _AnalyzeQueryPercentage);
    REGISTER_SETTING(*this, MaxRetries);
    REGISTER_SETTING(*this, UseFinalizeByKey);
    REGISTER_SETTING(*this, MaxNetworkRetries);
    REGISTER_SETTING(*this, RetryBackoffMs);
    REGISTER_SETTING(*this, CollectCoreDumps);
    REGISTER_SETTING(*this, FallbackPolicy).Parser([](const TString& v) { return FromString<EFallbackPolicy>(v); });
    REGISTER_SETTING(*this, PullRequestTimeoutMs);
    REGISTER_SETTING(*this, PingTimeoutMs);
    REGISTER_SETTING(*this, UseSimpleYtReader);
    REGISTER_SETTING(*this, OptLLVM);
    REGISTER_SETTING(*this, ChannelBufferSize);
    REGISTER_SETTING(*this, OutputChunkMaxSize);
    REGISTER_SETTING(*this, ChunkSizeLimit);
    REGISTER_SETTING(*this, MemoryLimit);
    REGISTER_SETTING(*this, EnableInsert);

    REGISTER_SETTING(*this, _LiteralTimeout);
    REGISTER_SETTING(*this, _TableTimeout);
    REGISTER_SETTING(*this, _LongWorkersAllocationWarnTimeout);
    REGISTER_SETTING(*this, _LongWorkersAllocationFailTimeout);

    REGISTER_SETTING(*this, _AllResultsBytesLimit);
    REGISTER_SETTING(*this, _RowsLimitPerWrite);

    REGISTER_SETTING(*this, EnableStrip);

    REGISTER_SETTING(*this, EnableComputeActor);
    REGISTER_SETTING(*this, ComputeActorType);
    REGISTER_SETTING(*this, _EnablePorto);
    REGISTER_SETTING(*this, _PortoMemoryLimit);

    REGISTER_SETTING(*this, EnableFullResultWrite);

    REGISTER_SETTING(*this, _FallbackOnRuntimeErrors);
    REGISTER_SETTING(*this, WorkerFilter);
    REGISTER_SETTING(*this, _EnablePrecompute);
    REGISTER_SETTING(*this, EnableDqReplicate);
    REGISTER_SETTING(*this, WatermarksMode);
    REGISTER_SETTING(*this, WatermarksGranularityMs);
    REGISTER_SETTING(*this, WatermarksLateArrivalDelayMs);
    REGISTER_SETTING(*this, WatermarksEnableIdlePartitions);
    REGISTER_SETTING(*this, UseAggPhases);
    REGISTER_SETTING(*this, ParallelOperationsLimit).Lower(1).Upper(128);
    REGISTER_SETTING(*this, HashJoinMode).Parser([](const TString& v) { return FromString<NDq::EHashJoinMode>(v); });
    REGISTER_SETTING(*this, HashShuffleTasksRatio).Lower(0.5).Upper(5);
    REGISTER_SETTING(*this, HashShuffleMaxTasks).Lower(1).Upper(1000);

    REGISTER_SETTING(*this, UseWideChannels);
    REGISTER_SETTING(*this, UseWideBlockChannels)
        .ValueSetter([this](const TString&, bool value) {
            UseWideBlockChannels = value;
            if (value) {
                UseWideChannels = true;
            }
        });
    REGISTER_SETTING(*this, UseFastPickleTransport);
    REGISTER_SETTING(*this, UseOOBTransport);

    REGISTER_SETTING(*this, AggregateStatsByStage);
    REGISTER_SETTING(*this, EnableChannelStats);
    REGISTER_SETTING(*this, ExportStats);
    REGISTER_SETTING(*this, TaskRunnerStats).Parser([](const TString& v) { return FromString<ETaskRunnerStats>(v); });
    REGISTER_SETTING(*this, _SkipRevisionCheck);
    REGISTER_SETTING(*this, UseBlockReader);
    REGISTER_SETTING(*this, SpillingEngine)
        .Parser([](const TString& v) {
            return FromString<TDqSettings::ESpillingEngine>(v);
        });

    REGISTER_SETTING(*this, EnableSpillingInChannels)
        .ValueSetter([this](const TString&, bool value) {
            EnableSpillingInChannels = value;
            if (value) {
                SplitStageOnDqReplicate = false;
                EnableDqReplicate = true;
            }
        });
    REGISTER_SETTING(*this, DisableLLVMForBlockStages);
    REGISTER_SETTING(*this, SplitStageOnDqReplicate)
        .ValueSetter([this](const TString&, bool value) {
            SplitStageOnDqReplicate = value;
            if (!value) {
                EnableDqReplicate = true;
            }
        });

    REGISTER_SETTING(*this, _MaxAttachmentsSize);
    REGISTER_SETTING(*this, DisableCheckpoints);
    REGISTER_SETTING(*this, EnableSpillingNodes)
        .Parser([](const TString& v) {
            ui64 res = 0;
            TVector<TString> vec;
            StringSplitter(v).SplitBySet(",;| ").AddTo(&vec);
            for (auto& s: vec) {
                if (s.empty()) {
                    throw yexception() << "Empty value item";
                }
                auto value = FromStringWithDefault<EEnabledSpillingNodes>(s, EEnabledSpillingNodes::None);
                res |= ui64(value);
            }
            return res;
        });
}

} // namespace NYql
