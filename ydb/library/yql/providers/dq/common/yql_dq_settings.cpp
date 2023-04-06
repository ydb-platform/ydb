#include "yql_dq_settings.h"

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
    REGISTER_SETTING(*this, UseFastPickleTransport);
}

} // namespace NYql
