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
    REGISTER_SETTING(*this, MaxNetworkRetries);
    REGISTER_SETTING(*this, RetryBackoffMs);
    REGISTER_SETTING(*this, CollectCoreDumps);
    REGISTER_SETTING(*this, FallbackPolicy);
    REGISTER_SETTING(*this, PullRequestTimeoutMs);
    REGISTER_SETTING(*this, PingTimeoutMs);
    REGISTER_SETTING(*this, UseSimpleYtReader);
    REGISTER_SETTING(*this, OptLLVM);
    REGISTER_SETTING(*this, ChannelBufferSize);
    REGISTER_SETTING(*this, OutputChunkMaxSize);
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
    REGISTER_SETTING(*this, EnablePorto);
    REGISTER_SETTING(*this, _PortoMemoryLimit);

    REGISTER_SETTING(*this, EnableFullResultWrite);

    REGISTER_SETTING(*this, _FallbackOnRuntimeErrors);
    REGISTER_SETTING(*this, WorkerFilter);
}

} // namespace NYql
