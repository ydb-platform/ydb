#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <util/generic/vector.h>
#include <optional>

namespace NKikimr::NKqp {

struct TKqpStatsCompile {
    bool FromCache = false;
    ui64 DurationUs = 0;
    ui64 CpuTimeUs = 0;
};

struct TKqpQueryStats {
    ui64 DurationUs = 0;
    ui64 QueuedTimeUs = 0;
    std::optional<TKqpStatsCompile> Compilation;

    ui64 WorkerCpuTimeUs = 0;
    ui64 ReadSetsCount = 0;
    ui64 MaxShardProgramSize = 0;
    ui64 MaxShardReplySize = 0;
    ui64 LocksBrokenAsBreaker = 0;
    ui64 LocksBrokenAsVictim = 0;

    TVector<NYql::NDqProto::TDqExecutionStats> Executions;

    const TVector<NYql::NDqProto::TDqExecutionStats>& GetExecutions() const;
    ui64 GetWorkerCpuTimeUs() const;

    NKqpProto::TKqpStatsQuery ToProto() const;
};

}
