#pragma once

#include "task_graph.h"
#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/generic/string.h>
#include <memory>
#include <mutex>
#include <optional>

namespace NYql::NDq {

struct TTaskSchedulingSnapshot;
struct TTaskRunProgress;

// A passive observation. Missing fields mean unsupported/unknown, not zero.
// Bytes have transport-buffer semantics; they exclude deserialized values held
// by an operator and need not equal the task's memory consumption.
struct TTaskChannelSnapshot {
    std::optional<ui64> BufferedBytes, BufferedChunks, InflightBytes, SpilledBytes, CapacityBytes;
    std::optional<bool> Empty, Finished, FinishPushed, EarlyFinished, Aborted, Paused;
    std::optional<ui32> FillLevel;
    NJson::TJsonValue ToJson(ui64 channelId, bool input) const;
};

struct TTaskTraceSettings {
    bool Enabled = false;
    ui32 MaxQueries = 16;
    ui32 MaxEvents = 100000;
    ui64 MaxBytes = 8 * 1024 * 1024;
    TDuration Retention = TDuration::Minutes(10);
};

// One recorder per query on a node. Enabled handles share it directly: there is
// no node-wide lock on the execution path and no disk I/O in a worker.
class TQueryTaskTrace {
public:
    TQueryTaskTrace(NActors::TActorId query, ui32 nodeId, TTaskTraceSettings settings);
    void Event(TStringBuf name, ui64 taskId, NActors::TActorId actor, NJson::TJsonValue args,
        TMonotonic at = TMonotonic::Now(), std::optional<ui64> durationUs = {});
    void State(const TTaskSchedulingSnapshot& state);
    void Graph(const TQueryTaskGraph& graph, const TVector<ui64>& localTasks);
    void Run(ui64 taskId, NActors::TActorId actor, TMonotonic start, const TTaskRunProgress& progress,
        NJson::TJsonValue before, NJson::TJsonValue after, bool failed, std::optional<ui32> runnerStatus = {});
    void Finish();
    bool IsCompleted() const;
    bool IsExpired(TMonotonic now) const;
    TString Export() const;
    NJson::TJsonValue Summary() const;

private:
    NJson::TJsonValue SummaryLocked() const;
    const NActors::TActorId Query;
    const ui32 NodeId;
    const TTaskTraceSettings Settings;
    const TMonotonic Start = TMonotonic::Now();
    const TInstant WallStart = TInstant::Now();
    mutable std::mutex Mutex;
    TVector<TString> Events;
    ui64 Bytes = 0, Dropped = 0;
    TMonotonic Completed;
};

// Bounded admission and lazy retention. Completed traces survive graph cleanup;
// an active trace is never evicted to admit another query.
class TTaskTraceRegistry {
public:
    TTaskTraceRegistry(ui32 nodeId, TTaskTraceSettings settings);
    std::shared_ptr<TQueryTaskTrace> GetOrCreate(NActors::TActorId query);
    TString Export(TStringBuf query = {});
private:
    void PruneLocked();
    const ui32 NodeId;
    const TTaskTraceSettings Settings;
    std::mutex Mutex;
    THashMap<NActors::TActorId, std::shared_ptr<TQueryTaskTrace>> Queries;
    ui64 Rejected = 0;
};

} // namespace NYql::NDq
