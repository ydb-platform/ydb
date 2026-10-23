#pragma once

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>
#include <optional>

namespace NYql::NDq {

struct TTaskGraphNode {
    ui64 TaskId = 0;
    ui32 StageId = 0;
    // A placement hint; actual local ownership is registered separately.
    ui32 PlannedNodeId = 0;
    ui32 InputsCount = 0;
    ui32 OutputsCount = 0;
    bool HasSource = false;
    bool HasSink = false;
    bool HasEffects = false;
};

struct TTaskGraphChannel {
    ui64 ChannelId = 0;
    ui64 SrcTaskId = 0;
    // Zero denotes a result endpoint outside the task graph.
    ui64 DstTaskId = 0;
    ui32 SrcOutputIndex = 0;
    ui32 DstInputIndex = 0;
    ui32 RoutingKind = 0;
};

// Immutable, query-wide topology. Each node receives the same topology, while
// the local registry records actual task ownership independently of placement
// hints (which can change when a task start is retried on another node).
class TQueryTaskGraph {
public:
    TQueryTaskGraph(TVector<TTaskGraphNode> tasks, TVector<TTaskGraphChannel> channels);
    const TTaskGraphNode* FindTask(ui64 taskId) const;
    const TVector<ui64>& GetInputs(ui64 taskId) const;
    const TVector<ui64>& GetOutputs(ui64 taskId) const;
    bool IsResultTask(ui64 taskId) const;
    std::optional<ui32> GetResultDistance(ui64 taskId) const;

    const TVector<TTaskGraphNode> Tasks;
    const TVector<TTaskGraphChannel> Channels;

private:
    THashMap<ui64, size_t> TaskIndex;
    THashMap<ui64, TVector<ui64>> Inputs, Outputs;
    THashMap<ui64, ui32> ResultDistances;
};

} // namespace NYql::NDq
