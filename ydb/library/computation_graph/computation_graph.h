#pragma once

#include <library/cpp/json/json_value.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NComputationGraph {

enum class ENodeType {
    Input,
    Operation,
    Output,
};

enum class ENodeState {
    Pending,
    Running,
    Finished,
};

struct TNodeStats {
    ui64 IngressRows = 0;
    ui64 IngressBytes = 0;
    ui64 EgressRows = 0;
    ui64 EgressBytes = 0;
    ui64 InputRows = 0;
    ui64 InputBytes = 0;
    ui64 OutputRows = 0;
    ui64 OutputBytes = 0;
    ui64 CpuTimeUs = 0;
};

struct TNode {
    ui32 Id = 0;
    ui32 Level = 0;
    TString Name;
    ENodeType Type = ENodeType::Operation;
    ENodeState State = ENodeState::Pending;
    ui32 Tasks = 0;
    ui32 FinishedTasks = 0;
    ui32 PhysicalStageId = 0;
    ui64 UpdateTimeMs = 0;
    TNodeStats Stats;
};

struct TLink {
    ui32 Source = 0;
    ui32 Target = 0;
};

struct TGraph {
    TVector<TNode> Nodes;
    TVector<TLink> Links;
};

// `doc` is {"meta","Plan","SimplifiedPlan"} or a bare plan node.
// An empty / "{}" document yields an empty graph. Never throws on malformed input.
TGraph BuildGraph(const NJson::TJsonValue& doc);

// {"nodes":[{id, level, name, type, state?, tasks?, finishedTasks?, stats?}], "links":[{source, target}]}
NJson::TJsonValue ToJson(const TGraph& graph);

} // namespace NKikimr::NComputationGraph
