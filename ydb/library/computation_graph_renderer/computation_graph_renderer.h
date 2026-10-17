#pragma once

#include <library/cpp/json/json_value.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NComputationGraphRenderer {

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

struct TNode {
    ui32 Id = 0;
    ui32 Level = 0;
    TString Name;
    ENodeType Type = ENodeType::Operation;
    ENodeState State = ENodeState::Pending;
    ui32 Tasks = 0;
    ui32 FinishedTasks = 0;
};

struct TLink {
    ui32 Source = 0;
    ui32 Target = 0;
};

struct TGraph {
    TVector<TNode> Nodes;
    TVector<TLink> Links;
};

std::optional<TGraph> BuildGraph(const NJson::TJsonValue& doc);

TString ToSvg(const TGraph& graph);

} // namespace NKikimr::NComputationGraphRenderer
