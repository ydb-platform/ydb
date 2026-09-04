#pragma once

#include "metrics.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace NPlan2Svg {

class TPlan;
class TStage;

class TConnection {

public:
    TConnection(ui32 groupId, TStage& stage, const TString& nodeType, ui32 planNodeId) : GroupId(groupId), Stage(stage), NodeType(nodeType), PlanNodeId(planNodeId) {
    }

    const ui32 GroupId;
    TStage& Stage;
    TString NodeType;
    std::shared_ptr<TStage> FromStage;
    std::shared_ptr<TSingleMetric> InputBytes;
    std::shared_ptr<TSingleMetric> InputRows;
    ui64 InputChunks = 0;
    ui64 InputLocalBytes = 0;
    std::shared_ptr<TScalarMetric> InputChunkSize;
    std::vector<std::string> KeyColumns;
    std::vector<std::string> SortColumns;
    TString HashFunc;
    bool Parallel = false;
    bool CteConnection = false;
    ui32 CteIndentX = 0;
    std::shared_ptr<TSingleMetric> CteOutputBytes;
    std::shared_ptr<TSingleMetric> CteOutputRows;
    std::shared_ptr<TSingleMetric> CteOperatorOutputRows;
    ui64 CteOutputChunks = 0;
    ui64 CteOutputLocalBytes = 0;
    std::shared_ptr<TScalarMetric> CteOutputChunkSize;
    const NJson::TJsonValue* StatsNode = nullptr;
    const ui32 PlanNodeId;
    TStringBuilder Svg;
    TStringBuilder CteSvg;
    bool Blocks = false;
};

// What one operator entry in the plan says about itself, as read out of the JSON
// before any of it is attached to a stage.
struct TOperatorDescription {
    TString Info;
    TString OperatorType;
    bool External = false;
};

class TOperatorInput {

public:
    // Internal
    ui32 OperatorId = 0;
    // External
    ui32 PlanNodeId = 0;
    std::optional<ui32> StageId;
    // CTE Ref
    TString PrecomputeRef;
    std::shared_ptr<TSingleMetric> Rows;
};

class TOperatorInfo {

public:
    TOperatorInfo(const TString& name, const TString& info) : Name(name), Info(info) {
    }

    TString Name;
    TString Info;
    std::shared_ptr<TSingleMetric> OutputRows;
    std::vector<TOperatorInput> Inputs;
    std::shared_ptr<TSingleMetric> InputThroughput;
    TString Estimations;
    bool Blocks = false;
};

class TPlan;

class TStage {

public:
    TStage(ui32 groupId, TPlan* plan, const TString& nodeType) : GroupId(groupId), Plan(plan), NodeType(nodeType) {
    }

    const ui32 GroupId;
    TPlan* Plan;
    TString NodeType;
    std::vector<std::shared_ptr<TConnection>> Connections;
    ui32 IndentX = 0;
    ui32 IndentY = 0;
    ui32 OffsetY = 0;
    ui32 Height = 0;
    std::shared_ptr<TSingleMetric> CpuTime;
    std::shared_ptr<TSingleMetric> WaitInputTime;
    std::shared_ptr<TSingleMetric> WaitOutputTime;
    std::shared_ptr<TSingleMetric> MemoryUsage;
    std::shared_ptr<TSingleMetric> MaxMemoryUsage;
    std::shared_ptr<TSingleMetric> OutputBytes;
    std::shared_ptr<TSingleMetric> OutputRows;
    ui64 OutputChunks = 0;
    ui64 OutputLocalBytes = 0;
    std::shared_ptr<TScalarMetric> OutputChunkSize;
    std::shared_ptr<TSingleMetric> SpillingComputeTime;
    std::shared_ptr<TSingleMetric> SpillingComputeBytes;
    std::shared_ptr<TSingleMetric> SpillingChannelTime;
    std::shared_ptr<TSingleMetric> SpillingChannelBytes;
    TString IngressName;
    bool BuiltInIngress = false;
    std::shared_ptr<TSingleMetric> IngressBytes;
    std::shared_ptr<TSingleMetric> IngressRows;
    std::shared_ptr<TSingleMetric> EgressBytes;
    std::shared_ptr<TSingleMetric> EgressRows;
    std::shared_ptr<TSingleMetric> InputThroughput;
    std::vector<TOperatorInfo> Operators;
    ui64 BaseTime = 0;
    ui32 PlanNodeId = 0;
    ui32 OutputPlanNodeId = 0;
    ui32 PhysicalStageId = 0;
    ui32 OutputPhysicalStageId = 0; // only first/main, not CTE-clone
    ui32 Tasks = 0;
    ui32 FinishedTasks = 0;
    const NJson::TJsonValue* StatsNode = nullptr;
    ui64 MinTime = 0;
    ui64 MaxTime = 0;
    ui64 UpdateTime = 0;
    bool External = false;
    TStringBuilder Svg;
    TConnection* IngressConnection = nullptr;
    std::vector<std::pair<ui64, ui64>> HotRegions;
    ui64 CriticalCpuTotal = 0;
    std::shared_ptr<TConnection> CriticalCpuConnection;
    ui64 CriticalTimeTotal = 0;
    std::shared_ptr<TConnection> CriticalTimeConnection;

};

class TClusterNode {
public:
    TClusterNode(ui32 nodeId)
        : NodeId(nodeId), MemPhysicalUsage("RSS", true), MemSysAllocated("Allocated"), MemSysFragmented("Fragmented")
        , MemArrowDefault("Arrow"), MemMkqlAllocated("MKQL Allocated"), MemMkqlFreeList("MKQL FreeList")
        , OutputInflightBytes("Output"), LocalInflightBytes("Local"), InputInflightBytes("Input")
    {
    }
    const ui32 NodeId;
    ui32 Tasks = 0;
    ui32 FinishedTasks = 0;
    ui32 OffsetY = 0;
    ui32 Height = 0;
    std::shared_ptr<TSingleMetric> MemoryUsage;
    std::shared_ptr<TSingleMetric> MaxMemoryUsage;
    std::shared_ptr<TSingleMetric> CpuTime;

    TMutableMetric MemPhysicalUsage;
    TMutableMetric MemSysAllocated;
    TMutableMetric MemSysFragmented;
    TMutableMetric MemArrowDefault;
    TMutableMetric MemMkqlAllocated;
    TMutableMetric MemMkqlFreeList;
    TMutableMetric OutputInflightBytes;
    TMutableMetric LocalInflightBytes;
    TMutableMetric InputInflightBytes;
};

} // namespace NPlan2Svg
