#pragma once

#include "config.h"
#include "metrics.h"
#include "model.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NPlan2Svg {

class TPlanVisualizer;

class TPlan {

public:
    TPlan(ui32 groupId, const TString& nodeType, TPlanViewConfig& config, TPlanVisualizer& viz)
        : GroupId(groupId), NodeType(nodeType), Config(config), Viz(viz) {
        CpuTime = std::make_shared<TSummaryMetric>();
        ExternalCpuTime = std::make_shared<TSummaryMetric>();
        WaitInputTime = std::make_shared<TSummaryMetric>();
        WaitOutputTime = std::make_shared<TSummaryMetric>();
        MemoryUsage = std::make_shared<TSummaryMetric>();
        MaxMemoryUsage = std::make_shared<TSummaryMetric>();
        EgressBytes = std::make_shared<TSummaryMetric>();
        EgressRows = std::make_shared<TSummaryMetric>();
        OutputBytes = std::make_shared<TSummaryMetric>();
        OutputRows = std::make_shared<TSummaryMetric>();
        OutputChunkSize = std::make_shared<TSummaryMetric>();
        InputBytes = std::make_shared<TSummaryMetric>();
        InputRows = std::make_shared<TSummaryMetric>();
        IngressBytes = std::make_shared<TSummaryMetric>();
        IngressRows = std::make_shared<TSummaryMetric>();
        InputChunkSize = std::make_shared<TSummaryMetric>();
        ExternalBytes = std::make_shared<TSummaryMetric>();
        ExternalRows = std::make_shared<TSummaryMetric>();
        SpillingComputeTime = std::make_shared<TSummaryMetric>();
        SpillingComputeBytes = std::make_shared<TSummaryMetric>();
        SpillingChannelTime = std::make_shared<TSummaryMetric>();
        SpillingChannelBytes = std::make_shared<TSummaryMetric>();
        OperatorInputRows = std::make_shared<TSummaryMetric>();
        OperatorOutputRows = std::make_shared<TSummaryMetric>();
        StageInputThroughput = std::make_shared<TSummaryMetric>();
        NodeOutputBytes = std::make_shared<TSummaryMetric>();
        NodeCpuTime = std::make_shared<TSummaryMetric>();
        NodeMemoryUsage = std::make_shared<TSummaryMetric>();
        NodeMaxMemoryUsage = std::make_shared<TSummaryMetric>();
        NodeInputBytes = std::make_shared<TSummaryMetric>();
        NodeIngressBytes = std::make_shared<TSummaryMetric>();
    }

    void Load(const NJson::TJsonValue& node);
    void MergeTotalCpu(std::shared_ptr<TSingleMetric> cpuTime);
    void LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, TConnection* outputConnection);
    void LoadSource(const NJson::TJsonValue& node, std::vector<TOperatorInfo>& stageOperators, const NJson::TJsonValue* ingressRowsNode);
    void LoadNode(const NJson::TJsonValue& node);
    void MarkStageIndent(ui32 indentX, ui32& offsetY, std::shared_ptr<TStage> stage);
    void MarkLayout();
    void ResolveCteRefs();
    void ResolveOperatorInputs();
    void PrintSeries(TStringBuilder& canvas, std::vector<std::pair<ui64, ui64>> series, ui64 maxValue, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor, bool closed = true);
    void PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, const TString& color, bool backgroundRect = false);
    void PrintWaitTime(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& fillColor);
    void PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintValues(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h, std::shared_ptr<TSingleMetric>& metric, const TString& mediumColor, const TString& lightColor, const TString& textSum, const TString& tooltip, ui32 taskCount, const TString& iconRef, const TString& iconColor, const TString& iconScale, bool backgroundRect = false, const TString& peerId = "", ui64 split = 0, const std::shared_ptr<TScalarMetric>& scalar = nullptr);
    void PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h,  std::initializer_list<std::pair<TMutableMetric*, TString>> history, ui64 scale, const TString& iconRef, const TString& iconColor, const TString& iconScale);
    void PrepareSvg(ui64 maxTime, ui32 timelineDelta, ui32& offsetY);
    void PrintSvg(TStringBuilder& builder, ui64 maxTime, ui32 timelineDelta);
    void PrintStage(TStringBuilder& builder, std::shared_ptr<TStage>& stage, TConnection* c);
    void PrintNodes(TStringBuilder& builder, ui64 maxTime, ui32 timelineDelta);
    void CalcHotPath();
    void CalcCriticals(TStage& stage);
    TString GetCriticalCpuGroups();
    TString GetCriticalTimeGroups();
    const ui32 GroupId;
    TString NodeType;
    std::vector<std::shared_ptr<TStage>> Stages;
    std::shared_ptr<TSummaryMetric> CpuTime;
    std::shared_ptr<TSummaryMetric> ExternalCpuTime;
    std::shared_ptr<TSummaryMetric> WaitInputTime;
    std::shared_ptr<TSummaryMetric> WaitOutputTime;
    std::shared_ptr<TSummaryMetric> MemoryUsage;
    std::shared_ptr<TSummaryMetric> MaxMemoryUsage;
    std::shared_ptr<TSummaryMetric> EgressBytes;
    std::shared_ptr<TSummaryMetric> EgressRows;
    std::shared_ptr<TSummaryMetric> OutputBytes;
    std::shared_ptr<TSummaryMetric> OutputRows;
    std::shared_ptr<TSummaryMetric> OutputChunkSize;
    std::shared_ptr<TSummaryMetric> InputBytes;
    std::shared_ptr<TSummaryMetric> InputRows;
    std::shared_ptr<TSummaryMetric> InputChunkSize;
    std::shared_ptr<TSummaryMetric> IngressBytes;
    std::shared_ptr<TSummaryMetric> IngressRows;
    std::shared_ptr<TSummaryMetric> ExternalBytes;
    std::shared_ptr<TSummaryMetric> ExternalRows;
    std::shared_ptr<TSummaryMetric> SpillingComputeTime;
    std::shared_ptr<TSummaryMetric> SpillingComputeBytes;
    std::shared_ptr<TSummaryMetric> SpillingChannelTime;
    std::shared_ptr<TSummaryMetric> SpillingChannelBytes;
    std::shared_ptr<TSummaryMetric> OperatorInputRows;
    std::shared_ptr<TSummaryMetric> OperatorOutputRows;
    std::shared_ptr<TSummaryMetric> StageInputThroughput;
    std::shared_ptr<TSummaryMetric> NodeOutputBytes;
    std::shared_ptr<TSummaryMetric> NodeMemoryUsage;
    std::shared_ptr<TSummaryMetric> NodeMaxMemoryUsage;
    std::shared_ptr<TSummaryMetric> NodeCpuTime;
    std::shared_ptr<TSummaryMetric> NodeInputBytes;
    std::shared_ptr<TSummaryMetric> NodeIngressBytes;
    std::vector<ui64> TotalCpuTimes;
    std::vector<ui64> TotalCpuValues;
    TMetricHistory TotalCpuTime;
    ui64 MaxTime = 1;
    ui64 BaseTime = 0;
    ui64 TimeOffset = 0;
    ui32 OffsetY = 0;
    ui32 Height = 0;
    ui32 Tasks = 0;
    ui64 UpdateTime = 0;
    std::vector<std::pair<std::string, std::shared_ptr<TConnection>>> CteRefs;
    TString CtePlanRef;
    TPlan* CtePlan = nullptr;
    TPlanViewConfig& Config;
    TPlanVisualizer& Viz;
    std::unordered_map<ui32, TConnection*> NodeToConnection;
    std::unordered_map<TStage*, TConnection*> StageToExternalConnection;
    std::unordered_set<ui32> NodeToSource;
    TStringBuilder SummaryBuilder;
    std::vector<std::shared_ptr<TClusterNode>> Nodes;
    ui32 NodeIndentY = 0;
};

} // namespace NPlan2Svg
