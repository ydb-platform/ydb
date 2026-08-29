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

class TVisualizer;

class TPlan {

public:
    TPlan(ui32 groupId, const TString& nodeType, TPlanViewConfig& config, TVisualizer& viz)
        : GroupId(groupId), NodeType(nodeType), Config(config), Viz(viz) {}

    void Load(const NJson::TJsonValue& node);
    void MergeTotalCpu(std::shared_ptr<TSingleMetric> cpuTime);
    void LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, TConnection* outputConnection);
    void LoadSource(const NJson::TJsonValue& node, std::vector<TOperatorInfo>& stageOperators, const NJson::TJsonValue* ingressRowsNode);
    void LoadNode(const NJson::TJsonValue& node);
    void MarkStageIndent(ui32 indentX, ui32& offsetY, std::shared_ptr<TStage> stage);
    void MarkLayout();
    void ResolveCteRefs();
    void ResolveOperatorInputs();
    void PrintSeries(TStringBuilder& canvas, std::vector<std::pair<ui64, ui64>> series, ui64 maxValue, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor, bool closed = true);
    void PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, TStringBuf color, bool backgroundRect = false);
    void PrintWaitTime(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, TStringBuf fillColor);
    void PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor = "");
    void PrintValues(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, TStringBuf lineColor, TStringBuf fillColor = "");
    void PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h, std::shared_ptr<TSingleMetric>& metric, const TColorTriple& colors, const TString& textSum, const TString& tooltip, ui32 taskCount, TStringBuf iconRef, TStringBuf iconColor, TStringBuf iconScale, bool backgroundRect = false, const TString& peerId = "", ui64 split = 0, const std::shared_ptr<TScalarMetric>& scalar = nullptr);
    void PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h,  std::initializer_list<std::pair<TMutableMetric*, TStringBuf>> history, ui64 scale, TStringBuf iconRef, TStringBuf iconColor, TStringBuf iconScale);
    // The timeline strip every data flow draws: the bar, then the wait time
    // overlay under the connection canvas, then the derivative curve over it.
    void PrintDataFlowTimeline(TStringBuilder& builder, const TString& title, const std::shared_ptr<TSingleMetric>& bytes, ui32 x, ui32 y, ui32 w, const TColorTriple& colors, bool backgroundRect = false);
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
    TSummaryMetric CpuTime;
    TSummaryMetric ExternalCpuTime;
    TSummaryMetric WaitInputTime;
    TSummaryMetric WaitOutputTime;
    TSummaryMetric MemoryUsage;
    TSummaryMetric MaxMemoryUsage;
    TSummaryMetric EgressBytes;
    TSummaryMetric EgressRows;
    TSummaryMetric OutputBytes;
    TSummaryMetric OutputRows;
    TSummaryMetric OutputChunkSize;
    TSummaryMetric InputBytes;
    TSummaryMetric InputRows;
    TSummaryMetric InputChunkSize;
    TSummaryMetric IngressBytes;
    TSummaryMetric IngressRows;
    TSummaryMetric ExternalBytes;
    TSummaryMetric ExternalRows;
    TSummaryMetric SpillingComputeTime;
    TSummaryMetric SpillingComputeBytes;
    TSummaryMetric SpillingChannelTime;
    TSummaryMetric SpillingChannelBytes;
    TSummaryMetric OperatorInputRows;
    TSummaryMetric OperatorOutputRows;
    TSummaryMetric StageInputThroughput;
    TSummaryMetric NodeOutputBytes;
    TSummaryMetric NodeMemoryUsage;
    TSummaryMetric NodeMaxMemoryUsage;
    TSummaryMetric NodeCpuTime;
    TSummaryMetric NodeInputBytes;
    TSummaryMetric NodeIngressBytes;
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
    TVisualizer& Viz;
    std::unordered_map<ui32, TConnection*> NodeToConnection;
    std::unordered_map<TStage*, TConnection*> StageToExternalConnection;
    std::unordered_set<ui32> NodeToSource;
    TStringBuilder SummaryBuilder;
    std::vector<std::shared_ptr<TClusterNode>> Nodes;
    ui32 NodeIndentY = 0;
};

} // namespace NPlan2Svg
