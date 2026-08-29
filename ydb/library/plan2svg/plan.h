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

// Where a horizontal strip is drawn: the column it sits in, and the row it
// occupies within that column.
struct TViewBox {
    ui32 Left;
    ui32 Width;
    ui32 Top;
    ui32 Height;
};

// The icon drawn in the gutter left of a summary bar. Ref is empty for the bars
// that have none, and the other two fields are then unread.
struct TIcon {
    TStringBuf Ref;
    TStringBuf Color;
    TStringBuf Scale;
};

// Everything a summary bar draws besides its box: the metric it measures, the
// text and tooltip over it, and the decorations, all of which are optional -
// icon, peer id, the local/remote split marker, the mean chunk size dashes, and
// the skew badge, which is only considered when TaskCount is set.
struct TStageSummary {
    TSingleMetric* Metric = nullptr;
    TColorTriple Colors;
    TStringBuf Text;
    TStringBuf Tooltip;
    ui32 TaskCount = 0;
    TIcon Icon;
    bool BackgroundRect = false;
    TStringBuf PeerId;
    ui64 Split = 0;
    TScalarMetric* Scalar = nullptr;
};

class TPlan {

public:
    TPlan(ui32 groupId, const TString& nodeType, TPlanViewConfig& config, TVisualizer& viz)
        : GroupId(groupId), NodeType(nodeType), Config(config), Viz(viz) {}

    void Load(const NJson::TJsonValue& node);
    void MergeTotalCpu(std::shared_ptr<TSingleMetric> cpuTime);
    void LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, TConnection* outputConnection);
    void LoadOperators(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue& operators, std::vector<TOperatorInfo>& externalOperators);
    void LoadTableIngress(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue& subNode, const TString& name, std::vector<TOperatorInfo>& externalOperators);
    const NJson::TJsonValue* LoadStageStats(const std::shared_ptr<TStage>& stage, TStage* externalStage, TConnection* outputConnection);
    void LoadSubPlans(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue& plans, const NJson::TJsonValue* inputNode, TConnection* outputConnection, ui64& inputBytes);
    void LoadConnection(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue& plan, TString subNodeType, ui32 connectionPlanNodeId, const NJson::TJsonValue* inputNode, ui64& inputBytes);
    void LoadBuiltInIngress(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue& plan, const TString& subNodeType, ui32 connectionPlanNodeId);
    void LoadStageTimings(const std::shared_ptr<TStage>& stage, const NJson::TJsonValue* inputNode);
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
    void PrintStageSummary(TStringBuilder& background, const TViewBox& box, const TStageSummary& bar);
    void PrintStageSummary(TStringBuilder& background, const TViewBox& box, std::initializer_list<std::pair<TMutableMetric*, TStringBuf>> history, ui64 scale, const TIcon& icon);
    // The timeline strip every data flow draws: the bar, then the wait time
    // overlay under the connection canvas, then the derivative curve over it.
    void PrintDataFlowTimeline(TStringBuilder& builder, const TString& title, const std::shared_ptr<TSingleMetric>& bytes, ui32 x, ui32 y, ui32 w, const TColorTriple& colors, bool backgroundRect = false);
    // The dashed line down the task gutter, covering the share of the stage's
    // tasks that have not finished yet. Draws nothing until at least one task has.
    void PrintUnfinishedTasks(TStringBuilder& builder, ui32 tasks, ui32 finishedTasks);
    // A red circle with one letter in it, hung at the bottom of a strip and
    // explained by its tooltip.
    void PrintWarningBadge(TStringBuilder& builder, ui32 cx, ui32 bottom, const TString& title, TStringBuf label);
    // The highlighted amount a stage spilled, right aligned over its summary bar.
    void PrintSpillingBadge(TStringBuilder& builder, ui32 top, const TString& label, TSingleMetric* bytes);
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
