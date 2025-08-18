#pragma once

#include <vector>
#include <map>
#include <set>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

class TStage;

class TSummaryMetric {

public:
    ui64 Value = 0;
    ui32 Count = 0;
    ui64 Min = 0;
    ui64 Max = 0;

    void Add(ui64 value) {
        if (Count) {
            Min = std::min(Min, value);
            Max = std::max(Max, value);
        } else {
            Min = value;
            Max = value;
        }
        Value += value;
        Count++;
    }

    ui64 Average() {
        return Count ? (Value / Count) : 0;
    }
};

struct TAggregation {
    ui64 Min = 0;
    ui64 Max = 0;
    ui64 Avg = 0;
    ui64 Sum = 0;
    ui32 Count = 0;

    TAggregation() {}
    TAggregation(ui64 value) : Min(value), Max(value), Avg(value), Sum(value), Count(1) {}
    bool Load(const NJson::TJsonValue& node);
};

struct TMetricHistory {
    std::vector<std::pair<ui64, ui64>> Deriv;
    ui64 MaxDeriv = 0;
    std::vector<std::pair<ui64, ui64>> Values;
    ui64 MaxValue = 0;
    ui64 MinTime = 0;
    ui64 MaxTime = 0;

    void Load(const NJson::TJsonValue& node, ui64 explicitMinTime, ui64 explicitMaxTime);
    void Load(std::vector<ui64>& times, std::vector<ui64>& values, ui64 explicitMinTime, ui64 explicitMaxTime);
};

class TSingleMetric {

public:
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary, const NJson::TJsonValue& node,
        ui64 minTime = 0, ui64 maxTime = 0,
        const NJson::TJsonValue* firstMessageNode = nullptr,
        const NJson::TJsonValue* lastMessageNode = nullptr,
        const NJson::TJsonValue* waitTimeUsNode = nullptr);
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary, ui64 value);
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary);

    std::shared_ptr<TSummaryMetric> Summary;
    TAggregation Details;

    TMetricHistory History;
    TMetricHistory WaitTime;
    ui64 MinTime = 0;
    ui64 MaxTime = 0;
    TAggregation FirstMessage;
    TAggregation LastMessage;
};

class TConnection {

public:
    TConnection(TStage& stage, const TString& nodeType, ui32 planNodeId) : Stage(stage), NodeType(nodeType), PlanNodeId(planNodeId) {
    }

    TStage& Stage;
    TString NodeType;
    std::shared_ptr<TStage> FromStage;
    std::shared_ptr<TSingleMetric> InputBytes;
    std::shared_ptr<TSingleMetric> InputRows;
    std::vector<std::string> KeyColumns;
    std::vector<std::string> SortColumns;
    bool CteConnection = false;
    ui32 CteIndentX = 0;
    ui32 CteOffsetY = 0;
    std::shared_ptr<TSingleMetric> CteOutputBytes;
    std::shared_ptr<TSingleMetric> CteOutputRows;
    const NJson::TJsonValue* StatsNode = nullptr;
    const ui32 PlanNodeId;
    TStringBuilder Builder;
};

class TOperatorInput {

public:
    ui32 OperatorId = 0;
    ui32 PlanNodeId = 0;
    std::optional<ui32> StageId;
    TString PrecomputeRef;
    std::shared_ptr<TSingleMetric> Rows;

    bool IsAssigned() {
        return static_cast<bool>(OperatorId) || static_cast<bool>(PlanNodeId) || static_cast<bool>(PrecomputeRef);
    }
};

class TOperatorInfo {

public:
    TOperatorInfo(const TString& name, const TString& info) : Name(name), Info(info) {
    }

    TString Name;
    TString Info;
    std::shared_ptr<TSingleMetric> OutputRows;
    std::shared_ptr<TSingleMetric> OutputThroughput;
    TOperatorInput Input1;
    TOperatorInput Input2;
    std::shared_ptr<TSingleMetric> InputThroughput;
    TString Estimations;
};

class TPlan;

class TStage {

public:
    TStage(TPlan* plan, const TString& nodeType) : Plan(plan), NodeType(nodeType) {
    }

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
    std::shared_ptr<TSingleMetric> MaxMemoryUsage;
    std::shared_ptr<TSingleMetric> OutputBytes;
    std::shared_ptr<TSingleMetric> OutputRows;
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
    TStringBuilder Builder;
    TConnection* IngressConnection = nullptr;
};

struct TColorPalette {
    TColorPalette();
    TString StageMain;
    TString StageClone;
    TString StageText;
    TString StageTextHighlight;
    TString StageGrid;
    TString IngressDark;
    TString IngressMedium;
    TString IngressLight;
    TString InputDark;
    TString InputMedium;
    TString InputLight;
    TString EgressDark;
    TString EgressMedium;
    TString EgressLight;
    TString OutputDark;
    TString OutputMedium;
    TString OutputLight;
    TString MemMedium;
    TString MemLight;
    TString CpuMedium;
    TString CpuLight;
    TString ConnectionFill;
    TString ConnectionLine;
    TString ConnectionText;
    TString MinMaxLine;
    TString TextLight;
    TString TextInverted;
    TString TextSummary;
    TString SpillingBytesDark;
    TString SpillingBytesMedium;
    TString SpillingBytesLight;
    TString SpillingTimeDark;
    TString SpillingTimeMedium;
    TString SpillingTimeLight;
};

struct TPlanViewConfig {
    TPlanViewConfig();
    ui32 HeaderLeft;
    ui32 HeaderWidth;
    ui32 OperatorLeft;
    ui32 OperatorWidth;
    ui32 TaskLeft;
    ui32 TaskWidth;
    ui32 SummaryLeft;
    ui32 SummaryWidth;
    ui32 TimelineLeft;
    ui32 TimelineWidth;
    ui32 Width;
    TColorPalette Palette;
    bool Simplified = false;
};

class TPlanVisualizer;

class TPlan {

public:
    TPlan(const TString& nodeType, TPlanViewConfig& config, TPlanVisualizer& viz)
        : NodeType(nodeType), Config(config), Viz(viz) {
        CpuTime = std::make_shared<TSummaryMetric>();
        ExternalCpuTime = std::make_shared<TSummaryMetric>();
        WaitInputTime = std::make_shared<TSummaryMetric>();
        WaitOutputTime = std::make_shared<TSummaryMetric>();
        MaxMemoryUsage = std::make_shared<TSummaryMetric>();
        EgressBytes = std::make_shared<TSummaryMetric>();
        EgressRows = std::make_shared<TSummaryMetric>();
        OutputBytes = std::make_shared<TSummaryMetric>();
        OutputRows = std::make_shared<TSummaryMetric>();
        InputBytes = std::make_shared<TSummaryMetric>();
        InputRows = std::make_shared<TSummaryMetric>();
        IngressBytes = std::make_shared<TSummaryMetric>();
        IngressRows = std::make_shared<TSummaryMetric>();
        ExternalBytes = std::make_shared<TSummaryMetric>();
        ExternalRows = std::make_shared<TSummaryMetric>();
        SpillingComputeTime = std::make_shared<TSummaryMetric>();
        SpillingComputeBytes = std::make_shared<TSummaryMetric>();
        SpillingChannelTime = std::make_shared<TSummaryMetric>();
        SpillingChannelBytes = std::make_shared<TSummaryMetric>();
        OperatorInputRows = std::make_shared<TSummaryMetric>();
        OperatorOutputRows = std::make_shared<TSummaryMetric>();
        OperatorInputThroughput = std::make_shared<TSummaryMetric>();
        OperatorOutputThroughput = std::make_shared<TSummaryMetric>();
        StageInputThroughput = std::make_shared<TSummaryMetric>();
    }

    void Load(const NJson::TJsonValue& node);
    void MergeTotalCpu(std::shared_ptr<TSingleMetric> cpuTime);
    void LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node, TConnection* outputConnection);
    void LoadSource(const NJson::TJsonValue& node, std::vector<TOperatorInfo>& stageOperators, const NJson::TJsonValue* ingressRowsNode);
    void MarkStageIndent(ui32 indentX, ui32& offsetY, std::shared_ptr<TStage> stage);
    void MarkLayout();
    void ResolveCteRefs();
    void ResolveOperatorInputs();
    void PrintSeries(TStringBuilder& canvas, std::vector<std::pair<ui64, ui64>> series, ui64 maxValue, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor);
    void PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, const TString& color, bool backgroundRect = false);
    void PrintWaitTime(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& fillColor);
    void PrintDeriv(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintValues(TStringBuilder& canvas, TMetricHistory& history, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintStageSummary(TStringBuilder& background, ui32 viewLeft, ui32 viewWidth, ui32 y0, ui32 h, std::shared_ptr<TSingleMetric> metric, const TString& mediumColor, const TString& lightColor, const TString& textSum, const TString& tooltip, ui32 taskCount, const TString& iconRef, const TString& iconColor, const TString& iconScale, bool backgroundRect = false, const TString& peerId = "");
    void PrepareSvg(ui64 maxTime, ui32 timelineDelta, ui32& offsetY);
    void PrintSvg(TStringBuilder& builder);
    TString NodeType;
    std::vector<std::shared_ptr<TStage>> Stages;
    std::shared_ptr<TSummaryMetric> CpuTime;
    std::shared_ptr<TSummaryMetric> ExternalCpuTime;
    std::shared_ptr<TSummaryMetric> WaitInputTime;
    std::shared_ptr<TSummaryMetric> WaitOutputTime;
    std::shared_ptr<TSummaryMetric> MaxMemoryUsage;
    std::shared_ptr<TSummaryMetric> EgressBytes;
    std::shared_ptr<TSummaryMetric> EgressRows;
    std::shared_ptr<TSummaryMetric> OutputBytes;
    std::shared_ptr<TSummaryMetric> OutputRows;
    std::shared_ptr<TSummaryMetric> InputBytes;
    std::shared_ptr<TSummaryMetric> InputRows;
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
    std::shared_ptr<TSummaryMetric> OperatorInputThroughput;
    std::shared_ptr<TSummaryMetric> OperatorOutputThroughput;
    std::shared_ptr<TSummaryMetric> StageInputThroughput;
    std::vector<ui64> TotalCpuTimes;
    std::vector<ui64> TotalCpuValues;
    TMetricHistory TotalCpuTime;
    ui64 MaxTime = 1000;
    ui64 BaseTime = 0;
    ui64 TimeOffset = 0;
    ui32 OffsetY = 0;
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
    TStringBuilder Builder;
};

class TPlanVisualizer {

public:

    void LoadPlans(const TString& plans, bool simplified = false);
    void LoadPlans(const NJson::TJsonValue& root);
    void LoadPlan(const TString& planNodeType, const NJson::TJsonValue& root);
    void PostProcessPlans();
    TString PrintSvg();
    TString PrintSvgSafe();

    std::vector<std::shared_ptr<TPlan>> Plans;
    ui64 MaxTime = 1000;
    ui64 BaseTime = 0;
    ui64 UpdateTime = 0;
    TPlanViewConfig Config;
    std::map<std::string, std::shared_ptr<TStage>> CteStages;
    std::map<std::string, TPlan*> CteSubPlans;
};
