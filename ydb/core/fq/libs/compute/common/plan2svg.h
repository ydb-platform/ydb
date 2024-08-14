#pragma once

#include <vector>
#include <map>

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
};

class TSingleMetric {

public:
    TSingleMetric(std::shared_ptr<TSummaryMetric> summary, const NJson::TJsonValue& node,
        const NJson::TJsonValue* firstMessageNode = nullptr,
        const NJson::TJsonValue* lastMessageNode = nullptr,
        const NJson::TJsonValue* waitTimeUsNode = nullptr);

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
    TConnection(const TString& nodeType) : NodeType(nodeType) {
    }

    TString NodeType;
    std::shared_ptr<TStage> FromStage;
    std::shared_ptr<TSingleMetric> InputBytes;
    std::shared_ptr<TSingleMetric> InputRows;
    std::vector<std::string> KeyColumns;
    bool CteConnection = false;
    ui32 CteIndentX = 0;
    ui32 CteOffsetY = 0;
    const NJson::TJsonValue* StatsNode = nullptr;
};

class TSource {

public:
    TSource(const TString& nodeType) : NodeType(nodeType) {
    }

    TString NodeType;
    std::shared_ptr<TSingleMetric> IngressBytes;
    std::shared_ptr<TSingleMetric> IngressRows;
    std::vector<std::string> Info;
};

class TStage {

public:
    TStage(const TString& nodeType) : NodeType(nodeType) {
    }

    TString NodeType;
    std::shared_ptr<TSource> Source;
    std::shared_ptr<TSingleMetric> IngressBytes;
    std::vector<std::shared_ptr<TConnection>> Connections;
    ui32 IndentX = 0;
    ui32 IndentY = 0;
    ui32 OffsetY = 0;
    ui32 Height = 0;
    std::shared_ptr<TSingleMetric> CpuTime;
    std::shared_ptr<TSingleMetric> MaxMemoryUsage;
    std::shared_ptr<TSingleMetric> OutputBytes;
    std::shared_ptr<TSingleMetric> OutputRows;
    std::vector<std::string> Info;
    ui64 BaseTime = 0;
    ui32 PlanNodeId = 0;
    ui32 PhysicalStageId = 0;
    ui32 Tasks = 0;
    const NJson::TJsonValue* StatsNode = nullptr;
};

struct TColorPalette {
    TColorPalette();
    TString StageDark;
    TString StageLight;
    TString StageText;
    TString StageGrid;
    TString IngressDark;
    TString IngressMedium;
    TString IngressLight;
    TString InputDark;
    TString InputMedium;
    TString InputLight;
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
};

struct TPlanViewConfig {
    TPlanViewConfig();
    ui32 HeaderWidth;
    ui32 SummaryWidth;
    ui32 Width;
    TColorPalette Palette;
};

class TPlan {

public:
    TPlan(const TString& nodeType, TPlanViewConfig& config, std::map<std::string, std::shared_ptr<TStage>>& cteStages)
        : NodeType(nodeType), Config(config), CteStages(cteStages) {
        CpuTime = std::make_shared<TSummaryMetric>();
        MaxMemoryUsage = std::make_shared<TSummaryMetric>();
        OutputBytes = std::make_shared<TSummaryMetric>();
        OutputRows = std::make_shared<TSummaryMetric>();
        InputBytes = std::make_shared<TSummaryMetric>();
        InputRows = std::make_shared<TSummaryMetric>();
        IngressBytes = std::make_shared<TSummaryMetric>();
        IngressRows = std::make_shared<TSummaryMetric>();
    }

    void Load(const NJson::TJsonValue& node);
    void LoadStage(std::shared_ptr<TStage> stage, const NJson::TJsonValue& node);
    void LoadSource(std::shared_ptr<TSource> source, const NJson::TJsonValue& node);
    void MarkStageIndent(ui32 indentX, ui32& offsetY, std::shared_ptr<TStage> stage);
    void MarkLayout();
    void ResolveCteRefs();
    void PrintTimeline(TStringBuilder& background, TStringBuilder& canvas, const TString& title, TAggregation& firstMessage, TAggregation& lastMessage, ui32 x, ui32 y, ui32 w, ui32 h, const TString& color);
    void PrintWaitTime(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& fillColor);
    void PrintDeriv(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintValues(TStringBuilder& canvas, std::shared_ptr<TSingleMetric> metric, ui32 x, ui32 y, ui32 w, ui32 h, const TString& title, const TString& lineColor, const TString& fillColor = "");
    void PrintSvg(ui64 maxTime, ui32& offsetY, TStringBuilder& background, TStringBuilder& canvas);
    TString NodeType;
    std::vector<std::shared_ptr<TStage>> Stages;
    std::shared_ptr<TSummaryMetric> CpuTime;
    std::shared_ptr<TSummaryMetric> MaxMemoryUsage;
    std::shared_ptr<TSummaryMetric> OutputBytes;
    std::shared_ptr<TSummaryMetric> OutputRows;
    std::shared_ptr<TSummaryMetric> InputBytes;
    std::shared_ptr<TSummaryMetric> InputRows;
    std::shared_ptr<TSummaryMetric> IngressBytes;
    std::shared_ptr<TSummaryMetric> IngressRows;
    ui64 MaxTime = 1000;
    ui64 BaseTime = 0;
    ui64 TimeOffset = 0;
    ui32 OffsetY = 0;
    ui32 Tasks = 0;
    std::vector<std::pair<std::string, std::shared_ptr<TConnection>>> CteRefs;
    TPlanViewConfig& Config;
    std::map<std::string, std::shared_ptr<TStage>>& CteStages;
};

class TPlanVisualizer {

public:

    void LoadPlans(const TString& plans);
    void LoadPlan(const TString& planNodeType, const NJson::TJsonValue& root);
    void PostProcessPlans();
    TString PrintSvg();
    TString PrintSvgSafe();

    std::vector<TPlan> Plans;
    ui64 MaxTime = 1000;
    ui64 BaseTime = 0;
    TPlanViewConfig Config;
    std::map<std::string, std::shared_ptr<TStage>> CteStages;
};