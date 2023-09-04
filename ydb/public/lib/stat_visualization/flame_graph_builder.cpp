#include "flame_graph_builder.h"
#include "svg_script.h"
#include "stat_visalization_error.h"

#include <ydb/public/lib/ydb_cli/common/common.h>
#include <library/cpp/json/json_reader.h>
#include <util/folder/path.h>
#include <util/generic/fwd.h>
#include <util/generic/utility.h>
#include <util/generic/strbuf.h>
#include <util/string/printf.h>

namespace NKikimr::NVisual {
using namespace NJson;
namespace {
constexpr float rectHeight = 15;
constexpr float minElementWidth = 1;

constexpr float interElementOffset = 3;

// Offsets from image limits
constexpr float horOffset = 10;
constexpr float vertOffset = 30;

constexpr float textSideOffset = 3;
constexpr float textTopOffset = 10.5;

constexpr float viewPortWidth = 1200;

TMap<EFlameGraphType, TString> TypeName = {
        {CPU,          "CPU"},
        {TIME,         "TIME_MS"},
        {BYTES_OUTPUT, "OUT_B"},
        {TASKS,        "TASKS"}
};

struct TWeight {
    explicit TWeight(ui64 self)
            : Self(self) {};
    ui64 Self;
    ui64 Total = 0;
};

struct TCombinedWeights {
    TCombinedWeights()
            : Cpu(0), Bytes(0), Ms(0), Tasks(0) {}

    TCombinedWeights(ui64 cpu, ui64 bytes, ui64 ms, ui64 tasks)
            : Cpu(cpu), Bytes(bytes), Ms(ms), Tasks(tasks) {}

    void AddSelfToTotal() {
        Cpu.Self = (Cpu.Total + Cpu.Self) ? Cpu.Self : 1;
        Bytes.Self = (Bytes.Total + Bytes.Self) ? Bytes.Self : 1;
        Ms.Self = (Ms.Total + Ms.Self) ? Ms.Self : 1;
        Tasks.Self = (Tasks.Total + Tasks.Self) ? Tasks.Self : 1;

        Cpu.Total += Cpu.Self;
        Bytes.Total += Bytes.Self;
        Ms.Total += Ms.Self;
        Tasks.Total += Tasks.Self;
    }

    TCombinedWeights operator+(const TCombinedWeights &rhs) {
        Cpu.Total += rhs.Cpu.Total;
        Bytes.Total += rhs.Bytes.Total;
        Ms.Total += rhs.Ms.Total;
        Tasks.Total += rhs.Tasks.Total;
        return *this;
    }

    TWeight operator[](EFlameGraphType type) const {
        switch (type) {
            case CPU:
                return Cpu;
            case TIME:
                return Ms;
            case BYTES_OUTPUT:
                return Bytes;
            case TASKS:
                return Tasks;
            case ALL:
                throw yexception() << "Unsupported value for EFlameGraphType";
        }
    }

    // Cpu usage from this step stats
    TWeight Cpu;
    // Output bytes of step
    TWeight Bytes;
    // Time spent
    TWeight Ms;
    // Number of tasks used
    TWeight Tasks;

};
}

class TPlanGraphEntry {
public:
    TPlanGraphEntry(const TString &name, ui64 weightCpu, ui64 weightBytes, ui64 weightMs, ui64 weightTasks)
            : Name(name)//
            , Weights(weightCpu, weightBytes, weightMs, weightTasks) {};

    void AddChild(THolder<TPlanGraphEntry> &&child) {
        Children.emplace_back(std::move(child));
    }


    /// Builds svg for graph, starting from this node
    void SerializeToSvg(TOFStream &stream, float viewportHeight, EFlameGraphType type) const {
        auto baseParentWeight = Weights[type];
        SerializeToSvgImpl(stream,
                           horOffset, viewportHeight - vertOffset - rectHeight,
                           baseParentWeight.Total, type, viewPortWidth - 2 * horOffset);
    }

    /// Returns current depth of graph
    ui64 CalculateDepth(ui32 curDepth) {
        ui64 maxDepth = curDepth + 1;
        for (auto &child: Children) {
            maxDepth = Max(child->CalculateDepth(curDepth + 1), maxDepth);
        }

        return maxDepth;
    }

    /// After graph is build, we can recalculate weights considering children
    ///
    /// Not all plan entries has own statistics, for such entries we recalculate the weight as
    /// weight of all children
    TCombinedWeights CalculateWeight() {
        TCombinedWeights childrenWeights;
        for (auto &child: Children) {
            childrenWeights = childrenWeights + child->CalculateWeight();
        }

        Weights = Weights + childrenWeights;
        Weights.AddSelfToTotal();

        return Weights;
    }

    /// Returns Svg element corresponding to current graph and calls itself recursively for children
    float SerializeToSvgImpl(TOFStream &stream,
                             float xOffset,
                             float yOffset,
                             ui64 parentWeight,
                             EFlameGraphType type,
                             float parentWidth) const {
        float width = parentWidth * (static_cast<float>(Weights[type].Total) / static_cast<float>(parentWeight));
        auto weight = Weights[type];

        float xChildOffset = xOffset;
        for (const auto &child: Children) {
            xChildOffset += child->SerializeToSvgImpl(stream, xChildOffset, yOffset - rectHeight - interElementOffset,
                                                      weight.Total, type, width);
        }

        // Full description of step
        TString stepInfo = Sprintf("%s %s(self: %lu, total: %lu)",
                                   Name.c_str(), TypeName[type].c_str(), weight.Self, weight.Total);

        // Step name(we have to manually cut it, according to available space
        // 7 is found empirically
        auto symbolsAvailable = std::lround((width - 2 * textSideOffset) / 7);
        TString stepName;
        if (symbolsAvailable <= 2) {
            stepName = "";
        } else if (static_cast<ui64>(symbolsAvailable) < Name.length()) {
            stepName = Name.substr(0, symbolsAvailable - 2) + "..";
        } else {
            stepName = Name;
        }

        // Color falls more to red, if step takes more cpu, than it's children
        float selfToChildren = 0;
        if (weight.Total > weight.Self) {
            selfToChildren =
                    1 -
                    Min(static_cast<float>(weight.Self) / static_cast<float>(weight.Total - weight.Self),
                        1.f);
        }
        TString color = Sprintf("rgb(255, %ld, 0)", std::lround(selfToChildren * 255));

        stream << Sprintf(FG_SVG_GRAPH_ELEMENT.data(),
                          stepInfo.c_str(), // full text
                          TypeName[type].c_str(),
                          xOffset, yOffset, // position
                          Max(width - interElementOffset, minElementWidth), rectHeight, // width and height
                          color.c_str(), // element background color
                          xOffset + textSideOffset, yOffset + textTopOffset, // Text position
                          stepName.c_str() // short text
        );
        return width;
    }


public:
    TString Name;


    TCombinedWeights Weights;
    TVector<THolder<TPlanGraphEntry>> Children;
};


class TFlameGraphBuilder {
public:
    explicit TFlameGraphBuilder(const TString &resultFile)
            : ResultFile(resultFile) {};

    void ParsePlanJson(const TString &statReportJson) {
        auto stringBuf = TStringBuf(statReportJson);

        TJsonValue planJson;
        try {
            ReadJsonTree(stringBuf, &planJson, true/*throw on error*/);
        }
        catch (const TJsonException &ex) {
            Cerr << "Can't parse query statistics: " << ex.what();
        }

        // Parse json from table query command
        auto plan = planJson.GetValueByPath("Plan");
        if (plan != nullptr) {
            auto planGraph = ParsePlan(plan);
            planGraph->CalculateWeight();
            PlanGraphs.emplace_back(std::move(planGraph));
            return;
        }

        // Parse json from yql command
        if (auto queries = planJson.GetValueByPath("queries")) {
            for (auto query: queries->GetArray()) {
                plan = query.GetValueByPath("Plan");
                auto planGraph = ParsePlan(plan);
                planGraph->CalculateWeight();
                PlanGraphs.emplace_back(std::move(planGraph));
                return;
            }
        }


    }

    void GenerateSvg(EFlameGraphType type) {
        bool multipleQueries = PlanGraphs.size() > 1;
        ui16 queryIndex = 0;
        for (auto &planGraph: PlanGraphs) {
            auto resultStream = OpenResultStream(ResultFile, multipleQueries, queryIndex++);
            GenerateSvg(type, planGraph, resultStream);
        }
    }

    void GenerateSvg(EFlameGraphType type, const THolder<TPlanGraphEntry> &planGraph, TOFStream &resultStream) {
        auto depth = planGraph->CalculateDepth(0);

        auto viewPortHeight = (2 * vertOffset) + (static_cast<float>(depth) * (rectHeight + 2 * interElementOffset));
        viewPortHeight *= static_cast<float>(TypeName.size());

        // offsets for static elements
        float detailsElementPos = viewPortHeight - 17;
        constexpr float searchPos = viewPortWidth - 110;


        resultStream
                << Sprintf(FG_SVG_HEADER.data(), viewPortWidth, viewPortHeight, viewPortWidth, viewPortHeight)
                << FG_SVG_SCRIPT
                << Sprintf(FG_SVG_BACKGROUND.data(), viewPortWidth, viewPortHeight)
                << Sprintf(FG_SVG_INFO_BAR.data(), detailsElementPos)
                << FG_SVG_RESET_ZOOM
                << Sprintf(FG_SVG_SEARCH.data(), searchPos);
        (void) type;
        float i = 1;
        for (const auto &it: TypeName) {
            resultStream << Sprintf(FG_SVG_TITLE.data(),
                                    vertOffset + (viewPortHeight * (i - 1) / static_cast<float>(TypeName.size())),
                                    it.second.c_str());
            planGraph->SerializeToSvg(resultStream,
                                      viewPortHeight * i / static_cast<float>(TypeName.size()),
                                      it.first);
            i += 1;
        }

        resultStream << FG_SVG_FOOTER;
    }

private:
    static TOFStream OpenResultStream(const TString &resultFile, bool multipleQueries, ui16 queryIndex) {
        (void) multipleQueries;
        auto file = resultFile;
        if (file.StartsWith("~")) {
            file = NYdb::NConsoleClient::HomeDir + file.substr(1);
        }
        TFsPath fsPath(file);
        if (fsPath.IsDirectory()) {
            // Using plain throw instead of ythrow, as we don't need source info in the message
            throw TStatusVisualizationError() << file << " is a directory "
                                              << " please specify valid file name";
        }

        if (multipleQueries) {
            // Add index to file name, to save graphs for different queries in plan
            if (fsPath.GetExtension() == "svg") {
                auto fileName = fsPath.GetName().substr(0, fsPath.GetName().length() - 4);
                fsPath = TFsPath(fsPath.Parent()) / Sprintf("%s.%d.svg", fileName.c_str(), queryIndex);
            } else {
                fsPath = TFsPath(fsPath.Parent()) /
                         Sprintf("%s.%d", fsPath.GetName().c_str(), queryIndex);
            }
        }

        if (fsPath.Exists() && !fsPath.IsDirectory()) {
            return {fsPath};
        }

        auto parentPath = fsPath.Parent();
        if (!parentPath.Exists()) {
            correctpath(file);
            fsPath = TFsPath(file);
            parentPath = fsPath.Parent();
            if (!parentPath.Exists()) {
                throw TStatusVisualizationError() << "Directory " << parentPath
                                                  << " for flame graph output doesn't exist";
            }
        }

        return {fsPath};
    }

    THolder<TPlanGraphEntry> ParsePlan(TJsonValue *plan) {
        Y_ENSURE(plan);
        TJsonValue nodeType;
        if (!plan->GetValue("Node Type", &nodeType)) {
            ythrow TStatusVisualizationError() << "Node plat doesn't have node type";
        }
        TString stageDescription = nodeType.GetStringSafe();

        TJsonValue tables;
        if (plan->GetValue("Tables", &tables)) {
            stageDescription += " [";
            bool firstTable = true;
            for (const auto &table: tables.GetArray()) {
                if (!firstTable) {
                    stageDescription += ", ";
                }
                firstTable = false;
                stageDescription += table.GetStringSafe();
            }
            stageDescription += "]";
        }
        auto cpuUsage = plan->GetValueByPath("Stats/TotalCpuTimeUs", '/');
        auto outBytes = plan->GetValueByPath("Stats/TotalOutputBytes", '/');
        auto ms = plan->GetValueByPath("Stats/TotalDurationMs", '/');
        auto tasks = plan->GetValueByPath("Stats/TotalTasks", '/');

        auto planEntry = MakeHolder<TPlanGraphEntry>(stageDescription,
                                                     cpuUsage ? cpuUsage->GetUIntegerSafe() : 0,
                                                     outBytes ? outBytes->GetUIntegerSafe() : 0,
                                                     ms ? ms->GetUIntegerSafe() : 0,
                                                     tasks ? ms->GetUIntegerSafe() : 0
        );

        TJsonValue children;
        if (plan->GetValue("Plans", &children)) {
            for (auto child: children.GetArray()) {
                planEntry->AddChild(std::move(ParsePlan(&child)));
            }
        }
        return planEntry;
    }

private:
    TString ResultFile;

    TVector<THolder<TPlanGraphEntry>> PlanGraphs;
};


void GenerateFlameGraphSvg(const TString &resultFile, const TString &statReportJson, EFlameGraphType type) {
    TFlameGraphBuilder impl(resultFile);
    impl.ParsePlanJson(statReportJson);
    impl.GenerateSvg(type);
}

}
