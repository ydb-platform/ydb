#include "flame_graph_builder.h"
#include "flame_graph_entry.h"
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

    void GenerateSvg() {
        bool multipleQueries = PlanGraphs.size() > 1;
        ui16 queryIndex = 0;
        for (auto &planGraph: PlanGraphs) {
            auto resultStream = OpenResultStream(ResultFile, multipleQueries, queryIndex++);
            GenerateSvg(planGraph, resultStream);
        }
    }

    static void GenerateSvg(const THolder<TPlanGraphEntry> &planGraph, TOFStream &resultStream) {
        auto depth = planGraph->CalculateDepth(0);

        auto viewPortHeight = (2 * VERTICAL_OFFSET) + (static_cast<double>(depth) * (RECT_HEIGHT + 2 * INTER_ELEMENT_OFFSET));
        viewPortHeight *= static_cast<double>(TPlanGraphEntry::PlanGraphTypeName().size());

        // offsets for static elements
        constexpr float detailsElementOffset = 17;
        constexpr float searchPos = VIEWPORT_WIDTH - 110;

        TString tmpTaskElements;
        TStringOutput tmpTaskStream(tmpTaskElements);
        resultStream
                << Sprintf(FG_SVG_HEADER.data(), VIEWPORT_WIDTH, viewPortHeight, VIEWPORT_WIDTH, viewPortHeight)
                << FG_SVG_SCRIPT
                << Sprintf(FG_SVG_BACKGROUND.data(), VIEWPORT_WIDTH, viewPortHeight)
                << FG_SVG_RESET_ZOOM
                << Sprintf(FG_SVG_SEARCH.data(), searchPos);
        float i = 1;
        for (const auto &it: TPlanGraphEntry::PlanGraphTypeName()) {
            resultStream << Sprintf(FG_SVG_TITLE.data(),
                                    VERTICAL_OFFSET + (viewPortHeight * (i - 1) /
                                                       static_cast<double>(TPlanGraphEntry::PlanGraphTypeName().size())),
                                    it.second.c_str());
            auto typedVertOffset =
                    viewPortHeight * i / static_cast<double>(TPlanGraphEntry::PlanGraphTypeName().size());
            planGraph->SerializeToSvg(resultStream,
                                      tmpTaskStream,
                                      typedVertOffset,
                                      it.first);
            resultStream << Sprintf(FG_SVG_INFO_BAR.data(), it.second.c_str(), typedVertOffset - detailsElementOffset);
            i += 1;
        }

        resultStream << FG_SVG_TASK_PROFILE_BACKGROUND;
        resultStream << tmpTaskElements;
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

        auto stageId = plan->GetValueByPath("PlanNodeId", '/');
        auto cpuUsage = plan->GetValueByPath("Stats/TotalCpuTimeUs", '/');
        auto outBytes = plan->GetValueByPath("Stats/TotalOutputBytes", '/');
        auto ms = plan->GetValueByPath("Stats/TotalDurationMs", '/');
        auto tasks = plan->GetValueByPath("Stats/TotalTasks", '/');

        auto taskProfile = parseTasksProfile(plan->GetValueByPath("Stats", '/'));

        auto planEntry = MakeHolder<TPlanGraphEntry>(stageDescription,
                                                     stageId ? stageId->GetUIntegerSafe() : 0,
                                                     cpuUsage ? cpuUsage->GetUIntegerSafe() : 0,
                                                     outBytes ? outBytes->GetUIntegerSafe() : 0,
                                                     ms ? ms->GetUIntegerSafe() : 0,
                                                     tasks ? tasks->GetUIntegerSafe() : 0,
                                                     std::move(taskProfile)
        );

        TJsonValue children;
        if (plan->GetValue("Plans", &children)) {
            for (auto child: children.GetArray()) {
                planEntry->AddChild(std::move(ParsePlan(&child)));
            }
        }
        return planEntry;
    }


    static TVector<TTaskInfo> parseTasksProfile(TJsonValue *stats) {
        TJsonValue computeNodes;
        if (!stats || !stats->GetValue("ComputeNodes", &computeNodes)) {
            return {};
        }
        TVector<TTaskInfo> taskInfo;
        for (auto &node: computeNodes.GetArray()) {
            TJsonValue tasks;
            if (!node.GetValue("Tasks", &tasks)) {
                continue;
            }
            for (auto &task: tasks.GetArray()) {
                auto taskId = task.GetValueByPath("TaskId");
                if (!taskId) {
                    continue;
                }
                auto cpu = task.GetValueByPath("ComputeTimeUs");
                auto bytes = task.GetValueByPath("OutputBytes");

                auto startMs = task.GetValueByPath("FirstRowTimeMs");
                auto endMs = task.GetValueByPath("FinishTimeMs");
                ui64 duration = 0;
                if (startMs && endMs) {
                    duration = endMs->GetIntegerSafe() - startMs->GetUIntegerSafe();
                }
                TMap<EFlameGraphType, double> taskStats = {
                        {EFlameGraphType::CPU,          cpu ? cpu->GetDoubleRobust() : 0},
                        {EFlameGraphType::TIME,         duration},
                        {EFlameGraphType::BYTES_OUTPUT, bytes ? bytes->GetDoubleRobust() : 0}
                };


                taskInfo.push_back({.TaskId =  taskId->GetUIntegerSafe(), .TaskStats = taskStats});
            }
        }
        return taskInfo;
    }

private:
    TString ResultFile;

    TVector<THolder<TPlanGraphEntry>> PlanGraphs;
};


void GenerateFlameGraphSvg(const TString &resultFile, const TString &statReportJson) {
    TFlameGraphBuilder impl(resultFile);
    impl.ParsePlanJson(statReportJson);
    impl.GenerateSvg();
}

}
