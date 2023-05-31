#include "yql_dq_statistics_json.h"

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

namespace {

struct TCountersByTask {
    THashMap<TString, TOperationStatistics> Inputs;
    THashMap<TString, TOperationStatistics> Outputs;
    TOperationStatistics Generic;
};

} // namespace

void CollectTaskRunnerStatisticsByStage(NYson::TYsonWriter& writer, const TOperationStatistics& taskRunner, bool totalOnly)
{
    THashMap<TString, TOperationStatistics> taskRunnerStage;
    THashMap<TString, TOperationStatistics> taskRunnerInput;
    THashMap<TString, TOperationStatistics> taskRunnerOutput;

    for (const auto& entry : taskRunner.Entries) {
        TString prefix, name;
        std::map<TString, TString> labels;
        if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
            continue;
        }
        auto maybeInput = labels.find("Input");
        auto maybeOutput = labels.find("Output");
        auto maybeStage = labels.find("Stage");
        if (maybeStage == labels.end()) {
            maybeStage = labels.find("Task");
        }
        if (maybeStage == labels.end()) {
            continue;
        }

        if (maybeInput != labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerInput[maybeStage->second].Entries.push_back(newEntry);
        }
        if (maybeOutput != labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerOutput[maybeStage->second].Entries.push_back(newEntry);
        }
        if (maybeInput == labels.end() && maybeOutput == labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerStage[maybeStage->second].Entries.push_back(newEntry);
        }
    }

    writer.OnBeginMap();
    for (const auto& [stageId, stat] : taskRunnerStage) {
        const auto& inputStat = taskRunnerInput[stageId];
        const auto& outputStat = taskRunnerOutput[stageId];

        writer.OnKeyedItem("Stage=" + stageId);
        {
            writer.OnBeginMap();

            writer.OnKeyedItem("Input");
            NCommon::WriteStatistics(writer, totalOnly, {{0, inputStat}});

            writer.OnKeyedItem("Output");
            NCommon::WriteStatistics(writer, totalOnly, {{0, outputStat}});

            writer.OnKeyedItem("Task");
            NCommon::WriteStatistics(writer, totalOnly, {{0, stat}});

            writer.OnEndMap();
        }
    }

    writer.OnEndMap();
}

void CollectTaskRunnerStatisticsByTask(NYson::TYsonWriter& writer, const TOperationStatistics& taskRunner)
{
    THashMap<TString, TCountersByTask> countersByTask;

    for (const auto& entry : taskRunner.Entries) {
        TString prefix, name;
        std::map<TString, TString> labels;
        if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
            continue;
        }
        auto maybeInput = labels.find("Input");
        auto maybeOutput = labels.find("Output");
        auto maybeTask = labels.find("Task");

        if (maybeTask == labels.end()) {
            continue;
        }

        auto newEntry = entry; newEntry.Name = name;
        auto& counters = countersByTask[maybeTask->second];
        if (maybeInput != labels.end()) {
            counters.Inputs[maybeInput->second].Entries.emplace_back(newEntry);
        }
        if (maybeOutput != labels.end()) {
            counters.Outputs[maybeOutput->second].Entries.emplace_back(newEntry);
        }
        if (maybeInput == labels.end() && maybeOutput == labels.end()) {
            counters.Generic.Entries.emplace_back(newEntry);
        }
    }

    writer.OnBeginMap();

    for (const auto& [task, counters] : countersByTask) {
        writer.OnKeyedItem("Task=" + task);

        writer.OnBeginMap();

        for (const auto& [input, stat] : counters.Inputs) {
            writer.OnKeyedItem("Input=" + input);
            NCommon::WriteStatistics(writer, stat);
        }

        for (const auto& [output, stat] : counters.Outputs) {
            writer.OnKeyedItem("Output=" + output);
            NCommon::WriteStatistics(writer, stat);
        }

        writer.OnKeyedItem("Generic");
        NCommon::WriteStatistics(writer, counters.Generic);

        writer.OnEndMap();
    }

    writer.OnEndMap();
}

} // namespace NYql
