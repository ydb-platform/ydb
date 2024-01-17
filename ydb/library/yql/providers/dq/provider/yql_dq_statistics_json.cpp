#include "yql_dq_statistics_json.h"

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

namespace {

struct TCountersByTask {
    THashMap<TString, TOperationStatistics> Inputs;
    THashMap<TString, TOperationStatistics> Outputs;
    THashMap<TString, TOperationStatistics> Sources;
    THashMap<TString, TOperationStatistics> Sinks;
    TOperationStatistics Generic;
};

void WriteEntries(NYson::TYsonWriter& writer, TStringBuf prefix, const THashMap<TString, TOperationStatistics>& data) {

    for (const auto& [k, v] : data) {
        writer.OnKeyedItem(prefix + k);
        NCommon::WriteStatistics(writer, v);
    }
}

} // namespace

void CollectTaskRunnerStatisticsByStage(NYson::TYsonWriter& writer, const TOperationStatistics& taskRunner, bool totalOnly)
{
    THashMap<TString, TOperationStatistics> taskRunnerStage;
    THashMap<TString, TOperationStatistics> taskRunnerInput;
    THashMap<TString, TOperationStatistics> taskRunnerOutput;
    THashMap<TString, TOperationStatistics> taskRunnerSource;
    THashMap<TString, TOperationStatistics> taskRunnerSink;

    for (const auto& entry : taskRunner.Entries) {
        TString prefix, name;
        std::map<TString, TString> labels;
        if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
            continue;
        }
        auto maybeInput = labels.find("Input");
        auto maybeOutput = labels.find("Output");
        auto maybeSource = labels.find("Source");
        auto maybeSink = labels.find("Sink");
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
        if (maybeSource != labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerSource[maybeStage->second].Entries.push_back(newEntry);
        }
        if (maybeSink != labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerSink[maybeStage->second].Entries.push_back(newEntry);
        }
        if (maybeInput == labels.end() && maybeOutput == labels.end() && maybeSource == labels.end() && maybeSink == labels.end()) {
            auto newEntry = entry; newEntry.Name = name;
            taskRunnerStage[maybeStage->second].Entries.push_back(newEntry);
        }
    }

    writer.OnBeginMap();
    for (const auto& [stageId, stat] : taskRunnerStage) {
        const auto& inputStat = taskRunnerInput[stageId];
        const auto& outputStat = taskRunnerOutput[stageId];
        const auto& sourceStat = taskRunnerSource[stageId];
        const auto& sinkStat = taskRunnerSink[stageId];

        writer.OnKeyedItem("Stage=" + stageId);
        {
            writer.OnBeginMap();

            writer.OnKeyedItem("Input");
            NCommon::WriteStatistics(writer, totalOnly, {{0, inputStat}});

            writer.OnKeyedItem("Output");
            NCommon::WriteStatistics(writer, totalOnly, {{0, outputStat}});

            writer.OnKeyedItem("Source");
            NCommon::WriteStatistics(writer, totalOnly, {{0, sourceStat}});

            writer.OnKeyedItem("Sink");
            NCommon::WriteStatistics(writer, totalOnly, {{0, sinkStat}});

            writer.OnKeyedItem("Task");
            NCommon::WriteStatistics(writer, totalOnly, {{0, stat}});

            writer.OnEndMap();
        }
    }

    writer.OnEndMap();
}

void CollectTaskRunnerStatisticsByTask(NYson::TYsonWriter& writer, const TOperationStatistics& taskRunner)
{
    THashMap<TString, THashMap<TString, TCountersByTask>> countersByStage;

    for (const auto& entry : taskRunner.Entries) {
        TString prefix, name;
        std::map<TString, TString> labels;
        if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
            continue;
        }
        auto maybeInput = labels.find("InputChannel");
        auto maybeOutput = labels.find("OutputChannel");
        auto maybeSource = labels.find("Source");
        auto maybeSink = labels.find("Sink");
        auto maybeTask = labels.find("Task");
        auto maybeStage = labels.find("Stage");

        if (maybeTask == labels.end()) {
            continue;
        }
        if (maybeStage == labels.end()) {
            continue;
        }

        auto newEntry = entry; newEntry.Name = name;
        auto& countersByTask = countersByStage[maybeStage->second];
        auto& counters = countersByTask[maybeTask->second];
        if (maybeInput != labels.end()) {
            counters.Inputs[maybeInput->second].Entries.emplace_back(newEntry);
        }
        if (maybeOutput != labels.end()) {
            counters.Outputs[maybeOutput->second].Entries.emplace_back(newEntry);
        }
        if (maybeSource != labels.end()) {
            counters.Sources[maybeSource->second].Entries.emplace_back(newEntry);
        }
        if (maybeSink != labels.end()) {
            counters.Sinks[maybeSink->second].Entries.emplace_back(newEntry);
        }
        if (maybeInput == labels.end() && maybeOutput == labels.end()) {
            counters.Generic.Entries.emplace_back(newEntry);
        }
    }

    writer.OnBeginMap();

    for (const auto& [stage, countersByTask] : countersByStage) {
        writer.OnKeyedItem("Stage=" + stage);
        writer.OnBeginMap();

        for (const auto& [task, counters] : countersByTask) {
            writer.OnKeyedItem("Task=" + task);

            writer.OnBeginMap();

            WriteEntries(writer, "Input=", counters.Inputs);
            WriteEntries(writer, "Output=", counters.Outputs);
            WriteEntries(writer, "Source=", counters.Sources);
            WriteEntries(writer, "Sink=", counters.Sinks);

            writer.OnKeyedItem("Generic");
            NCommon::WriteStatistics(writer, counters.Generic);

            writer.OnEndMap();
        }
        writer.OnEndMap();
    }

    writer.OnEndMap();
}

} // namespace NYql
