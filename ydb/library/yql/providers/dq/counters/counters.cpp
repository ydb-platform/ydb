#include "counters.h"
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

namespace NYql {

void AddHistogram(THashMap<i64, ui64>& aggregatedHist, const THashMap<i64, ui64>& hist) {
    for (const auto& [k, v] : hist) {
        aggregatedHist[k] += v;
    }
}

TCounters AggregateQueryStatsByStage(TCounters& queryStat, const THashMap<ui64, ui64>& task2Stage) {
    TCounters aggregatedQueryStat;
    THashMap<TString, THashSet<ui64>> stage2Tasks;
    THashMap<TString, THashSet<ui64>> stage2Output;
    THashMap<TString, THashSet<ui64>> stage2Input;

    /* Depends on missing TotalTime statistics
    THashMap<ui64, i64> BusyTime;
    for (const auto& [k, v] : queryStat.Get()) {
        std::map<TString, TString> labels;
        TString prefix, name;
        if (NCommon::ParseCounterName(&prefix, &labels, &name, k)) {
            if (prefix == "TaskRunner") {
                auto maybeTask = labels.find("Task");
                if (maybeTask == labels.end()) {
                    continue;
                }
                ui64 taskId;
                if (!TryFromString(maybeTask->second, taskId)) {
                    continue;
                }
                if (name == "TotalTime") {
                    BusyTime[taskId] += v.Sum;
                } else if (name == "WaitTime") {
                    BusyTime[taskId] -= v.Sum;
                }
            }
        }
    }
    */
    /*
    for (const auto& [taskId, value] : BusyTime) {
        TCounters::TEntry entry = {value, value, value, value, 1};
        queryStat.AddCounter(queryStat.GetCounterName("TaskRunner", {{"Task", ToString(taskId)}}, "BusyTime"), entry);
    }
    */
    for (const auto& [k, v] : queryStat.Get()) {
        std::map<TString, TString> labels;
        TString prefix, name;
        if (k.StartsWith("TaskRunner") && NCommon::ParseCounterName(&prefix, &labels, &name, k)) {
            auto maybeTask = labels.find("Task");
            if (maybeTask == labels.end()) {
                aggregatedQueryStat.AddCounter(k, v);
                continue;
            }
            ui64 taskId;
            if (!TryFromString(maybeTask->second, taskId)) {
                continue;
            }
            auto maybeStage = task2Stage.find(taskId);
            TString stageId = maybeStage == task2Stage.end()
                ? "0"
                : ToString(maybeStage->second);
            ui64 channelId;
            bool input = false;
            bool output = false;
            auto maybeInputChannel = labels.find("InputChannel");
            if (maybeInputChannel != labels.end()) {
                if (!TryFromString(maybeInputChannel->second, channelId)) {
                    continue;
                }
                ui32 stage = 0;
                auto maybeSrcStageId = labels.find("SrcStageId");
                if (maybeSrcStageId != labels.end()) {
                    TryFromString(maybeSrcStageId->second, stage);
                    labels.erase(maybeSrcStageId);
                }
                stage2Input[stageId].insert(channelId);
                stage2Input["Total"].insert(channelId);
                labels.erase(maybeInputChannel);
                labels["Input"] = ToString(stage);
                input = true;
            }
            auto maybeOutputChannel = labels.find("OutputChannel");
            if (maybeOutputChannel != labels.end()) {
                if (!TryFromString(maybeOutputChannel->second, channelId)) {
                    continue;
                }
                ui32 stage = 0;
                auto maybeDstStageId = labels.find("DstStageId");
                if (maybeDstStageId != labels.end()) {
                    TryFromString(maybeDstStageId->second, stage);
                    labels.erase(maybeDstStageId);
                }
                stage2Output[stageId].insert(channelId);
                stage2Output["Total"].insert(channelId);
                labels.erase(maybeOutputChannel);
                labels["Output"] = ToString(stage);
                output = true;
            }
            labels.erase(maybeTask);
            labels["Stage"] = ToString(stageId);
            stage2Tasks[stageId].insert(taskId);
            stage2Tasks["Total"].insert(taskId);
            aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner", labels, name), v);
            if (input || output) {
                if (input) {
                    labels["Input"] = "Total";
                }
                if (output) {
                    labels["Output"] = "Total";
                }
                aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner", labels, name), v);
            }
            labels["Stage"] = "Total";
            aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner", labels, name), v);
        } else {
            aggregatedQueryStat.AddCounter(k, v);
        }
    }
    for (const auto& [k, v] : queryStat.GetHistograms()) {
        std::map<TString, TString> labels;
        TString prefix, name;
        if (k.StartsWith("TaskRunner") && NCommon::ParseCounterName(&prefix, &labels, &name, k)) {
            auto maybeTask = labels.find("Task");
            if (maybeTask == labels.end()) {
                AddHistogram(aggregatedQueryStat.GetHistogram(k), v);
                continue;
            }
            ui64 taskId;
            if (!TryFromString(maybeTask->second, taskId)) {
                continue;
            }
            auto maybeStage = task2Stage.find(taskId);
            TString stageId = maybeStage == task2Stage.end()
                ? "0"
                : ToString(maybeStage->second);
            labels.erase(maybeTask);
            labels["Stage"] = ToString(stageId);
            AddHistogram(aggregatedQueryStat.GetHistogram(queryStat.GetCounterName("TaskRunner", labels, name)), v);
        } else {
            AddHistogram(aggregatedQueryStat.GetHistogram(k), v);
        }
    }
    for (const auto& [stageId, v] : stage2Tasks) {
        aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner",
            {{"Stage", stageId}}, "TasksCount"), static_cast<ui64>(v.size()));
    }
    for (const auto& [stageId, v] : stage2Input) {
        aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner",
            {{"Stage", stageId},{"Input", "Total"}}, "ChannelsCount"), static_cast<ui64>(v.size()));
    }
    for (const auto& [stageId, v] : stage2Output) {
        aggregatedQueryStat.AddCounter(queryStat.GetCounterName("TaskRunner",
            {{"Stage", stageId},{"Output", "Total"}}, "ChannelsCount"), static_cast<ui64>(v.size()));
    }
    aggregatedQueryStat.AddCounter("StagesCount", static_cast<ui64>(stage2Tasks.size()));

    return aggregatedQueryStat;
}

} // namespace NYql
