#include "tasks_packer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NFq {

namespace NTasksPacker {

void Pack(TVector<NYql::NDqProto::TDqTask>& tasks, THashMap<i64, TString>& stagePrograms) {
    for (auto& task : tasks) {
        auto stageId = task.GetStageId();
        auto& p = stagePrograms[stageId];
        if (!p) {
            p = std::move(*task.MutableProgram()->MutableRaw());
            task.MutableProgram()->MutableRaw()->clear();
            continue;
        }
        if (p == task.GetProgram().GetRaw()) {
            task.MutableProgram()->MutableRaw()->clear();
        }
    }
}

void UnPack(TVector<NYql::NDqProto::TDqTask>& tasks, const THashMap<i64, TString>& stagePrograms) {
    for (auto& task : tasks) {
        if (task.GetProgram().GetRaw()) {
            continue;
        }
        auto stageId = task.GetStageId();
        auto it = stagePrograms.find(stageId);
        YQL_ENSURE(it != stagePrograms.end());
        *task.MutableProgram()->MutableRaw() = it->second;
    }
}

void UnPack(
    google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src,
    const google::protobuf::Map<i64, TString>& stagePrograms)
{
    if (stagePrograms.empty()) {
        dst = src;
        return;
    }
    TVector<NYql::NDqProto::TDqTask> tasks;
    tasks.reserve(src.size());
    for (const auto& srcTask : src) {
        if (srcTask.GetProgram().GetRaw()) {
            tasks.emplace_back(srcTask);
            continue;
        }
        auto stageId = srcTask.GetStageId();
        auto it = stagePrograms.find(stageId);
        YQL_ENSURE(it != stagePrograms.end());

        NYql::NDqProto::TDqTask task = srcTask;
        *task.MutableProgram()->MutableRaw() = it->second;
        tasks.emplace_back(task);
    }
    dst.Assign(tasks.begin(), tasks.end());
}

} // namespace NTasksPacker

} // namespace NFq
