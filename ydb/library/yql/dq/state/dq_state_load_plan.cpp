#include "dq_state_load_plan.h"

#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NYql::NDq {
namespace {
// Pq specific
// TODO: rewrite this code to not depend on concrete providers (now it is only pq)
struct TTopic {
    TString DatabaseId;
    TString Database;
    TString TopicPath;

    bool operator==(const TTopic& t) const {
        return DatabaseId == t.DatabaseId && Database == t.Database && TopicPath == t.TopicPath;
    }
};

struct TTopicHash {
    size_t operator()(const TTopic& t) const {
        return MultiHash(t.DatabaseId, t.Database, t.TopicPath);
    }
};

struct TTaskSource {
    ui64 TaskId = 0;
    ui64 InputIndex = 0;

    bool operator==(const TTaskSource& t) const {
        return TaskId == t.TaskId && InputIndex == t.InputIndex;
    }
};

struct TTaskSourceHash {
    size_t operator()(const TTaskSource& t) const {
        return THash<std::tuple<ui64, ui64>>()(std::tie(t.TaskId, t.InputIndex));
    }
};

using TPartitionsMapping = THashMultiMap<ui64, TTaskSource>; // Task can have multiple sources for one partition, so multimap.

struct TTopicMappingInfo {
    TPartitionsMapping PartitionsMapping;
    bool Used = false;
};

using TTopicsMapping = THashMap<TTopic, TTopicMappingInfo, TTopicHash>;

// Error in case of normal mode and warning if force one is on.
#define ISSUE(stream)                                                   \
    AddForceWarningOrError(TStringBuilder() << stream, issues, force);  \
    if (!force) {                                                       \
        result = false;                                                 \
    }                                                                   \
    /**/

void AddForceWarningOrError(const TString& message, TIssues& issues, bool force) {
    TIssue issue(message);
    if (force) {
        issue.SetCode(TIssuesIds::WARNING, TSeverityIds::S_WARNING);
    }
    issues.AddIssue(std::move(issue));
}

bool IsTopicInput(const NYql::NDqProto::TTaskInput& taskInput) {
    return taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource && taskInput.GetSource().GetType() == "PqSource";
}

bool ParseTopicInput(
    const NYql::NDqProto::TDqTask& task,
    const NYql::NDqProto::TTaskInput& taskInput,
    ui64 inputIndex,
    bool force,
    bool isSourceGraph,
    NYql::NPq::NProto::TDqPqTopicSource& srcDesc,
    NPq::TTopicPartitionsSet& partitionsSet,
    TIssues& issues)
{
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
    bool result = true;
#pragma clang diagnostic pop
    const char* queryKindStr = isSourceGraph ? "source" : "destination";
    const google::protobuf::Any& settingsAny = taskInput.GetSource().GetSettings();
    if (!settingsAny.Is<NYql::NPq::NProto::TDqPqTopicSource>()) {
        ISSUE("Can't read " << queryKindStr << " query params: input " << inputIndex << " of task " << task.GetId() << " has incorrect type");
        return false;
    }
    if (!settingsAny.UnpackTo(&srcDesc)) {
        ISSUE("Can't read " << queryKindStr << " query params: failed to unpack input " << inputIndex << " of task " << task.GetId());
        return false;
    }

    const TMaybe<NPq::TTopicPartitionsSet> foundPartitionsSet = NPq::GetTopicPartitionsSet(task.GetMeta());
    if (!foundPartitionsSet) {
        ISSUE("Can't read " << queryKindStr << " query params: failed to load partitions of topic `" << srcDesc.GetTopicPath() << "` from input " << inputIndex << " of task " << task.GetId());
        return false;
    }
    partitionsSet = *foundPartitionsSet;
    return true;
}

void AddToMapping(
    const NYql::NPq::NProto::TDqPqTopicSource& srcDesc,
    const NPq::TTopicPartitionsSet& partitionsSet,
    ui64 taskId,
    ui64 inputIndex,
    TTopicsMapping& mapping)
{
    TTopicMappingInfo& info = mapping[TTopic{srcDesc.GetDatabaseId(), srcDesc.GetDatabase(), srcDesc.GetTopicPath()}];
    ui64 currentPartition = partitionsSet.EachTopicPartitionGroupId;
    do {
        info.PartitionsMapping.emplace(currentPartition, TTaskSource{taskId, inputIndex});
        currentPartition += partitionsSet.DqPartitionsCount;
    } while (currentPartition < partitionsSet.TopicPartitionsCount);
}

void InitForeignPlan(const NYql::NDqProto::TDqTask& task, NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan) {
    taskPlan.SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
    taskPlan.MutableProgram()->SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
    for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
        const NYql::NDqProto::TTaskInput& taskInput = task.GetInputs(inputIndex);
        if (taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource) {
            NDqProto::NDqStateLoadPlan::TSourcePlan& sourcePlan = *taskPlan.AddSources();
            sourcePlan.SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            sourcePlan.SetInputIndex(inputIndex);
        }
    }
    for (ui64 outputIndex = 0; outputIndex < task.OutputsSize(); ++outputIndex) {
        const NYql::NDqProto::TTaskOutput& taskOutput = task.GetOutputs(outputIndex);
        if (taskOutput.GetTypeCase() == NYql::NDqProto::TTaskOutput::kSink) {
            NDqProto::NDqStateLoadPlan::TSinkPlan& sinkPlan = *taskPlan.AddSinks();
            sinkPlan.SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            sinkPlan.SetOutputIndex(outputIndex);
        }
    }
}

NDqProto::NDqStateLoadPlan::TSourcePlan& FindSourcePlan(NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan, ui64 inputIndex) {
    for (NDqProto::NDqStateLoadPlan::TSourcePlan& plan : *taskPlan.MutableSources()) {
        if (plan.GetInputIndex() == inputIndex) {
            return plan;
        }
    }
    Y_ABORT("Source plan for input index %lu was not found", inputIndex);
}

} // namespace

bool MakeContinueFromStreamingOffsetsPlan(
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst,
    const bool force,
    THashMap<ui64, NDqProto::NDqStateLoadPlan::TTaskPlan>& plan,
    TIssues& issues)
{
#define FORCE_MSG(msg) (force ? ". " msg : ". Use force mode to ignore this issue")

    bool result = true;
    // Build src mapping
    TTopicsMapping srcMapping;
    for (const NYql::NDqProto::TDqTask& task : src) {
        for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
            const NYql::NDqProto::TTaskInput& taskInput = task.GetInputs(inputIndex);
            if (IsTopicInput(taskInput)) {
                NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                NPq::TTopicPartitionsSet partitionsSet;
                if (!ParseTopicInput(task, taskInput, inputIndex, force, true, srcDesc, partitionsSet, issues)) {
                    if (!force) {
                        result = false;
                    }
                    continue;
                }

                AddToMapping(srcDesc, partitionsSet, task.GetId(), inputIndex, srcMapping);
            }
        }
    }

    // Watch dst query and build plan
    for (const NYql::NDqProto::TDqTask& task : dst) {
        NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan = plan[task.GetId()];
        taskPlan.SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY); // default if no topic sources
        bool foreignStatePlanInited = false;
        for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
            const NYql::NDqProto::TTaskInput& taskInput = task.GetInputs(inputIndex);
            if (IsTopicInput(taskInput)) {
                NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                NPq::TTopicPartitionsSet partitionsSet;
                if (!ParseTopicInput(task, taskInput, inputIndex, force, false, srcDesc, partitionsSet, issues)) {
                    if (!force) {
                        result = false;
                    }
                    continue;
                }
                const auto mappingInfoIt = srcMapping.find(TTopic{srcDesc.GetDatabaseId(), srcDesc.GetDatabase(), srcDesc.GetTopicPath()});
                if (mappingInfoIt == srcMapping.end()) {
                    ISSUE("Topic `" << srcDesc.GetTopicPath() << "` is not found in previous query" << FORCE_MSG("Query will use fresh offsets for its partitions"));
                    continue;
                }
                TTopicMappingInfo& mappingInfo = mappingInfoIt->second;
                mappingInfo.Used = true;

                THashSet<TTaskSource, TTaskSourceHash> tasksSet;

                // Process all partitions
                ui64 currentPartition = partitionsSet.EachTopicPartitionGroupId;
                do {
                    auto [taskBegin, taskEnd] = mappingInfo.PartitionsMapping.equal_range(currentPartition);
                    if (taskBegin == taskEnd) {
                        ISSUE("Topic `" << srcDesc.GetTopicPath() << "` partition " << currentPartition << " is not found in previous query" << FORCE_MSG("Query will use fresh offsets for it"));
                    } else {
                        if (std::distance(taskBegin, taskEnd) > 1) {
                            ISSUE("Topic `" << srcDesc.GetTopicPath() << "` partition " << currentPartition << " has ambiguous offsets source in previous query checkpoint" << FORCE_MSG("Query will use minimum offset to avoid skipping data"));
                        }
                        for (; taskBegin != taskEnd; ++taskBegin) {
                            tasksSet.insert(taskBegin->second);
                        }
                    }
                    currentPartition += partitionsSet.DqPartitionsCount;
                } while (currentPartition < partitionsSet.TopicPartitionsCount);

                if (!tasksSet.empty()) {
                    if (!foreignStatePlanInited) {
                        foreignStatePlanInited = true;
                        InitForeignPlan(task, taskPlan);
                    }
                    NDqProto::NDqStateLoadPlan::TSourcePlan& sourcePlan = FindSourcePlan(taskPlan, inputIndex);
                    sourcePlan.SetStateType(NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
                    for (const TTaskSource& taskSource : tasksSet) {
                        NDqProto::NDqStateLoadPlan::TSourcePlan::TForeignTaskSource& taskSourceProto = *sourcePlan.AddForeignTasksSources();
                        taskSourceProto.SetTaskId(taskSource.TaskId);
                        taskSourceProto.SetInputIndex(taskSource.InputIndex);
                    }
                }
            }
        }
    }
    for (const auto& [topic, mappingInfo] : srcMapping) {
        if (!mappingInfo.Used) {
            ISSUE("Topic `" << topic.TopicPath << "` is read in previous query but is not read in new query" << FORCE_MSG("Reading offsets will be lost in next checkpoint"));
        }
    }
    return result;

#undef FORCE_MSG
}

} // namespace NYql::NDq
