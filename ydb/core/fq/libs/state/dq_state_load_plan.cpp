#include "dq_stage_state_recovery_info.h"
#include "dq_state_load_plan_impl.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/public/issue/protos/issue_id.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace NFq {

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

void AddForceWarningOrError(const TString& message, NYql::TIssues& issues, bool force) {
    NYql::TIssue issue(message);
    if (force) {
        issue.SetCode(NYql::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING);
    }
    issues.AddIssue(std::move(issue));
}

bool IsTopicInput(const NYql::NDqProto::TTaskInput& taskInput) {
    return taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource && taskInput.GetSource().GetType() == NYql::PqSource;
}

bool ParseTopicInput(
    const NYql::NDqProto::TDqTask& task,
    const NYql::NDqProto::TTaskInput& taskInput,
    ui64 inputIndex,
    bool force,
    bool isSourceGraph,
    NYql::NPq::NProto::TDqPqTopicSource& srcDesc,
    std::vector<NYql::NPq::TTopicPartitionsSet>& partitionsSets,
    NYql::TIssues& issues)
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

    partitionsSets = NYql::NPq::GetTopicPartitionsSets(task);
    if (partitionsSets.empty()) {
        ISSUE("Can't read " << queryKindStr << " query params: failed to load partitions of topic `" << srcDesc.GetTopicPath() << "` from input " << inputIndex << " of task " << task.GetId());
        return false;
    }

    return true;
}

void AddToMapping(
    const NYql::NPq::NProto::TDqPqTopicSource& srcDesc,
    const std::vector<NYql::NPq::TTopicPartitionsSet>& partitionsSets,
    ui64 taskId,
    ui64 inputIndex,
    TTopicsMapping& mapping)
{
    TTopicMappingInfo& info = mapping[TTopic{srcDesc.GetDatabaseId(), srcDesc.GetDatabase(), srcDesc.GetTopicPath()}];
    for (const auto& partitionsSet : partitionsSets) {
        ui64 currentPartition = partitionsSet.EachTopicPartitionGroupId;
        do {
            info.PartitionsMapping.emplace(currentPartition, TTaskSource{taskId, inputIndex});
            currentPartition += partitionsSet.DqPartitionsCount;
        } while (currentPartition < partitionsSet.TopicPartitionsCount);
    }
}

void InitForeignPlan(const NYql::NDqProto::TDqTask& task, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan) {
    taskPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
    taskPlan.MutableProgram()->SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
    for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
        const NYql::NDqProto::TTaskInput& taskInput = task.GetInputs(inputIndex);
        if (taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource) {
            NYql::NDqProto::NDqStateLoadPlan::TSourcePlan& sourcePlan = *taskPlan.AddSources();
            sourcePlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            sourcePlan.SetInputIndex(inputIndex);
        }
    }
    for (ui64 outputIndex = 0; outputIndex < task.OutputsSize(); ++outputIndex) {
        const NYql::NDqProto::TTaskOutput& taskOutput = task.GetOutputs(outputIndex);
        if (taskOutput.GetTypeCase() == NYql::NDqProto::TTaskOutput::kSink) {
            NYql::NDqProto::NDqStateLoadPlan::TSinkPlan& sinkPlan = *taskPlan.AddSinks();
            sinkPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
            sinkPlan.SetOutputIndex(outputIndex);
        }
    }
}

NYql::NDqProto::NDqStateLoadPlan::TSourcePlan& FindSourcePlan(NYql::NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan, ui64 inputIndex) {
    for (NYql::NDqProto::NDqStateLoadPlan::TSourcePlan& plan : *taskPlan.MutableSources()) {
        if (plan.GetInputIndex() == inputIndex) {
            return plan;
        }
    }
    Y_ABORT("Source plan for input index %lu was not found", inputIndex);
}

} // anonymous namespace

bool MakeContinueFromStreamingOffsetsPlan(
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst,
    const bool force,
    THashMap<ui64, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan>& plan,
    NYql::TIssues& issues)
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
                std::vector<NYql::NPq::TTopicPartitionsSet> partitionsSets;
                if (!ParseTopicInput(task, taskInput, inputIndex, force, true, srcDesc, partitionsSets, issues)) {
                    if (!force) {
                        result = false;
                    }
                    continue;
                }

                AddToMapping(srcDesc, partitionsSets, task.GetId(), inputIndex, srcMapping);
            }
        }
    }

    // Watch dst query and build plan
    for (const NYql::NDqProto::TDqTask& task : dst) {
        NYql::NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan = plan[task.GetId()];
        taskPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY); // default if no topic sources
        bool foreignStatePlanInited = false;
        for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
            const NYql::NDqProto::TTaskInput& taskInput = task.GetInputs(inputIndex);
            if (IsTopicInput(taskInput)) {
                NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                std::vector<NYql::NPq::TTopicPartitionsSet> partitionsSets;
                if (!ParseTopicInput(task, taskInput, inputIndex, force, false, srcDesc, partitionsSets, issues)) {
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
                for (const auto& partitionsSet : partitionsSets) {
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
                }

                if (!tasksSet.empty()) {
                    if (!foreignStatePlanInited) {
                        foreignStatePlanInited = true;
                        InitForeignPlan(task, taskPlan);
                    }
                    NYql::NDqProto::NDqStateLoadPlan::TSourcePlan& sourcePlan = FindSourcePlan(taskPlan, inputIndex);
                    sourcePlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
                    for (const TTaskSource& taskSource : tasksSet) {
                        NYql::NDqProto::NDqStateLoadPlan::TSourcePlan::TForeignTaskSource& taskSourceProto = *sourcePlan.AddForeignTasksSources();
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

namespace {

class TReplayGraph {
    static constexpr ui64 WATERMARK_GENERATOR_EARLY_LIMIT = TDuration::Minutes(5).MicroSeconds();

public:
    struct TPartition {
        TTopic Topic;
        TString Endpoint;
        TString Cluster;
        ui64 Id = 0;
        std::optional<ui64> Offset = std::nullopt;

        bool operator==(const TPartition& other) const {
            return Topic == other.Topic && Endpoint == other.Endpoint && Cluster == other.Cluster && Id == other.Id;
        }

        struct THash {
            size_t operator()(const TPartition& partition) const {
                return MultiHash(TTopicHash()(partition.Topic), partition.Endpoint, partition.Cluster, partition.Id);
            }
        };
    };

    struct TPartitionProgress {
        // Information from last query checkpoint, not set in case of restoring without previous checkpoint
        std::optional<ui64> StartingMessageTimestampMs;
        std::optional<ui64> Offset; // Set only after partition session start

        // Minimal event time boundary for stateful operators replay, not set if there is no stateful dependencies for this partition
        std::optional<ui64> EventTime;

        // Priority order:
        // 1. EventTime
        // 2. Offset
        // 3. StartingMessageTimestampMs
    };

    using TProgress = THashMap<TReplayGraph::TPartition, TPartitionProgress, TReplayGraph::TPartition::THash>;

private:
    struct TSource {
        ui64 InputIndex = 0;
        NYql::NPq::NProto::TDqPqTopicSource Description;
        TVector<TPartition> Partitions;
        std::optional<ui64> StartingMessageTimestampMs;
    };

    class TTask {
        YDB_READONLY_OPT(ui64, InputBound); // Measured in event time us

    public:
        const NYql::NDqProto::TDqTask* Task = nullptr;
        TStageStateRecoveryInfo Info;
        TVector<ui64> Parents;
        TVector<TSource> Sources;
        bool WatermarkAvailable = false; // All dependent source stages have a watermark generator.

        void SetInputBound(const ui64 bound) {
            InputBound = std::min(InputBound.value_or(bound), bound);
        }

        void SetOutputBound(const ui64 bound) {
            SetInputBound(Info.InputStartForOutput(bound));
        }
    };

public:
    explicit TReplayGraph(const NProto::TGraphParams& graph) {
        TStageStateRecoveryContext context;

        THashMap<ui32, TStageStateRecoveryInfo> stages;
        stages.reserve(graph.GetStageProgram().size());
        Tasks.reserve(graph.GetTasks().size());
        for (const auto& task : graph.GetTasks()) {
            if (NYql::NDq::GetTaskCheckpointingMode(task) == NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                continue;
            }

            auto [it, inserted] = Tasks.emplace(task.GetId(), TTask{});
            Y_VALIDATE(inserted, "Duplicate task ID in history replay graph");

            auto& info = it->second;
            info.Task = &task;

            const auto* program = &task.GetProgram().GetRaw();
            if (program->empty()) {
                const auto stage = graph.GetStageProgram().find(task.GetStageId());
                YQL_ENSURE(stage != graph.GetStageProgram().end(), "Missing program for stage " << task.GetStageId());
                program = &stage->second;
            }
            info.Info = stages.try_emplace(task.GetStageId(), task.GetProgram().GetRuntimeVersion(), *program, context).first->second;

            for (ui64 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
                if (const auto& input = task.GetInputs(inputIndex); input.HasSource()) {
                    if (!NYql::NDq::IsInfiniteSourceType(input.GetSource().GetType())) {
                        continue;
                    }
                    YQL_ENSURE(IsTopicInput(input), "History replay requires topic inputs");

                    TSource source{.InputIndex = inputIndex};
                    std::vector<NYql::NPq::TTopicPartitionsSet> partitionSets;
                    NYql::TIssues issues;
                    Y_VALIDATE(ParseTopicInput(task, input, inputIndex, /* force */ false, /* isSourceGraph */ true, source.Description, partitionSets, issues), "Invalid topic input: " << issues.ToOneLineString());

                    if (source.Description.GetFederatedClusters().empty()) {
                        AddPartitions({}, source.Description.GetEndpoint(), source.Description.GetDatabase(), /* partitionsCount */ 0, partitionSets, source);
                    } else {
                        THashSet<TString> clusters;
                        for (const auto& cluster : source.Description.GetFederatedClusters()) {
                            Y_VALIDATE(clusters.insert(cluster.GetName()).second, "Duplicate federated topic cluster " << cluster.GetName());
                            AddPartitions(
                                cluster.GetName(),
                                cluster.GetName().empty() ? source.Description.GetEndpoint() : cluster.GetEndpoint(),
                                cluster.GetName().empty() ? source.Description.GetDatabase() : cluster.GetDatabase(),
                                cluster.GetPartitionsCount(),
                                partitionSets,
                                source
                            );
                        }
                    }

                    info.Sources.push_back(std::move(source));
                } else {
                    for (const auto& channel : input.GetChannels()) {
                        if (channel.GetCheckpointingMode() != NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                            YQL_ENSURE(input.HasUnionAll(), "History replay requires union-all channels");
                            info.Parents.push_back(channel.GetSrcTaskId());
                        }
                    }
                }
            }
        }

        ValidateGraph();
    }

    void PropagateExplicitOutputBound(const ui64 bound) {
        for (auto& [taskId, task] : Tasks) {
            for (const auto& output : task.Task->GetOutputs()) {
                if (output.HasSink() || output.HasEffects()) {
                    task.SetOutputBound(bound);
                    break;
                }
            }
        }

        PropagateBounds();
    }

    // Progress transfering between graphs
    TProgress ReadReplayProgress(const TCheckpointTaskStates& states) {
        PropagateCheckpointedBounds(states);

        TProgress result;
        for (const auto& [taskId, task] : Tasks) {
            const auto* state = states.FindPtr(taskId);
            YQL_ENSURE(state && state->MiniKqlProgram, "Missing checkpoint state for task " << taskId);

            THashMap<ui64, const NYql::NDq::TSourceState*> stateSourcesMap;
            stateSourcesMap.reserve(state->Sources.size());
            for (const auto& source : state->Sources) {
                YQL_ENSURE(stateSourcesMap.emplace(source.InputIndex, &source).second, "Duplicate source input in checkpoint");
            }

            for (const auto& source : task.Sources) {
                const auto sourceIt = stateSourcesMap.find(source.InputIndex);
                YQL_ENSURE(sourceIt != stateSourcesMap.end() && sourceIt->second, "Missing topic source checkpoint for history replay");

                std::optional<ui64> StartingMessageTimestampMs;
                THashMap<std::pair<TString, ui64>, ui64> partitions; // (cluster, partition) -> offset
                for (const auto& data : sourceIt->second->Data) {
                    NYql::NPq::NProto::TDqPqTopicSourceState saved;
                    YQL_ENSURE(data.Version == 1, "Unsupported topic checkpoint version");
                    YQL_ENSURE(saved.ParseFromString(data.Blob), "Invalid topic checkpoint");

                    const auto startTs = saved.GetStartingMessageTimestampMs();
                    StartingMessageTimestampMs = std::min(StartingMessageTimestampMs.value_or(startTs), startTs);

                    for (const auto& partition : saved.GetPartitions()) {
                        YQL_ENSURE(partitions.emplace(std::make_pair(partition.GetCluster(), partition.GetPartition()), partition.GetOffset()).second, "Ambiguous topic partition checkpoint");
                    }
                }

                YQL_ENSURE(StartingMessageTimestampMs, "Missing topic checkpoint data");

                for (const auto& partition : source.Partitions) {
                    const auto* progress = partitions.FindPtr(std::make_pair(partition.Cluster, partition.Id));
                    YQL_ENSURE(result.emplace(partition, TPartitionProgress{
                        .StartingMessageTimestampMs = StartingMessageTimestampMs,
                        .Offset = progress ? std::optional(*progress) : std::nullopt,
                        .EventTime = task.GetInputBoundOptional(),
                    }).second, "History replay requires unique inputs in the previous query");
                }
            }
        }

        return result;
    }

    // Forward propagation of replay progress time from another graph
    void ApplyReplayProgress(const TProgress& progress) {
        THashSet<TPartition, TPartition::THash> used;
        THashMap<ui64, ui64> boundaries; // Boundary for task = minimal boundary across restored parent
        boundaries.reserve(Tasks.size());
        for (ui64 taskId : TaskOrder) {
            auto& task = Tasks.at(taskId);

            std::optional<ui64> boundary;
            for (auto& source : task.Sources) {
                std::optional<ui64> startingMessageTimestampMs;

                for (auto& partition : source.Partitions) {
                    const auto* partitionProgress = progress.FindPtr(partition);
                    YQL_ENSURE(partitionProgress, "Input partition is absent from the previous query");
                    YQL_ENSURE(used.emplace(partition).second, "History replay requires unique inputs in the new query");

                    partition.Offset = partitionProgress->Offset;

                    if (const auto partitionTimestamp = partitionProgress->StartingMessageTimestampMs) {
                        startingMessageTimestampMs = std::min(startingMessageTimestampMs.value_or(*partitionTimestamp), *partitionTimestamp);
                    }

                    if (partitionProgress->EventTime) {
                        const auto eventTime = *partitionProgress->EventTime;
                        boundary = std::min(boundary.value_or(eventTime), eventTime);
                    }
                }

                source.StartingMessageTimestampMs = startingMessageTimestampMs;
            }

            for (ui64 parentId : task.Parents) {
                if (const auto* parentBoundary = boundaries.FindPtr(parentId)) {
                    boundary = std::min(boundary.value_or(*parentBoundary), *parentBoundary);
                }
            }

            if (boundary) {
                boundaries.emplace(taskId, *boundary);

                for (const auto& output : task.Task->GetOutputs()) {
                    if (output.HasSink() || output.HasEffects()) {
                        task.SetOutputBound(*boundary);
                        break;
                    }
                }
            }
        }

        YQL_ENSURE(used.size() == progress.size(), "History replay requires the same input partitions");
        PropagateBounds();
    }

    TStateLoadPlan BuildReplayTaskPlans(const bool useSourceDisposition, const bool fromCheckpoint) const {
        using namespace NKikimr::NMiniKQL;
        using namespace NYql::NDqProto::NDqStateLoadPlan;

        TStateLoadPlan plan;
        for (const auto& [taskId, task] : Tasks) {
            auto& taskPlan = plan[taskId];
            taskPlan.SetStateType(STATE_TYPE_FOREIGN);

            if (auto& programPlan = *taskPlan.MutableProgram(); task.Info.Hopping && task.HasInputBound()) {
                const auto hopTimeUs = task.Info.Hopping->HopTimeUs;
                const auto inputBound = task.GetInputBoundUnsafe();
                Y_VALIDATE(inputBound % hopTimeUs == 0, "Unaligned hopping recovery time");

                TString state;
                TNodeStateHelper::AddNodeState(state, THoppingRecoveryState::MakeRecoveryState(inputBound / hopTimeUs));
                programPlan.SetState(std::move(state));
                programPlan.SetStateType(STATE_TYPE_FOREIGN);
            } else {
                programPlan.SetStateType(STATE_TYPE_EMPTY);
            }

            for (const auto& source : task.Sources) {
                auto& sourcePlan = *taskPlan.AddSources();
                sourcePlan.SetInputIndex(source.InputIndex);

                if (useSourceDisposition) {
                    sourcePlan.SetStateType(STATE_TYPE_EMPTY);
                    continue;
                }

                sourcePlan.SetStateType(STATE_TYPE_FOREIGN);
                sourcePlan.SetStateVersion(1);

                NYql::NPq::NProto::TDqPqTopicSourceState state;
                auto& topic = *state.AddTopics();
                topic.SetDatabaseId(source.Description.GetDatabaseId());
                topic.SetDatabase(source.Description.GetDatabase());
                topic.SetTopicPath(source.Description.GetTopicPath());
                topic.SetEndpoint(source.Description.GetEndpoint());

                if (task.HasInputBound()) {
                    const auto inputBound = task.GetInputBoundUnsafe();
                    // Checkpoint bounds originate in the old graph's event time,
                    // even when the new graph has removed its watermark generator.
                    const auto earlyLimit = fromCheckpoint || task.WatermarkAvailable ? WATERMARK_GENERATOR_EARLY_LIMIT : 0;
                    YQL_ENSURE(inputBound >= earlyLimit, "History replay time underflow: required input precedes timestamp zero");
                    state.SetStartingMessageTimestampMs((inputBound - earlyLimit) / 1000);
                } else {
                    Y_VALIDATE(source.StartingMessageTimestampMs, "Missing starting message timestamp and has no input bound");
                    state.SetStartingMessageTimestampMs(*source.StartingMessageTimestampMs);

                    for (const auto& partition : source.Partitions) {
                        if (partition.Offset) {
                            auto& saved = *state.AddPartitions();
                            saved.SetPartition(partition.Id);
                            saved.SetCluster(partition.Cluster);
                            saved.SetOffset(*partition.Offset);
                        }
                    }
                }

                sourcePlan.SetState(state.SerializeAsString());
            }
        }

        return plan;
    }

private:
    static void AddPartitions(const TString& cluster, const TString& endpoint, const TString& database, const ui64 partitionsCount, const std::vector<NYql::NPq::TTopicPartitionsSet>& partitionSets, TSource& source) {
        const TTopic topic{source.Description.GetDatabaseId(), database, source.Description.GetTopicPath()};
        for (const auto& set : partitionSets) {
            Y_VALIDATE(set.DqPartitionsCount, "Invalid topic partition mapping");

            const auto count = partitionsCount ? partitionsCount : set.TopicPartitionsCount;
            for (ui64 p = set.EachTopicPartitionGroupId; p < count; p += set.DqPartitionsCount) {
                source.Partitions.push_back({topic, endpoint, cluster, p});
            }
        }
    };

    // Backward propagation of saved boundaries in checkpoint for stateful operators
    void PropagateCheckpointedBounds(const TCheckpointTaskStates& states) {
        for (auto& [taskId, task] : Tasks) {
            const auto* state = states.FindPtr(taskId);
            YQL_ENSURE(state && state->MiniKqlProgram, "Missing checkpoint state for task " << taskId);

            for (const auto& sink : state->Sinks) {
                YQL_ENSURE(sink.OutputIndex < task.Task->OutputsSize(), "Invalid sink checkpoint output index");
                const auto& output = task.Task->GetOutputs(sink.OutputIndex);

                if (output.HasSink() && output.GetSink().GetType() == "PqSink") {
                    NYql::NPq::NProto::TDqPqTopicSink settings;
                    Y_VALIDATE(output.GetSink().GetSettings().UnpackTo(&settings), "Invalid PQ sink settings for history replay");
                    YQL_ENSURE(!settings.GetEnableDeduplication(), "History replay does not support PQ sinks with deduplication: producer sequence numbers cannot be replayed");

                    NYql::NPq::NProto::TDqPqTopicSinkState saved;
                    YQL_ENSURE(sink.Data.Version == 1 && saved.ParseFromString(sink.Data.Blob), "Invalid PQ sink checkpoint for history replay");
                    YQL_ENSURE(!saved.GetDeferredPublicationIntId(), "History replay does not support PQ sinks with exactly-once delivery: deferred publications cannot be replayed");
                    continue;
                }

                YQL_ENSURE(sink.Data.Blob.empty() && !sink.Data.Version, "A sink has state and does not support history replay");
            }

            TStringBuf programState(state->MiniKqlProgram->Data.Blob);
            if (!task.Info.Hopping) {
                YQL_ENSURE(programState.empty(), "Checkpoint contains state outside hopping operators that cannot be replayed");
                continue;
            }

            const auto size = NKikimr::NMiniKQL::ReadUi64(programState);
            if (size == std::numeric_limits<ui64>::max()) {
                YQL_ENSURE(programState.empty(), "Hopping checkpoint has additional operator state that cannot be replayed");
                continue;
            }

            YQL_ENSURE(size == programState.size(), "Hopping checkpoint has additional operator state that cannot be replayed");
            const auto info = THoppingRecoveryState::Read(programState);
            const auto hopTimeUs = task.Info.Hopping->HopTimeUs;

            if (const auto minWindowStartIndex = info.GetMinWindowStartIndex()) {
                YQL_ENSURE(info.GetMinWindowStartIndex() <= Max<ui64>() / hopTimeUs, "Hopping recovery time overflow");
                task.SetInputBound(minWindowStartIndex * hopTimeUs);
            } else {
                YQL_ENSURE(!info.GetKeysCount(), "Cannot determine hopping recovery input bound, there was no successful watermarks for task " << taskId);
            }
        }

        PropagateBounds();
    }

    // Backward propagation of InputBound in TTasks graph
    void PropagateBounds() {
        for (auto it = TaskOrder.rbegin(); it != TaskOrder.rend(); ++it) {
            const auto& task = Get(*it);
            if (!task.HasInputBound()) {
                continue;
            }

            const auto inputBound = task.GetInputBoundUnsafe();
            for (ui64 parentId : task.Parents) {
                Get(parentId).SetOutputBound(inputBound);
            }
        }
    }

    TTask& Get(ui64 taskId) {
        const auto it = Tasks.find(taskId);
        Y_VALIDATE(it != Tasks.end(), "Missing task " << taskId << " in history replay graph");
        return it->second;
    }

    // Build topological tasks order, validate and propagate watermark availability
    void ValidateGraph() {
        struct TDependencies {
            size_t PendingParents = 0;
            TVector<ui64> Children;
        };

        THashMap<ui64, TDependencies> dependencies;
        dependencies.reserve(Tasks.size());
        TaskOrder.reserve(Tasks.size());
        for (const auto& [taskId, task] : Tasks) {
            dependencies[taskId].PendingParents = task.Parents.size();

            if (task.Parents.empty()) {
                TaskOrder.push_back(taskId);
            }

            for (ui64 parentId : task.Parents) {
                dependencies[parentId].Children.push_back(taskId);
            }
        }

        for (size_t index = 0; index < TaskOrder.size(); ++index) {
            const auto taskId = TaskOrder[index];
            auto& task = Get(taskId);

            YQL_ENSURE(!task.Parents.empty() || std::any_of(task.Sources.begin(), task.Sources.end(), [](const auto& source) {
                return !source.Partitions.empty();
            }), "History replay requires a topic on every input path");

            task.WatermarkAvailable = task.Parents.empty() || std::all_of(task.Parents.begin(), task.Parents.end(), [&](ui64 parentId) {
                return Get(parentId).WatermarkAvailable;
            });
            if (!task.Sources.empty()) {
                task.WatermarkAvailable &= task.Info.HasWatermarkGenerator;
            }

            YQL_ENSURE(!task.Info.Hopping || task.WatermarkAvailable, "History replay requires a watermark generator before each hopping operator");

            for (ui64 childId : dependencies.at(taskId).Children) {
                if (--dependencies.at(childId).PendingParents == 0) {
                    TaskOrder.push_back(childId);
                }
            }
        }

        Y_VALIDATE(TaskOrder.size() == Tasks.size(), "Cycle in history replay graph");
    }

    THashMap<ui64, TTask> Tasks;
    TVector<ui64> TaskOrder; // Tasks graph topological order
};

} // anonymous namespace

bool MakeHistoryReplayPlan(
    const NProto::TGraphParams& src,
    const NProto::TGraphParams& dst,
    const TCheckpointTaskStates& states, TStateLoadPlan& plan, NYql::TIssues& issues)
{
    try {
        YQL_ENSURE(!src.GetTasks().empty() && !dst.GetTasks().empty(), "History replay requires both query graphs");

        TReplayGraph previous(src);
        TReplayGraph next(dst);
        next.ApplyReplayProgress(previous.ReadReplayProgress(states));
        plan = next.BuildReplayTaskPlans(/* useSourceDisposition */ false, /* fromCheckpoint */ true);
        return true;
    } catch (const std::exception& e) {
        issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cannot replay streaming query history: " << e.what()));
        return false;
    }
}

bool MakeOutputStartTimeReplayPlan(const NProto::TGraphParams& tasks, ui64 outputStartTimeUs, bool useSourceDisposition, TStateLoadPlan& plan, NYql::TIssues& issues) {
    try {
        YQL_ENSURE(!tasks.GetTasks().empty(), "Replay from OUTPUT_FROM requires a query graph");

        TReplayGraph graph(tasks);
        graph.PropagateExplicitOutputBound(outputStartTimeUs);
        plan = graph.BuildReplayTaskPlans(useSourceDisposition, /* fromCheckpoint */ false);
        return true;
    } catch (const std::exception& e) {
        issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cannot start from OUTPUT_FROM: " << e.what()));
        return false;
    }
}

} // namespace NFq
