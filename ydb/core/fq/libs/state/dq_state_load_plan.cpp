#include "dq_stage_state_recovery_info.h"
#include "dq_state_load_plan_impl.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/public/issue/protos/issue_id.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/join.h>

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
    TString Endpoint;
    TString Cluster;

    bool operator==(const TTopic& t) const {
        return DatabaseId == t.DatabaseId && Database == t.Database && TopicPath == t.TopicPath && Endpoint == t.Endpoint && Cluster == t.Cluster;
    }

    struct THash {
        size_t operator()(const TTopic& t) const {
            return MultiHash(t.DatabaseId, t.Database, t.TopicPath, t.Endpoint, t.Cluster);
        }
    };
};

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
    const char* queryKindStr = isSourceGraph ? "source" : "destination";
    const google::protobuf::Any& settingsAny = taskInput.GetSource().GetSettings();
    if (!settingsAny.Is<NYql::NPq::NProto::TDqPqTopicSource>()) {
        AddForceWarningOrError(TStringBuilder() << "Can't read " << queryKindStr << " query params: input " << inputIndex << " of task " << task.GetId() << " has incorrect type", issues, force);
        return false;
    }

    if (!settingsAny.UnpackTo(&srcDesc)) {
        AddForceWarningOrError(TStringBuilder() << "Can't read " << queryKindStr << " query params: failed to unpack input " << inputIndex << " of task " << task.GetId(), issues, force);
        return false;
    }

    partitionsSets = NYql::NPq::GetTopicPartitionsSets(task);
    if (partitionsSets.empty()) {
        AddForceWarningOrError(TStringBuilder() << "Can't read " << queryKindStr << " query params: failed to load partitions of topic `" << srcDesc.GetTopicPath() << "` from input " << inputIndex << " of task " << task.GetId(), issues, force);
        return false;
    }

    return true;
}

template <typename TCallback>
void ForEachTopicPartition(const NYql::NPq::NProto::TDqPqTopicSource& source, const std::vector<NYql::NPq::TTopicPartitionsSet>& sets, TCallback callback) {
    const auto add = [&](const TString& cluster, const TString& endpoint, const TString& database, const ui64 partitionsCount) {
        const TTopic topic{source.GetDatabaseId(), database, source.GetTopicPath(), endpoint, cluster};
        for (const auto& set : sets) {
            YQL_ENSURE(set.DqPartitionsCount, "Invalid topic partition mapping");
            const auto count = partitionsCount ? partitionsCount : set.TopicPartitionsCount;
            for (ui64 partition = set.EachTopicPartitionGroupId; partition < count; partition += set.DqPartitionsCount) {
                callback(topic, partition);
            }
        }
    };

    if (source.GetFederatedClusters().empty()) {
        add({}, source.GetEndpoint(), source.GetDatabase(), 0);
    } else {
        THashSet<TString> clusters;
        for (const auto& cluster : source.GetFederatedClusters()) {
            const auto& name = cluster.GetName();
            YQL_ENSURE(clusters.insert(name).second, "Duplicate federated topic cluster " << name);
            add(name, name ? cluster.GetEndpoint() : source.GetEndpoint(), name ? cluster.GetDatabase() : source.GetDatabase(), cluster.GetPartitionsCount());
        }
    }
}

class TContinuationPlanBuilder {
    // Stateful operators info

    struct THashRouting {
        TString Settings;
        TVector<const NKikimr::NMiniKQL::TType*> KeyTypes;
        bool Block = false;

        bool IsSame(const THashRouting& other) const {
            if (Settings != other.Settings || Block != other.Block || KeyTypes.size() != other.KeyTypes.size()) {
                return false;
            }

            for (size_t i = 0; i < KeyTypes.size(); ++i) {
                if (!KeyTypes[i]->IsSameType(*other.KeyTypes[i])) {
                    return false;
                }
            }

            return true;
        }
    };

    struct TAggregation {
        const TStageStateInfo* Stage = nullptr;
        const NKikimr::NMiniKQL::TType* KeyType = nullptr;
        const NKikimr::NMiniKQL::TType* SavedStateType = nullptr;
    };

    using TOperatorsMap = THashMap<TString, TAggregation>;

    // Topics partitions info

    struct TTaskSource {
        ui64 TaskId = 0;
        ui64 InputIndex = 0;
        TString Consumer;

        bool operator==(const TTaskSource& t) const {
            return TaskId == t.TaskId && InputIndex == t.InputIndex;
        }

        struct THash {
            size_t operator()(const TTaskSource& t) const {
                return ::THash<std::tuple<ui64, ui64>>()(std::tie(t.TaskId, t.InputIndex));
            }
        };
    };

    using TPartitionsMapping = THashMultiMap<ui64, TTaskSource>; // Task can have multiple sources for one partition, so multimap.

    struct TTopicMappingInfo {
        TPartitionsMapping PartitionsMapping;
        bool Used = false;
    };

    using TTopicsMapping = THashMap<TTopic, TTopicMappingInfo, TTopic::THash>;

public:
    TContinuationPlanBuilder(const TGraphStateInfo& src, const TGraphStateInfo& dst, const bool force, NYql::TIssues& issues)
        : Force(force)
        , Issues(issues)
    {
        const auto guard = src.BindAllocator();
        YQL_ENSURE(src.GetContext() == dst.GetContext(), "Recovery graphs must share a type environment");

        BuildSourcesContinuation(src, dst);
        BuildStatefulOperatorsContinuation(src, dst);
    }

    bool IsValid() const {
        return Valid;
    }

    void ExtractPlan(TStateLoadPlan& plan, TSourceRecoverySet& sourcesToPrepare) {
        plan = std::move(Plan);
        sourcesToPrepare = std::move(ChangedConsumers);
    }

private:
    //// Stateful operators recovery

    static THashRouting GetAggregationRouting(const TGraphStateInfo& graph, const TStageStateInfo& aggregation) {
        struct TOutputChannel {
            size_t OutputIndex = 0;
            size_t Slot = 0;
        };

        struct TProducer {
            const NYql::NDqProto::TDqTask* Task = nullptr;
            const TStageStateInfo* Stage = nullptr;
            TMaybe<THashMap<ui64, TOutputChannel>> OutputChannels;
        };

        if (aggregation.Tasks.size() == 1) {
            return {};
        }

        THashMap<ui64, TProducer> producers;
        producers.reserve(std::max(aggregation.Tasks.size(), graph.GetStages().size()));
        for (const auto& stage : graph.GetStages()) {
            for (const auto* task : stage.Tasks) {
                producers.emplace(task->GetId(), TProducer{task, &stage, {}});
            }
        }

        TMaybe<THashRouting> result;
        for (size_t taskIndex = 0; taskIndex < aggregation.Tasks.size(); ++taskIndex) {
            bool hasShuffle = false;

            const auto& target = *aggregation.Tasks[taskIndex];
            for (const auto& input : target.GetInputs()) {
                YQL_ENSURE(!input.HasSource() || !NYql::NDq::IsInfiniteSourceType(input.GetSource().GetType()), "Unsupported aggregation input routing, expected direct hash shuffle");
                for (const auto& channel : input.GetChannels()) {
                    if (channel.GetCheckpointingMode() == NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                        continue;
                    }

                    auto* const producer = producers.FindPtr(channel.GetSrcTaskId());
                    YQL_ENSURE(producer, "Missing shuffle producer task " << channel.GetSrcTaskId());

                    if (!producer->OutputChannels) {
                        auto& outputChannels = producer->OutputChannels.ConstructInPlace();
                        for (size_t outputIndex = 0; outputIndex < producer->Task->OutputsSize(); ++outputIndex) {
                            const auto& output = producer->Task->GetOutputs(outputIndex);
                            for (size_t slot = 0; slot < output.ChannelsSize(); ++slot) {
                                YQL_ENSURE(outputChannels.emplace(output.GetChannels(slot).GetId(), TOutputChannel{outputIndex, slot}).second, "Unexpected aggregation hash shuffle routing: duplicate output channel");
                            }
                        }
                    }

                    const auto* const sourceChannel = producer->OutputChannels->FindPtr(channel.GetId());
                    YQL_ENSURE(sourceChannel, "Missing aggregation input channel " << channel.GetId());
                    const auto& output = producer->Task->GetOutputs(sourceChannel->OutputIndex);
                    const auto& outgoing = output.GetChannels(sourceChannel->Slot);

                    YQL_ENSURE(output.HasHashPartition() && !output.HasTransform(), "Unsupported aggregation input routing, expected direct hash shuffle without output transforms");
                    YQL_ENSURE(outgoing.GetDstTaskId() == target.GetId() && sourceChannel->Slot == taskIndex, "Unexpected aggregation hash shuffle routing");

                    auto settings = output.GetHashPartition();
                    YQL_ENSURE(static_cast<size_t>(output.ChannelsSize()) == aggregation.Tasks.size() && settings.GetPartitionsCount() == output.ChannelsSize() && settings.KeyColumnsSize(), "Invalid aggregation hash shuffle settings");
                    if (settings.GetHashKindCase() == NYql::NDqProto::TTaskOutputHashPartition::HASHKIND_NOT_SET) {
                        settings.MutableHashV1(); // Runtime default.
                    }

                    const auto& types = producer->Stage->OutputTypes;
                    YQL_ENSURE(sourceChannel->OutputIndex < types.size(), "Missing shuffle output type for aggregation recovery");

                    THashRouting hash;
                    hash.Settings = settings.SerializeAsString();
                    hash.Block = true;
                    hash.KeyTypes.reserve(settings.KeyColumnsSize());
                    for (const auto& key : settings.GetKeyColumns()) {
                        const auto column = NYql::NDq::GetColumnInfo(types[sourceChannel->OutputIndex], key);
                        hash.KeyTypes.emplace_back(column.OriginalType);
                        hash.Block &= column.IsBlockOrScalar();
                    }

                    YQL_ENSURE(!result || result->IsSame(hash), "Inconsistent hash shuffle contracts for aggregation recovery");
                    result = std::move(hash);
                    hasShuffle = true;
                }
            }

            YQL_ENSURE(hasShuffle, "Missing direct hash shuffle for aggregation recovery");
        }

        YQL_ENSURE(result, "Missing aggregation tasks");
        return std::move(*result);
    }

    THashMap<TString, TAggregation> CollectStatefulOperators(const TGraphStateInfo& graph, const bool previous) {
        THashMap<TString, TAggregation> result;
        for (const auto& stage : graph.GetStages()) {
            // Collect supported stateful operators.

            for (const auto* callable : stage.StatefulOperators) {
                const TStringBuf name = callable->GetType()->GetName();
                if (name == "KqpStreamingAggregation"sv) {
                    YQL_ENSURE(callable->GetInputsCount() >= 12, "Invalid streaming aggregation program in stage " << stage.StageId);
                    if (const auto binding = callable->GetInput(8); binding.IsImmediate() && binding.GetStaticType()->IsTuple()) {
                        const auto* tuple = AS_VALUE(NKikimr::NMiniKQL::TTupleLiteral, binding);
                        YQL_ENSURE(tuple->GetValuesCount() == 2, "Invalid streaming aggregation output table binding");

                        const auto path = tuple->GetValue(0);
                        YQL_ENSURE(path.IsImmediate() && path.GetStaticType()->IsData(), "Invalid streaming aggregation output table path");

                        const TString table(AS_VALUE(NKikimr::NMiniKQL::TDataLiteral, path)->AsValue().AsStringRef());
                        YQL_ENSURE(!table.empty(), "Empty streaming aggregation output table path");
                        YQL_ENSURE(result.emplace(table, TAggregation{
                            .Stage = &stage,
                            .KeyType = callable->GetInput(3).GetStaticType(),
                            .SavedStateType = callable->GetInput(9).GetStaticType(),
                        }).second, "Ambiguous streaming aggregation output table binding: " << table);
                    } else if (previous) {
                        StateLossError(TStringBuilder() << "Unsupported checkpointed streaming aggregation setup for offset recovery in stage " << stage.StageId << ", recovery allowed only for streaming aggregation with table binding");
                    }
                } else if (previous) {
                    StateLossError(TStringBuilder() << "Unsupported checkpointed operator for offset recovery: " << name << " in stage " << stage.StageId);
                }
            }

            if (!previous) {
                continue;
            }

            // Validate that sinks do not hold state.

            THashSet<TString> sinks;
            for (const auto* task : stage.Tasks) {
                for (const auto& output : task->GetOutputs()) {
                    if (!output.HasSink() || output.GetSink().GetType() != "PqSink") {
                        continue;
                    }

                    const auto& sink = output.GetSink();
                    if (!sinks.insert(sink.SerializeAsString()).second) {
                        continue;
                    }

                    NYql::NPq::NProto::TDqPqTopicSink settings;
                    YQL_ENSURE(sink.GetSettings().UnpackTo(&settings), "Invalid PQ sink settings for offset recovery");

                    if (settings.GetEnableDeduplication()) {
                        StateLossError("Offset recovery does not support PQ sinks with deduplication: producer sequence numbers cannot be transferred");
                    }

                    if (!settings.GetDeferredPublicationExtIdPrefix().empty()) {
                        StateLossError("Offset recovery does not support PQ sinks with exactly-once delivery: deferred publications cannot be transferred");
                    }
                }
            }
        }

        return result;
    }

    void BuildStatefulOperatorsContinuation(const TGraphStateInfo& src, const TGraphStateInfo& dst) {
        const auto previous = CollectStatefulOperators(src, /* previous */ true);
        const auto next = CollectStatefulOperators(dst, /* previous */ false);

        for (const auto& [table, old] : previous) {
            const auto it = next.find(table);
            if (it == next.end()) {
                TVector<TString> availableTables;
                availableTables.reserve(next.size());
                for (const auto& [name, _] : next) {
                    availableTables.push_back(name);
                }

                std::sort(availableTables.begin(), availableTables.end());
                StateLossError(TStringBuilder() << "Streaming aggregation output table is missing in the new query: " << table
                    << ", available output tables: " << (availableTables.empty() ? "none" : JoinSeq(", ", availableTables)));
                continue;
            }

            const auto& target = it->second;
            if (!old.KeyType->IsSameType(*target.KeyType)) {
                StateLossError(TStringBuilder() << "Streaming aggregation key type changed for output table " << table
                    << ", previous: " << NKikimr::NMiniKQL::PrintNode(old.KeyType, /* singleLine */ true)
                    << ", new: " << NKikimr::NMiniKQL::PrintNode(target.KeyType, /* singleLine */ true));
                continue;
            }

            if (!old.SavedStateType->IsSameType(*target.SavedStateType)) {
                StateLossError(TStringBuilder() << "Streaming aggregation saved state type changed for output table " << table
                    << ", previous: " << NKikimr::NMiniKQL::PrintNode(old.SavedStateType, /* singleLine */ true)
                    << ", new: " << NKikimr::NMiniKQL::PrintNode(target.SavedStateType, /* singleLine */ true));
                continue;
            }

            if (old.Stage->Tasks.size() != target.Stage->Tasks.size()) {
                StateLossError(TStringBuilder() << "Streaming aggregation task count changed for output table " << table << ": " << old.Stage->Tasks.size() << " -> " << target.Stage->Tasks.size() << " on stage " << target.Stage->StageId);
                continue;
            }

            try {
                if (!GetAggregationRouting(src, *old.Stage).IsSame(GetAggregationRouting(dst, *target.Stage))) {
                    StateLossError(TStringBuilder() << "Streaming aggregation shuffle routing changed for output table " << table);
                    continue;
                }
            } catch (const std::exception& e) {
                StateLossError(TStringBuilder() << "Cannot validate streaming aggregation shuffle routing for output table " << table << ": " << e.what());
                continue;
            }

            if (old.Stage->StatefulOperators.size() != 1 || target.Stage->StatefulOperators.size() != 1) {
                StateLossError(TStringBuilder() << "Cannot transfer a mixed program checkpoint for output table " << table);
                continue;
            }

            for (size_t i = 0; i < target.Stage->Tasks.size(); ++i) {
                const auto& task = *target.Stage->Tasks[i];
                auto& taskPlan = Plan[task.GetId()];
                if (taskPlan.GetStateType() != NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
                    InitForeignPlan(task, taskPlan);
                }

                auto& program = *taskPlan.MutableProgram();
                YQL_ENSURE(!program.HasForeignTaskId(), "Ambiguous foreign program checkpoint for task " << task.GetId());
                program.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
                program.SetForeignTaskId(old.Stage->Tasks[i]->GetId());
            }
        }
    }

    //// Sources recovery

    static NYql::NDqProto::NDqStateLoadPlan::TSourcePlan& FindSourcePlan(NYql::NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan, const ui64 inputIndex) {
        for (auto& plan : *taskPlan.MutableSources()) {
            if (plan.GetInputIndex() == inputIndex) {
                return plan;
            }
        }
        Y_ABORT("Source plan for input index %lu was not found", inputIndex);
    }

    TTopicsMapping BuildScrInputMapping(const TGraphStateInfo& src) {
        TTopicsMapping srcMapping;
        for (const auto& task : src.GetGraph()->GetTasks()) {
            for (size_t inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
                const auto& taskInput = task.GetInputs(inputIndex);
                if (IsTopicInput(taskInput)) {
                    NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                    std::vector<NYql::NPq::TTopicPartitionsSet> partitionsSets;
                    if (!ParseTopicInput(task, taskInput, inputIndex, Force, /* isSourceGraph */ true, srcDesc, partitionsSets, Issues)) {
                        if (!Force) {
                            Valid = false;
                        }
                        continue;
                    }

                    const auto& consumer = srcDesc.GetConsumerName();

                    ForEachTopicPartition(srcDesc, partitionsSets, [&, taskId = task.GetId()](const TTopic& topic, const ui64 partition) {
                        auto& topicInfo = srcMapping[topic];
                        topicInfo.PartitionsMapping.emplace(partition, TTaskSource{taskId, inputIndex, consumer});
                    });
                }
            }
        }

        return srcMapping;
    }

    void BuildSourcesContinuation(const TGraphStateInfo& src, const TGraphStateInfo& dst) {
        auto srcMapping = BuildScrInputMapping(src);

        for (const auto& task : dst.GetGraph()->GetTasks()) {
            auto& taskPlan = Plan[task.GetId()];
            taskPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY); // Default if no topic sources or stateful operators

            bool foreignStatePlanInitted = false;
            for (size_t inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
                if (const auto& taskInput = task.GetInputs(inputIndex); IsTopicInput(taskInput)) {
                    NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                    std::vector<NYql::NPq::TTopicPartitionsSet> partitionsSets;
                    if (!ParseTopicInput(task, taskInput, inputIndex, Force, /* isSourceGraph */ false, srcDesc, partitionsSets, Issues)) {
                        if (!Force) {
                            Valid = false;
                        }
                        continue;
                    }

                    const auto& consumer = srcDesc.GetConsumerName();

                    THashSet<TTaskSource, TTaskSource::THash> tasksSet;
                    ForEachTopicPartition(srcDesc, partitionsSets, [&](const TTopic& topic, ui64 partition) {
                        const auto mappingInfoIt = srcMapping.find(topic);
                        if (mappingInfoIt == srcMapping.end()) {
                            SourceError(TStringBuilder() << "Topic `" << srcDesc.GetTopicPath() << "` is not found in previous query", "Query will use fresh offsets for its partitions");
                            return;
                        }

                        auto& mappingInfo = mappingInfoIt->second;
                        mappingInfo.Used = true;

                        auto [taskBegin, taskEnd] = mappingInfo.PartitionsMapping.equal_range(partition);
                        if (taskBegin == taskEnd) {
                            SourceError(TStringBuilder() << "Topic `" << srcDesc.GetTopicPath() << "` partition " << partition << " is not found in previous query", "Query will use fresh offsets for it");
                        } else {
                            if (std::distance(taskBegin, taskEnd) > 1) {
                                SourceError(TStringBuilder() << "Topic `" << srcDesc.GetTopicPath() << "` partition " << partition << " has ambiguous offsets source in previous query checkpoint", "Query will use minimum offset to avoid skipping data");
                            }
                            for (; taskBegin != taskEnd; ++taskBegin) {
                                if (consumer != taskBegin->second.Consumer) {
                                    ChangedConsumers.emplace(task.GetId(), inputIndex);
                                }
                                tasksSet.insert(taskBegin->second);
                            }
                        }
                    });

                    if (!tasksSet.empty()) {
                        if (!std::exchange(foreignStatePlanInitted, true)) {
                            InitForeignPlan(task, taskPlan);
                        }

                        auto& sourcePlan = FindSourcePlan(taskPlan, inputIndex);
                        sourcePlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);

                        for (const TTaskSource& taskSource : tasksSet) {
                            auto& taskSourceProto = *sourcePlan.AddForeignTasksSources();
                            taskSourceProto.SetTaskId(taskSource.TaskId);
                            taskSourceProto.SetInputIndex(taskSource.InputIndex);
                        }
                    }
                }
            }
        }

        for (const auto& [topic, mappingInfo] : srcMapping) {
            if (!mappingInfo.Used) {
                SourceError(TStringBuilder() << "Topic `" << topic.TopicPath << "` is read in previous query but is not read in new query", "Reading offsets will be lost in next checkpoint");
            }
        }
    }

    //// Helpers

    static void InitForeignPlan(const NYql::NDqProto::TDqTask& task, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan& taskPlan) {
        taskPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN);
        taskPlan.MutableProgram()->SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);

        for (size_t inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
            if (const auto& taskInput = task.GetInputs(inputIndex); taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource) {
                auto& sourcePlan = *taskPlan.AddSources();
                sourcePlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
                sourcePlan.SetInputIndex(inputIndex);
            }
        }

        for (size_t outputIndex = 0; outputIndex < task.OutputsSize(); ++outputIndex) {
            if (const auto& taskOutput = task.GetOutputs(outputIndex); taskOutput.GetTypeCase() == NYql::NDqProto::TTaskOutput::kSink) {
                auto& sinkPlan = *taskPlan.AddSinks();
                sinkPlan.SetStateType(NYql::NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY);
                sinkPlan.SetOutputIndex(outputIndex);
            }
        }
    }

    void StateLossError(const TString& message) {
        AddForceWarningOrError(message + (Force ? ", FORCE=true discards this state" : ""), Issues, Force);
        Valid &= Force;
    }

    void SourceError(const TString& message, const char* forceMessage) {
        AddForceWarningOrError(Force ? message + ". " + forceMessage : message, Issues, Force);
        Valid &= Force;
    }

    const bool Force = false;
    bool Valid = true;
    NYql::TIssues& Issues;
    TStateLoadPlan Plan;
    TSourceRecoverySet ChangedConsumers;
};

} // anonymous namespace

bool MakeContinueFromStreamingOffsetsPlan(const TGraphStateInfo& src, const TGraphStateInfo& dst, const bool force, TStateLoadPlan& plan, TSourceRecoverySet& sourcesToPrepare, NYql::TIssues& issues) {
    plan.clear();
    sourcesToPrepare.clear();

    try {
        if (TContinuationPlanBuilder planBuilder(src, dst, force, issues); planBuilder.IsValid()) {
            planBuilder.ExtractPlan(plan, sourcesToPrepare);
            return true;
        }
        return false;
    } catch (const std::exception& e) {
        issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cannot continue from streaming offsets: " << e.what()));
        return false;
    }
}

namespace {

class TReplayGraph {
    static constexpr ui64 WATERMARK_GENERATOR_EARLY_LIMIT = TDuration::Minutes(5).MicroSeconds();

public:
    struct TPartition {
        TTopic Topic;
        ui64 Id = 0;
        std::optional<ui64> Offset = std::nullopt;

        bool operator==(const TPartition& other) const {
            return Topic == other.Topic && Id == other.Id;
        }

        struct THash {
            size_t operator()(const TPartition& partition) const {
                return MultiHash(TTopic::THash()(partition.Topic), partition.Id);
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
    explicit TReplayGraph(const TGraphStateInfo& discovered) {
        const auto& graph = *discovered.GetGraph();
        const auto guard = discovered.BindAllocator();

        THashMap<ui32, TStageStateRecoveryInfo> stages;
        stages.reserve(discovered.GetStages().size());
        for (const auto& stage : discovered.GetStages()) {
            stages.emplace(stage.StageId, TStageStateRecoveryInfo(stage, TStageStateRecoveryInfo::EMode::HistoryReplay));
        }

        Tasks.reserve(graph.GetTasks().size());
        for (const auto& task : graph.GetTasks()) {
            if (NYql::NDq::GetTaskCheckpointingMode(task) == NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
                continue;
            }

            auto [it, inserted] = Tasks.emplace(task.GetId(), TTask{});
            Y_VALIDATE(inserted, "Duplicate task ID in history replay graph");

            auto& info = it->second;
            info.Task = &task;
            info.Info = stages.at(task.GetStageId());

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

                    ForEachTopicPartition(source.Description, partitionSets, [&](const TTopic& topic, const ui64 partition) {
                        source.Partitions.push_back({topic, partition});
                    });

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
                    const auto* progress = partitions.FindPtr(std::make_pair(partition.Topic.Cluster, partition.Id));
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
                            saved.SetCluster(partition.Topic.Cluster);
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

bool MakeHistoryReplayPlan(const TGraphStateInfo& src, const TGraphStateInfo& dst, const TCheckpointTaskStates& states, TStateLoadPlan& plan, NYql::TIssues& issues) {
    try {
        YQL_ENSURE(!src.GetStages().empty() && !dst.GetStages().empty(), "History replay requires both query graphs");

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

bool MakeOutputStartTimeReplayPlan(const TGraphStateInfo& tasks, ui64 outputStartTimeUs, bool useSourceDisposition, TStateLoadPlan& plan, NYql::TIssues& issues) {
    try {
        YQL_ENSURE(!tasks.GetStages().empty(), "Replay from OUTPUT_FROM requires a query graph");

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
