#include "kqp_graph_replanning.h"

#include <ydb/core/fq/libs/state/dq_stage_state_recovery_info.h>
#include <ydb/library/yql/providers/pq/common/pq_partitions.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>

namespace NKikimr::NKqp {

NFq::NProto::TGraphParams MakeCheckpointGraphParams(const NKikimrKqp::TQueryPhysicalGraph& graph) {
    NFq::NProto::TGraphParams result;
    const auto& query = graph.GetPreparedQuery().GetPhysicalQuery();
    TVector<ui32> stageBases;
    ui32 base = 0;
    for (const auto& tx : query.GetTransactions()) {
        stageBases.push_back(base);
        base += tx.StagesSize();
    }
    for (const auto& saved : graph.GetTasks()) {
        auto& task = *result.AddTasks();
        task = saved.GetDqTask();
        YQL_ENSURE(saved.GetTxId() < stageBases.size(), "Missing checkpoint task transaction");
        const auto localStage = task.GetStageId() - stageBases.at(saved.GetTxId());
        const auto& tx = query.GetTransactions(saved.GetTxId());
        YQL_ENSURE(localStage < static_cast<ui32>(tx.StagesSize()), "Missing checkpoint task stage");
        const auto& program = tx.GetStages(localStage).GetProgram();
        result.MutableStageProgram()->try_emplace(task.GetStageId(), program.GetRaw());
        task.MutableProgram()->SetRuntimeVersion(program.GetRuntimeVersion());
    }
    return result;
}

bool CollectReplanningConstraints(const NKikimrKqp::TQueryPhysicalGraph& previous,
    TTaskPlanningConstraints& constraints, TString& fallbackReason)
{
    constraints = {};
    try {
        YQL_ENSURE(previous.HasPreparedQuery(), "Missing compiled query");
        const NFq::TGraphStateContext context;
        THashSet<TString> identities;
        ui32 txId = 0;
        ui32 stageBase = 0;
        for (const auto& tx : previous.GetPreparedQuery().GetPhysicalQuery().GetTransactions()) {
            ui32 stageIdx = 0;
            for (const auto& stage : tx.GetStages()) {
                for (const auto& sink : stage.GetSinks()) {
                    if (sink.GetExternalSink().GetType() == "PqSink") {
                        NYql::NPq::NProto::TDqPqTopicSink settings;
                        YQL_ENSURE(sink.GetExternalSink().GetSettings().UnpackTo(&settings), "Invalid PQ sink");
                        YQL_ENSURE(!settings.GetEnableDeduplication()
                            && settings.GetDeferredPublicationExtIdPrefix().empty(), "PQ sink requires exact graph recovery");
                    }
                }
                const auto info = NFq::AnalyzeStageProgram(stage.GetProgram(), stage.GetProgram().GetRaw(), context);
                if (!info.StatefulOperators.empty()) {
                    const auto guard = context.BindAllocator();
                    YQL_ENSURE(info.StatefulOperators.size() == 1, "Mixed stateful stage");
                    YQL_ENSURE(stage.SourcesSize() == 0, "Stateful source stage requires exact graph recovery");
                    const auto* op = info.StatefulOperators.front();
                    const auto name = op->GetType()->GetName();
                    if (name == "KqpStreamingAggregation") {
                        const auto identity = NFq::GetStreamingAggregationIdentity(*op);
                        // Unbound in-memory aggregations use the resolver's
                        // identical compiled-stage contract instead of a table identity.
                        YQL_ENSURE(identity.empty() || identities.insert(identity).second,
                            "Ambiguous streaming aggregation binding");
                    } else {
                        YQL_ENSURE(name == "MultiHoppingCore", "Unsupported checkpointed operator: " << name);
                    }
                    const NYql::NDq::TStageId id(txId, stageBase + stageIdx);
                    ui32 count = 0;
                    for (const auto& saved : previous.GetTasks()) {
                        count += saved.GetTxId() == id.TxId && saved.GetDqTask().GetStageId() == id.StageId;
                    }
                    YQL_ENSURE(count, "Missing saved stateful tasks");
                    constraints.FixedTaskCountByStage[id] = count;
                }
                ++stageIdx;
            }
            stageBase += tx.StagesSize();
            ++txId;
        }
        return true;
    } catch (const std::exception& e) {
        constraints = {};
        fallbackReason = e.what();
        return false;
    }
}

namespace {

void NormalizeTableSinkSettings(google::protobuf::Any& value) {
    NKikimrKqp::TKqpTableSinkSettings settings;
    if (value.UnpackTo(&settings)) {
        settings.ClearBufferActorId();
        settings.ClearLockTxId();
        settings.ClearLockNodeId();
        settings.ClearMvccSnapshot();
        settings.ClearQuerySpanId();
        settings.ClearBufferLookupDiagnosticsExecutionId();
        value.PackFrom(settings);
    }
}

NKikimrKqp::TQueryPhysicalGraph NormalizeGraph(const NKikimrKqp::TQueryPhysicalGraph& graph) {
    auto result = graph;
    result.ClearPreparedQuery();
    result.ClearZeroCheckpointSaved();
    result.ClearRequiresCompatibleStateRecovery();
    // IDs are deliberately retained: an ID-only change still requires foreign recovery.
    std::sort(result.MutableTasks()->begin(), result.MutableTasks()->end(), [](const auto& a, const auto& b) {
        return a.GetDqTask().GetId() < b.GetDqTask().GetId();
    });
    for (auto& saved : *result.MutableTasks()) {
        auto& task = *saved.MutableDqTask();
        task.ClearExecuter();
        task.ClearMetaId();
        task.ClearProgram();
        task.ClearParameters();
        task.ClearSecureParams();
        task.ClearRequestContext();
        task.ClearInitialTaskMemoryLimit();
        task.MutableTaskParams()->erase("current_execution_generation");
        task.MutableTaskParams()->erase("checkpoints_enabled");
        task.MutableTaskParams()->erase("fq.job_id");
        // Control-plane actor IDs are serialized as task parameters, but do
        // not describe the materialized task/channel topology.
        if (graph.HasPreparedQuery()) {
            ui32 base = 0;
            ui32 txId = 0;
            for (const auto& tx : graph.GetPreparedQuery().GetPhysicalQuery().GetTransactions()) {
                if (txId++ == saved.GetTxId() && task.GetStageId() >= base
                    && task.GetStageId() - base < static_cast<ui32>(tx.StagesSize()))
                {
                    for (const auto& [key, settings] : tx.GetStages(task.GetStageId() - base).GetStageControlPlaneActors()) {
                        task.MutableTaskParams()->erase(key);
                    }
                    break;
                }
                base += tx.StagesSize();
            }
        }
        for (auto& input : *task.MutableInputs()) {
            for (auto& channel : *input.MutableChannels()) {
                channel.ClearId();
                channel.ClearSrcEndpoint();
                channel.ClearDstEndpoint();
            }
        }
        for (auto& output : *task.MutableOutputs()) {
            if (output.HasSink()) {
                NormalizeTableSinkSettings(*output.MutableSink()->MutableSettings());
            }
            if (output.HasTransform()) {
                NormalizeTableSinkSettings(*output.MutableTransform()->MutableSettings());
            }
            for (auto& channel : *output.MutableChannels()) {
                channel.ClearId();
                channel.ClearSrcEndpoint();
                channel.ClearDstEndpoint();
            }
        }
    }
    return result;
}

} // namespace

bool MaterializedGraphsEqual(const NKikimrKqp::TQueryPhysicalGraph& previous,
    const NKikimrKqp::TQueryPhysicalGraph& candidate)
{
    return google::protobuf::util::MessageDifferencer::Equals(NormalizeGraph(previous), NormalizeGraph(candidate));
}

void RefreshPqSourcePartitions(NKqpProto::TKqpExternalSource& source,
    const THashMap<TString, ui32>& clusterPartitions, ui32 maxPartitions, ui32 maxTasksPerStage)
{
    NYql::NPq::NProto::TDqPqTopicSource settings;
    YQL_ENSURE(source.GetSettings().UnpackTo(&settings), "Invalid PQ source settings");
    YQL_ENSURE(maxPartitions, "Empty PQ partition snapshot");
    for (auto& cluster : *settings.MutableFederatedClusters()) {
        const auto* count = clusterPartitions.FindPtr(cluster.GetName());
        YQL_ENSURE(count && *count, "Incomplete federated PQ partition snapshot");
        cluster.SetPartitionsCount(*count);
    }

    TVector<ui64> selected;
    if (settings.GetUsedPartitionPredicate()) {
        THashSet<ui64> seen;
        for (const auto& raw : source.GetPartitionedTaskParams()) {
            NYql::NPq::NProto::TDqReadTaskParams params;
            YQL_ENSURE(params.ParseFromString(raw), "Invalid compiled PQ partition ranges");
            for (const auto& part : params.GetPartitioningParams()) {
                YQL_ENSURE(part.GetDqPartitionsCount(), "Invalid PQ partition stride");
                for (ui64 id = part.GetEachTopicPartitionGroupId(); id < part.GetTopicPartitionsCount(); id += part.GetDqPartitionsCount()) {
                    YQL_ENSURE(id < maxPartitions, "Selected PQ partition no longer exists");
                    YQL_ENSURE(seen.insert(id).second, "Duplicate selected PQ partition");
                    selected.push_back(id);
                }
            }
        }
        std::sort(selected.begin(), selected.end());
        YQL_ENSURE(!selected.empty(), "Empty PQ partition selection");
    }
    const ui64 partitionCount = settings.GetUsedPartitionPredicate() ? selected.size() : maxPartitions;
    const ui64 taskCount = NYql::NDq::GetExpectedTopicReadTasks(partitionCount, maxTasksPerStage, !maxTasksPerStage);
    YQL_ENSURE(taskCount, "Empty PQ planning partitioning");
    source.ClearPartitionedTaskParams();
    for (ui64 slot = 0; slot < taskCount; ++slot) {
        NYql::NPq::NProto::TDqReadTaskParams params;
        if (settings.GetUsedPartitionPredicate()) {
            for (ui64 index = slot; index < selected.size(); index += taskCount) {
                auto& part = *params.AddPartitioningParams();
                part.SetTopicPartitionsCount(maxPartitions);
                part.SetEachTopicPartitionGroupId(selected[index]);
                part.SetDqPartitionsCount(maxPartitions);
            }
        } else {
            auto& part = *params.AddPartitioningParams();
            part.SetTopicPartitionsCount(maxPartitions);
            part.SetEachTopicPartitionGroupId(slot);
            part.SetDqPartitionsCount(taskCount);
        }
        source.AddPartitionedTaskParams(params.SerializeAsString());
    }
    source.MutableSettings()->PackFrom(settings);
}

} // namespace NKikimr::NKqp
