#include "physical_graph_rescaling.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/fq/libs/state/dq_stage_state_recovery_info.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/common/pq_partitions.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

void PatchQueryPhysicalGraphForRescaling(
    NKikimrKqp::TQueryPhysicalGraph& graph,
    const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot)
{
    YDB_LOG_INFO("Starting PQ source rescaling",
        {"resourceCount", resourceSnapshot.size()}, {"taskCount", graph.TasksSize()});
    if (!graph.HasPreparedQuery()) {
        YDB_LOG_INFO("Skipping PQ source rescaling: no prepared query");
        return;
    }

    // Use TStageId (txId, stageIdx) as map key — it has a THash specialization.
    using TStageKey = NYql::NDq::TStageId;

    if (resourceSnapshot.empty()) {
        YDB_LOG_INFO("Skipping PQ source rescaling: empty resource snapshot");
        return;
    }

    const auto& physQuery = graph.GetPreparedQuery().GetPhysicalQuery();

    // Inspect the whole query before mutating any task or channel. Programs are
    // shared via prepared stages, but saved tasks can also carry inline programs.
    try {
        // Offset-only foreign restore cannot finish a pending sink publication:
        // it drops sink state, including the publication id needed for commit.
        const auto hasDeferredPublication = [](const auto& sink) {
            if (sink.GetType() != "PqSink") {
                return false;
            }
            NYql::NPq::NProto::TDqPqTopicSink settings;
            YQL_ENSURE(sink.GetSettings().UnpackTo(&settings), "Cannot decode PQ sink settings for rescaling");
            return !settings.GetDeferredPublicationExtIdPrefix().empty();
        };
        const auto hasState = [](const auto& program) {
            NFq::NProto::TGraphParams graph;
            auto& task = *graph.AddTasks();
            task.SetId(1);
            task.SetStageId(1);
            task.MutableProgram()->CopyFrom(program);
            // Keep the synthetic task checkpointable so state discovery does not skip it.
            task.AddInputs()->AddChannels();

            const NFq::TGraphStateContext context;
            const NFq::TGraphStateInfo graphInfo(graph, context);
            YQL_ENSURE(graphInfo.GetStages().size() == 1, "Cannot analyze stage state for rescaling");
            return NFq::TStageStateRecoveryInfo(graphInfo.GetStages().front()).HasState;
        };
        for (const auto& tx : physQuery.GetTransactions()) {
            for (const auto& stage : tx.GetStages()) {
                for (const auto& sink : stage.GetSinks()) {
                    if (hasDeferredPublication(sink.GetExternalSink())) {
                        YDB_LOG_INFO("Skipping PQ source rescaling: query uses deferred publication");
                        return;
                    }
                }
                if (hasState(stage.GetProgram())) {
                    YDB_LOG_INFO("Skipping PQ source rescaling: query contains stateful operators");
                    return;
                }
            }
        }
        for (const auto& task : graph.GetTasks()) {
            const auto& dqTask = task.GetDqTask();
            for (const auto& output : dqTask.GetOutputs()) {
                if (output.HasSink() && hasDeferredPublication(output.GetSink())) {
                    YDB_LOG_INFO("Skipping PQ source rescaling: saved task uses deferred publication");
                    return;
                }
            }
            if (!dqTask.GetProgram().GetRaw().empty()) {
                if (hasState(dqTask.GetProgram())) {
                    YDB_LOG_INFO("Skipping PQ source rescaling: inline program contains stateful operators");
                    return;
                }
            } else {
                YQL_ENSURE(task.GetTxId() < static_cast<ui64>(physQuery.TransactionsSize()), "Missing task transaction");
                const auto& tx = physQuery.GetTransactions(task.GetTxId());
                YQL_ENSURE(dqTask.GetStageId() < static_cast<ui64>(tx.StagesSize()), "Missing task stage");
                // The shared program was checked above.
            }
        }
    } catch (const std::exception& e) {
        YDB_LOG_INFO("Skipping PQ source rescaling: query analysis failed", {"error", e.what()});
        return;
    }

    // Helper to encode a pair of stage keys as a string for use as hash-map keys
    // where THashMap<TStageKey, THashMap<TStageKey,...>> would be needed.
    auto stageKeyToStr = [](const TStageKey& sk) -> TString {
        return TStringBuilder() << sk.TxId << ":" << sk.StageId;
    };
    auto connKey = [&](const TStageKey& src, const TStageKey& dst, ui32 inputIdx) -> TString {
        return stageKeyToStr(src) + "->" + stageKeyToStr(dst) + ":" + ToString(inputIdx);
    };

    // Phase 1: Build stage→task index map, find max IDs
    THashMap<TStageKey, TVector<int>> stageToTaskIndices;
    ui64 maxTaskId = 0;
    ui64 maxChannelId = 0;

    for (size_t i = 0; i < graph.TasksSize(); ++i) {
        const auto& task = graph.GetTasks(i);
        TStageKey key{task.GetTxId(), task.GetDqTask().GetStageId()};
        stageToTaskIndices[key].push_back(i);
        maxTaskId = Max(maxTaskId, task.GetDqTask().GetId());
        for (const auto& input : task.GetDqTask().GetInputs()) {
            for (const auto& ch : input.GetChannels()) {
                maxChannelId = Max(maxChannelId, ch.GetId());
            }
        }
        for (const auto& output : task.GetDqTask().GetOutputs()) {
            for (const auto& ch : output.GetChannels()) {
                maxChannelId = Max(maxChannelId, ch.GetId());
            }
        }
    }

    YDB_LOG_INFO("Indexed stages for PQ source rescaling",
        {"stageCount", stageToTaskIndices.size()}, {"maxTaskId", maxTaskId}, {"maxChannelId", maxChannelId});

    // Phase 2: Identify PQ source stages
    THashSet<TStageKey> pqSourceStages;
    for (size_t txIdx = 0; txIdx < physQuery.TransactionsSize(); ++txIdx) {
        const auto& tx = physQuery.GetTransactions(txIdx);
        for (size_t stageIdx = 0; stageIdx < tx.StagesSize(); ++stageIdx) {
            const auto& stage = tx.GetStages(stageIdx);
            if (stage.SourcesSize() > 0) {
                const auto& src = stage.GetSources(0);
                if (src.GetTypeCase() == NKqpProto::TKqpSource::kExternalSource &&
                    src.GetExternalSource().GetType() == NYql::PqSource) {
                    pqSourceStages.insert(TStageKey{(ui64)txIdx, (ui32)stageIdx});
                    YDB_LOG_DEBUG("Found PQ source stage for rescaling",
                        {"txId", txIdx}, {"stageId", stageIdx});
                }
            }
        }
    }

    YDB_LOG_INFO("Identified PQ source stages for rescaling", {"stageCount", pqSourceStages.size()});
    if (pqSourceStages.empty()) {
        YDB_LOG_INFO("Skipping PQ source rescaling: no PQ source stages");
        return;
    }

    // Compute partitions per stage from the saved tasks.
    // The true partition count for a PQ task is the number of topic partitions it
    // would read, computed the same way the read actor does:
    //   GetPartitionsToRead(ExtractReadTaskParams({}, readRanges), {})
    THashMap<TStageKey, ui32> stagePartitionCounts;
    THashMap<TStageKey, TVector<NPq::NProto::TDqReadTaskParams::TPartitioningParams>> stagePartitions;
    for (size_t i = 0; i < graph.TasksSize(); ++i) {
        YDB_LOG_DEBUG("Inspecting task for PQ source rescaling", {"taskIndex", i});

        const auto& task = graph.GetTasks(i);
        TStageKey sk{task.GetTxId(), task.GetDqTask().GetStageId()};
        if (!pqSourceStages.contains(sk)) {
            continue;
        }

        const auto& dqTask = task.GetDqTask();

        TVector<TString> readRanges(dqTask.GetReadRanges().begin(), dqTask.GetReadRanges().end());
        const auto readTaskParams = NYql::NDq::ExtractReadTaskParams({}, readRanges);
        // Expand ranges before changing the graph. A selected partition count
        // is not the topic size: pruning may select noncontiguous partition IDs.
        for (const auto& params : readTaskParams) {
            for (const auto& pp : params.GetPartitioningParams()) {
                const ui64 topicCount = pp.GetTopicPartitionsCount();
                const ui64 step = pp.GetDqPartitionsCount();
                YQL_ENSURE(step > 0, "Invalid PQ partition stride");
                for (ui64 partition = pp.GetEachTopicPartitionGroupId(); partition < topicCount;) {
                    auto singleton = pp;
                    singleton.SetEachTopicPartitionGroupId(partition);
                    singleton.SetDqPartitionsCount(topicCount);
                    stagePartitions[sk].push_back(std::move(singleton));
                    if (step >= topicCount - partition) {
                        break;
                    }
                    partition += step;
                }
            }
        }
        const auto partitionKeys = NYql::NDq::GetPartitionsToRead(readTaskParams, {});
        ui32 taskPartitions = (ui32)partitionKeys.size();

        YDB_LOG_DEBUG("Collected PQ task partitions for rescaling",
            {"stageId", stageKeyToStr(sk)}, {"taskId", dqTask.GetId()},
            {"readRangesCount", dqTask.ReadRangesSize()}, {"partitionCount", taskPartitions});
        for (const auto& readTaskParam : readTaskParams) {
            for (size_t ppIdx = 0; ppIdx < readTaskParam.PartitioningParamsSize(); ++ppIdx) {
                const auto& pp = readTaskParam.GetPartitioningParams(ppIdx);
                YDB_LOG_DEBUG("PQ task partitioning parameters before rescaling",
                    {"taskId", dqTask.GetId()}, {"paramsIndex", ppIdx},
                    {"dqPartitionsCount", pp.GetDqPartitionsCount()},
                    {"topicPartitionsCount", pp.GetTopicPartitionsCount()},
                    {"eachTopicPartitionGroupId", pp.GetEachTopicPartitionGroupId()});
            }
        }

        stagePartitionCounts[sk] += taskPartitions;
    }

    YDB_LOG_INFO("Computing PQ source task counts for rescaling");

    // Preserve the saved query and stage limits. As in CountReadTasksFromSource,
    // the thread-based limit is only a fallback when there is no task count hint.
    const ui64 tasksByThreads = static_cast<ui64>(TStagePredictor::GetUsableThreads()) * resourceSnapshot.size();
    THashMap<TStageKey, ui32> newTaskCounts;
    for (const auto& sk : pqSourceStages) {

        YDB_LOG_DEBUG("Computing PQ source stage task count", {"stageId", stageKeyToStr(sk)});

        ui32 partitions = stagePartitionCounts.Value(sk, 0);
        if (partitions == 0) {
            YDB_LOG_INFO("Skipping PQ source stage without partitions", {"stageId", stageKeyToStr(sk)});
            continue; // No partitions → skip
        }

        const auto maxTasksPerStage = physQuery.GetMaxTasksPerStage();
        const ui64 tasksByPartitions = NYql::NDq::GetExpectedTopicReadTasks(
            partitions,
            maxTasksPerStage,
            /* groupPartitions */ !maxTasksPerStage);
        const auto& stage = physQuery.GetTransactions(sk.TxId).GetStages(sk.StageId);
        const ui32 newCount = std::min<ui64>(tasksByPartitions,
            stage.GetTaskCount() ? stage.GetTaskCount() : tasksByThreads);

        // Only scale up: preserve existing tasks and their checkpoint identities.
        auto it2 = stageToTaskIndices.find(sk);
        ui32 current = it2 != stageToTaskIndices.end() ? (ui32)it2->second.size() : 0;
        if (newCount > current) {
            newTaskCounts[sk] = newCount;
        }
    }

    YDB_LOG_INFO("Selected PQ source stages for rescaling", {"stageCount", newTaskCounts.size()});
    if (newTaskCounts.empty()) {
        YDB_LOG_INFO("Skipping PQ source rescaling: no task count increases");
        return;
    }

    // Phase 3: BFS from PQ source stages through Map connections.
    // rescaleMap[stageKey] = new task count for that stage.
    // Seeded from newTaskCounts computed above.
    THashMap<TStageKey, ui32> rescaleMap;

    TQueue<TStageKey> bfsQueue;
    for (const auto& sk : pqSourceStages) {
        auto newCountIt = newTaskCounts.find(sk);
        if (newCountIt == newTaskCounts.end()) {
            YDB_LOG_DEBUG("Skipping unchanged PQ source stage in rescaling traversal",
                {"stageId", stageKeyToStr(sk)});
            continue; // not in the rescale set
        }
        YDB_LOG_INFO("Seeding PQ source rescaling traversal",
            {"stageId", stageKeyToStr(sk)}, {"newTaskCount", newCountIt->second});
        rescaleMap[sk] = newCountIt->second;
        bfsQueue.push(sk);
    }

    while (!bfsQueue.empty()) {
        TStageKey srcKey = bfsQueue.front();
        bfsQueue.pop();

        const auto& srcTx = physQuery.GetTransactions(srcKey.TxId);
        for (size_t dstStageIdx = 0; dstStageIdx < srcTx.StagesSize(); ++dstStageIdx) {
            const auto& dstStage = srcTx.GetStages(dstStageIdx);
            for (size_t inputIdx = 0; inputIdx < dstStage.InputsSize(); ++inputIdx) {
                const auto& conn = dstStage.GetInputs(inputIdx);
                if (conn.GetStageIndex() != srcKey.StageId) continue;

                TStageKey dstKey{srcKey.TxId, (ui32)dstStageIdx};

                // Cascade rescaling through 1-to-1 connections.
                if (conn.GetTypeCase() == NKqpProto::TKqpPhyConnection::kMap ||
                    conn.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    if (rescaleMap.find(dstKey) == rescaleMap.end()) {
                        rescaleMap[dstKey] = rescaleMap.at(srcKey);
                        bfsQueue.push(dstKey);
                    }
                }
            }
        }
    }

    YDB_LOG_INFO("Completed PQ source rescaling traversal",
        {"stageCount", rescaleMap.size()});
    if (rescaleMap.empty()) {
        YDB_LOG_INFO("Skipping PQ source rescaling: no stages after traversal");
        return;
    }

    // Rebuild every connection incident to a rescaled stage. In particular, this
    // includes inputs coming from branches that were not visited by the downstream
    // BFS. Connections unrelated to rescaled stages must remain untouched.
    struct TConnectionInfo {
        TStageKey SrcStage;
        TStageKey DstStage;
        NKqpProto::TKqpPhyConnection::TypeCase ConnType;
        ui32 OutputIndex;
        ui32 InputIndex;
    };
    TVector<TConnectionInfo> connectionsToRebuild;
    THashSet<TString> seenConnections; // encoded as "src->dst:inputIdx"
    const auto getFinalTaskCount = [&](const TStageKey& stageKey) -> ui32 {
        if (const auto it = rescaleMap.find(stageKey); it != rescaleMap.end()) {
            return it->second;
        }
        if (const auto it = stageToTaskIndices.find(stageKey); it != stageToTaskIndices.end()) {
            return static_cast<ui32>(it->second.size());
        }
        return 0;
    };
    for (size_t txIdx = 0; txIdx < physQuery.TransactionsSize(); ++txIdx) {
        const auto& tx = physQuery.GetTransactions(txIdx);
        for (size_t dstStageIdx = 0; dstStageIdx < tx.StagesSize(); ++dstStageIdx) {
            const auto& dstStage = tx.GetStages(dstStageIdx);
            for (size_t inputIdx = 0; inputIdx < dstStage.InputsSize(); ++inputIdx) {
                const auto& conn = dstStage.GetInputs(inputIdx);
                const TStageKey srcKey{txIdx, conn.GetStageIndex()};
                const TStageKey dstKey{txIdx, dstStageIdx};
                if (!rescaleMap.contains(srcKey) && !rescaleMap.contains(dstKey)) {
                    continue;
                }

                const TString ck = connKey(srcKey, dstKey, inputIdx);
                if (!seenConnections.insert(ck).second) {
                    continue;
                }

                const auto connType = conn.GetTypeCase();
                const bool supported =
                    connType == NKqpProto::TKqpPhyConnection::kUnionAll ||
                    connType == NKqpProto::TKqpPhyConnection::kMerge ||
                    connType == NKqpProto::TKqpPhyConnection::kMap ||
                    connType == NKqpProto::TKqpPhyConnection::kStreamLookup ||
                    connType == NKqpProto::TKqpPhyConnection::kHashShuffle ||
                    connType == NKqpProto::TKqpPhyConnection::kBroadcast ||
                    connType == NKqpProto::TKqpPhyConnection::kParallelUnionAll;
                if (!supported) {
                    // Keep the original graph intact and restart without rescaling.
                    return;
                }

                if (connType == NKqpProto::TKqpPhyConnection::kMap ||
                    connType == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    const ui32 srcTaskCount = getFinalTaskCount(srcKey);
                    const ui32 dstTaskCount = getFinalTaskCount(dstKey);
                    YQL_ENSURE(srcTaskCount == dstTaskCount,
                        "Task count mismatch on one-to-one connection while rescaling PQ source: "
                        << stageKeyToStr(srcKey) << " (" << srcTaskCount << ") -> "
                        << stageKeyToStr(dstKey) << " (" << dstTaskCount << ")");
                }

                connectionsToRebuild.push_back(TConnectionInfo{
                    .SrcStage = srcKey,
                    .DstStage = dstKey,
                    .ConnType = connType,
                    .OutputIndex = conn.GetOutputIndex(),
                    .InputIndex = conn.GetInputIndex(),
                });
            }
        }
    }

    // Phase 4: Collect channel metadata (InMemory, checkpointing, watermarks) from existing connections.
    // ReadRanges are redistributed in Phase 9 using the partitions saved in Phase 2.
    struct TChannelMeta {
        bool InMemory = true;
        NYql::NDqProto::ECheckpointingMode CheckpointingMode = NYql::NDqProto::CHECKPOINTING_MODE_DISABLED;
        NYql::NDqProto::EWatermarksMode WatermarksMode = NYql::NDqProto::WATERMARKS_MODE_DISABLED;
    };
    THashMap<TString, TChannelMeta> connMeta; // key: "src->dst:inputIdx"

    for (const auto& ci : connectionsToRebuild) {
        const TString mk = connKey(ci.SrcStage, ci.DstStage, ci.InputIndex);
        if (connMeta.count(mk)) continue;
        auto srcIt = stageToTaskIndices.find(ci.SrcStage);
        if (srcIt == stageToTaskIndices.end() || srcIt->second.empty()) continue;
        const auto& dqTask = graph.GetTasks(srcIt->second[0]).GetDqTask();
        if (ci.OutputIndex < (ui32)dqTask.OutputsSize()) {
            const auto& output = dqTask.GetOutputs(ci.OutputIndex);
            for (const auto& ch : output.GetChannels()) {
                if (ch.GetSrcStageId() != ci.SrcStage.StageId ||
                    ch.GetDstStageId() != ci.DstStage.StageId) {
                    continue;
                }
                connMeta[mk] = TChannelMeta{
                    .InMemory = ch.GetInMemory(),
                    .CheckpointingMode = ch.GetCheckpointingMode(),
                    .WatermarksMode = ch.GetWatermarksMode(),
                };
                break;
            }
        }
    }

    YDB_LOG_INFO("Collected channel metadata for PQ source rescaling", {"connectionCount", connMeta.size()});

    // Phase 5: Determine which tasks to remove (excess tasks at end of scaled-down stages)
    // and which new tasks to clone (scaled-up stages)
    THashSet<int> taskIndicesToRemove;
    TVector<std::pair<ui64, NYql::NDqProto::TDqTask>> newTasks; // (txId, cloned DqTask)

    for (const auto& [sk, newCount] : rescaleMap) {
        auto it = stageToTaskIndices.find(sk);
        if (it == stageToTaskIndices.end()) continue;
        const auto& taskIndices = it->second;
        ui32 currentCount = (ui32)taskIndices.size();

        if (newCount < currentCount) {
            for (ui32 i = newCount; i < currentCount; ++i) {
                taskIndicesToRemove.insert(taskIndices[i]);
            }
        } else if (newCount > currentCount && !taskIndices.empty()) {
            const auto& templateDqTask = graph.GetTasks(taskIndices[0]).GetDqTask();
            for (ui32 i = currentCount; i < newCount; ++i) {
                NYql::NDqProto::TDqTask clone;
                clone.CopyFrom(templateDqTask);
                clone.SetId(++maxTaskId);
                for (auto& input : *clone.MutableInputs()) {
                    input.ClearChannels();
                }
                for (auto& output : *clone.MutableOutputs()) {
                    output.ClearChannels();
                }
                clone.ClearReadRanges();
                newTasks.emplace_back(sk.TxId, std::move(clone));
            }
        }
    }

    YDB_LOG_INFO("Prepared task changes for PQ source rescaling",
        {"removedTaskCount", taskIndicesToRemove.size()}, {"newTaskCount", newTasks.size()});

    auto removeConnectionChannels = [](auto* channels, const TConnectionInfo& ci) {
        for (int i = channels->size() - 1; i >= 0; --i) {
            const auto& channel = channels->Get(i);
            if (channel.GetSrcStageId() == ci.SrcStage.StageId &&
                channel.GetDstStageId() == ci.DstStage.StageId) {
                channels->DeleteSubrange(i, 1);
            }
        }
    };

    // Phase 6: Rebuild task list. Remove only channels belonging to connections
    // incident to rescaled stages; preserve every unrelated input and output.
    TVector<NKikimrKqp::TQueryPhysicalGraph::TTask> finalTasks;
    finalTasks.reserve(graph.TasksSize() - (int)taskIndicesToRemove.size() + (int)newTasks.size());

    for (size_t i = 0; i < graph.TasksSize(); ++i) {
        if (taskIndicesToRemove.count(i)) continue;
        auto taskCopy = graph.GetTasks(i);
        TStageKey sk{taskCopy.GetTxId(), taskCopy.GetDqTask().GetStageId()};
        for (const auto& ci : connectionsToRebuild) {
            if (sk == ci.SrcStage && ci.OutputIndex < (ui32)taskCopy.GetDqTask().OutputsSize()) {
                removeConnectionChannels(
                    taskCopy.MutableDqTask()->MutableOutputs(ci.OutputIndex)->MutableChannels(), ci);
            }
            if (sk == ci.DstStage && ci.InputIndex < (ui32)taskCopy.GetDqTask().InputsSize()) {
                removeConnectionChannels(
                    taskCopy.MutableDqTask()->MutableInputs(ci.InputIndex)->MutableChannels(), ci);
            }
        }
        finalTasks.push_back(std::move(taskCopy));
    }
    for (auto& [txId, dqTask] : newTasks) {
        NKikimrKqp::TQueryPhysicalGraph::TTask t;
        t.SetTxId(txId);
        *t.MutableDqTask() = std::move(dqTask);
        finalTasks.push_back(std::move(t));
    }

    graph.MutableTasks()->Clear();
    for (auto& t : finalTasks) {
        *graph.AddTasks() = std::move(t);
    }

    YDB_LOG_INFO("Rebuilt task list for PQ source rescaling", {"taskCount", finalTasks.size()});

    // Phase 7: Rebuild stage→task index mapping after modifications
    THashMap<TStageKey, TVector<int>> newStageToTaskIndices;
    for (size_t i = 0; i < graph.TasksSize(); ++i) {
        const auto& task = graph.GetTasks(i);
        TStageKey sk{task.GetTxId(), task.GetDqTask().GetStageId()};
        newStageToTaskIndices[sk].push_back(i);
    }

    YDB_LOG_INFO("Reindexed stages after PQ source rescaling", {"stageCount", newStageToTaskIndices.size()});

    // Phase 8: Rebuild channels for affected connections
    auto makeChannel = [&](ui64 chId, ui32 srcStageIdx, ui32 dstStageIdx,
                           ui64 srcTaskId, ui64 dstTaskId,
                           const TChannelMeta& meta) -> NYql::NDqProto::TChannel {
        NYql::NDqProto::TChannel ch;
        ch.SetId(chId);
        ch.SetSrcStageId(srcStageIdx);
        ch.SetDstStageId(dstStageIdx);
        ch.SetSrcTaskId(srcTaskId);
        ch.SetDstTaskId(dstTaskId);
        ch.SetInMemory(meta.InMemory);
        ch.SetCheckpointingMode(meta.CheckpointingMode);
        ch.SetWatermarksMode(meta.WatermarksMode);
        return ch;
    };

    for (const auto& ci : connectionsToRebuild) {
        const auto& srcTaskIndices = newStageToTaskIndices[ci.SrcStage];
        const auto& dstTaskIndices = newStageToTaskIndices[ci.DstStage];

        const TString mk = connKey(ci.SrcStage, ci.DstStage, ci.InputIndex);
        TChannelMeta meta;
        auto metaIt = connMeta.find(mk);
        if (metaIt != connMeta.end()) meta = metaIt->second;

        switch (ci.ConnType) {
            case NKqpProto::TKqpPhyConnection::kUnionAll:
            case NKqpProto::TKqpPhyConnection::kMerge: {
                // N→1: all src tasks send to the single dst task
                if (dstTaskIndices.empty()) break;
                auto* dstDqTask = graph.MutableTasks(dstTaskIndices[0])->MutableDqTask();
                if (ci.InputIndex >= (ui32)dstDqTask->InputsSize()) break;
                auto* dstInput = dstDqTask->MutableInputs(ci.InputIndex);

                for (int srcIdx : srcTaskIndices) {
                    auto* srcDqTask = graph.MutableTasks(srcIdx)->MutableDqTask();
                    if (ci.OutputIndex >= (ui32)srcDqTask->OutputsSize()) continue;
                    auto* srcOutput = srcDqTask->MutableOutputs(ci.OutputIndex);

                    auto ch = makeChannel(++maxChannelId, ci.SrcStage.StageId, ci.DstStage.StageId,
                                         srcDqTask->GetId(), dstDqTask->GetId(), meta);
                    *srcOutput->AddChannels() = ch;
                    *dstInput->AddChannels() = std::move(ch);
                }
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap:
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                // 1-to-1: equal counts guaranteed by BFS cascade
                for (size_t i = 0; i < srcTaskIndices.size() && i < dstTaskIndices.size(); ++i) {
                    auto* srcDqTask = graph.MutableTasks(srcTaskIndices[i])->MutableDqTask();
                    auto* dstDqTask = graph.MutableTasks(dstTaskIndices[i])->MutableDqTask();
                    if (ci.OutputIndex >= (ui32)srcDqTask->OutputsSize()) continue;
                    if (ci.InputIndex >= (ui32)dstDqTask->InputsSize()) continue;

                    auto ch = makeChannel(++maxChannelId, ci.SrcStage.StageId, ci.DstStage.StageId,
                                         srcDqTask->GetId(), dstDqTask->GetId(), meta);
                    *srcDqTask->MutableOutputs(ci.OutputIndex)->AddChannels() = ch;
                    *dstDqTask->MutableInputs(ci.InputIndex)->AddChannels() = std::move(ch);
                }
                break;
            }
            case NKqpProto::TKqpPhyConnection::kBroadcast:
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                // N×M: each src task sends to all dst tasks
                ui32 numDst = (ui32)dstTaskIndices.size();
                for (int srcIdx : srcTaskIndices) {
                    auto* srcDqTask = graph.MutableTasks(srcIdx)->MutableDqTask();
                    if (ci.OutputIndex >= (ui32)srcDqTask->OutputsSize()) continue;
                    auto* srcOutput = srcDqTask->MutableOutputs(ci.OutputIndex);
                    if (ci.ConnType == NKqpProto::TKqpPhyConnection::kHashShuffle && srcOutput->HasHashPartition()) {
                        srcOutput->MutableHashPartition()->SetPartitionsCount(numDst);
                    }
                    for (int dstIdx : dstTaskIndices) {
                        auto* dstDqTask = graph.MutableTasks(dstIdx)->MutableDqTask();
                        if (ci.InputIndex >= (ui32)dstDqTask->InputsSize()) continue;
                        auto ch = makeChannel(++maxChannelId, ci.SrcStage.StageId, ci.DstStage.StageId,
                                             srcDqTask->GetId(), dstDqTask->GetId(), meta);
                        *srcOutput->AddChannels() = ch;
                        *dstDqTask->MutableInputs(ci.InputIndex)->AddChannels() = std::move(ch);
                    }
                }
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                // N→M round-robin: each src task → one dst task
                ui32 numDst = (ui32)dstTaskIndices.size();
                if (numDst == 0) break;
                ui32 roundRobinIdx = 0;
                for (int srcIdx : srcTaskIndices) {
                    auto* srcDqTask = graph.MutableTasks(srcIdx)->MutableDqTask();
                    if (ci.OutputIndex >= (ui32)srcDqTask->OutputsSize()) continue;
                    auto* srcOutput = srcDqTask->MutableOutputs(ci.OutputIndex);

                    int dstIdx = dstTaskIndices[roundRobinIdx % numDst];
                    ++roundRobinIdx;
                    auto* dstDqTask = graph.MutableTasks(dstIdx)->MutableDqTask();
                    if (ci.InputIndex >= (ui32)dstDqTask->InputsSize()) continue;

                    auto ch = makeChannel(++maxChannelId, ci.SrcStage.StageId, ci.DstStage.StageId,
                                         srcDqTask->GetId(), dstDqTask->GetId(), meta);
                    *srcOutput->AddChannels() = ch;
                    *dstDqTask->MutableInputs(ci.InputIndex)->AddChannels() = std::move(ch);
                }
                break;
            }
            default:
                YQL_ENSURE(false,
                    "Unsupported connection type while rescaling PQ source: " << static_cast<ui32>(ci.ConnType));
        }
    }

    YDB_LOG_INFO("Rebuilt channels for PQ source rescaling", {"connectionCount", connectionsToRebuild.size()});

    // RestoreTasksGraphInfo() recreates runtime channels by appending them in
    // their serialized-id order and checks that the generated id matches the
    // one in the physical graph.  Removing channels of rescaled connections
    // leaves holes in the original id sequence, while rebuilt channels are
    // allocated above the old maximum.  Compact the ids after rebuilding so
    // every channel can be addressed through TDqTasksGraph::GetChannel(id).
    THashMap<ui64, ui64> channelIdRemapping;
    ui64 nextChannelId = 0;
    for (const auto& task : graph.GetTasks()) {
        for (const auto& output : task.GetDqTask().GetOutputs()) {
            for (const auto& channel : output.GetChannels()) {
                channelIdRemapping.emplace(channel.GetId(), ++nextChannelId);
            }
        }
    }
    for (auto& task : *graph.MutableTasks()) {
        for (auto& input : *task.MutableDqTask()->MutableInputs()) {
            for (auto& channel : *input.MutableChannels()) {
                const auto it = channelIdRemapping.find(channel.GetId());
                YQL_ENSURE(it != channelIdRemapping.end(),
                    "Input channel " << channel.GetId() << " has no output counterpart after rescaling");
                channel.SetId(it->second);
            }
        }
        for (auto& output : *task.MutableDqTask()->MutableOutputs()) {
            for (auto& channel : *output.MutableChannels()) {
                const auto it = channelIdRemapping.find(channel.GetId());
                YQL_ENSURE(it != channelIdRemapping.end(),
                    "Output channel " << channel.GetId() << " has no remapped id after rescaling");
                channel.SetId(it->second);
            }
        }
    }

    YDB_LOG_INFO("Compacted channel IDs after PQ source rescaling", {"channelCount", nextChannelId});

    // Phase 9: Redistribute the actual selected partitions, preserving pruning.
    // Leave ranges of unchanged PQ stages untouched.
    for (const auto& sk : pqSourceStages) {
        if (!rescaleMap.contains(sk)) continue;
        auto it = newStageToTaskIndices.find(sk);
        if (it == newStageToTaskIndices.end() || it->second.empty()) continue;
        const auto& taskIndices = it->second;
        const auto& partitions = stagePartitions.at(sk);

        for (size_t i = 0; i < taskIndices.size(); ++i) {
            int taskIdx = taskIndices[i];
            auto* dqTask = graph.MutableTasks(taskIdx)->MutableDqTask();
            dqTask->ClearReadRanges();

            NPq::NProto::TDqReadTaskParams params;
            for (size_t partitionIdx = i; partitionIdx < partitions.size(); partitionIdx += taskIndices.size()) {
                *params.AddPartitioningParams() = partitions[partitionIdx];
            }

            TString serialized;
            YQL_ENSURE(params.SerializeToString(&serialized), "Failed to serialize TDqReadTaskParams");
            *dqTask->AddReadRanges() = std::move(serialized);

        }
    }
    YDB_LOG_INFO("Completed PQ source rescaling and read range redistribution", {"taskCount", graph.TasksSize()});
}

} // namespace NKikimr::NKqp
