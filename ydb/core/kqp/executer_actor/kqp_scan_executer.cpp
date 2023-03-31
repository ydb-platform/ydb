#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_partition_helper.h"
#include "kqp_result_channel.h"
#include "kqp_tasks_graph.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

class TKqpScanExecuter : public TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan> {
    using TBase = TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
        const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
        : TBase(std::move(request), database, userToken, counters, executerRetriesConfig, TWilsonKqp::ScanExecuter, "ScanExecuter")
    {
        YQL_ENSURE(Request.Transactions.size() == 1);
        YQL_ENSURE(Request.DataShardLocks.empty());
        YQL_ENSURE(!Request.ValidateLocks);
        YQL_ENSURE(!Request.EraseLocks);
        YQL_ENSURE(Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);
        YQL_ENSURE(Request.Snapshot.IsValid());

        size_t resultsSize = Request.Transactions[0].Body->ResultsSize();
        YQL_ENSURE(resultsSize != 0);

        bool streamResult = Request.Transactions[0].Body->GetResults(0).GetIsStream();

        if (streamResult) {
            YQL_ENSURE(resultsSize == 1);
        } else {
            for (size_t i = 1; i < resultsSize; ++i) {
                YQL_ENSURE(Request.Transactions[0].Body->GetResults(i).GetIsStream() == streamResult);
            }
        }
    }

public:
    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvPrivate::TEvResourcesSnapshot, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvWakeup, HandleTimeout);
                default:
                    UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
            }

        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

private:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDqCompute::TEvState, HandleComputeStats);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvWakeup, HandleTimeout);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                IgnoreFunc(TEvKqpNode::TEvCancelKqpTasksResponse);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        LOG_T("Recv stream data ack, seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << ev->Get()->Record.GetFreeSpace()
            << ", enough: " << ev->Get()->Record.GetEnough()
            << ", from: " << ev->Sender);

        if (ResultChannelProxies.empty()) {
            return;
        }

        // Forward only for stream results, data results acks event theirselves.
        YQL_ENSURE(!ResponseEv->TxResults.empty() && ResponseEv->TxResults[0].IsStream);

        auto channelIt = ResultChannelProxies.begin();
        auto handle = ev->Forward(channelIt->second->SelfId());
        channelIt->second->Receive(handle, TlsActivationContext->AsActorContext());
    }

private:
    void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, bool reverse, bool sorted,
        NKikimr::NMiniKQL::TType* resultType, const TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>& readOlapRange)
    {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
            YQL_ENSURE(taskMeta.ReadInfo.Reverse == reverse);

            if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
                YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program.empty());
                return;
            }

            YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program == readOlapRange->GetOlapProgram());
            return;
        }

        taskMeta.ReadInfo.ItemsLimit = itemsLimit;
        taskMeta.ReadInfo.Reverse = reverse;
        taskMeta.ReadInfo.Sorted = sorted;

        if (resultType) {
            YQL_ENSURE(resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct
                || resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);

            auto* resultStructType = static_cast<NKikimr::NMiniKQL::TStructType*>(resultType);
            ui32 resultColsCount = resultStructType->GetMembersCount();

            taskMeta.ReadInfo.ResultColumnsTypes.reserve(resultColsCount);
            for (ui32 i = 0; i < resultColsCount; ++i) {
                auto memberType = resultStructType->GetMemberType(i);
                if (memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional) {
                    memberType = static_cast<NKikimr::NMiniKQL::TOptionalType*>(memberType)->GetItemType();
                }
                // TODO: support pg types
                YQL_ENSURE(memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data,
                    "Expected simple data types to be read from column shard");
                auto memberDataType = static_cast<NKikimr::NMiniKQL::TDataType*>(memberType);
                taskMeta.ReadInfo.ResultColumnsTypes.push_back(NScheme::TTypeInfo(memberDataType->GetSchemeType()));
            }
        }

        if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
            return;
        }

        taskMeta.ReadInfo.OlapProgram.Program = readOlapRange->GetOlapProgram();

        for (auto& name: readOlapRange->GetOlapProgramParameterNames()) {
            taskMeta.ReadInfo.OlapProgram.ParameterNames.insert(name);
        }
    };

    static ui32 GetMaxTasksPerNodeEstimate(TStageInfo& stageInfo) {
        // TODO: take into account number of active scans on node
        const auto& stage = GetStage(stageInfo);
        const bool heavyProgram = stage.GetProgram().GetSettings().GetHasSort() ||
                            stage.GetProgram().GetSettings().GetHasMapJoin();

        if (heavyProgram) {
            return 4;
        } else {
            return 16;
        }
    }

    TTask& AssignTaskToShard(
        TStageInfo& stageInfo, const ui64 shardId,
        THashMap<ui64, std::vector<ui64>>& nodeTasks,
        THashMap<ui64, ui64>& assignedShardsCount,
        const bool sorted, const bool isOlapScan)
    {
        ui64 nodeId = ShardIdToNodeId.at(shardId);
        if (stageInfo.Meta.IsOlap() && sorted) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.NodeId = nodeId;
            return task;
        }

        auto& tasks = nodeTasks[nodeId];
        auto& cnt = assignedShardsCount[nodeId];

        const ui32 maxScansPerNode = isOlapScan ? 1 : GetMaxTasksPerNodeEstimate(stageInfo);
        if (cnt < maxScansPerNode) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.NodeId = nodeId;
            tasks.push_back(task.Id);
            ++cnt;
            return task;
        } else {
            ui64 taskIdx = cnt % maxScansPerNode;
            ++cnt;
            return TasksGraph.GetTask(tasks[taskIdx]);
        }
    }

    void BuildScanTasks(TStageInfo& stageInfo) {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, ui64> assignedShardsCount;

        auto& stage = GetStage(stageInfo);

        const auto& table = TableKeys.GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto columns = BuildKqpColumns(op, table);
            auto partitions = PrunePartitions(TableKeys, op, stageInfo, HolderFactory(), TypeEnv());
            const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
            auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

            if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
                stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(),
                                                   op.GetReadRange().GetSkipNullKeys().end());
                // not supported for scan queries
                YQL_ENSURE(!readSettings.Reverse);
            }

            for (auto& [shardId, shardInfo] : partitions) {
                YQL_ENSURE(!shardInfo.KeyWriteRanges);

                auto& task = AssignTaskToShard(stageInfo, shardId, nodeTasks, assignedShardsCount, readSettings.Sorted, isOlapScan);

                for (auto& [name, value] : shardInfo.Params) {
                    auto ret = task.Meta.Params.emplace(name, std::move(value));
                    YQL_ENSURE(ret.second);
                }

                TTaskMeta::TShardReadInfo readInfo = {
                    .Ranges = std::move(*shardInfo.KeyReadRanges), // sorted & non-intersecting
                    .Columns = columns,
                    .ShardId = shardId,
                };

                if (readSettings.ItemsLimitParamName && !task.Meta.Params.contains(readSettings.ItemsLimitParamName)) {
                    task.Meta.Params.emplace(readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes);
                }

                if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                    const auto& readRange = op.GetReadOlapRange();
                    FillReadInfo(task.Meta, readSettings.ItemsLimit, readSettings.Reverse, readSettings.Sorted, readSettings.ResultType, readRange);
                } else {
                    FillReadInfo(task.Meta, readSettings.ItemsLimit, readSettings.Reverse, readSettings.Sorted, nullptr, TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>());
                }

                if (!task.Meta.Reads) {
                    task.Meta.Reads.ConstructInPlace();
                }

                task.Meta.Reads->emplace_back(std::move(readInfo));
            }
        }

        LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");

        for (const auto& pair : nodeTasks) {
            for (const auto& taskIdx : pair.second) {
                auto& task = TasksGraph.GetTask(taskIdx);
                YQL_ENSURE(task.Meta.Reads.Defined());
                auto& taskReads = task.Meta.Reads.GetRef();

                /*
                 * Sort read ranges so that sequential scan of that ranges produce sorted result.
                 *
                 * Partition pruner feed us with set of non-intersecting ranges with filled right boundary.
                 * So we may sort ranges based solely on the their rightmost point.
                 */
                std::sort(taskReads.begin(), taskReads.end(), [&](const auto& lhs, const auto& rhs){
                    if (lhs.ShardId == rhs.ShardId)
                        return false;

                    const std::pair<const TSerializedCellVec*, bool> k1 = lhs.Ranges.GetRightBorder();
                    const std::pair<const TSerializedCellVec*, bool> k2 = rhs.Ranges.GetRightBorder();

                    const int cmp = CompareBorders<false, false>(
                        k1.first->GetCells(),
                        k2.first->GetCells(),
                        k1.second,
                        k2.second,
                        keyTypes);

                    return (cmp < 0);
                });

                LOG_D("Stage " << stageInfo.Id << " create datashard scan task: " << taskIdx
                    << ", node: " << pair.first
                    << ", meta: " << task.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
            }
        }
    }

    void BuildComputeTasks(TStageInfo& stageInfo) {
        auto& stage = GetStage(stageInfo);

        ui32 partitionsCount = 1;
        for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
            const auto& input = stage.GetInputs(inputIndex);

            // Current assumptions:
            // 1. `Broadcast` can not be the 1st stage input unless it's a single input
            // 2. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll`
            if (inputIndex == 0) {
                if (stage.InputsSize() > 1) {
                    YQL_ENSURE(input.GetTypeCase() != NKqpProto::TKqpPhyConnection::kBroadcast);
                }
            } else {
                switch (input.GetTypeCase()) {
                    case NKqpProto::TKqpPhyConnection::kBroadcast:
                    case NKqpProto::TKqpPhyConnection::kHashShuffle:
                    case NKqpProto::TKqpPhyConnection::kUnionAll:
                    case NKqpProto::TKqpPhyConnection::kMerge:
                    case NKqpProto::TKqpPhyConnection::kStreamLookup:
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase());
                }
            }

            auto& originStageInfo = TasksGraph.GetStageInfo(TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                    partitionsCount = std::max(partitionsCount, (ui32)originStageInfo.Tasks.size() / 2);
                    ui32 nodes = ShardsOnNode.size();
                    if (nodes) {
                        // <= 2 tasks on node
                        partitionsCount = std::min(partitionsCount, std::min(24u, nodes * 2));
                    } else {
                        partitionsCount = std::min(partitionsCount, 24u);
                    }
                    break;
                }

                case NKqpProto::TKqpPhyConnection::kMap:
                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                    partitionsCount = originStageInfo.Tasks.size();
                    break;

                default:
                    break;
            }
        }

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
        }
    }

    void GetResourcesSnapshot() {
        GetKqpResourceManager()->RequestClusterResourcesInfo(
            [as = TlsActivationContext->ActorSystem(), self = SelfId()](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                TAutoPtr<IEventHandle> eh = new IEventHandle(self, self, new TEvPrivate::TEvResourcesSnapshot(std::move(resources)));
                as->Send(eh);
            });
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        TSet<ui64> shardIds;
        for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
            if (stageInfo.Meta.ShardKey) {
                for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    shardIds.insert(partition.ShardId);
                }
            }
        }
        if (shardIds) {
            LOG_D("Start resolving tablets nodes... (" << shardIds.size() << ")");
            auto kqpShardsResolver = CreateKqpShardsResolver(this->SelfId(), TxId, std::move(shardIds));
            KqpShardsResolverId = this->RegisterWithSameMailbox(kqpShardsResolver);
        } else {
            GetResourcesSnapshot();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        GetResourcesSnapshot();
    }

    void HandleResolve(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        if (ev->Get()->Snapshot.empty()) {
            LOG_E("Can not find default state storage group for database " << Database);
        }

        Execute(std::move(ev->Get()->Snapshot));
    }

    void Execute(TVector<NKikimrKqp::TKqpNodeResources>&& snapshot) {
        LWTRACK(KqpScanExecuterStartExecute, ResponseEv->Orbit, TxId);
        NWilson::TSpan prepareTasksSpan(TWilsonKqp::ScanExecuterPrepareTasks, ExecuterStateSpan.GetTraceId(), "PrepareTasks", NWilson::EFlags::AUTO_END);

        auto& tx = Request.Transactions[0];
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = TasksGraph.GetStageInfo(TStageId(0, stageIdx));

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            Y_VERIFY_DEBUG(!stage.GetIsEffectsStage());

            if (stage.SourcesSize() > 0) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource:
                        BuildScanTasksFromSource(stageInfo, Request.Snapshot);
                        break;
                    default:
                        YQL_ENSURE(false, "unknown source type");
                }
            } else if (stageInfo.Meta.ShardOperations.empty()) {
                BuildComputeTasks(stageInfo);
            } else if (stageInfo.Meta.IsSysView()) {
                BuildSysViewScanTasks(stageInfo);
            } else if (stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()) {
                BuildScanTasks(stageInfo);
            } else {
                YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
            }

            BuildKqpStageChannels(TasksGraph, TableKeys, stageInfo, TxId, AppData()->EnableKqpSpilling);
        }

        ResponseEv->InitTxResult(tx.Body);
        BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, 0);

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Scan, AppData()->EnableKqpSpilling, validateIssue)) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        // NodeId -> {Tasks}
        THashMap<ui64, TVector<NYql::NDqProto::TDqTask>> scanTasks;
        ui32 nShardScans = 0;
        ui32 nScanTasks = 0;

        TVector<NYql::NDqProto::TDqTask> computeTasks;

        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            auto& stage = GetStage(stageInfo);

            NYql::NDqProto::TDqTask taskDesc;
            taskDesc.SetId(task.Id);
            ActorIdToProto(SelfId(), taskDesc.MutableExecuter()->MutableActorId());

            for (auto& input : task.Inputs) {
                FillInputDesc(*taskDesc.AddInputs(), input);
            }

            for (auto& output : task.Outputs) {
                FillOutputDesc(*taskDesc.AddOutputs(), output);
            }

            taskDesc.MutableProgram()->CopyFrom(stage.GetProgram());
            taskDesc.SetStageId(task.StageId.StageId);

            PrepareKqpTaskParameters(stage, stageInfo, task, taskDesc, TypeEnv(), HolderFactory());

            if (task.Meta.NodeId || stageInfo.Meta.IsSysView()) {
                NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta protoTaskMeta;

                FillTableMeta(stageInfo, protoTaskMeta.MutableTable());

                const auto& tableInfo = TableKeys.GetTable(stageInfo.Meta.TableId);
                for (const auto& keyColumnName : tableInfo.KeyColumns) {
                    const auto& keyColumn = tableInfo.Columns.at(keyColumnName);
                    auto columnType = NScheme::ProtoColumnTypeFromTypeInfo(keyColumn.Type);
                    protoTaskMeta.AddKeyColumnTypes(columnType.TypeId);
                    if (columnType.TypeInfo) {
                        *protoTaskMeta.AddKeyColumnTypeInfos() = *columnType.TypeInfo;
                    } else {
                        *protoTaskMeta.AddKeyColumnTypeInfos() = NKikimrProto::TTypeInfo();
                    }
                }

                switch (tableInfo.TableKind) {
                    case ETableKind::Unknown:
                    case ETableKind::SysView: {
                        protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);
                        break;
                    }
                    case ETableKind::Datashard: {
                        if (AppData()->FeatureFlags.GetEnableArrowFormatAtDatashard()) {
                            protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::ARROW);
                        } else {
                            protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);
                        }
                        break;
                    }
                    case ETableKind::Olap: {
                        protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::ARROW);
                        break;
                    }
                }

                for (bool skipNullKey : stageInfo.Meta.SkipNullKeys) {
                    protoTaskMeta.AddSkipNullKeys(skipNullKey);
                }

                // Task with source
                if (!task.Meta.Reads) {
                    scanTasks[task.Meta.NodeId].emplace_back(std::move(taskDesc));
                    nScanTasks++;
                    continue;
                }

                YQL_ENSURE(!task.Meta.Writes);

                if (!task.Meta.Reads->empty()) {
                    protoTaskMeta.SetReverse(task.Meta.ReadInfo.Reverse);
                    protoTaskMeta.SetItemsLimit(task.Meta.ReadInfo.ItemsLimit);
                    protoTaskMeta.SetSorted(task.Meta.ReadInfo.Sorted);

                    for (auto columnType : task.Meta.ReadInfo.ResultColumnsTypes) {
                        auto* protoResultColumn = protoTaskMeta.AddResultColumns();
                        protoResultColumn->SetId(0);
                        auto protoColumnType = NScheme::ProtoColumnTypeFromTypeInfo(columnType);
                        protoResultColumn->SetType(protoColumnType.TypeId);
                        if (protoColumnType.TypeInfo) {
                            *protoResultColumn->MutableTypeInfo() = *protoColumnType.TypeInfo;
                        }
                    }

                    if (tableInfo.TableKind == ETableKind::Olap) {
                        auto* olapProgram = protoTaskMeta.MutableOlapProgram();
                        olapProgram->SetProgram(task.Meta.ReadInfo.OlapProgram.Program);

                        auto [schema, parameters] = SerializeKqpTasksParametersForOlap(stage, stageInfo, task);
                        olapProgram->SetParametersSchema(schema);
                        olapProgram->SetParameters(parameters);
                    } else {
                        YQL_ENSURE(task.Meta.ReadInfo.OlapProgram.Program.empty());
                    }

                    for (auto& column : task.Meta.Reads->front().Columns) {
                        auto* protoColumn = protoTaskMeta.AddColumns();
                        protoColumn->SetId(column.Id);
                        auto columnType = NScheme::ProtoColumnTypeFromTypeInfo(column.Type);
                        protoColumn->SetType(columnType.TypeId);
                        if (columnType.TypeInfo) {
                            *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                        }
                        protoColumn->SetName(column.Name);
                    }
                }

                for (auto& read : *task.Meta.Reads) {
                    auto* protoReadMeta = protoTaskMeta.AddReads();
                    protoReadMeta->SetShardId(read.ShardId);
                    read.Ranges.SerializeTo(protoReadMeta);

                    YQL_ENSURE((int) read.Columns.size() == protoTaskMeta.GetColumns().size());
                    for (ui64 i = 0; i < read.Columns.size(); ++i) {
                        YQL_ENSURE(read.Columns[i].Id == protoTaskMeta.GetColumns()[i].GetId());
                        YQL_ENSURE(read.Columns[i].Type.GetTypeId() == protoTaskMeta.GetColumns()[i].GetType());
                    }

                    nShardScans++;
                    if (Stats) {
                        Stats->AffectedShards.insert(read.ShardId);
                    }
                }

                LOG_D(
                    "task: " << task.Id <<
                    ", node: " << task.Meta.NodeId <<
                    ", meta: " << protoTaskMeta.ShortDebugString()
                );

                taskDesc.MutableMeta()->PackFrom(protoTaskMeta);

                if (stageInfo.Meta.IsSysView()) {
                    computeTasks.emplace_back(std::move(taskDesc));
                } else {
                    scanTasks[task.Meta.NodeId].emplace_back(std::move(taskDesc));
                    nScanTasks++;
                }
            } else {
                computeTasks.emplace_back(std::move(taskDesc));
            }
        }

        if (computeTasks.size() + nScanTasks > Request.MaxComputeActors) {
            LOG_N("Too many compute actors: computeTasks=" << computeTasks.size() << ", scanTasks=" << nScanTasks);
            TBase::ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Requested too many execution units: " << (computeTasks.size() + nScanTasks)));
            return;
        }

        bool fitSize = AllOf(scanTasks, [this](const auto& x){ return ValidateTaskSize(x.second); })
                    && ValidateTaskSize(computeTasks);
        if (!fitSize) {
            return;
        }

        if (prepareTasksSpan) {
            prepareTasksSpan.End();
        }

        LOG_D("Total tasks: " << TasksGraph.GetTasks().size() << ", readonly: true"
            << ", " << nScanTasks << " scan tasks on " << scanTasks.size() << " nodes"
            << ", totalShardScans: " << nShardScans << ", execType: Scan"
            << ", snapshot: {" << Request.Snapshot.TxId << ", " << Request.Snapshot.Step << "}");

        ExecuteScanTx(std::move(computeTasks), std::move(scanTasks), std::move(snapshot));

        Become(&TKqpScanExecuter::ExecuteState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ScanExecuterExecuteState, ExecuterSpan.GetTraceId(), "ExecuteState", NWilson::EFlags::AUTO_END);
        }
    }

public:
    void Finalize() {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);

        if (Stats) {
            ReportEventElapsedTime();

            Stats->FinishTs = TInstant::Now();
            Stats->Finish();

            if (CollectFullStats(Request.StatsMode)) {
                const auto& tx = Request.Transactions[0].Body;
                auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
            }
        }

        LWTRACK(KqpScanExecuterFinalize, ResponseEv->Orbit, TxId, LastTaskId, LastComputeActorId, ResponseEv->ResultsSize());

        if (ExecuterSpan) {
            ExecuterSpan.EndOk();
        }

        LOG_D("Sending response to: " << Target);
        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    void ExecuteScanTx(TVector<NYql::NDqProto::TDqTask>&& computeTasks, THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks,
        TVector<NKikimrKqp::TKqpNodeResources>&& snapshot) {
        LWTRACK(KqpScanExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, computeTasks.size(), scanTasks.size());
        for (const auto& [_, tasks]: scanTasks) {
            for (const auto& task : tasks) {
                PendingComputeTasks.insert(task.GetId());
            }
        }

        for (auto& taskDesc : computeTasks) {
            PendingComputeTasks.insert(taskDesc.GetId());
        }

        Planner = CreateKqpPlanner(TxId, SelfId(), std::move(computeTasks),
            std::move(scanTasks), Request.Snapshot,
            Database, UserToken, Deadline.GetOrElse(TInstant::Zero()), Request.StatsMode,
            Request.DisableLlvmForUdfStages, Request.LlvmEnabled, AppData()->EnableKqpSpilling,
            Request.RlPath, ExecuterSpan, std::move(snapshot), ExecuterRetriesConfig);
        LOG_D("Execute scan tx, computeTasks: " << Planner->GetComputeTasksNumber() << ", scanTasks: " << Planner->GetMainTasksNumber());

        Planner->ProcessTasksForScanExecuter();
    }

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues) override
    {
        if (!PendingComputeTasks.empty()) {
            LOG_D("terminate pending resources request: " << Ydb::StatusIds::StatusCode_Name(status));

            auto ev = MakeHolder<TEvKqpNode::TEvCancelKqpTasksRequest>();
            ev->Record.SetTxId(TxId);
            ev->Record.SetReason(Ydb::StatusIds::StatusCode_Name(status));

            Send(MakeKqpNodeServiceID(SelfId().NodeId()), ev.Release());
        }

        TBase::ReplyErrorAndDie(status, issues);
    }

    void PassAway() override {
        for (auto channelPair: ResultChannelProxies) {
            LOG_D("terminate result channel " << channelPair.first << " proxy at " << channelPair.second->SelfId());

            TAutoPtr<IEventHandle> ev = new IEventHandle(
                channelPair.second->SelfId(), SelfId(), new TEvents::TEvPoison
            );
            channelPair.second->Receive(ev, TActivationContext::AsActorContext());
        }

        for (auto& [shardId, nodeId] : ShardIdToNodeId) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }

        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->ScanTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        TBase::PassAway();
    }
public:
    void FillEndpointDesc(NYql::NDqProto::TEndpoint& endpoint, const TTask& task) {
        if (task.ComputeActorId) {
            ActorIdToProto(task.ComputeActorId, endpoint.MutableActorId());
        }
    }

    void FillChannelDesc(NYql::NDqProto::TChannel& channelDesc, const TChannel& channel) {
        channelDesc.SetId(channel.Id);
        channelDesc.SetSrcTaskId(channel.SrcTask);
        channelDesc.SetDstTaskId(channel.DstTask);

        YQL_ENSURE(channel.SrcTask);
        FillEndpointDesc(*channelDesc.MutableSrcEndpoint(), TasksGraph.GetTask(channel.SrcTask));

        if (channel.DstTask) {
            FillEndpointDesc(*channelDesc.MutableDstEndpoint(), TasksGraph.GetTask(channel.DstTask));
        } else {
            auto proxy = GetOrCreateChannelProxy(channel);
            ActorIdToProto(proxy->SelfId(), channelDesc.MutableDstEndpoint()->MutableActorId());
        }

        channelDesc.SetIsPersistent(IsCrossShardChannel(TasksGraph, channel));
        channelDesc.SetInMemory(channel.InMemory);
    }
};

} // namespace

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
{
    return new TKqpScanExecuter(std::move(request), database, userToken, counters, executerRetriesConfig);
}

} // namespace NKqp
} // namespace NKikimr
