#include "kqp_tasks_graph.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

// #define DBG_TRACE

void LogStage(const NActors::TActorContext& ctx, const TStageInfo& stageInfo) {
    // TODO: Print stage details, including input types and program.
    LOG_DEBUG_S(ctx, NKikimrServices::KQP_EXECUTER, "StageInfo: StageId #" << stageInfo.Id
        << ", InputsCount: " << stageInfo.InputsCount
        << ", OutputsCount: " << stageInfo.OutputsCount);
}

bool HasReads(const TStageInfo& stageInfo) {
    return stageInfo.Meta.ShardOperations.contains(TKeyDesc::ERowOperation::Read);
}

bool HasWrites(const TStageInfo& stageInfo) {
    return stageInfo.Meta.ShardOperations.contains(TKeyDesc::ERowOperation::Update) ||
           stageInfo.Meta.ShardOperations.contains(TKeyDesc::ERowOperation::Erase);
}

void FillKqpTasksGraphStages(TKqpTasksGraph& tasksGraph, const TVector<IKqpGateway::TPhysicalTxData>& txs) {
    for (size_t txIdx = 0; txIdx < txs.size(); ++txIdx) {
        auto& tx = txs[txIdx];

        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            NYql::NDq::TStageId stageId(txIdx, stageIdx);

            TStageInfoMeta meta(tx);

            for (auto& source : stage.GetSources()) {
                if (source.HasReadRangesSource()) {
                    YQL_ENSURE(source.GetInputIndex() == 0);
                    YQL_ENSURE(stage.SourcesSize() == 1);
                    meta.TableId = MakeTableId(source.GetReadRangesSource().GetTable());
                    meta.TablePath = source.GetReadRangesSource().GetTable().GetPath();
                    meta.ShardOperations.insert(TKeyDesc::ERowOperation::Read);
                }
            }

            bool stageAdded = tasksGraph.AddStageInfo(
                TStageInfo(stageId, stage.InputsSize() + stage.SourcesSize(), stage.GetOutputsCount(), std::move(meta)));
            YQL_ENSURE(stageAdded);

            auto& stageInfo = tasksGraph.GetStageInfo(stageId);
            LogStage(TlsActivationContext->AsActorContext(), stageInfo);

            THashSet<TTableId> tables;
            for (auto& op : stage.GetTableOps()) {
                if (!stageInfo.Meta.TableId) {
                    YQL_ENSURE(!stageInfo.Meta.TablePath);
                    stageInfo.Meta.TableId = MakeTableId(op.GetTable());
                    stageInfo.Meta.TablePath = op.GetTable().GetPath();
                    stageInfo.Meta.TableKind = ETableKind::Unknown;
                    tables.insert(MakeTableId(op.GetTable()));
                } else {
                    YQL_ENSURE(stageInfo.Meta.TableId == MakeTableId(op.GetTable()));
                    YQL_ENSURE(stageInfo.Meta.TablePath == op.GetTable().GetPath());
                }

                switch (op.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
                    case NKqpProto::TKqpPhyTableOperation::kLookup:
                        stageInfo.Meta.ShardOperations.insert(TKeyDesc::ERowOperation::Read);
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                        stageInfo.Meta.ShardOperations.insert(TKeyDesc::ERowOperation::Update);
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        stageInfo.Meta.ShardOperations.insert(TKeyDesc::ERowOperation::Erase);
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected table operation: " << (ui32) op.GetTypeCase());
                }
            }

            YQL_ENSURE(tables.empty() || tables.size() == 1);
            YQL_ENSURE(!HasReads(stageInfo) || !HasWrites(stageInfo));
        }
    }
}

void BuildKqpTaskGraphResultChannels(TKqpTasksGraph& tasksGraph, const TKqpPhyTxHolder::TConstPtr& tx, ui64 txIdx) {
    for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
        const auto& result = tx->GetResults(i);
        const auto& connection = result.GetConnection();
        const auto& inputStageInfo = tasksGraph.GetStageInfo(TStageId(txIdx, connection.GetStageIndex()));
        const auto& outputIdx = connection.GetOutputIndex();

        YQL_ENSURE(inputStageInfo.Tasks.size() == 1, "actual count: " << inputStageInfo.Tasks.size());
        auto originTaskId = inputStageInfo.Tasks[0];

        auto& channel = tasksGraph.AddChannel();
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIdx;
        channel.DstTask = 0;
        channel.DstInputIndex = i;
        channel.InMemory = true;

        auto& originTask = tasksGraph.GetTask(originTaskId);

        auto& taskOutput = originTask.Outputs[outputIdx];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "Create result channelId: " << channel.Id
            << " from task: " << originTaskId << " with index: " << outputIdx);
    }
}

void BuildMapShardChannels(TKqpTasksGraph& graph, const TStageInfo& stageInfo, ui32 inputIndex,
    const TStageInfo& inputStageInfo, ui32 outputIndex, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    THashMap<ui64, ui64> shardToTaskMap;
    for (auto& taskId : stageInfo.Tasks) {
        auto& task = graph.GetTask(taskId);
        auto result = shardToTaskMap.insert(std::make_pair(task.Meta.ShardId, taskId));
        YQL_ENSURE(result.second);
    }

    for (auto& originTaskId : inputStageInfo.Tasks) {
        auto& originTask = graph.GetTask(originTaskId);

        auto targetTaskId = shardToTaskMap.FindPtr(originTask.Meta.ShardId);
        YQL_ENSURE(targetTaskId);
        auto& targetTask = graph.GetTask(*targetTaskId);

        auto& channel = graph.AddChannel();
        channel.SrcTask = originTask.Id;
        channel.SrcOutputIndex = outputIndex;
        channel.DstTask = targetTask.Id;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTask.Id, targetTask.Id, "MapShard/Map", !channel.InMemory);
    }
}

void BuildShuffleShardChannels(TKqpTasksGraph& graph, const TStageInfo& stageInfo, ui32 inputIndex,
    const TStageInfo& inputStageInfo, ui32 outputIndex, const TKqpTableKeys& tableKeys, bool enableSpilling,
    const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Meta.ShardKey);
    THashMap<ui64, const TKeyDesc::TPartitionInfo*> partitionsMap;
    for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
        partitionsMap[partition.ShardId] = &partition;
    }

    auto table = tableKeys.GetTable(stageInfo.Meta.TableId);

    for (auto& originTaskId : inputStageInfo.Tasks) {
        auto& originTask = graph.GetTask(originTaskId);
        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TKqpTaskOutputType::ShardRangePartition;
        taskOutput.KeyColumns = table.KeyColumns;

        for (auto& targetTaskId : stageInfo.Tasks) {
            auto& targetTask = graph.GetTask(targetTaskId);

            auto targetPartition = partitionsMap.FindPtr(targetTask.Meta.ShardId);
            YQL_ENSURE(targetPartition);

            auto& channel = graph.AddChannel();
            channel.SrcTask = originTask.Id;
            channel.SrcOutputIndex = outputIndex;
            channel.DstTask = targetTask.Id;
            channel.DstInputIndex = inputIndex;
            channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

            taskOutput.Meta.ShardPartitions.insert(std::make_pair(channel.Id, *targetPartition));
            taskOutput.Channels.push_back(channel.Id);

            auto& taskInput = targetTask.Inputs[inputIndex];
            taskInput.Channels.push_back(channel.Id);

            logFunc(channel.Id, originTask.Id, targetTask.Id, "ShuffleShard/ShardRangePartition", !channel.InMemory);
        }
    }
}

void BuildStreamLookupChannels(TKqpTasksGraph& graph, const TStageInfo& stageInfo, ui32 inputIndex,
    const TStageInfo& inputStageInfo, ui32 outputIndex, const TKqpTableKeys& tableKeys,
    const NKqpProto::TKqpPhyCnStreamLookup& streamLookup, bool enableSpilling, const TChannelLogFunc& logFunc) {
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    NKikimrKqp::TKqpStreamLookupSettings settings;
    settings.MutableTable()->CopyFrom(streamLookup.GetTable());

    auto table = tableKeys.GetTable(MakeTableId(streamLookup.GetTable()));
    for (const auto& keyColumn : table.KeyColumns) {
        auto columnIt = table.Columns.find(keyColumn);
        YQL_ENSURE(columnIt != table.Columns.end(), "Unknown column: " << keyColumn);

        auto* keyColumnProto = settings.AddKeyColumns();
        keyColumnProto->SetName(keyColumn);
        keyColumnProto->SetId(columnIt->second.Id);
        keyColumnProto->SetTypeId(columnIt->second.Type.GetTypeId());
    }

    for (const auto& keyColumn : streamLookup.GetKeyColumns()) {
        auto columnIt = table.Columns.find(keyColumn);
        YQL_ENSURE(columnIt != table.Columns.end(), "Unknown column: " << keyColumn);
        settings.AddLookupKeyColumns(keyColumn);
    }

    for (const auto& column : streamLookup.GetColumns()) {
        auto columnIt = table.Columns.find(column);
        YQL_ENSURE(columnIt != table.Columns.end(), "Unknown column: " << column);

        auto* columnProto = settings.AddColumns();
        columnProto->SetName(column);
        columnProto->SetId(columnIt->second.Id);
        columnProto->SetTypeId(columnIt->second.Type.GetTypeId());
    }

    TTransform streamLookupTransform;
    streamLookupTransform.Type = "StreamLookupInputTransformer";
    streamLookupTransform.InputType = streamLookup.GetLookupKeysType();
    streamLookupTransform.OutputType = streamLookup.GetResultType();
    streamLookupTransform.Settings.PackFrom(settings);

    for (ui32 taskId = 0; taskId < inputStageInfo.Tasks.size(); ++taskId) {
        auto& originTask = graph.GetTask(inputStageInfo.Tasks[taskId]);
        auto& targetTask = graph.GetTask(stageInfo.Tasks[taskId]);

        auto& channel = graph.AddChannel();
        channel.SrcTask = originTask.Id;
        channel.SrcOutputIndex = outputIndex;
        channel.DstTask = targetTask.Id;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Transform = streamLookupTransform;
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTask.Id, targetTask.Id, "StreamLookup/Map", !channel.InMemory);
    }
}

void BuildKqpStageChannels(TKqpTasksGraph& tasksGraph, const TKqpTableKeys& tableKeys, const TStageInfo& stageInfo,
    ui64 txId, bool enableSpilling)
{
    auto& stage = GetStage(stageInfo);

    if (stage.GetIsEffectsStage()) {
        YQL_ENSURE(stageInfo.OutputsCount == 1);

        for (auto& taskId : stageInfo.Tasks) {
            auto& task = tasksGraph.GetTask(taskId);
            auto& taskOutput = task.Outputs[0];
            taskOutput.Type = TTaskOutputType::Effects;
        }
    }

    auto log = [&stageInfo, txId](ui64 channel, ui64 from, ui64 to, TStringBuf type, bool spilling) {
        LOG_DEBUG_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << txId << ". "
            << "Stage " << stageInfo.Id << " create channelId: " << channel
            << " from task: " << from << " to task: " << to << " of type " << type
            << (spilling ? " with spilling" : " without spilling"));
    };

    for (const auto& input : stage.GetInputs()) {
        ui32 inputIdx = input.GetInputIndex();
        const auto& inputStageInfo = tasksGraph.GetStageInfo(TStageId(stageInfo.Id.TxId, input.GetStageIndex()));
        const auto& outputIdx = input.GetOutputIndex();

        switch (input.GetTypeCase()) {
            case NKqpProto::TKqpPhyConnection::kUnionAll:
                BuildUnionAllChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kHashShuffle:
                BuildHashShuffleChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx,
                    input.GetHashShuffle().GetKeyColumns(), enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kBroadcast:
                BuildBroadcastChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kMap:
                BuildMapChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kMapShard:
                BuildMapShardChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kShuffleShard:
                BuildShuffleShardChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, tableKeys,
                    enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kMerge: {
                TVector<TSortColumn> sortColumns;
                sortColumns.reserve(input.GetMerge().SortColumnsSize());

                for (const auto& sortColumn : input.GetMerge().GetSortColumns()) {
                    sortColumns.emplace_back(
                        TSortColumn(sortColumn.GetColumn(), sortColumn.GetAscending())
                    );
                }
                // TODO: spilling?
                BuildMergeChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, sortColumns, log);
                break;
            }
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                BuildStreamLookupChannels(tasksGraph, stageInfo, inputIdx, inputStageInfo, outputIdx, tableKeys,
                    input.GetStreamLookup(), enableSpilling, log);
                break;
            }

            default:
                YQL_ENSURE(false, "Unexpected stage input type: " << (ui32)input.GetTypeCase());
        }
    }
}

bool IsCrossShardChannel(TKqpTasksGraph& tasksGraph, const TChannel& channel) {
    YQL_ENSURE(channel.SrcTask);

    if (!channel.DstTask) {
        return false;
    }

    ui64 targetShard = tasksGraph.GetTask(channel.DstTask).Meta.ShardId;
    if (!targetShard) {
        return false;
    }

    return targetShard != tasksGraph.GetTask(channel.SrcTask).Meta.ShardId;
}

const NKqpProto::TKqpPhyStage& GetStage(const TStageInfo& stageInfo) {
    auto& txBody = stageInfo.Meta.Tx.Body;
    YQL_ENSURE(stageInfo.Id.StageId < txBody->StagesSize());

    return txBody->GetStages(stageInfo.Id.StageId);
}

void TShardKeyRanges::AddPoint(TSerializedCellVec&& point) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(point));
    }
}

void TShardKeyRanges::AddRange(TSerializedTableRange&& range) {
    Y_VERIFY_DEBUG(!range.Point);
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(range));
    }
}

void TShardKeyRanges::Add(TSerializedPointOrRange&& pointOrRange) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(pointOrRange));
        if (std::holds_alternative<TSerializedTableRange>(Ranges.back())) {
            Y_VERIFY_DEBUG(!std::get<TSerializedTableRange>(Ranges.back()).Point);
        }
    }
}

void TShardKeyRanges::CopyFrom(const TVector<TSerializedPointOrRange>& ranges) {
    if (!IsFullRange()) {
        Ranges = ranges;
        for (auto& x : Ranges) {
            if (std::holds_alternative<TSerializedTableRange>(x)) {
                Y_VERIFY_DEBUG(!std::get<TSerializedTableRange>(x).Point);
            }
        }
    }
};

void TShardKeyRanges::MakeFullRange(TSerializedTableRange&& range) {
    Ranges.clear();
    FullRange.emplace(std::move(range));
}

void TShardKeyRanges::MakeFullPoint(TSerializedCellVec&& point) {
    Ranges.clear();
    FullRange.emplace(TSerializedTableRange(std::move(point.GetBuffer()), "", true, true));
    FullRange->Point = true;
}

void TShardKeyRanges::MakeFull(TSerializedPointOrRange&& pointOrRange) {
    if (std::holds_alternative<TSerializedTableRange>(pointOrRange)) {
        MakeFullRange(std::move(std::get<TSerializedTableRange>(pointOrRange)));
    } else {
        MakeFullPoint(std::move(std::get<TSerializedCellVec>(pointOrRange)));
    }
}


void TShardKeyRanges::MergeWritePoints(TShardKeyRanges&& other, const TVector<NScheme::TTypeInfo>& keyTypes) {
#ifdef DBG_TRACE
    Cerr << (TStringBuilder() << "-- merge " << ToString(keyTypes, *AppData()->TypeRegistry)
        << " with " << other.ToString(keyTypes, *AppData()->TypeRegistry) << Endl);
#endif

    if (IsFullRange()) {
        return;
    }

    if (other.IsFullRange()) {
        std::swap(Ranges, other.Ranges);
        FullRange.swap(other.FullRange);
        return;
    }

    TVector<TSerializedPointOrRange> result;
    result.reserve(Ranges.size() + other.Ranges.size());

    ui64 i = 0, j = 0;
    while (true) {
        if (i >= Ranges.size()) {
            while (j < other.Ranges.size()) {
                result.emplace_back(std::move(other.Ranges[j++]));
            }
            break;
        }
        if (j >= other.Ranges.size()) {
            while (i < Ranges.size()) {
                result.emplace_back(std::move(Ranges[i++]));
            }
            break;
        }

        auto& x = Ranges[i];
        auto& y = other.Ranges[j];

        int cmp = 0;

        // ensure `x` and `y` are points
        YQL_ENSURE(std::holds_alternative<TSerializedCellVec>(x));
        YQL_ENSURE(std::holds_alternative<TSerializedCellVec>(y));

#if 1
        // common case for multi-effects transactions
        cmp = CompareTypedCellVectors(
            std::get<TSerializedCellVec>(x).GetCells().data(),
            std::get<TSerializedCellVec>(y).GetCells().data(),
            keyTypes.data(), keyTypes.size());
#else
        if (x.IsPoint() && y.IsPoint()) {
            // common case for multi-effects transactions
            cmp = CompareTypedCellVectors(x.From.GetCells().data(), y.From.GetCells().data(), keyTypes.data(), keyTypes.size());
        } else if (x.IsPoint()) {
            cmp = ComparePointAndRange(x.From.GetCells(), y.ToTableRange(), keyTypes, keyTypes);
        } else if (y.IsPoint()) {
            cmp = -ComparePointAndRange(y.From.GetCells(), x.ToTableRange(), keyTypes, keyTypes);
        } else {
            cmp = CompareRanges(x.ToTableRange(), y.ToTableRange(), keyTypes);
        }
#endif

        if (cmp < 0) {
            result.emplace_back(std::move(x));
            ++i;
        } else if (cmp > 0) {
            result.emplace_back(std::move(y));
            ++j;
        } else {
            result.emplace_back(std::move(x));
            ++i;
            ++j;
#if 0
            if (CompareTypedCellVectors(x.From.GetCells().data(), y.From.GetCells().data(), keyTypes.data(), keyTypes.size()) == 0 &&
                x.FromInclusive == y.FromInclusive &&
                CompareTypedCellVectors(x.To.GetCells().data(), y.To.GetCells().data(), keyTypes.data(), keyTypes.size()) == 0 &&
                x.ToInclusive == y.ToInclusive)
            {
                result.emplace_back(std::move(x));
                ++i;
                ++j;
            } else if (x.Point) {
                YQL_ENSURE(!y.Point);
                result.emplace_back(TSerializedTableRange(y.From.GetCells(), y.FromInclusive, x.From.GetCells(), true));
                ++i;
                y.From = x.From;
                y.FromInclusive = false;
            } else if (y.Point) {
                YQL_ENSURE(!x.Point);
                result.emplace_back(TSerializedTableRange(x.From.GetCells(), x.FromInclusive, y.From.GetCells(), true));
                ++j;
                x.From = y.From;
                x.FromInclusive = false;
            } else {
                YQL_ENSURE(!x.Point && !y.Point);

                int cmpLeft = CompareBorders<true, true>(x.From.GetCells(), y.From.GetCells(), x.FromInclusive, y.FromInclusive, keyTypes);
                int cmpRight = CompareBorders<false, false>(x.From.GetCells(), y.From.GetCells(), x.FromInclusive, y.FromInclusive, keyTypes);

                if (cmpLeft <= 0 && cmpRight >= 0) {
                    // y \in x
                    result.emplace_back(std::move(x));
                    ++i;
                    ++j;
                } else if (cmpLeft >= 0 && cmpRight <= 0) {
                    // x \in y
                    result.emplace_back(std::move(y));
                    ++i;
                    ++j;
                } else if (cmpLeft < 0) {
                    result.emplace_back(TSerializedTableRange(x.From.GetCells(), x.FromInclusive, y.From.GetCells(), !y.FromInclusive));
                    x.From = y.From;
                    x.FromInclusive = y.FromInclusive;
                } else if (cmpLeft > 0) {
                    result.emplace_back(TSerializedTableRange(y.From.GetCells(), y.FromInclusive, x.From.GetCells(), !x.FromInclusive));
                    y.From = x.From;
                    y.FromInclusive = x.FromInclusive;
                } else /* cmpLeft == cmpRight */ {
                    if (cmpRight < 0) {
                        y.From = x.To;
                        y.FromInclusive = !x.ToInclusive;
                        result.emplace_back(std::move(x));
                        ++i;
                    } else if (cmpRight > 0) {
                        x.From = y.To;
                        x.FromInclusive = !y.ToInclusive;
                        result.emplace_back(std::move(y));
                        ++j;
                    } else {
                        // x == y
                        YQL_ENSURE(false);
                    }
                }
            }
#endif
        }
    }

    Ranges = std::move(result);

#ifdef DBG_TRACE
    Cerr << (TStringBuilder() << "--- merge result: " << ToString(keyTypes, *AppData()->TypeRegistry) << Endl);
#endif
}

TString TShardKeyRanges::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const
{
    TStringBuilder sb;
    sb << "TShardKeyRanges{ ";
    if (IsFullRange()) {
        sb << "full " << DebugPrintRange(keyTypes, FullRange->ToTableRange(), typeRegistry);
    } else {
        if (Ranges.empty()) {
            sb << "<empty> ";
        }
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                sb << DebugPrintPoint(keyTypes, std::get<TSerializedCellVec>(range).GetCells(), typeRegistry) << ", ";
            } else {
                sb << DebugPrintRange(keyTypes, std::get<TSerializedTableRange>(range).ToTableRange(), typeRegistry) << ", ";
            }
        }
    }
    sb << "}";
    return sb;
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange* proto) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->MutableFullRange();
        FullRange->Serialize(protoRange);
    } else {
        auto* protoRanges = proto->MutableRanges();
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                const auto& x = std::get<TSerializedCellVec>(range);
                protoRanges->AddKeyPoints(x.GetBuffer());
            } else {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_VERIFY_DEBUG(!x.Point);
                auto& keyRange = *protoRanges->AddKeyRanges();
                x.Serialize(keyRange);
            }
        }
    }
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta_TReadOpMeta* proto) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->AddKeyRanges();
        FullRange->Serialize(protoRange);
    } else {
        for (auto& range : Ranges) {
            auto& keyRange = *proto->AddKeyRanges();
            if (std::holds_alternative<TSerializedTableRange>(range)) {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_VERIFY_DEBUG(!x.Point);
                x.Serialize(keyRange);
            } else {
                const auto& x = std::get<TSerializedCellVec>(range);
                keyRange.SetFrom(x.GetBuffer());
                keyRange.SetTo(x.GetBuffer());
                keyRange.SetFromInclusive(true);
                keyRange.SetToInclusive(true);
            }
        }
    }
}

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->MutableRanges()->AddKeyRanges();
        FullRange->Serialize(protoRange);
    } else {
        bool usePoints = true;
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedTableRange>(range)) {
                usePoints = false;
            }
        }
        auto* protoRanges = proto->MutableRanges();
        for (auto& range : Ranges) {
            if (std::holds_alternative<TSerializedCellVec>(range)) {
                if (usePoints) {
                    const auto& x = std::get<TSerializedCellVec>(range);
                    protoRanges->AddKeyPoints(x.GetBuffer());
                } else {
                    const auto& x = std::get<TSerializedCellVec>(range);
                    auto& keyRange = *protoRanges->AddKeyRanges();
                    keyRange.SetFrom(x.GetBuffer());
                    keyRange.SetTo(x.GetBuffer());
                    keyRange.SetFromInclusive(true);
                    keyRange.SetToInclusive(true);
                }
            } else {
                auto& x = std::get<TSerializedTableRange>(range);
                Y_VERIFY_DEBUG(!x.Point);
                auto& keyRange = *protoRanges->AddKeyRanges();
                x.Serialize(keyRange);
            }
        }
    }
}

std::pair<const TSerializedCellVec*, bool> TShardKeyRanges::GetRightBorder() const {
    if (FullRange) {
        return !FullRange->Point ? std::make_pair(&FullRange->To, true) : std::make_pair(&FullRange->From, true);
    }

    YQL_ENSURE(!Ranges.empty());
    const auto& last = Ranges.back();
    if (std::holds_alternative<TSerializedCellVec>(last)) {
        return std::make_pair(&std::get<TSerializedCellVec>(last), true);
    }

    const auto& lastRange = std::get<TSerializedTableRange>(last);
    return !lastRange.Point ? std::make_pair(&lastRange.To, lastRange.ToInclusive) : std::make_pair(&lastRange.From, true);
}

TString TTaskMeta::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const
{
    TStringBuilder sb;
    sb << "TTaskMeta{ ShardId: " << ShardId << ", Params: [";

    for (auto& [name, value] : Params) {
        sb << name << ", ";
    }

    sb << "], Reads: { ";

    if (Reads) {
        for (ui64 i = 0; i < Reads->size(); ++i) {
            auto& read = (*Reads)[i];
            sb << "[" << i << "]: { columns: [";
            for (auto& x : read.Columns) {
                sb << x.Name << ", ";
            }
            sb << "], ranges: " << read.Ranges.ToString(keyTypes, typeRegistry) << " }";
            if (i != Reads->size() - 1) {
                sb << ", ";
            }
        }
    } else {
        sb << "none";
    }

    sb << " }, Writes: { ";

    if (Writes) {
        sb << "ranges: " << Writes->Ranges.ToString(keyTypes, typeRegistry);
    } else {
        sb << "none";
    }

    sb << " } }";

    return sb;
}

} // namespace NKqp
} // namespace NKikimr
