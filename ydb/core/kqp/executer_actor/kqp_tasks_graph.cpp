#include "kqp_tasks_graph.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/tx/datashard/range_ops.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;


void LogStage(const NActors::TActorContext& ctx, const TStageInfo& stageInfo) {
    LOG_DEBUG_S(ctx, NKikimrServices::KQP_EXECUTER, stageInfo.DebugString());
}

NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::EReadType ReadTypeToProto(const TTaskMeta::TReadInfo::EReadType& type) {
    switch (type) {
        case TTaskMeta::TReadInfo::EReadType::Rows:
            return NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::ROWS;
        case TTaskMeta::TReadInfo::EReadType::Blocks:
            return NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::BLOCKS;
    }

    YQL_ENSURE(false, "Invalid read type in task meta.");
}

TTaskMeta::TReadInfo::EReadType ReadTypeFromProto(const NKqpProto::TKqpPhyOpReadOlapRanges::EReadType& type) {
    switch (type) {
        case NKqpProto::TKqpPhyOpReadOlapRanges::ROWS:
            return TTaskMeta::TReadInfo::EReadType::Rows;
        case NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS:
            return TTaskMeta::TReadInfo::EReadType::Blocks;
        default:
            YQL_ENSURE(false, "Invalid read type from TKqpPhyOpReadOlapRanges protobuf.");
    }
}


std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const TStageInfo& stageInfo, const TTask& task)
{
    const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    std::vector<std::shared_ptr<arrow::Field>> columns;
    std::vector<std::shared_ptr<arrow::Array>> data;
    auto& parameterNames = task.Meta.ReadInfo.OlapProgram.ParameterNames;

    columns.reserve(parameterNames.size());
    data.reserve(parameterNames.size());

    for (auto& name : stage.GetProgramParameters()) {
        if (!parameterNames.contains(name)) {
            continue;
        }

        auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(name);
        YQL_ENSURE(NYql::NArrow::IsArrowCompatible(type), "Incompatible parameter type. Can't convert to arrow");

        std::unique_ptr<arrow::ArrayBuilder> builder = NYql::NArrow::MakeArrowBuilder(type);
        NYql::NArrow::AppendElement(value, builder.get(), type);

        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);

        YQL_ENSURE(status.ok(), "Failed to build arrow array of variables.");

        auto field = std::make_shared<arrow::Field>(name, array->type());

        columns.emplace_back(std::move(field));
        data.emplace_back(std::move(array));
    }

    auto schema = std::make_shared<arrow::Schema>(std::move(columns));
    auto recordBatch = arrow::RecordBatch::Make(schema, 1, data);

    return std::make_pair<TString, TString>(
        NArrow::SerializeSchema(*schema),
        NArrow::SerializeBatchNoCompression(recordBatch)
    );
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
            YQL_ENSURE(!stageInfo.Meta.HasReads() || !stageInfo.Meta.HasWrites());
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
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

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

bool IsCrossShardChannel(const TKqpTasksGraph& tasksGraph, const TChannel& channel) {
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

        // common case for multi-effects transactions
        cmp = CompareTypedCellVectors(
            std::get<TSerializedCellVec>(x).GetCells().data(),
            std::get<TSerializedCellVec>(y).GetCells().data(),
            keyTypes.data(), keyTypes.size());

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
        }
    }

    Ranges = std::move(result);
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

void AddSnapshotInfoToTaskInputs(const TKqpTasksGraph& tasksGraph, NYql::NDqProto::TDqTask& task) {
    const auto& snapshot = tasksGraph.GetMeta().Snapshot;
    for (auto& input : *task.MutableInputs()) {
        if (input.HasTransform()) {
            YQL_ENSURE(snapshot.IsValid());
            auto transform = input.MutableTransform();
            YQL_ENSURE(transform->GetType() == "StreamLookupInputTransformer",
                "Unexpected input transform type: " << transform->GetType());

            const google::protobuf::Any& settingsAny = transform->GetSettings();
            YQL_ENSURE(settingsAny.Is<NKikimrKqp::TKqpStreamLookupSettings>(), "Expected settings type: "
                << NKikimrKqp::TKqpStreamLookupSettings::descriptor()->full_name()
                << " , but got: " << settingsAny.type_url());

            NKikimrKqp::TKqpStreamLookupSettings settings;
            YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings");

            settings.MutableSnapshot()->SetStep(snapshot.Step);
            settings.MutableSnapshot()->SetTxId(snapshot.TxId);

            transform->MutableSettings()->PackFrom(settings);
        }
        if (input.HasSource() && input.GetSource().GetType() == NYql::KqpReadRangesSourceName) {
            auto source = input.MutableSource();
            const google::protobuf::Any& settingsAny = source->GetSettings();

            YQL_ENSURE(settingsAny.Is<NKikimrTxDataShard::TKqpReadRangesSourceSettings>(), "Expected settings type: "
                << NKikimrTxDataShard::TKqpReadRangesSourceSettings::descriptor()->full_name()
                << " , but got: " << settingsAny.type_url());

            NKikimrTxDataShard::TKqpReadRangesSourceSettings settings;
            YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings");

            if (snapshot.IsValid()) {
                settings.MutableSnapshot()->SetStep(snapshot.Step);
                settings.MutableSnapshot()->SetTxId(snapshot.TxId);
            }

            source->MutableSettings()->PackFrom(settings);
        }
    }
}

void FillEndpointDesc(NDqProto::TEndpoint& endpoint, const TTask& task) {
    if (task.ComputeActorId) {
        ActorIdToProto(task.ComputeActorId, endpoint.MutableActorId());
    } else if (task.Meta.ShardId) {
        endpoint.SetTabletId(task.Meta.ShardId);
    }
}

void FillChannelDesc(const TKqpTasksGraph& tasksGraph, NDqProto::TChannel& channelDesc, const TChannel& channel) {
    channelDesc.SetId(channel.Id);
    channelDesc.SetSrcTaskId(channel.SrcTask);
    channelDesc.SetDstTaskId(channel.DstTask);

    const auto& resultChannelProxies = tasksGraph.GetMeta().ResultChannelProxies;

    YQL_ENSURE(channel.SrcTask);
    const auto& srcTask = tasksGraph.GetTask(channel.SrcTask);
    FillEndpointDesc(*channelDesc.MutableSrcEndpoint(), srcTask);

    if (channel.DstTask) {
        FillEndpointDesc(*channelDesc.MutableDstEndpoint(), tasksGraph.GetTask(channel.DstTask));
    } else if (!resultChannelProxies.empty()) {
        auto it = resultChannelProxies.find(channel.Id);
        YQL_ENSURE(it != resultChannelProxies.end());
        ActorIdToProto(it->second, channelDesc.MutableDstEndpoint()->MutableActorId());
    } else {
        // For non-stream execution, collect results in executer and forward with response.
        ActorIdToProto(srcTask.Meta.ExecuterId, channelDesc.MutableDstEndpoint()->MutableActorId());
    }

    channelDesc.SetIsPersistent(IsCrossShardChannel(tasksGraph, channel));
    channelDesc.SetInMemory(channel.InMemory);
}

void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta) {
    meta->SetTablePath(stageInfo.Meta.TablePath);
    meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
    meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
    meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
    meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
    meta->SetTableKind((ui32)stageInfo.Meta.TableKind);
}

void FillTaskMeta(const TStageInfo& stageInfo, const TTask& task, const TKqpTableKeys& tableKeys, NYql::NDqProto::TDqTask& taskDesc) {
    if (task.Meta.ShardId && (task.Meta.Reads || task.Meta.Writes)) {
        NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta protoTaskMeta;

        FillTableMeta(stageInfo, protoTaskMeta.MutableTable());

        if (task.Meta.Reads) {
            for (auto& read : *task.Meta.Reads) {
                auto* protoReadMeta = protoTaskMeta.AddReads();
                read.Ranges.SerializeTo(protoReadMeta->MutableRange());
                for (auto& column : read.Columns) {
                    auto* protoColumn = protoReadMeta->AddColumns();
                    protoColumn->SetId(column.Id);
                    auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
                    protoColumn->SetType(columnType.TypeId);
                    if (columnType.TypeInfo) {
                        *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                    }
                    protoColumn->SetName(column.Name);
                }
                protoReadMeta->SetItemsLimit(task.Meta.ReadInfo.ItemsLimit);
                protoReadMeta->SetReverse(task.Meta.ReadInfo.Reverse);
            }
        }
        if (task.Meta.Writes) {
            auto* protoWrites = protoTaskMeta.MutableWrites();
            task.Meta.Writes->Ranges.SerializeTo(protoWrites->MutableRange());
            if (task.Meta.Writes->IsPureEraseOp()) {
                protoWrites->SetIsPureEraseOp(true);
            }

            for (const auto& [_, columnWrite] : task.Meta.Writes->ColumnWrites) {
                auto& protoColumnWrite = *protoWrites->AddColumns();

                auto& protoColumn = *protoColumnWrite.MutableColumn();
                protoColumn.SetId(columnWrite.Column.Id);
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(columnWrite.Column.Type, columnWrite.Column.TypeMod);
                protoColumn.SetType(columnType.TypeId);
                if (columnType.TypeInfo) {
                    *protoColumn.MutableTypeInfo() = *columnType.TypeInfo;
                }
                protoColumn.SetName(columnWrite.Column.Name);

                protoColumnWrite.SetMaxValueSizeBytes(columnWrite.MaxValueSizeBytes);
            }
        }

        taskDesc.MutableMeta()->PackFrom(protoTaskMeta);
    }  else if (task.Meta.ScanTask || stageInfo.Meta.IsSysView()) {
        NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta protoTaskMeta;

        FillTableMeta(stageInfo, protoTaskMeta.MutableTable());

        const auto& tableInfo = tableKeys.GetTable(stageInfo.Meta.TableId);
        for (const auto& keyColumnName : tableInfo.KeyColumns) {
            const auto& keyColumn = tableInfo.Columns.at(keyColumnName);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(keyColumn.Type, keyColumn.TypeMod);
            protoTaskMeta.AddKeyColumnTypes(columnType.TypeId);
            if (columnType.TypeInfo) {
                *protoTaskMeta.AddKeyColumnTypeInfos() = *columnType.TypeInfo;
            }
        }

        for (bool skipNullKey : stageInfo.Meta.SkipNullKeys) {
            protoTaskMeta.AddSkipNullKeys(skipNullKey);
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

        YQL_ENSURE(!task.Meta.Writes);

        if (!task.Meta.Reads->empty()) {
            protoTaskMeta.SetReverse(task.Meta.ReadInfo.Reverse);
            protoTaskMeta.SetItemsLimit(task.Meta.ReadInfo.ItemsLimit);
            if (task.Meta.HasEnableShardsSequentialScan()) {
                protoTaskMeta.SetEnableShardsSequentialScan(task.Meta.GetEnableShardsSequentialScanUnsafe());
            }
            protoTaskMeta.SetReadType(ReadTypeToProto(task.Meta.ReadInfo.ReadType));

            for (auto columnType : task.Meta.ReadInfo.ResultColumnsTypes) {
                auto* protoResultColumn = protoTaskMeta.AddResultColumns();
                protoResultColumn->SetId(0);
                auto protoColumnType = NScheme::ProtoColumnTypeFromTypeInfoMod(columnType, "");
                protoResultColumn->SetType(protoColumnType.TypeId);
                if (protoColumnType.TypeInfo) {
                    *protoResultColumn->MutableTypeInfo() = *protoColumnType.TypeInfo;
                }
            }

            if (tableInfo.TableKind == ETableKind::Olap) {
                auto* olapProgram = protoTaskMeta.MutableOlapProgram();
                olapProgram->SetProgram(task.Meta.ReadInfo.OlapProgram.Program);

                auto [schema, parameters] = SerializeKqpTasksParametersForOlap(stageInfo, task);
                olapProgram->SetParametersSchema(schema);
                olapProgram->SetParameters(parameters);
            } else {
                YQL_ENSURE(task.Meta.ReadInfo.OlapProgram.Program.empty());
            }

            for (auto& column : task.Meta.Reads->front().Columns) {
                auto* protoColumn = protoTaskMeta.AddColumns();
                protoColumn->SetId(column.Id);
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, "");
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
        }


        taskDesc.MutableMeta()->PackFrom(protoTaskMeta);
    }
}

void FillOutputDesc(const TKqpTasksGraph& tasksGraph, NYql::NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output) {
    switch (output.Type) {
        case TTaskOutputType::Map:
            YQL_ENSURE(output.Channels.size() == 1);
            outputDesc.MutableMap();
            break;

        case TTaskOutputType::HashPartition: {
            auto& hashPartitionDesc = *outputDesc.MutableHashPartition();
            for (auto& column : output.KeyColumns) {
                hashPartitionDesc.AddKeyColumns(column);
            }
            hashPartitionDesc.SetPartitionsCount(output.PartitionsCount);
            break;
        }

        case TKqpTaskOutputType::ShardRangePartition: {
            auto& rangePartitionDesc = *outputDesc.MutableRangePartition();
            auto& columns = *rangePartitionDesc.MutableKeyColumns();
            for (auto& column : output.KeyColumns) {
                *columns.Add() = column;
            }

            auto& partitionsDesc = *rangePartitionDesc.MutablePartitions();
            for (auto& pair : output.Meta.ShardPartitions) {
                auto& range = *pair.second->Range;
                auto& partitionDesc = *partitionsDesc.Add();
                partitionDesc.SetEndKeyPrefix(range.EndKeyPrefix.GetBuffer());
                partitionDesc.SetIsInclusive(range.IsInclusive);
                partitionDesc.SetIsPoint(range.IsPoint);
                partitionDesc.SetChannelId(pair.first);
            }
            break;
        }

        case TTaskOutputType::Broadcast: {
            outputDesc.MutableBroadcast();
            break;
        }

        case TTaskOutputType::Effects: {
            outputDesc.MutableEffects();
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected task output type " << output.Type);
        }
    }

    for (auto& channel : output.Channels) {
        auto& channelDesc = *outputDesc.AddChannels();
        FillChannelDesc(tasksGraph, channelDesc, tasksGraph.GetChannel(channel));
    }
}

void FillInputDesc(const TKqpTasksGraph& tasksGraph, NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input) {
    switch (input.Type()) {
        case NYql::NDq::TTaskInputType::Source:
            inputDesc.MutableSource()->SetType(input.SourceType);
            inputDesc.MutableSource()->SetWatermarksMode(input.WatermarksMode);
            inputDesc.MutableSource()->MutableSettings()->CopyFrom(*input.SourceSettings);
            break;
        case NYql::NDq::TTaskInputType::UnionAll: {
            inputDesc.MutableUnionAll();
            break;
        }
        case NYql::NDq::TTaskInputType::Merge: {
            auto& mergeProto = *inputDesc.MutableMerge();
            YQL_ENSURE(std::holds_alternative<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo));
            auto& sortColumns = std::get<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo).SortColumns;
            for (const auto& sortColumn : sortColumns) {
                auto newSortCol = mergeProto.AddSortColumns();
                newSortCol->SetColumn(sortColumn.Column.c_str());
                newSortCol->SetAscending(sortColumn.Ascending);
            }
            break;
        }
        default:
            YQL_ENSURE(false, "Unexpected task input type: " << (int) input.Type());
    }

    for (ui64 channel : input.Channels) {
        auto& channelDesc = *inputDesc.AddChannels();
        FillChannelDesc(tasksGraph, channelDesc, tasksGraph.GetChannel(channel));
    }

    if (input.Transform) {
        auto* transformProto = inputDesc.MutableTransform();
        transformProto->SetType(input.Transform->Type);
        transformProto->SetInputType(input.Transform->InputType);
        transformProto->SetOutputType(input.Transform->OutputType);
        *transformProto->MutableSettings() = input.Transform->Settings;
    }
}

NYql::NDqProto::TDqTask SerializeTaskToProto(const TKqpTasksGraph& tasksGraph, const TTask& task)
{
    auto& stageInfo = tasksGraph.GetStageInfo(task.StageId);
    NYql::NDqProto::TDqTask result;
    ActorIdToProto(task.Meta.ExecuterId, result.MutableExecuter()->MutableActorId());
    result.SetId(task.Id);
    result.SetStageId(stageInfo.Id.StageId);
    result.SetUseLlvm(task.GetUseLlvm());
    if (task.HasMetaId()) {
        result.SetMetaId(task.GetMetaIdUnsafe());
    }

    for (const auto& [paramName, paramValue] : task.Meta.DqTaskParams) {
        (*result.MutableTaskParams())[paramName] = paramValue;
    }

    for (const auto& [paramName, paramValue] : task.Meta.DqSecureParams) {
        (*result.MutableSecureParams())[paramName] = paramValue;
    }

    for (const auto& input : task.Inputs) {
        FillInputDesc(tasksGraph, *result.AddInputs(), input);
    }

    for (const auto& output : task.Outputs) {
        FillOutputDesc(tasksGraph, *result.AddOutputs(), output);
    }

    const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    result.MutableProgram()->CopyFrom(stage.GetProgram());
    for (auto& paramName : stage.GetProgramParameters()) {
        auto& dqParams = *result.MutableParameters();
        if (task.Meta.ShardId) {
            dqParams[paramName] = stageInfo.Meta.Tx.Params->GetShardParam(task.Meta.ShardId, paramName);
        } else {
            dqParams[paramName] = stageInfo.Meta.Tx.Params->SerializeParamValue(paramName);
        }
    }

    AddSnapshotInfoToTaskInputs(tasksGraph, result);
    FillTaskMeta(stageInfo, task, tasksGraph.GetMeta().TableKeys, result);

    return result;
}

TString TTaskMeta::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const
{
    TStringBuilder sb;
    sb << "TTaskMeta{ ShardId: " << ShardId << ", Reads: { ";

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
