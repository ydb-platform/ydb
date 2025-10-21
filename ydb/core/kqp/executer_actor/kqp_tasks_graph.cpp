#include "kqp_tasks_graph.h"

#include "kqp_partition_helper.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/program/resolver.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

struct TShardRangesWithShardId {
    TMaybe<ui64> ShardId;
    const TShardKeyRanges* Ranges;
};

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

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const TStageInfo& stageInfo, const TTask& task) {
    const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    std::vector<std::shared_ptr<arrow::Field>> columns;
    std::vector<std::shared_ptr<arrow::Array>> data;

    if (const auto& parameterNames = task.Meta.ReadInfo.OlapProgram.ParameterNames; !parameterNames.empty()) {
        columns.reserve(parameterNames.size());
        data.reserve(parameterNames.size());

        for (const auto& name : stage.GetProgramParameters()) {
            if (!parameterNames.contains(name)) {
                continue;
            }

            const auto [type, value] = stageInfo.Meta.Tx.Params->GetParameterUnboxedValue(name);
            YQL_ENSURE(NYql::NArrow::IsArrowCompatible(type), "Incompatible parameter type. Can't convert to arrow");

            std::unique_ptr<arrow::ArrayBuilder> builder = NYql::NArrow::MakeArrowBuilder(type);
            NYql::NArrow::AppendElement(value, builder.get(), type);

            std::shared_ptr<arrow::Array> array;
            const auto status = builder->Finish(&array);

            YQL_ENSURE(status.ok(), "Failed to build arrow array of variables.");

            auto field = std::make_shared<arrow::Field>(name, array->type());

            columns.emplace_back(std::move(field));
            data.emplace_back(std::move(array));
        }
    }

    auto schema = std::make_shared<arrow::Schema>(std::move(columns));
    auto recordBatch = arrow::RecordBatch::Make(schema, 1, data);

    return std::make_pair<TString, TString>(
        NArrow::SerializeSchema(*schema),
        NArrow::SerializeBatchNoCompression(recordBatch)
    );
}

void TKqpTasksGraph::FillKqpTasksGraphStages(const TVector<IKqpGateway::TPhysicalTxData>& txs) {
    for (size_t txIdx = 0; txIdx < txs.size(); ++txIdx) {
        auto& tx = txs[txIdx];

        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            NYql::NDq::TStageId stageId(txIdx, stageIdx);

            TStageInfoMeta meta(tx);

            ui64 stageSourcesCount = 0;
            for (const auto& source : stage.GetSources()) {
                switch (source.GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource: {
                        YQL_ENSURE(source.GetInputIndex() == 0);
                        YQL_ENSURE(stage.SourcesSize() == 1);
                        meta.TableId = MakeTableId(source.GetReadRangesSource().GetTable());
                        meta.TablePath = source.GetReadRangesSource().GetTable().GetPath();
                        meta.ShardOperations.insert(TKeyDesc::ERowOperation::Read);
                        meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                        stageSourcesCount++;
                        break;
                    }

                    case NKqpProto::TKqpSource::kExternalSource: {
                        if (!source.GetExternalSource().GetEmbedded()) {
                            stageSourcesCount++;
                        }
                        break;
                    }

                    default: {
                        YQL_ENSURE(false, "unknown source type");
                    }
                }
            }

            for (const auto& input : stage.GetInputs()) {
                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    meta.TableId = MakeTableId(input.GetStreamLookup().GetTable());
                    meta.TablePath = input.GetStreamLookup().GetTable().GetPath();
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                    YQL_ENSURE(meta.TableConstInfo);
                    meta.TableKind = meta.TableConstInfo->TableKind;
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorResolve) {
                    meta.TableId = MakeTableId(input.GetVectorResolve().GetTable());
                    meta.TablePath = input.GetVectorResolve().GetTable().GetPath();
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                    YQL_ENSURE(meta.TableConstInfo);
                    meta.TableKind = meta.TableConstInfo->TableKind;

                    YQL_ENSURE(!meta.IndexMetas.size());
                    meta.IndexMetas.emplace_back();
                    meta.IndexMetas.back().TableId = MakeTableId(input.GetVectorResolve().GetLevelTable());
                    meta.IndexMetas.back().TablePath = input.GetVectorResolve().GetLevelTable().GetPath();
                    meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                    meta.TableId = MakeTableId(input.GetSequencer().GetTable());
                    meta.TablePath = input.GetSequencer().GetTable().GetPath();
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                }
            }

            for (auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    YQL_ENSURE(sink.GetOutputIndex() == 0);
                    YQL_ENSURE(stage.SinksSize() == 1);
                    meta.TablePath = settings.GetTable().GetPath();
                    if (settings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE) {
                        meta.ShardOperations.insert(TKeyDesc::ERowOperation::Erase);
                    } else {
                        meta.ShardOperations.insert(TKeyDesc::ERowOperation::Update);
                    }

                    if (settings.GetType() != NKikimrKqp::TKqpTableSinkSettings::MODE_FILL) {
                        meta.TableId = MakeTableId(settings.GetTable());
                        meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);

                        for (const auto& indexSettings : settings.GetIndexes()) {
                            meta.IndexMetas.emplace_back();
                            meta.IndexMetas.back().TableId = MakeTableId(indexSettings.GetTable());
                            meta.IndexMetas.back().TablePath = indexSettings.GetTable().GetPath();
                            meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                        }
                    }
                }
            }

            bool stageAdded = AddStageInfo(TStageInfo(stageId, stage.InputsSize() + stageSourcesCount, stage.GetOutputsCount(), std::move(meta)));
            YQL_ENSURE(stageAdded);

            auto& stageInfo = GetStageInfo(stageId);
            LogStage(TlsActivationContext->AsActorContext(), stageInfo);

            THashSet<TTableId> tables;
            for (auto& op : stage.GetTableOps()) {
                if (!stageInfo.Meta.TableId) {
                    YQL_ENSURE(!stageInfo.Meta.TablePath);
                    stageInfo.Meta.TableId = MakeTableId(op.GetTable());
                    stageInfo.Meta.TablePath = op.GetTable().GetPath();
                    stageInfo.Meta.TableKind = ETableKind::Unknown;
                    stageInfo.Meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(stageInfo.Meta.TableId);
                    tables.insert(MakeTableId(op.GetTable()));
                } else {
                    YQL_ENSURE(stageInfo.Meta.TableId == MakeTableId(op.GetTable()));
                    YQL_ENSURE(stageInfo.Meta.TablePath == op.GetTable().GetPath());
                }

                switch (op.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
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

void TKqpTasksGraph::BuildKqpTaskGraphResultChannels(const TKqpPhyTxHolder::TConstPtr& tx, ui64 txIdx) {
    for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
        const auto& result = tx->GetResults(i);
        const auto& connection = result.GetConnection();
        const auto& inputStageInfo = GetStageInfo(TStageId(txIdx, connection.GetStageIndex()));
        const auto& outputIdx = connection.GetOutputIndex();

        if (inputStageInfo.Tasks.size() < 1) {
            // it's empty result from a single partition stage
            continue;
        }

        YQL_ENSURE(inputStageInfo.Tasks.size() == 1, "actual count: " << inputStageInfo.Tasks.size());
        auto originTaskId = inputStageInfo.Tasks[0];

        auto& channel = AddChannel();
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIdx;
        channel.DstTask = 0;
        channel.DstInputIndex = i;
        channel.InMemory = true;

        auto& originTask = GetTask(originTaskId);

        auto& taskOutput = originTask.Outputs[outputIdx];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "Create result channelId: " << channel.Id
            << " from task: " << originTaskId << " with index: " << outputIdx);
    }
}

void TKqpTasksGraph::BuildTransformChannels(const TTransform& transform, const TTaskInputMeta& meta, const TString& name,
    const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo, ui32 outputIndex, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    for (ui32 taskId = 0; taskId < inputStageInfo.Tasks.size(); ++taskId) {
        auto& originTask = GetTask(inputStageInfo.Tasks[taskId]);
        auto& targetTask = GetTask(stageInfo.Tasks[taskId]);

        auto& channel = AddChannel();
        channel.SrcTask = originTask.Id;
        channel.SrcOutputIndex = outputIndex;
        channel.DstTask = targetTask.Id;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Meta = meta;
        taskInput.Transform = transform;
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTask.Id, targetTask.Id, name, !channel.InMemory);
    }
}

void TKqpTasksGraph::BuildSequencerChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo, ui32 outputIndex,
    const NKqpProto::TKqpPhyCnSequencer& sequencer, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    NKikimrKqp::TKqpSequencerSettings* settings = GetMeta().Allocate<NKikimrKqp::TKqpSequencerSettings>();
    settings->MutableTable()->CopyFrom(sequencer.GetTable());
    settings->SetDatabase(GetMeta().Database);

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    THashSet<TString> autoIncrementColumns(sequencer.GetAutoIncrementColumns().begin(), sequencer.GetAutoIncrementColumns().end());

    for(const auto& column: sequencer.GetColumns()) {
        auto columnIt = tableInfo->Columns.find(column);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << column);
        const auto& columnInfo = columnIt->second;

        auto* columnProto = settings->AddColumns();
        columnProto->SetName(column);
        columnProto->SetId(columnInfo.Id);
        columnProto->SetTypeId(columnInfo.Type.GetTypeId());

        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(columnInfo.Type, columnInfo.TypeMod);
        if (columnType.TypeInfo) {
            *columnProto->MutableTypeInfo() = *columnType.TypeInfo;
        }

        auto aic = autoIncrementColumns.find(column);
        if (aic != autoIncrementColumns.end()) {
            auto sequenceIt = tableInfo->Sequences.find(column);
            if (sequenceIt != tableInfo->Sequences.end()) {
                auto sequencePath = sequenceIt->second.first;
                auto sequencePathId = sequenceIt->second.second;
                columnProto->SetDefaultFromSequence(sequencePath);
                sequencePathId.ToMessage(columnProto->MutableDefaultFromSequencePathId());
                columnProto->SetDefaultKind(
                    NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_SEQUENCE);
            } else {
                auto literalIt = tableInfo->DefaultFromLiteral.find(column);
                YQL_ENSURE(literalIt != tableInfo->DefaultFromLiteral.end());
                columnProto->MutableDefaultFromLiteral()->CopyFrom(literalIt->second);
                columnProto->SetDefaultKind(
                    NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_LITERAL);
            }
        }
    }

    TTransform transform;
    transform.Type = "SequencerInputTransformer";
    transform.InputType = sequencer.GetInputType();
    transform.OutputType = sequencer.GetOutputType();
    TTaskInputMeta meta;
    meta.SequencerSettings = settings;
    BuildTransformChannels(transform, meta, "Sequencer/Map", stageInfo, inputIndex,
        inputStageInfo, outputIndex, enableSpilling, logFunc);
}

void TKqpTasksGraph::BuildChannelBetweenTasks(const TStageInfo& stageInfo, const TStageInfo& inputStageInfo, ui64 originTaskId,
    ui64 targetTaskId, ui32 inputIndex, ui32 outputIndex, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    auto& originTask = GetTask(originTaskId);
    auto& targetTask = GetTask(targetTaskId);

    auto& channel = AddChannel();
    channel.SrcStageId = inputStageInfo.Id;
    channel.SrcTask = originTaskId;
    channel.SrcOutputIndex = outputIndex;
    channel.DstStageId = stageInfo.Id;
    channel.DstTask = targetTask.Id;
    channel.DstInputIndex = inputIndex;
    channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

    auto& taskInput = targetTask.Inputs[inputIndex];
    taskInput.Channels.push_back(channel.Id);

    auto& taskOutput = originTask.Outputs[outputIndex];
    taskOutput.Type = TTaskOutputType::Map;
    taskOutput.Channels.push_back(channel.Id);
    logFunc(channel.Id, originTaskId, targetTask.Id, "ParallelUnionAll/Map", !channel.InMemory);
}

void TKqpTasksGraph::BuildParallelUnionAllChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo,
    ui32 outputIndex, bool enableSpilling, const TChannelLogFunc& logFunc, ui64 &nextOriginTaskId)
{
    const ui64 inputStageTasksSize = inputStageInfo.Tasks.size();
    const ui64 originStageTasksSize = stageInfo.Tasks.size();
    Y_ENSURE(originStageTasksSize);
    Y_ENSURE(nextOriginTaskId < originStageTasksSize);

    for (ui64 i = 0; i < inputStageTasksSize; ++i) {
        const auto originTaskId = inputStageInfo.Tasks[i];
        const auto targetTaskId = stageInfo.Tasks[nextOriginTaskId];
        BuildChannelBetweenTasks(stageInfo, inputStageInfo, originTaskId, targetTaskId, inputIndex, outputIndex, enableSpilling, logFunc);
        nextOriginTaskId = (nextOriginTaskId + 1) % originStageTasksSize;
    }
}

void TKqpTasksGraph::BuildStreamLookupChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo, ui32 outputIndex,
    const NKqpProto::TKqpPhyCnStreamLookup& streamLookup, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    NKikimrKqp::TKqpStreamLookupSettings* settings = GetMeta().Allocate<NKikimrKqp::TKqpStreamLookupSettings>();

    settings->MutableTable()->CopyFrom(streamLookup.GetTable());

    auto columnToProto = [] (TString columnName,
        TMap<TString, NSharding::IShardingBase::TColumn>::const_iterator columnIt,
        ::NKikimrKqp::TKqpColumnMetadataProto* columnProto)
    {
        columnProto->SetName(columnName);
        columnProto->SetId(columnIt->second.Id);
        columnProto->SetTypeId(columnIt->second.Type.GetTypeId());

        if (NScheme::NTypeIds::IsParametrizedType(columnIt->second.Type.GetTypeId())) {
            ProtoFromTypeInfo(columnIt->second.Type, columnIt->second.TypeMod, *columnProto->MutableTypeInfo());
        }
    };

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    for (const auto& keyColumn : tableInfo->KeyColumns) {
        auto columnIt = tableInfo->Columns.find(keyColumn);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << keyColumn);

        auto* keyColumnProto = settings->AddKeyColumns();
        columnToProto(keyColumn, columnIt, keyColumnProto);
    }

    for (const auto& keyColumn : streamLookup.GetKeyColumns()) {
        auto columnIt = tableInfo->Columns.find(keyColumn);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << keyColumn);
        settings->AddLookupKeyColumns(keyColumn);
    }

    for (const auto& column : streamLookup.GetColumns()) {
        auto columnIt = tableInfo->Columns.find(column);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << column);

        auto* columnProto = settings->AddColumns();
        columnToProto(column, columnIt, columnProto);
    }

    settings->SetLookupStrategy(streamLookup.GetLookupStrategy());
    settings->SetKeepRowsOrder(streamLookup.GetKeepRowsOrder());
    settings->SetAllowNullKeysPrefixSize(streamLookup.GetAllowNullKeysPrefixSize());
    settings->SetIsolationLevel(GetMeta().RequestIsolationLevel);

    if (streamLookup.GetIsTableImmutable()
        && GetMeta().RequestIsolationLevel == NKikimrKqp::EIsolationLevel::ISOLATION_LEVEL_READ_STALE)
    {
        settings->SetAllowUseFollowers(true);
        settings->SetIsTableImmutable(true);
    }

    TTransform streamLookupTransform;
    streamLookupTransform.Type = "StreamLookupInputTransformer";
    streamLookupTransform.InputType = streamLookup.GetLookupKeysType();
    streamLookupTransform.OutputType = streamLookup.GetResultType();
    TTaskInputMeta meta;
    meta.StreamLookupSettings = settings;
    BuildTransformChannels(streamLookupTransform, meta, "StreamLookup/Map", stageInfo, inputIndex,
        inputStageInfo, outputIndex, enableSpilling, logFunc);
}

void TKqpTasksGraph::BuildVectorResolveChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo, ui32 outputIndex,
    const NKqpProto::TKqpPhyCnVectorResolve& vectorResolve, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    auto* settings = GetMeta().Allocate<NKikimrTxDataShard::TKqpVectorResolveSettings>();

    *settings->MutableIndexSettings() = vectorResolve.GetIndexSettings();

    YQL_ENSURE(stageInfo.Meta.IndexMetas.size() == 1);
    const auto& levelTableInfo = stageInfo.Meta.IndexMetas.back().TableConstInfo;

    auto* levelMeta = settings->MutableLevelTable();
    auto& kqpMeta = vectorResolve.GetLevelTable();
    levelMeta->SetTablePath(kqpMeta.GetPath());
    levelMeta->MutableTableId()->SetTableId(kqpMeta.GetTableId());
    levelMeta->MutableTableId()->SetOwnerId(kqpMeta.GetOwnerId());
    levelMeta->SetSchemaVersion(kqpMeta.GetVersion());
    levelMeta->SetTableKind((ui32)levelTableInfo->TableKind);

    settings->SetLevelTableParentColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::ParentColumn).Id);
    settings->SetLevelTableClusterColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::IdColumn).Id);
    settings->SetLevelTableCentroidColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::CentroidColumn).Id);
    *settings->MutableCopyColumnIndexes() = vectorResolve.GetCopyColumnIndexes();
    settings->SetVectorColumnIndex(vectorResolve.GetVectorColumnIndex());
    settings->SetClusterColumnOutPos(vectorResolve.GetClusterColumnOutPos());
    if (vectorResolve.HasRootClusterColumnIndex()) {
        settings->SetRootClusterColumnIndex(vectorResolve.GetRootClusterColumnIndex());
    }

    // Now fill InputTypes & InputTypeInfos

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    for (const auto& inputColumn : vectorResolve.GetColumns()) {
        if (inputColumn == NTableIndex::NKMeans::ParentColumn) {
            // Parent cluster ID for the prefixed index
            settings->AddInputColumnTypes(NScheme::NTypeIds::Uint64);
            *settings->AddInputColumnTypeInfos() = NKikimrProto::TTypeInfo();
            continue;
        }
        auto columnIt = tableInfo->Columns.find(inputColumn);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << inputColumn);

        settings->AddInputColumnTypes(columnIt->second.Type.GetTypeId());
        if (NScheme::NTypeIds::IsParametrizedType(columnIt->second.Type.GetTypeId())) {
            ProtoFromTypeInfo(columnIt->second.Type, columnIt->second.TypeMod, *settings->AddInputColumnTypeInfos());
        } else {
            *settings->AddInputColumnTypeInfos() = NKikimrProto::TTypeInfo();
        }
    }

    TTransform vectorResolveTransform;
    vectorResolveTransform.Type = "VectorResolveInputTransformer";
    vectorResolveTransform.InputType = vectorResolve.GetInputType();
    vectorResolveTransform.OutputType = vectorResolve.GetOutputType();
    TTaskInputMeta meta;
    meta.VectorResolveSettings = settings;
    BuildTransformChannels(vectorResolveTransform, meta, "VectorResolve/Map", stageInfo, inputIndex,
        inputStageInfo, outputIndex, enableSpilling, logFunc);
}

void TKqpTasksGraph::BuildDqSourceStreamLookupChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo,
    ui32 outputIndex, const NKqpProto::TKqpPhyCnDqSourceStreamLookup& dqSourceStreamLookup, const TChannelLogFunc& logFunc) {
    YQL_ENSURE(stageInfo.Tasks.size() == 1);

    auto* settings = GetMeta().Allocate<NDqProto::TDqInputTransformLookupSettings>();
    settings->SetLeftLabel(dqSourceStreamLookup.GetLeftLabel());
    settings->SetRightLabel(dqSourceStreamLookup.GetRightLabel());
    settings->SetJoinType(dqSourceStreamLookup.GetJoinType());
    settings->SetNarrowInputRowType(dqSourceStreamLookup.GetConnectionInputRowType());
    settings->SetNarrowOutputRowType(dqSourceStreamLookup.GetConnectionOutputRowType());
    settings->SetCacheLimit(dqSourceStreamLookup.GetCacheLimit());
    settings->SetCacheTtlSeconds(dqSourceStreamLookup.GetCacheTtlSeconds());
    settings->SetMaxDelayedRows(dqSourceStreamLookup.GetMaxDelayedRows());
    settings->SetIsMultiget(dqSourceStreamLookup.GetIsMultiGet());

    const auto& leftJointKeys = dqSourceStreamLookup.GetLeftJoinKeyNames();
    settings->MutableLeftJoinKeyNames()->Assign(leftJointKeys.begin(), leftJointKeys.end());

    const auto& rightJointKeys = dqSourceStreamLookup.GetRightJoinKeyNames();
    settings->MutableRightJoinKeyNames()->Assign(rightJointKeys.begin(), rightJointKeys.end());

    auto& streamLookupSource = *settings->MutableRightSource();
    streamLookupSource.SetSerializedRowType(dqSourceStreamLookup.GetLookupRowType());
    const auto& compiledSource = dqSourceStreamLookup.GetLookupSource();
    streamLookupSource.SetProviderName(compiledSource.GetType());
    *streamLookupSource.MutableLookupSource() = compiledSource.GetSettings();

    TTransform dqSourceStreamLookupTransform = {
        .Type = "StreamLookupInputTransform",
        .InputType = dqSourceStreamLookup.GetInputStageRowType(),
        .OutputType = dqSourceStreamLookup.GetOutputStageRowType(),
    };
    YQL_ENSURE(dqSourceStreamLookupTransform.Settings.PackFrom(*settings));

    for (const auto taskId : stageInfo.Tasks) {
        GetTask(taskId).Inputs[inputIndex].Transform = dqSourceStreamLookupTransform;
    }

    BuildUnionAllChannels(*this, stageInfo, inputIndex, inputStageInfo, outputIndex, /* enableSpilling */ false, logFunc);
}

void TKqpTasksGraph::BuildKqpStageChannels(TStageInfo& stageInfo, ui64 txId, bool enableSpilling, bool enableShuffleElimination) {
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    if (stage.GetIsEffectsStage() && stage.GetSinks().empty()) {
        YQL_ENSURE(stageInfo.OutputsCount == 1);

        for (auto& taskId : stageInfo.Tasks) {
            auto& task = GetTask(taskId);
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

    bool hasMap = false;
    auto& columnShardHashV1Params = stageInfo.Meta.ColumnShardHashV1Params;
    bool isFusedWithScanStage = (stageInfo.Meta.TableConstInfo != nullptr);
    if (enableShuffleElimination && !isFusedWithScanStage) { // taskIdHash can be already set if it is a fused stage, so hashpartition will derive columnv1 parameters from there
        for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
            const auto& input = stage.GetInputs(inputIndex);
            auto& originStageInfo = GetStageInfo(NYql::NDq::TStageId(stageInfo.Id.TxId, input.GetStageIndex()));
            ui32 outputIdx = input.GetOutputIndex();
            columnShardHashV1Params = originStageInfo.Meta.GetColumnShardHashV1Params(outputIdx);
            if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kMap || inputIndex == stage.InputsSize() - 1) { // this branch is only for logging purposes
                LOG_DEBUG_S(
                    *TlsActivationContext,
                    NKikimrServices::KQP_EXECUTER,
                    "Chosed "
                    << "[" << originStageInfo.Id.TxId << ":" << originStageInfo.Id.StageId << "]"
                    << " outputIdx: " << outputIdx << " to propagate through inputs stages of the stage "
                    << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]" << ": "
                    << columnShardHashV1Params.KeyTypesToString();
                );
            }
            if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kMap) {
                // We want to enforce sourceShardCount from map connection, cause it can be at most one map connection
                // and ColumnShardHash in Shuffle will use this parameter to shuffle on this map (same with taskIndexByHash mapping)
                hasMap = true;
                break;
            }
        }
    }

    // if it is stage, where we don't inherit parallelism.
    if (enableShuffleElimination && !hasMap && !isFusedWithScanStage && stageInfo.Tasks.size() > 0 && stage.InputsSize() > 0) {
        columnShardHashV1Params.SourceShardCount = stageInfo.Tasks.size();
        columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>(columnShardHashV1Params.SourceShardCount);
        for (std::size_t i = 0; i < columnShardHashV1Params.SourceShardCount; ++i) {
            (*columnShardHashV1Params.TaskIndexByHash)[i] = i;
        }

        for (auto& input : stage.GetInputs()) {
            if (input.GetTypeCase() != NKqpProto::TKqpPhyConnection::kHashShuffle) {
                continue;
            }

            const auto& hashShuffle = input.GetHashShuffle();
            if (hashShuffle.GetHashKindCase() != NKqpProto::TKqpPhyCnHashShuffle::kColumnShardHashV1) {
                continue;
            }

            Y_ENSURE(enableShuffleElimination, "OptShuffleElimination wasn't turned on, but ColumnShardHashV1 detected!");
            // ^ if the flag if false, and kColumnShardHashV1 detected - then the data which would be returned - would be incorrect,
            // because we didn't save partitioning in the BuildScanTasksFromShards.

            auto columnShardHashV1 = hashShuffle.GetColumnShardHashV1();
            columnShardHashV1Params.SourceTableKeyColumnTypes = std::make_shared<TVector<NScheme::TTypeInfo>>();
            columnShardHashV1Params.SourceTableKeyColumnTypes->reserve(columnShardHashV1.KeyColumnTypesSize());
            for (const auto& keyColumnType: columnShardHashV1.GetKeyColumnTypes()) {
                auto typeId = static_cast<NScheme::TTypeId>(keyColumnType);
                auto typeInfo =
                    typeId == NScheme::NTypeIds::Decimal? NScheme::TTypeInfo(NKikimr::NScheme::TDecimalType::Default()): NScheme::TTypeInfo(typeId);
                columnShardHashV1Params.SourceTableKeyColumnTypes->push_back(typeInfo);
            }
            break;
        }
    }

    ui64 nextOriginTaskId = 0;
    for (auto& input : stage.GetInputs()) {
        ui32 inputIdx = input.GetInputIndex();
        auto& inputStageInfo = GetStageInfo(TStageId(stageInfo.Id.TxId, input.GetStageIndex()));
        const auto& outputIdx = input.GetOutputIndex();

        switch (input.GetTypeCase()) {
            case NKqpProto::TKqpPhyConnection::kUnionAll:
                BuildUnionAllChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                std::optional<EHashShuffleFuncType> hashKind;
                auto forceSpilling = input.GetHashShuffle().GetUseSpilling();
                switch (input.GetHashShuffle().GetHashKindCase()) {
                    case NKqpProto::TKqpPhyCnHashShuffle::kHashV1: {
                        hashKind = EHashShuffleFuncType::HashV1;
                        break;
                    }
                    case NKqpProto::TKqpPhyCnHashShuffle::kHashV2: {
                        hashKind = EHashShuffleFuncType::HashV2;
                        break;
                    }
                    case NKqpProto::TKqpPhyCnHashShuffle::kColumnShardHashV1: {
                        Y_ENSURE(enableShuffleElimination, "OptShuffleElimination wasn't turned on, but ColumnShardHashV1 detected!");

                        LOG_DEBUG_S(
                            *TlsActivationContext,
                            NKikimrServices::KQP_EXECUTER,
                            "Propagating columnhashv1 params to stage"
                            << "[" << inputStageInfo.Id.TxId << ":" << inputStageInfo.Id.StageId << "]" << " which is input of stage "
                            << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]" << ": "
                            << columnShardHashV1Params.KeyTypesToString() << " "
                            << "[" << JoinSeq(",", input.GetHashShuffle().GetKeyColumns()) << "]";
                        );

                        Y_ENSURE(
                            columnShardHashV1Params.SourceTableKeyColumnTypes->size() == input.GetHashShuffle().KeyColumnsSize(),
                            TStringBuilder{}
                                << "Hashshuffle keycolumns and keytypes args count mismatch during executer stage, types: "
                                << columnShardHashV1Params.KeyTypesToString() << " for the columns: "
                                << "[" << JoinSeq(",", input.GetHashShuffle().GetKeyColumns()) << "]"
                        );

                        inputStageInfo.Meta.HashParamsByOutput[outputIdx] = columnShardHashV1Params;
                        hashKind = EHashShuffleFuncType::ColumnShardHashV1;
                        break;
                    }
                    default: {
                        Y_ENSURE(false, "undefined type of hash for shuffle");
                    }
                }

                Y_ENSURE(hashKind.has_value(), "HashKind wasn't set!");
                BuildHashShuffleChannels(
                    *this,
                    stageInfo,
                    inputIdx,
                    inputStageInfo,
                    outputIdx,
                    input.GetHashShuffle().GetKeyColumns(),
                    enableSpilling,
                    log,
                    hashKind.value(),
                    forceSpilling
                );
                break;
            }
            case NKqpProto::TKqpPhyConnection::kBroadcast:
                BuildBroadcastChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            case NKqpProto::TKqpPhyConnection::kMap:
                BuildMapChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
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
                BuildMergeChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, sortColumns, log);
                break;
            }
            case NKqpProto::TKqpPhyConnection::kSequencer: {
                BuildSequencerChannels(stageInfo, inputIdx, inputStageInfo, outputIdx,
                    input.GetSequencer(), enableSpilling, log);
                break;
            }

            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                BuildStreamLookupChannels(stageInfo, inputIdx, inputStageInfo, outputIdx,
                    input.GetStreamLookup(), enableSpilling, log);
                break;
            }

            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                BuildParallelUnionAllChannels(stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log, nextOriginTaskId);
                break;
            }

            case NKqpProto::TKqpPhyConnection::kVectorResolve: {
                BuildVectorResolveChannels(stageInfo, inputIdx, inputStageInfo, outputIdx, input.GetVectorResolve(), enableSpilling, log);
                break;
            }

            case NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup: {
                BuildDqSourceStreamLookupChannels(stageInfo, inputIdx, inputStageInfo, outputIdx,
                    input.GetDqSourceStreamLookup(), log);
                break;
            }

            default:
                YQL_ENSURE(false, "Unexpected stage input type: " << (ui32)input.GetTypeCase());
        }
    }
}


void TShardKeyRanges::AddPoint(TSerializedCellVec&& point) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(point));
    }
}

void TShardKeyRanges::AddRange(TSerializedTableRange&& range) {
    Y_DEBUG_ABORT_UNLESS(!range.Point);
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(range));
    }
}

void TShardKeyRanges::Add(TSerializedPointOrRange&& pointOrRange) {
    if (!IsFullRange()) {
        Ranges.emplace_back(std::move(pointOrRange));
        if (std::holds_alternative<TSerializedTableRange>(Ranges.back())) {
            Y_DEBUG_ABORT_UNLESS(!std::get<TSerializedTableRange>(Ranges.back()).Point);
        }
    }
}

void TShardKeyRanges::CopyFrom(const TVector<TSerializedPointOrRange>& ranges) {
    if (!IsFullRange()) {
        Ranges = ranges;
        for (auto& x : Ranges) {
            if (std::holds_alternative<TSerializedTableRange>(x)) {
                Y_DEBUG_ABORT_UNLESS(!std::get<TSerializedTableRange>(x).Point);
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

TString TShardKeyRanges::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const {
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

bool TShardKeyRanges::HasRanges() const {
    if (IsFullRange()) {
        return true;
    }
    for (const auto& range : Ranges) {
        if (std::holds_alternative<TSerializedTableRange>(range)) {
            return true;
        }
    }
    return false;
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
                Y_DEBUG_ABORT_UNLESS(!x.Point);
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
                Y_DEBUG_ABORT_UNLESS(!x.Point);
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

void TShardKeyRanges::SerializeTo(NKikimrTxDataShard::TKqpReadRangesSourceSettings* proto, bool allowPoints) const {
    if (IsFullRange()) {
        auto& protoRange = *proto->MutableRanges()->AddKeyRanges();
        FullRange->Serialize(protoRange);
    } else {
        bool usePoints = allowPoints;
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
                Y_DEBUG_ABORT_UNLESS(!x.Point);
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

void FillEndpointDesc(NDqProto::TEndpoint& endpoint, const TTask& task) {
    if (task.ComputeActorId) {
        ActorIdToProto(task.ComputeActorId, endpoint.MutableActorId());
    }
}

void TKqpTasksGraph::FillChannelDesc(NDqProto::TChannel& channelDesc, const TChannel& channel,
    const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion, bool enableSpilling) const
{
    channelDesc.SetId(channel.Id);
    channelDesc.SetSrcStageId(channel.SrcStageId.StageId);
    channelDesc.SetDstStageId(channel.DstStageId.StageId);
    channelDesc.SetSrcTaskId(channel.SrcTask);
    channelDesc.SetDstTaskId(channel.DstTask);
    channelDesc.SetEnableSpilling(enableSpilling);
    channelDesc.SetCheckpointingMode(channel.CheckpointingMode);
    channelDesc.SetWatermarksMode(channel.WatermarksMode);

    const auto& resultChannelProxies = GetMeta().ResultChannelProxies;

    YQL_ENSURE(channel.SrcTask);
    const auto& srcTask = GetTask(channel.SrcTask);
    FillEndpointDesc(*channelDesc.MutableSrcEndpoint(), srcTask);

    if (channel.DstTask) {
        FillEndpointDesc(*channelDesc.MutableDstEndpoint(), GetTask(channel.DstTask));
    } else if (!resultChannelProxies.empty()) {
        auto it = resultChannelProxies.find(channel.Id);
        YQL_ENSURE(it != resultChannelProxies.end());
        ActorIdToProto(it->second, channelDesc.MutableDstEndpoint()->MutableActorId());
    } else {
        // For non-stream execution, collect results in executer and forward with response.
        ActorIdToProto(srcTask.Meta.ExecuterId, channelDesc.MutableDstEndpoint()->MutableActorId());
    }

    channelDesc.SetIsPersistent(false);
    channelDesc.SetInMemory(channel.InMemory);
    if (chanTransportVersion == NKikimrConfig::TTableServiceConfig::CTV_OOB_PICKLE_1_0) {
        channelDesc.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0);
    } else {
        channelDesc.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0);
    }
}

void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta) {
    meta->SetTablePath(stageInfo.Meta.TablePath);
    meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
    meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
    meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
    meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
    meta->SetTableKind((ui32)stageInfo.Meta.TableKind);
}

void FillTaskMeta(const TStageInfo& stageInfo, const TTask& task, NYql::NDqProto::TDqTask& taskDesc) {
    if (task.Meta.ShardId && task.Meta.Writes) {
        NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta protoTaskMeta;

        FillTableMeta(stageInfo, protoTaskMeta.MutableTable());

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
        if (stageInfo.Meta.TableConstInfo->SysViewInfo) {
            *protoTaskMeta.MutableTable()->MutableSysViewDescription() = *stageInfo.Meta.TableConstInfo->SysViewInfo;
        }

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;

        for (const auto& keyColumnName : tableInfo->KeyColumns) {
            const auto& keyColumn = tableInfo->Columns.at(keyColumnName);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(keyColumn.Type, keyColumn.TypeMod);
            protoTaskMeta.AddKeyColumnTypes(columnType.TypeId);
            *protoTaskMeta.AddKeyColumnTypeInfos() = columnType.TypeInfo ?
                *columnType.TypeInfo :
                NKikimrProto::TTypeInfo();
        }

        for (bool skipNullKey : stageInfo.Meta.SkipNullKeys) {
            protoTaskMeta.AddSkipNullKeys(skipNullKey);
        }

        switch (tableInfo->TableKind) {
            case ETableKind::Unknown:
            case ETableKind::External:
            case ETableKind::SysView: {
                protoTaskMeta.SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
                break;
            }
            case ETableKind::Datashard: {
                if (AppData()->FeatureFlags.GetEnableArrowFormatAtDatashard()) {
                    protoTaskMeta.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
                } else {
                    protoTaskMeta.SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
                }
                break;
            }
            case ETableKind::Olap: {
                protoTaskMeta.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
                break;
            }
        }

        YQL_ENSURE(!task.Meta.Writes);

        if (!task.Meta.Reads->empty()) {
            protoTaskMeta.SetReverse(task.Meta.ReadInfo.IsReverse());
            protoTaskMeta.SetOptionalSorting((ui32)task.Meta.ReadInfo.GetSorting());
            protoTaskMeta.SetItemsLimit(task.Meta.ReadInfo.ItemsLimit);
            if (task.Meta.HasEnableShardsSequentialScan()) {
                protoTaskMeta.SetEnableShardsSequentialScan(task.Meta.GetEnableShardsSequentialScanUnsafe());
            }
            protoTaskMeta.SetReadType(ReadTypeToProto(task.Meta.ReadInfo.ReadType));

            for (auto&& i : task.Meta.ReadInfo.GroupByColumnNames) {
                protoTaskMeta.AddGroupByColumnNames(i.data(), i.size());
            }

            for (auto columnType : task.Meta.ReadInfo.ResultColumnsTypes) {
                auto* protoResultColumn = protoTaskMeta.AddResultColumns();
                protoResultColumn->SetId(0);
                auto protoColumnType = NScheme::ProtoColumnTypeFromTypeInfoMod(columnType, "");
                protoResultColumn->SetType(protoColumnType.TypeId);
                if (protoColumnType.TypeInfo) {
                    *protoResultColumn->MutableTypeInfo() = *protoColumnType.TypeInfo;
                }
            }

            if (tableInfo->TableKind == ETableKind::Olap) {
                auto* olapProgram = protoTaskMeta.MutableOlapProgram();
                auto [schema, parameters] = SerializeKqpTasksParametersForOlap(stageInfo, task);

                olapProgram->SetProgram(task.Meta.ReadInfo.OlapProgram.Program);

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

void TKqpTasksGraph::FillOutputDesc(NYql::NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output, ui32 outputIdx,
    bool enableSpilling, const TStageInfo& stageInfo) const
{
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

            Y_ENSURE(output.HashKind.has_value(), "HashKind wasn't set before the FillOutputDesc!");

            switch (output.HashKind.value()) {
                using enum EHashShuffleFuncType;
                case HashV1: {
                    hashPartitionDesc.MutableHashV1();
                    break;
                }
                case HashV2: {
                    hashPartitionDesc.MutableHashV2();
                    break;
                }
                case ColumnShardHashV1: {
                    auto& columnShardHashV1Params = stageInfo.Meta.GetColumnShardHashV1Params(outputIdx);
                    LOG_DEBUG_S(
                        *TlsActivationContext,
                        NKikimrServices::KQP_EXECUTER,
                        "Filling columnshardhashv1 params for sending it to runtime "
                        << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]"
                        << ": " << columnShardHashV1Params.KeyTypesToString()
                        << " for the columns: " << "[" << JoinSeq(",", output.KeyColumns) << "]"
                    );
                    Y_ENSURE(columnShardHashV1Params.SourceShardCount != 0, "ShardCount for ColumnShardHashV1 Shuffle can't be equal to 0");
                    Y_ENSURE(columnShardHashV1Params.TaskIndexByHash != nullptr, "TaskIndexByHash for ColumnShardHashV1 wasn't propagated to this stage");
                    Y_ENSURE(columnShardHashV1Params.SourceTableKeyColumnTypes != nullptr, "SourceTableKeyColumnTypes for ColumnShardHashV1 wasn't propagated to this stage");

                    Y_ENSURE(
                        columnShardHashV1Params.SourceTableKeyColumnTypes->size() == output.KeyColumns.size(),
                        TStringBuilder{}
                            << "Hashshuffle keycolumns and keytypes args count mismatch during executer FillOutputDesc stage, types: "
                            << columnShardHashV1Params.KeyTypesToString() << " for the columns: "
                            << "[" << JoinSeq(",", output.KeyColumns) << "]"
                    );

                    auto& columnShardHashV1 = *hashPartitionDesc.MutableColumnShardHashV1();
                    columnShardHashV1.SetShardCount(columnShardHashV1Params.SourceShardCount);

                    auto* columnTypes = columnShardHashV1.MutableKeyColumnTypes();
                    for (const auto& type: *columnShardHashV1Params.SourceTableKeyColumnTypes) {
                        columnTypes->Add(type.GetTypeId());
                    }

                    auto* taskIndexByHash = columnShardHashV1.MutableTaskIndexByHash();
                    for (std::size_t taskID: *columnShardHashV1Params.TaskIndexByHash) {
                        taskIndexByHash->Add(taskID);
                    }
                    break;
                }
            }
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

        case TTaskOutputType::Sink: {
            auto* sink = outputDesc.MutableSink();
            sink->SetType(output.SinkType);
            YQL_ENSURE(output.SinkSettings);
            sink->MutableSettings()->CopyFrom(*output.SinkSettings);
            break;
        }

        default: {
            YQL_ENSURE(false, "Unexpected task output type " << output.Type);
        }
    }

    for (auto& channel : output.Channels) {
        auto& channelDesc = *outputDesc.AddChannels();
        FillChannelDesc(channelDesc, GetChannel(channel), GetMeta().ChannelTransportVersion, enableSpilling);
    }
}

void TKqpTasksGraph::FillInputDesc(NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input, bool serializeAsyncIoSettings, bool& enableMetering) const {
    const auto& snapshot = GetMeta().Snapshot;
    const auto& lockTxId = GetMeta().LockTxId;

    switch (input.Type()) {
        case NYql::NDq::TTaskInputType::Source:
            inputDesc.MutableSource()->SetType(input.SourceType);
            inputDesc.MutableSource()->SetWatermarksMode(input.WatermarksMode);
            if (Y_LIKELY(input.Meta.SourceSettings)) {
                enableMetering = true;
                YQL_ENSURE(input.Meta.SourceSettings->HasTable());
                bool isTableImmutable = input.Meta.SourceSettings->GetIsTableImmutable();

                if (snapshot.IsValid() && !isTableImmutable) {
                    input.Meta.SourceSettings->MutableSnapshot()->SetStep(snapshot.Step);
                    input.Meta.SourceSettings->MutableSnapshot()->SetTxId(snapshot.TxId);
                }

                if (GetMeta().UseFollowers || isTableImmutable) {
                    input.Meta.SourceSettings->SetUseFollowers(GetMeta().UseFollowers || isTableImmutable);
                }

                if (serializeAsyncIoSettings) {
                    inputDesc.MutableSource()->MutableSettings()->PackFrom(*input.Meta.SourceSettings);
                }

                if (isTableImmutable) {
                    input.Meta.SourceSettings->SetAllowInconsistentReads(true);
                }

            } else {
                YQL_ENSURE(input.SourceSettings);
                inputDesc.MutableSource()->MutableSettings()->CopyFrom(*input.SourceSettings);
            }

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
        FillChannelDesc(channelDesc, GetChannel(channel), GetMeta().ChannelTransportVersion, false);
    }

    if (input.Transform) {
        auto* transformProto = inputDesc.MutableTransform();
        transformProto->SetType(input.Transform->Type);
        transformProto->SetInputType(input.Transform->InputType);
        transformProto->SetOutputType(input.Transform->OutputType);
        if (input.Meta.StreamLookupSettings) {
            enableMetering = true;
            YQL_ENSURE(input.Meta.StreamLookupSettings);
            bool isTableImmutable = input.Meta.StreamLookupSettings->GetIsTableImmutable() &&
                GetMeta().RequestIsolationLevel == NKikimrKqp::EIsolationLevel::ISOLATION_LEVEL_READ_STALE;

            if (snapshot.IsValid() && !isTableImmutable) {
                input.Meta.StreamLookupSettings->MutableSnapshot()->SetStep(snapshot.Step);
                input.Meta.StreamLookupSettings->MutableSnapshot()->SetTxId(snapshot.TxId);
            } else {
                YQL_ENSURE(GetMeta().AllowInconsistentReads || isTableImmutable, "Expected valid snapshot or enabled inconsistent read mode");
                input.Meta.StreamLookupSettings->SetAllowInconsistentReads(true);
            }

            if (lockTxId && !isTableImmutable) {
                input.Meta.StreamLookupSettings->SetLockTxId(*lockTxId);
                input.Meta.StreamLookupSettings->SetLockNodeId(GetMeta().LockNodeId);
            }

            if (GetMeta().LockMode && !isTableImmutable) {
                input.Meta.StreamLookupSettings->SetLockMode(*GetMeta().LockMode);
            }

            transformProto->MutableSettings()->PackFrom(*input.Meta.StreamLookupSettings);
        } else if (input.Meta.SequencerSettings) {
            transformProto->MutableSettings()->PackFrom(*input.Meta.SequencerSettings);
        } else if (input.Meta.VectorResolveSettings) {
            enableMetering = true;
            YQL_ENSURE(input.Meta.VectorResolveSettings);

            YQL_ENSURE(snapshot.IsValid());
            input.Meta.VectorResolveSettings->MutableSnapshot()->SetStep(snapshot.Step);
            input.Meta.VectorResolveSettings->MutableSnapshot()->SetTxId(snapshot.TxId);

            if (lockTxId) {
                input.Meta.VectorResolveSettings->SetLockTxId(*lockTxId);
                input.Meta.VectorResolveSettings->SetLockNodeId(GetMeta().LockNodeId);
            }

            if (GetMeta().LockMode) {
                input.Meta.VectorResolveSettings->SetLockMode(*GetMeta().LockMode);
            }

            transformProto->MutableSettings()->PackFrom(*input.Meta.VectorResolveSettings);
        } else {
            *transformProto->MutableSettings() = input.Transform->Settings;
        }
    }
}

void TKqpTasksGraph::SerializeTaskToProto(const TTask& task, NYql::NDqProto::TDqTask* result, bool serializeAsyncIoSettings) const {
    auto& stageInfo = GetStageInfo(task.StageId);
    ActorIdToProto(task.Meta.ExecuterId, result->MutableExecuter()->MutableActorId());
    result->SetId(task.Id);
    result->SetStageId(stageInfo.Id.StageId);
    result->SetUseLlvm(task.GetUseLlvm());
    result->SetEnableSpilling(false); // TODO: enable spilling
    if (task.HasMetaId()) {
        result->SetMetaId(task.GetMetaIdUnsafe());
    }
    bool enableMetering = false;

    for (const auto& [paramName, paramValue] : task.Meta.TaskParams) {
        (*result->MutableTaskParams())[paramName] = paramValue;
    }

    for (const auto& readRange : task.Meta.ReadRanges) {
        result->AddReadRanges(readRange);
    }

    for (const auto& [paramName, paramValue] : task.Meta.SecureParams) {
        (*result->MutableSecureParams())[paramName] = paramValue;
    }

    for (const auto& input : task.Inputs) {
        FillInputDesc(*result->AddInputs(), input, serializeAsyncIoSettings, enableMetering);
    }

    bool enableSpilling = false;
    if (task.Outputs.size() > 1) {
        enableSpilling = GetMeta().AllowWithSpilling;
    }
    for (ui32 outputIdx = 0; outputIdx < task.Outputs.size(); ++outputIdx) {
        const auto& output = task.Outputs[outputIdx];
        FillOutputDesc(*result->AddOutputs(), output, outputIdx, enableSpilling, stageInfo);
    }

    const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    result->MutableProgram()->CopyFrom(stage.GetProgram());

    for (auto& paramName : stage.GetProgramParameters()) {
        auto& dqParams = *result->MutableParameters();
        if (task.Meta.ShardId) {
            dqParams[paramName] = stageInfo.Meta.Tx.Params->GetShardParam(task.Meta.ShardId, paramName);
        } else {
            dqParams[paramName] = stageInfo.Meta.Tx.Params->SerializeParamValue(paramName);
        }
    }

    SerializeCtxToMap(*GetMeta().UserRequestContext, *result->MutableRequestContext());

    result->SetDisableMetering(!enableMetering);
    result->SetCreateSuspended(GetMeta().CreateSuspended);
    FillTaskMeta(stageInfo, task, *result);
}

NYql::NDqProto::TDqTask* TKqpTasksGraph::ArenaSerializeTaskToProto(const TTask& task, bool serializeAsyncIoSettings) {
    NYql::NDqProto::TDqTask* result = GetMeta().Allocate<NYql::NDqProto::TDqTask>();
    SerializeTaskToProto(task, result, serializeAsyncIoSettings);
    return result;
}

void TKqpTasksGraph::PersistTasksGraphInfo(NKikimrKqp::TQueryPhysicalGraph& result) const {
    auto& resultTasks = *result.MutableTasks();

    const auto& tasks = GetTasks();
    resultTasks.Reserve(tasks.size());
    for (const auto& task : tasks) {
        auto& resultTask = *resultTasks.Add();
        resultTask.SetTxId(task.StageId.TxId);

        auto* taskInfo = resultTask.MutableDqTask();
        SerializeTaskToProto(task, taskInfo, false);

        taskInfo->ClearProgram();
        taskInfo->ClearSecureParams();
    }
}

// Restored graph only requires to update authentication secrets
// and to reassign existing tasks between actual nodes.
void TKqpTasksGraph::RestoreTasksGraphInfo(const NKikimrKqp::TQueryPhysicalGraph& graphInfo) {
    const auto restoreDqTransform = [](const auto& protoInfo) -> TMaybe<TTransform> {
        if (!protoInfo.HasTransform()) {
            return Nothing();
        }

        const auto& transformInfo = protoInfo.GetTransform();
        return TTransform{
            .Type = transformInfo.GetType(),
            .InputType = transformInfo.GetInputType(),
            .OutputType = transformInfo.GetOutputType(),
            .Settings = transformInfo.GetSettings(),
        };
    };

    std::map<ui64, TChannel> channels;
    const auto restoreDqChannel = [&channels](ui64 txId, const NYql::NDqProto::TChannel& protoInfo) -> TChannel& {
        const auto [it, inserted] = channels.emplace(protoInfo.GetId(), TChannel());
        auto& channel = it->second;

        if (inserted) {
            channel.Id = protoInfo.GetId();
            channel.SrcStageId = TStageId(txId, protoInfo.GetSrcStageId());
            channel.SrcTask = protoInfo.GetSrcTaskId();
            channel.DstStageId = TStageId(txId, protoInfo.GetDstStageId());
            channel.DstTask = protoInfo.GetDstTaskId();
            channel.InMemory = protoInfo.GetInMemory();
        }

        return channel;
    };

    for (size_t taskIdx = 0; taskIdx < graphInfo.TasksSize(); ++taskIdx) {
        const auto& task = graphInfo.GetTasks(taskIdx);
        const auto txId = task.GetTxId();
        const auto& taskInfo = task.GetDqTask();

        auto& stageInfo = GetStageInfo({txId, taskInfo.GetStageId()});
        auto& newTask = AddTask(stageInfo);
        YQL_ENSURE(taskInfo.GetId() == newTask.Id);
        newTask.Meta.TaskParams.insert(taskInfo.GetTaskParams().begin(), taskInfo.GetTaskParams().end());
        newTask.Meta.ReadRanges.assign(taskInfo.GetReadRanges().begin(), taskInfo.GetReadRanges().end());

        for (size_t inputIdx = 0; inputIdx < taskInfo.InputsSize(); ++inputIdx) {
            const auto& inputInfo = taskInfo.GetInputs(inputIdx);
            auto& newInput = newTask.Inputs[inputIdx];
            newInput.Transform = restoreDqTransform(inputInfo);

            switch (inputInfo.GetTypeCase()) {
                case NDqProto::TTaskInput::kMerge: {
                    const auto& sortColumnsInfo = inputInfo.GetMerge().GetSortColumns();

                    TVector<TSortColumn> sortColumns;
                    sortColumns.reserve(sortColumnsInfo.size());
                    for (const auto& sortColumnInfo : sortColumnsInfo) {
                        sortColumns.emplace_back(sortColumnInfo.GetColumn(), sortColumnInfo.GetAscending());
                    }

                    newInput.ConnectionInfo = TMergeTaskInput(sortColumns);
                    break;
                }
                case NDqProto::TTaskInput::kSource: {
                    newInput.ConnectionInfo = TSourceInput();

                    const auto& sourceInfo = inputInfo.GetSource();
                    newInput.SourceType = sourceInfo.GetType();
                    if (sourceInfo.HasSettings()) {
                        newInput.SourceSettings = sourceInfo.GetSettings();
                    }
                    break;
                }
                case NDqProto::TTaskInput::kUnionAll: {
                    break;
                }
                case NDqProto::TTaskInput::TYPE_NOT_SET: {
                    YQL_ENSURE(false, "Unknown input type");
                    break;
                }
            }

            newInput.Channels.reserve(inputInfo.ChannelsSize());
            for (const auto& channelInfo : inputInfo.GetChannels()) {
                newInput.Channels.emplace_back(channelInfo.GetId());
                restoreDqChannel(txId, channelInfo).DstInputIndex = inputIdx;
            }
        }

        for (size_t outputIdx = 0; outputIdx < taskInfo.OutputsSize(); ++outputIdx) {
            const auto& outputInfo = taskInfo.GetOutputs(outputIdx);
            auto& newOutput = newTask.Outputs[outputIdx];
            newOutput.Transform = restoreDqTransform(outputInfo);

            switch (outputInfo.GetTypeCase()) {
                case NDqProto::TTaskOutput::kMap: {
                    newOutput.Type = TTaskOutputType::Map;
                    break;
                }
                case NDqProto::TTaskOutput::kRangePartition: {
                    YQL_ENSURE(false, "Range partition output type is not supported for restore");
                    break;
                }
                case NDqProto::TTaskOutput::kHashPartition: {
                    newOutput.Type = TTaskOutputType::HashPartition;

                    const auto& hashInfo = outputInfo.GetHashPartition();
                    newOutput.KeyColumns.assign(hashInfo.GetKeyColumns().begin(), hashInfo.GetKeyColumns().end());
                    newOutput.PartitionsCount = hashInfo.GetPartitionsCount();

                    switch (hashInfo.GetHashKindCase()) {
                        case NDqProto::TTaskOutputHashPartition::kHashV1:
                            newOutput.HashKind = EHashShuffleFuncType::HashV1;
                            break;
                        case NDqProto::TTaskOutputHashPartition::kHashV2:
                            newOutput.HashKind = EHashShuffleFuncType::HashV2;
                            break;
                        case NDqProto::TTaskOutputHashPartition::kColumnShardHashV1:
                            newOutput.HashKind = EHashShuffleFuncType::ColumnShardHashV1;
                            break;
                        case NDqProto::TTaskOutputHashPartition::HASHKIND_NOT_SET:
                            YQL_ENSURE(false, "Hash kind not set");
                            break;
                    }

                    break;
                }
                case NDqProto::TTaskOutput::kBroadcast: {
                    newOutput.Type = TTaskOutputType::Broadcast;
                    break;
                }
                case NDqProto::TTaskOutput::kEffects: {
                    newOutput.Type = TTaskOutputType::Effects;
                    break;
                }
                case NDqProto::TTaskOutput::kSink: {
                    newOutput.Type = TTaskOutputType::Sink;

                    const auto& sinkInfo = outputInfo.GetSink();
                    newOutput.SinkType = sinkInfo.GetType();
                    if (sinkInfo.HasSettings()) {
                        newOutput.SinkSettings = sinkInfo.GetSettings();
                    }
                    break;
                }
                case NDqProto::TTaskOutput::TYPE_NOT_SET: {
                    YQL_ENSURE(false, "Unknown output type");
                    break;
                }
            }
            YQL_ENSURE(newOutput.Type);

            newOutput.Channels.reserve(outputInfo.ChannelsSize());
            for (const auto& channelInfo : outputInfo.GetChannels()) {
                newOutput.Channels.emplace_back(channelInfo.GetId());
                restoreDqChannel(txId, channelInfo).SrcOutputIndex = outputIdx;
            }
        }
    }

    for (const auto& [id, channel] : channels) {
        auto& newChannel = AddChannel();
        newChannel = channel;
        YQL_ENSURE(id == newChannel.Id);
    }

    GetMeta().IsRestored = true;
}

void TKqpTasksGraph::BuildSysViewScanTasks(TStageInfo& stageInfo) {
    Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.IsSysView());

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    for (auto& op : stage.GetTableOps()) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

        auto& task = AddTask(stageInfo);
        TShardKeyRanges keyRanges;

        switch (op.GetTypeCase()) {
            case NKqpProto::TKqpPhyTableOperation::kReadRange:
                stageInfo.Meta.SkipNullKeys.assign(
                    op.GetReadRange().GetSkipNullKeys().begin(),
                    op.GetReadRange().GetSkipNullKeys().end()
                );
                keyRanges.Add(MakeKeyRange(
                    keyTypes, op.GetReadRange().GetKeyRange(),
                    stageInfo, holderFactory, typeEnv)
                );
                break;
            case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                keyRanges.CopyFrom(FillReadRanges(keyTypes, op.GetReadRanges(), stageInfo, typeEnv));
                break;
            default:
                YQL_ENSURE(false, "Unexpected table scan operation: " << (ui32) op.GetTypeCase());
        }

        TTaskMeta::TShardReadInfo readInfo = {
            .Ranges = std::move(keyRanges),
            .Columns = BuildKqpColumns(op, tableInfo),
        };

        auto readSettings = ExtractReadSettings(op, stageInfo, holderFactory, typeEnv);
        task.Meta.Reads.ConstructInPlace();
        task.Meta.Reads->emplace_back(std::move(readInfo));
        task.Meta.ReadInfo.SetSorting(readSettings.GetSorting());
        task.Meta.Type = TTaskMeta::TTaskType::Compute;

        // TODO: LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
    }
}

ui32 TKqpTasksGraph::GetMaxTasksAggregation(TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount) {
    auto& intros = stageInfo.Introspections;
    if (AggregationSettings.HasAggregationComputeThreads()) {
        auto threads = AggregationSettings.GetAggregationComputeThreads();
        intros.push_back("Considering AggregationComputeThreads value - " + ToString(threads));
        return std::max<ui32>(1, threads);
    } else if (nodesCount) {
        const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
        auto result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), previousTasksCount / nodesCount, intros) * nodesCount;
        intros.push_back("Predicted value for aggregation - " + ToString(result));
        return result;
    } else {
        intros.push_back("Unknown nodes count for aggregation - using value 1");
        return 1;
    }
}

bool TKqpTasksGraph::BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
    auto& intros = stageInfo.Introspections;
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    // TODO: move outside
    if (GetMeta().IsRestored) {
        for (const auto taskId : stageInfo.Tasks) {
            auto& task = GetTask(taskId);
            task.Meta.Type = TTaskMeta::TTaskType::Compute;
        }
        return false;
    }

    bool unknownAffectedShardCount = false;
    ui32 partitionsCount = 1;
    ui32 inputTasks = 0;
    bool isShuffle = false;
    bool forceMapTasks = false;
    bool isParallelUnionAll = false;
    ui32 mapConnectionCount = 0;

    for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
        const auto& input = stage.GetInputs(inputIndex);

        // Current assumptions:
        // 1. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll`
        // 2. Stages where 1st input is `Broadcast` are not partitioned.
        if (inputIndex > 0) {
            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kBroadcast:
                case NKqpProto::TKqpPhyConnection::kHashShuffle:
                case NKqpProto::TKqpPhyConnection::kUnionAll:
                case NKqpProto::TKqpPhyConnection::kMerge:
                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                case NKqpProto::TKqpPhyConnection::kMap:
                case NKqpProto::TKqpPhyConnection::kParallelUnionAll:
                case NKqpProto::TKqpPhyConnection::kVectorResolve:
                case NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase() << Endl);
                    // TODO: << this->DebugString());
            }
        }

        auto& originStageInfo = GetStageInfo(NYql::NDq::TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

        switch (input.GetTypeCase()) {
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                inputTasks += originStageInfo.Tasks.size();
                isShuffle = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                intros.push_back("Resetting compute tasks count because input " + ToString(inputIndex) + " is StreamLookup - " + ToString(partitionsCount));
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                partitionsCount = originStageInfo.Tasks.size();
                forceMapTasks = true;
                ++mapConnectionCount;
                intros.push_back("Resetting compute tasks count because input " + ToString(inputIndex) + " is Map - " + ToString(partitionsCount));
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                inputTasks += originStageInfo.Tasks.size();
                isParallelUnionAll = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kVectorResolve: {
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                intros.push_back("Resetting compute tasks count because input " + ToString(inputIndex) + " is VectorResolve - " + ToString(partitionsCount));
                break;
            }
            default:
                break;
        }

    }

    Y_ENSURE(mapConnectionCount <= 1, "Only a single map connection is allowed");

    if ((isShuffle || isParallelUnionAll) && !forceMapTasks) {
        if (stage.GetTaskCount()) {
            partitionsCount = stage.GetTaskCount();
            intros.push_back("Manually overridden - " + ToString(partitionsCount));
        } else {
            partitionsCount = std::max(partitionsCount, GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount));
        }
    }

    intros.push_back("Actual number of compute tasks - " + ToString(partitionsCount));

    for (ui32 i = 0; i < partitionsCount; ++i) {
        auto& task = AddTask(stageInfo);
        task.Meta.Type = TTaskMeta::TTaskType::Compute;
        // TODO: LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
    }

    return unknownAffectedShardCount;
}

void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, const NYql::ERequestSorting sorting) {
    if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
        // Validate parameters
        YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
        YQL_ENSURE(taskMeta.ReadInfo.GetSorting() == sorting);
        return;
    }

    taskMeta.ReadInfo.ItemsLimit = itemsLimit;
    taskMeta.ReadInfo.SetSorting(sorting);
    taskMeta.ReadInfo.ReadType = TTaskMeta::TReadInfo::EReadType::Rows;
}

TTaskMeta::TReadInfo::EReadType OlapReadTypeFromProto(const NKqpProto::TKqpPhyOpReadOlapRanges::EReadType& type) {
    switch (type) {
        case NKqpProto::TKqpPhyOpReadOlapRanges::ROWS:
            return TTaskMeta::TReadInfo::EReadType::Rows;
        case NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS:
            return TTaskMeta::TReadInfo::EReadType::Blocks;
        default:
            YQL_ENSURE(false, "Invalid read type from TKqpPhyOpReadOlapRanges protobuf.");
    }
}

void FillOlapReadInfo(TTaskMeta& taskMeta, NKikimr::NMiniKQL::TType* resultType, const TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>& readOlapRange) {
    if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
        // Validate parameters
        if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
            YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program.empty());
            return;
        }

        YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program == readOlapRange->GetOlapProgram());
        return;
    }

    if (resultType) {
        YQL_ENSURE(resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct
            || resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);

        auto* resultStructType = static_cast<NKikimr::NMiniKQL::TStructType*>(resultType);
        ui32 resultColsCount = resultStructType->GetMembersCount();

        taskMeta.ReadInfo.ResultColumnsTypes.reserve(resultColsCount);
        for (ui32 i = 0; i < resultColsCount; ++i) {
            taskMeta.ReadInfo.ResultColumnsTypes.emplace_back();
            auto memberType = resultStructType->GetMemberType(i);
            NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromMiniKQLType(memberType);
            taskMeta.ReadInfo.ResultColumnsTypes.back() = typeInfo;
        }
    }
    if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
        return;
    }
    {
        Y_ABORT_UNLESS(taskMeta.ReadInfo.GroupByColumnNames.empty());
        std::vector<std::string> groupByColumns;
        for (auto&& i : readOlapRange->GetGroupByColumnNames()) {
            groupByColumns.emplace_back(i);
        }
        std::swap(taskMeta.ReadInfo.GroupByColumnNames, groupByColumns);
    }
    taskMeta.ReadInfo.ReadType = OlapReadTypeFromProto(readOlapRange->GetReadType());
    taskMeta.ReadInfo.OlapProgram.Program = readOlapRange->GetOlapProgram();
    for (auto& name: readOlapRange->GetOlapProgramParameterNames()) {
        taskMeta.ReadInfo.OlapProgram.ParameterNames.insert(name);
    }
}

void MergeReadInfoToTaskMeta(TTaskMeta& meta, ui64 shardId, TMaybe<TShardKeyRanges>& keyReadRanges,
    const TPhysicalShardReadSettings& readSettings, const TVector<TTaskMeta::TColumn>& columns,
    const NKqpProto::TKqpPhyTableOperation& op, bool isPersistentScan)
{
    TTaskMeta::TShardReadInfo readInfo = {
        .Ranges = {},
        .Columns = columns,
    };
    if (keyReadRanges) {
        readInfo.Ranges = std::move(*keyReadRanges); // sorted & non-intersecting
    }

    if (isPersistentScan) {
        readInfo.ShardId = shardId;
    }

    FillReadInfo(meta, readSettings.ItemsLimit, readSettings.GetSorting());
    if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
        FillOlapReadInfo(meta, readSettings.ResultType, op.GetReadOlapRange());
    }

    if (!meta.Reads) {
        meta.Reads.ConstructInPlace();
    }

    meta.Reads->emplace_back(std::move(readInfo));
}

void TKqpTasksGraph::BuildDatashardTasks(TStageInfo& stageInfo, THashSet<ui64>* shardsWithEffects) {
    Y_ENSURE(shardsWithEffects);

    THashMap<ui64, ui64> shardTasks; // shardId -> taskId
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    auto getShardTask = [&](ui64 shardId) -> TTask& {
        // TODO: YQL_ENSURE(!txManager);
        auto it  = shardTasks.find(shardId);
        if (it != shardTasks.end()) {
            return GetTask(it->second);
        }
        auto& task = AddTask(stageInfo);
        task.Meta.Type = TTaskMeta::TTaskType::DataShard;
        task.Meta.ShardId = shardId;
        shardTasks.emplace(shardId, task.Id);

        return task;
    };

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    for (auto& op : stage.GetTableOps()) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());
        switch (op.GetTypeCase()) {
            case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
            case NKqpProto::TKqpPhyTableOperation::kDeleteRows: {
                YQL_ENSURE(stage.InputsSize() <= 1, "Effect stage with multiple inputs: " << stage.GetProgramAst());

                auto result = PartitionPruner->PruneEffect(op, stageInfo);
                for (auto& [shardId, shardInfo] : result) {
                    YQL_ENSURE(!shardInfo.KeyReadRanges);
                    YQL_ENSURE(shardInfo.KeyWriteRanges);

                    auto& task = getShardTask(shardId);

                    if (!task.Meta.Writes) {
                        task.Meta.Writes.ConstructInPlace();
                        task.Meta.Writes->Ranges = std::move(*shardInfo.KeyWriteRanges);
                    } else {
                        task.Meta.Writes->Ranges.MergeWritePoints(std::move(*shardInfo.KeyWriteRanges), keyTypes);
                    }

                    if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kDeleteRows) {
                        task.Meta.Writes->AddEraseOp();
                    } else {
                        task.Meta.Writes->AddUpdateOp();
                    }

                    for (const auto& [name, info] : shardInfo.ColumnWrites) {
                        auto& column = tableInfo->Columns.at(name);

                        auto& taskColumnWrite = task.Meta.Writes->ColumnWrites[column.Id];
                        taskColumnWrite.Column.Id = column.Id;
                        taskColumnWrite.Column.Type = column.Type;
                        taskColumnWrite.Column.Name = name;
                        taskColumnWrite.MaxValueSizeBytes = std::max(taskColumnWrite.MaxValueSizeBytes,
                            info.MaxValueSizeBytes);
                    }
                    shardsWithEffects->insert(shardId);
                }

                break;
            }

            case NKqpProto::TKqpPhyTableOperation::kReadOlapRange: {
                YQL_ENSURE(false, "The previous check did not work! Data query read does not support column shard tables." << Endl);
                    // TODO: << this->DebugString());
            }

            default: {
                YQL_ENSURE(false, "Unexpected table operation: " << (ui32) op.GetTypeCase() << Endl);
                    // TODO: << this->DebugString());
            }
        }
    }

    // TODO: LOG_D("Stage " << stageInfo.Id << " will be executed on " << shardTasks.size() << " shards.");

    // TODO:
    // for (auto& shardTask : shardTasks) {
    //     auto& task = GetTask(shardTask.second);
    //     LOG_D("ActorState: " << CurrentStateFuncName()
    //         << ", stage: " << stageInfo.Id << " create datashard task: " << shardTask.second
    //         << ", shard: " << shardTask.first
    //         << ", meta: " << task.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
    // }
}

void PrepareScanMetaForUsage(TTaskMeta& meta, const TVector<NScheme::TTypeInfo>& keyTypes) {
    YQL_ENSURE(meta.Reads.Defined());
    auto& taskReads = meta.Reads.GetRef();

    /*
     * Sort read ranges so that sequential scan of that ranges produce sorted result.
     *
     * Partition pruner feed us with set of non-intersecting ranges with filled right boundary.
     * So we may sort ranges based solely on the their rightmost point.
     */
    std::sort(taskReads.begin(), taskReads.end(), [&](const auto& lhs, const auto& rhs) {
        if (lhs.ShardId == rhs.ShardId) {
            return false;
        }

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
}

ui32 TKqpTasksGraph::GetScanTasksPerNode(TStageInfo& stageInfo, const bool isOlapScan, const ui64 nodeId, bool enableShuffleElimination) const {
    auto& intros = stageInfo.Introspections;
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    if (const auto taskCount = stage.GetTaskCount()) {
        intros.push_back("Manually overridden - " + ToString(taskCount));
        return taskCount;
    }

    ui32 result = 0;
    if (isOlapScan) {
        intros.push_back("This is OLAP scan");
        if (AggregationSettings.HasCSScanThreadsPerNode()) {
            result = AggregationSettings.GetCSScanThreadsPerNode();
            intros.push_back("Using the CSScanThreadsPerNode value - " + ToString(result));
        } else {
            const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
            result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), {}, intros);
            intros.push_back("Predicted value for OLAP scan - " + ToString(result));
        }
    } else {
        result = AggregationSettings.GetDSScanMinimalThreads();
        intros.push_back("Using the DSScanMinimalThreads value - " + ToString(result));
        if (stage.GetProgram().GetSettings().GetHasSort()) {
            result = std::max(result, AggregationSettings.GetDSBaseSortScanThreads());
            intros.push_back("Considering DSBaseSortScanThreads value because program has sort - " + ToString(result));
        }
        if (stage.GetProgram().GetSettings().GetHasMapJoin()) {
            result = std::max(result, AggregationSettings.GetDSBaseJoinScanThreads());
            intros.push_back("Considering DSBaseJoinScanThreads value because program has MapJoin - " + ToString(result));
        }
    }
    result = Max<ui32>(1, result);

    if (enableShuffleElimination) {
        result *= 2;
        intros.push_back("Multiply by 2 because of Shuffle Elimination - " + ToString(result));
    }

    intros.push_back("Predicted number of scan tasks per node " + ToString(nodeId) + " - " + ToString(result));
    return result;
}

void TKqpTasksGraph::BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, const TMap<ui64, ui64>& shardIdToNodeId,
    TQueryExecutionStats* stats)
{
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64, std::vector<TShardInfoWithId>> nodeShards;
    THashMap<ui64, ui64> assignedShardsCount;
    auto& intros = stageInfo.Introspections;
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    auto& columnShardHashV1Params = stageInfo.Meta.ColumnShardHashV1Params;
    bool shuffleEliminated = enableShuffleElimination && stage.GetIsShuffleEliminated();
    if (shuffleEliminated && stageInfo.Meta.ColumnTableInfoPtr) {
        const auto& tableDesc = stageInfo.Meta.ColumnTableInfoPtr->Description;
        columnShardHashV1Params.SourceShardCount = tableDesc.GetColumnShardCount();
        columnShardHashV1Params.SourceTableKeyColumnTypes = std::make_shared<TVector<NScheme::TTypeInfo>>();
        for (const auto& column: tableDesc.GetSharding().GetHashSharding().GetColumns()) {
            Y_ENSURE(stageInfo.Meta.TableConstInfo->Columns.contains(column), TStringBuilder{} << "Table doesn't have column: " << column);
            auto columnType = stageInfo.Meta.TableConstInfo->Columns.at(column).Type;
            columnShardHashV1Params.SourceTableKeyColumnTypes->push_back(columnType);
        }
    }

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;
    for (auto& op : stage.GetTableOps()) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

        auto columns = BuildKqpColumns(op, tableInfo);
        bool isFullScan;
        auto partitions = PartitionPruner->Prune(op, stageInfo, isFullScan);
        const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
        auto readSettings = ExtractReadSettings(op, stageInfo, TxAlloc->HolderFactory, TxAlloc->TypeEnv);

        if (isFullScan && readSettings.ItemsLimit) {
            Counters->Counters->FullScansExecuted->Inc();
        }

        if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
            stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(),
                                                op.GetReadRange().GetSkipNullKeys().end());
            // not supported for scan queries
            YQL_ENSURE(!readSettings.IsReverse());
        }

        for (auto&& i: partitions) {
            const ui64 nodeId = shardIdToNodeId.at(i.first);
            nodeShards[nodeId].emplace_back(TShardInfoWithId(i.first, std::move(i.second)));
        }

        if (stats) {
            for (auto&& i : nodeShards) {
                stats->AddNodeShardsCount(stageInfo.Id.StageId, i.first, i.second.size());
            }
        }

        if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && readSettings.IsSorted())) {
            THashMap<ui64 /* nodeId */, ui64 /* tasks count */> olapAndSortedTasksCount;

            auto AssignScanTaskToShard = [&](const ui64 shardId, const bool sorted) -> TTask& {
                ui64 nodeId = shardIdToNodeId.at(shardId);
                if (stageInfo.Meta.IsOlap() && sorted) {
                    auto& task = AddTask(stageInfo);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    ++olapAndSortedTasksCount[nodeId];
                    return task;
                }

                auto& tasks = nodeTasks[nodeId];
                auto& cnt = assignedShardsCount[nodeId];
                const ui32 maxScansPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                if (cnt < maxScansPerNode) {
                    auto& task = AddTask(stageInfo);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    tasks.push_back(task.Id);
                    ++cnt;
                    return task;
                } else {
                    ui64 taskIdx = cnt % maxScansPerNode;
                    ++cnt;
                    intros.push_back("Scan task for node " + ToString(nodeId) + " not created");
                    return GetTask(tasks[taskIdx]);
                }
            };

            for (auto&& pair : nodeShards) {
                auto& shardsInfo = pair.second;
                for (auto&& shardInfo : shardsInfo) {
                    auto& task = AssignScanTaskToShard(shardInfo.ShardId, readSettings.IsSorted());
                    MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                        columns, op, /*isPersistentScan*/ true);
                }
            }

            for (const auto& pair : nodeTasks) {
                for (const auto& taskIdx : pair.second) {
                    auto& task = GetTask(taskIdx);
                    task.Meta.SetEnableShardsSequentialScan(readSettings.IsSorted());
                    PrepareScanMetaForUsage(task.Meta, keyTypes);
                }

                intros.push_back("Actual number of scan tasks for node " + ToString(pair.first) + " - " + ToString(pair.second.size()));
            }

            for (const auto& [nodeId, count] : olapAndSortedTasksCount) {
                intros.push_back("Actual number of scan tasks (olap+sorted) for node " + ToString(nodeId) + " - " + ToString(count));
            }
        } else if (shuffleEliminated /* save partitioning for shuffle elimination */) {
            std::size_t stageInternalTaskId = 0;
            columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>();
            columnShardHashV1Params.TaskIndexByHash->resize(columnShardHashV1Params.SourceShardCount);

            for (auto&& pair : nodeShards) {
                const auto nodeId = pair.first;
                auto& shardsInfo = pair.second;
                std::size_t maxTasksPerNode = std::min<std::size_t>(shardsInfo.size(), GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true));
                std::vector<TTaskMeta> metas(maxTasksPerNode, TTaskMeta());
                {
                    for (std::size_t i = 0; i < shardsInfo.size(); ++i) {
                        auto&& shardInfo = shardsInfo[i];
                        MergeReadInfoToTaskMeta(
                            metas[i % maxTasksPerNode],
                            shardInfo.ShardId,
                            shardInfo.KeyReadRanges,
                            readSettings,
                            columns, op,
                            /*isPersistentScan*/ true
                        );
                    }

                    for (auto& meta: metas) {
                        PrepareScanMetaForUsage(meta, keyTypes);
                        // TODO:
                        // LOG_D("Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                        //     << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                    }
                }

                // in runtime we calc hash, which will be in [0; shardcount]
                // so we merge to mappings : hash -> shardID and shardID -> channelID for runtime
                THashMap<ui64, ui64> hashByShardId;
                Y_ENSURE(stageInfo.Meta.ColumnTableInfoPtr != nullptr, "ColumnTableInfoPtr is nullptr, maybe information about shards haven't beed delivered yet.");
                const auto& tableDesc = stageInfo.Meta.ColumnTableInfoPtr->Description;
                const auto& sharding = tableDesc.GetSharding();
                for (std::size_t i = 0; i < sharding.ColumnShardsSize(); ++i) {
                    hashByShardId.insert({sharding.GetColumnShards(i), i});
                }

                intros.push_back("Actual number of scan tasks from shards with shuffle elimination for node " + ToString(nodeId) + " - " + ToString(maxTasksPerNode));

                for (ui32 t = 0; t < maxTasksPerNode; ++t, ++stageInternalTaskId) {
                    auto& task = AddTask(stageInfo);
                    task.Meta = metas[t];
                    task.Meta.SetEnableShardsSequentialScan(false);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    task.SetMetaId(t);

                    for (const auto& readInfo: *task.Meta.Reads) {
                        Y_ENSURE(hashByShardId.contains(readInfo.ShardId));
                        (*columnShardHashV1Params.TaskIndexByHash)[hashByShardId[readInfo.ShardId]] = stageInternalTaskId;
                    }
                }
            }

            LOG_DEBUG_S(
                *TlsActivationContext,
                NKikimrServices::KQP_EXECUTER,
                "Stage with scan " << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]"
                << " has keys: " << columnShardHashV1Params.KeyTypesToString() << " and task count: " << stageInternalTaskId;
            );
        } else {
            ui32 metaId = 0;
            for (auto&& pair : nodeShards) {
                const auto nodeId = pair.first;
                auto& shardsInfo = pair.second;
                const ui32 metaGlueingId = ++metaId;
                TTaskMeta meta;
                {
                    for (auto&& shardInfo : shardsInfo) {
                        MergeReadInfoToTaskMeta(meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ true);
                    }
                    PrepareScanMetaForUsage(meta, keyTypes);
                    // TODO:
                    // LOG_D("Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                    //     << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                }

                const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                intros.push_back("Actual number of scan tasks from shards without shuffle elimination for node " + ToString(nodeId) + " - " + ToString(maxTasksPerNode));

                for (ui32 t = 0; t < maxTasksPerNode; ++t) {
                    auto& task = AddTask(stageInfo);
                    task.Meta = meta;
                    task.Meta.SetEnableShardsSequentialScan(false);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    task.SetMetaId(metaGlueingId);
                }
            }
        }
    }

    // TODO: LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");
}

void FillReadTaskFromSource(TTask& task, const TString& sourceName, const TString& structuredToken, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui64 nodeOffset) {
    if (structuredToken) {
        task.Meta.SecureParams.emplace(sourceName, structuredToken);
    }

    if (resourceSnapshot.empty()) {
        task.Meta.Type = TTaskMeta::TTaskType::Compute;
    } else {
        task.Meta.NodeId = resourceSnapshot[nodeOffset % resourceSnapshot.size()].GetNodeId();
        task.Meta.Type = TTaskMeta::TTaskType::Scan;
    }
}

void TKqpTasksGraph::BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount) {
    auto& intros = stageInfo.Introspections;
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

    ui32 taskCount = externalSource.GetPartitionedTaskParams().size();
    intros.push_back("Using number of PartitionedTaskParams from external source - " + ToString(taskCount));

    auto taskCountHint = stage.GetTaskCount();
    TString introHint;
    if (taskCountHint == 0) {
        taskCountHint = scheduledTaskCount;
        introHint = "(Using scheduled task count as hint for override - " + ToString(taskCountHint) + ")";
    }

    if (taskCountHint) {
        if (taskCount > taskCountHint) {
            taskCount = taskCountHint;
            if (!introHint.empty()) {
                intros.push_back(introHint);
            }
            intros.push_back("Manually overridden - " + ToString(taskCount));
        }
    } else if (!resourceSnapshot.empty()) {
        ui32 maxTaskcount = resourceSnapshot.size() * 2;
        if (taskCount > maxTaskcount) {
            taskCount = maxTaskcount;
            intros.push_back("Using less tasks because of resource snapshot size - " + ToString(taskCount));
        }
    }

    auto sourceName = externalSource.GetSourceName();
    TString structuredToken;
    if (sourceName) {
        structuredToken = NYql::CreateStructuredTokenParser(externalSource.GetAuthInfo()).ToBuilder().ReplaceReferences(GetMeta().SecureParams).ToJson();
    }

    ui64 nodeOffset = 0;
    for (size_t i = 0; i < resourceSnapshot.size(); ++i) {
        if (resourceSnapshot[i].GetNodeId() == GetMeta().ExecuterId.NodeId()) {
            nodeOffset = i;
            break;
        }
    }

    if (GetMeta().IsRestored) {
        for (const auto taskId : stageInfo.Tasks) {
            FillReadTaskFromSource(GetTask(taskId), sourceName, structuredToken, resourceSnapshot, nodeOffset++);
        }
        return;
    }

    TVector<ui64> tasksIds;
    tasksIds.reserve(taskCount);

    // generate all tasks
    for (ui32 i = 0; i < taskCount; i++) {
        auto& task = AddTask(stageInfo);

        if (!externalSource.GetEmbedded()) {
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            input.ConnectionInfo = NYql::NDq::TSourceInput{};
            input.SourceSettings = externalSource.GetSettings();
            input.SourceType = externalSource.GetType();
        }

        FillReadTaskFromSource(task, sourceName, structuredToken, resourceSnapshot, nodeOffset++);

        tasksIds.push_back(task.Id);
    }

    // distribute read ranges between them
    ui32 currentTaskIndex = 0;
    for (const TString& partitionParam : externalSource.GetPartitionedTaskParams()) {
        GetTask(tasksIds[currentTaskIndex]).Meta.ReadRanges.push_back(partitionParam);
        if (++currentTaskIndex >= tasksIds.size()) {
            currentTaskIndex = 0;
        }
    }
}

TVector<TVector<TShardRangesWithShardId>> DistributeShardsToTasks(TVector<TShardRangesWithShardId> shardsRanges, const size_t tasksCount, const TVector<NScheme::TTypeInfo>& keyTypes) {
    // TODO:
    // if (IsDebugLogEnabled()) {
    //     TStringBuilder sb;
    //     sb << "Distributing shards to tasks: [";
    //     for(size_t i = 0; i < shardsRanges.size(); i++) {
    //         sb << "# " << i << ": " << shardsRanges[i].Ranges->ToString(keyTypes, *AppData()->TypeRegistry);
    //     }

    //     sb << " ].";
    //     LOG_D(sb);
    // }

    std::sort(std::begin(shardsRanges), std::end(shardsRanges), [&](const TShardRangesWithShardId& lhs, const TShardRangesWithShardId& rhs) {
            return CompareBorders<true, true>(
                lhs.Ranges->GetRightBorder().first->GetCells(),
                rhs.Ranges->GetRightBorder().first->GetCells(),
                lhs.Ranges->GetRightBorder().second,
                rhs.Ranges->GetRightBorder().second,
                keyTypes) < 0;
        });

    // One shard (ranges set) can be assigned only to one task. Otherwise, we can break some optimizations like removing unnecessary shuffle.
    TVector<TVector<TShardRangesWithShardId>> result(tasksCount);
    size_t shardIndex = 0;
    for (size_t taskIndex = 0; taskIndex < tasksCount; ++taskIndex) {
        const size_t tasksLeft = tasksCount - taskIndex;
        const size_t shardsLeft = shardsRanges.size() - shardIndex;
        const size_t shardsPerCurrentTask = (shardsLeft + tasksLeft - 1) / tasksLeft;

        for (size_t currentShardIndex = 0; currentShardIndex < shardsPerCurrentTask; ++currentShardIndex, ++shardIndex) {
            result[taskIndex].push_back(shardsRanges[shardIndex]);
        }
    }
    return result;
}

TMaybe<size_t> TKqpTasksGraph::BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode,
    const TMap<ui64, ui64>& shardIdToNodeId, TQueryExecutionStats* stats)
{
    auto& intros = stageInfo.Introspections;
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64, ui64> assignedShardsCount;

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    bool singlePartitionedStage = stage.GetIsSinglePartition();

    YQL_ENSURE(stage.GetSources(0).HasReadRangesSource());
    YQL_ENSURE(stage.GetSources(0).GetInputIndex() == 0 && stage.SourcesSize() == 1);
    for (auto& input : stage.inputs()) {
        YQL_ENSURE(input.HasBroadcast());
    }

    auto& source = stage.GetSources(0).GetReadRangesSource();

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    YQL_ENSURE(tableInfo->TableKind != NKikimr::NKqp::ETableKind::Olap);

    auto columns = BuildKqpColumns(source, tableInfo);

    const auto& snapshot = GetMeta().Snapshot;

    TVector<ui64> createdTasksIds;
    auto createNewTask = [&](
            TMaybe<ui64> nodeId,
            ui64 taskLocation,
            TMaybe<ui64> shardId,
            TMaybe<ui64> maxInFlightShards) -> TTask& {
        auto& task = AddTask(stageInfo);
        task.Meta.Type = TTaskMeta::TTaskType::Scan;
        if (nodeId) {
            task.Meta.NodeId = *nodeId;
        }

        if (!nodeId || !GetMeta().ShardsResolved) {
            YQL_ENSURE(!GetMeta().ShardsResolved);
            task.Meta.ShardId = taskLocation;
        }

        const auto& stageSource = stage.GetSources(0);
        auto& input = task.Inputs[stageSource.GetInputIndex()];
        input.SourceType = NYql::KqpReadRangesSourceName;
        input.ConnectionInfo = NYql::NDq::TSourceInput{};

        // allocating source settings

        input.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;
        FillTableMeta(stageInfo, settings->MutableTable());

        settings->SetIsTableImmutable(source.GetIsTableImmutable());
        settings->SetIsolationLevel(GetMeta().RequestIsolationLevel);

        for (auto& keyColumn : keyTypes) {
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(keyColumn, "");
            *settings->AddKeyColumnTypeInfos() = columnType.TypeInfo ?
                *columnType.TypeInfo :
                NKikimrProto::TTypeInfo();
            settings->AddKeyColumnTypes(static_cast<ui32>(keyColumn.GetTypeId()));
        }

        for (auto& column : columns) {
            auto* protoColumn = settings->AddColumns();
            protoColumn->SetId(column.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
            protoColumn->SetType(columnType.TypeId);
            protoColumn->SetNotNull(column.NotNull);
            protoColumn->SetIsPrimary(column.IsPrimary);
            if (columnType.TypeInfo) {
                *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
            }
            protoColumn->SetName(column.Name);
        }

        if (GetMeta().CheckDuplicateRows) {
            for (auto& colName : tableInfo->KeyColumns) {
                const auto& tableColumn = tableInfo->Columns.at(colName);
                auto* protoColumn = settings->AddDuplicateCheckColumns();
                protoColumn->SetId(tableColumn.Id);
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(tableColumn.Type, tableColumn.TypeMod);
                protoColumn->SetType(columnType.TypeId);
                if (columnType.TypeInfo) {
                    *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                }
                protoColumn->SetName(colName);
            }
        }

        if (AppData()->FeatureFlags.GetEnableArrowFormatAtDatashard()) {
            settings->SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
        } else {
            settings->SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        }

        if (snapshot.IsValid()) {
            settings->MutableSnapshot()->SetStep(snapshot.Step);
            settings->MutableSnapshot()->SetTxId(snapshot.TxId);
        }

        if (GetMeta().RequestIsolationLevel == NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED) {
            settings->SetAllowInconsistentReads(true);
        }

        settings->SetReverse(source.GetReverse());
        settings->SetSorted(source.GetSorted());

        if (maxInFlightShards) {
            settings->SetMaxInFlightShards(*maxInFlightShards);
        }

        if (shardId) {
            settings->SetShardIdHint(*shardId);
        }

        if (GetMeta().MaxBatchSize) {
            settings->SetItemsLimit(*GetMeta().MaxBatchSize);
            settings->SetIsBatch(true);
        } else {
            ui64 itemsLimit = ExtractItemsLimit(stageInfo, source.GetItemsLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv);
            settings->SetItemsLimit(itemsLimit);
        }

        auto& lockTxId = GetMeta().LockTxId;
        if (lockTxId) {
            settings->SetLockTxId(*lockTxId);
            settings->SetLockNodeId(GetMeta().ExecuterId.NodeId());
        }

        if (GetMeta().LockMode) {
            settings->SetLockMode(*GetMeta().LockMode);
        }

        createdTasksIds.push_back(task.Id);
        return task;
    };

    THashMap<ui64, TVector<ui64>> nodeIdToTasks;
    THashMap<ui64, TVector<TShardRangesWithShardId>> nodeIdToShardKeyRanges;
    Y_DEFER {
        intros.push_back("Built scan tasks from source and shards to read for node");
        for (const auto& [nodeId, tasks] : nodeIdToTasks) {
            intros.push_back(ToString(nodeId) + " - " + ToString(tasks.size()) + ", " + ToString(nodeIdToShardKeyRanges.at(nodeId).size()));
        }
        intros.push_back("Total built scan tasks from source - " + ToString(createdTasksIds.size()));
    };

    auto addPartition = [&](
        ui64 taskLocation,
        TMaybe<ui64> nodeId,
        TMaybe<ui64> shardId,
        const TShardInfo& shardInfo,
        TMaybe<ui64> maxInFlightShards = Nothing())
    {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        if (!nodeId) {
            const auto nodeIdPtr = shardIdToNodeId.FindPtr(taskLocation);
            nodeId = nodeIdPtr ? TMaybe<ui64>{*nodeIdPtr} : Nothing();
        }

        YQL_ENSURE(!GetMeta().ShardsResolved || nodeId);

        YQL_ENSURE(stats);
        if (shardId) {
            stats->AffectedShards.insert(*shardId);
        }

        if (limitTasksPerNode && GetMeta().ShardsResolved) {
            const auto maxScanTasksPerNode = GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, *nodeId);
            auto& nodeTasks = nodeIdToTasks[*nodeId];
            if (nodeTasks.size() < maxScanTasksPerNode) {
                const auto& task = createNewTask(nodeId, taskLocation, {}, maxInFlightShards);
                nodeTasks.push_back(task.Id);
            }

            nodeIdToShardKeyRanges[*nodeId].push_back(TShardRangesWithShardId{shardId, &*shardInfo.KeyReadRanges});
        } else {
            auto& task = createNewTask(nodeId, taskLocation, shardId, maxInFlightShards);
            const auto& stageSource = stage.GetSources(0);
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

            shardInfo.KeyReadRanges->SerializeTo(settings);
        }
    };

    auto fillRangesForTasks = [&]() {
        for (const auto& [nodeId, shardsRanges] : nodeIdToShardKeyRanges) {
            const auto& tasks = nodeIdToTasks.at(nodeId);

            const auto rangesDistribution = DistributeShardsToTasks(shardsRanges, tasks.size(), keyTypes);
            YQL_ENSURE(rangesDistribution.size() == tasks.size());

            for (size_t taskIndex = 0; taskIndex < tasks.size(); ++taskIndex) {
                const auto taskId = tasks[taskIndex];
                auto& task = GetTask(taskId);
                const auto& stageSource = stage.GetSources(0);
                auto& input = task.Inputs[stageSource.GetInputIndex()];
                NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

                const auto& shardsRangesForTask = rangesDistribution[taskIndex];

                if (shardsRangesForTask.size() == 1 && shardsRangesForTask[0].ShardId) {
                    settings->SetShardIdHint(*shardsRangesForTask[0].ShardId);
                }

                bool hasRanges = false;
                for (const auto& shardRanges : shardsRangesForTask) {
                    hasRanges |= shardRanges.Ranges->HasRanges();
                }

                for (const auto& shardRanges : shardsRangesForTask) {
                    shardRanges.Ranges->SerializeTo(settings, !hasRanges);
                }
            }
        }
    };

    bool isFullScan = false;
    const THashMap<ui64, TShardInfo>& partitions = PartitionPruner->Prune(source, stageInfo, isFullScan);

    if (isFullScan && !source.HasItemsLimit()) {
        Counters->Counters->FullScansExecuted->Inc();
    }

    bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();

    if (partitions.size() > 0 && (isSequentialInFlight || singlePartitionedStage)) {
        auto [startShard, shardInfo] = PartitionPruner->MakeVirtualTablePartition(source, stageInfo);

        YQL_ENSURE(stats);
        for (auto& [shardId, _] : partitions) {
            stats->AffectedShards.insert(shardId);
        }

        TMaybe<ui64> inFlightShards = Nothing();
        if (isSequentialInFlight) {
            inFlightShards = source.GetSequentialInFlightShards();
        }

        if (shardInfo.KeyReadRanges) {
            const TMaybe<ui64> nodeId = singlePartitionedStage ? TMaybe<ui64>{GetMeta().ExecuterId.NodeId()} : Nothing();
            addPartition(startShard, nodeId, {}, shardInfo, inFlightShards);
            fillRangesForTasks();
            return singlePartitionedStage ? TMaybe<size_t>(partitions.size()) : Nothing();
        } else {
            return 0;
        }
    } else {
        for (auto& [shardId, shardInfo] : partitions) {
            addPartition(shardId, {}, shardId, shardInfo, {});
        }
        fillRangesForTasks();
        return partitions.size();
    }
}

struct TStageScheduleInfo {
    double StageCost = 0.0;
    ui32 TaskCount = 0;
};

static std::map<ui32, TStageScheduleInfo> ScheduleByCost(const IKqpGateway::TPhysicalTxData& tx, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
    std::map<ui32, TStageScheduleInfo> result;
    if (!resourceSnapshot.empty()) // can't schedule w/o node count
    {
        // collect costs and schedule stages with external sources only
        double totalCost = 0.0;
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stage = tx.Body->GetStages(stageIdx);
            if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                if (stage.GetStageCost() > 0.0 && stage.GetTaskCount() == 0) {
                    totalCost += stage.GetStageCost();
                    result.emplace(stageIdx, TStageScheduleInfo{.StageCost = stage.GetStageCost()});
                }
            }
        }
        // assign task counts
        if (!result.empty()) {
            // allow use 2/3 of threads in single stage
            ui32 maxStageTaskCount = (TStagePredictor::GetUsableThreads() * 2 + 2) / 3;
            // total limit per mode is x2
            ui32 maxTotalTaskCount = maxStageTaskCount * 2;
            for (auto& [_, stageInfo] : result) {
                // schedule tasks evenly between nodes
                stageInfo.TaskCount =
                    std::max<ui32>(
                        std::min(static_cast<ui32>(maxTotalTaskCount * stageInfo.StageCost / totalCost), maxStageTaskCount)
                        , 1
                    ) * resourceSnapshot.size();
            }
        }
    }
    return result;
}

void TKqpTasksGraph::FillSecureParamsFromStage(THashMap<TString, TString>& secureParams, const NKqpProto::TKqpPhyStage& stage) const {
    for (const auto& [secretName, authInfo] : stage.GetSecureParams()) {
        const auto& structuredToken = NYql::CreateStructuredTokenParser(authInfo).ToBuilder().ReplaceReferences(GetMeta().SecureParams).ToJson();
        const auto& structuredTokenParser = NYql::CreateStructuredTokenParser(structuredToken);
        YQL_ENSURE(structuredTokenParser.HasIAMToken(), "only token authentication supported for compute tasks");
        secureParams.emplace(secretName, structuredTokenParser.GetIAMToken());
    }
}

void TKqpTasksGraph::BuildExternalSinks(const NKqpProto::TKqpSink& sink, TKqpTasksGraph::TTaskType& task) const {
    const auto& extSink = sink.GetExternalSink();
    auto sinkName = extSink.GetSinkName();
    if (sinkName) {
        auto structuredToken = NYql::CreateStructuredTokenParser(extSink.GetAuthInfo()).ToBuilder().ReplaceReferences(GetMeta().SecureParams).ToJson();
        task.Meta.SecureParams.emplace(sinkName, structuredToken);
        if (GetMeta().UserRequestContext->TraceId) {
            task.Meta.TaskParams.emplace("fq.job_id", GetMeta().UserRequestContext->CustomerSuppliedId);
            // "fq.restart_count"
        }
    }

    auto& output = task.Outputs[sink.GetOutputIndex()];
    output.Type = TTaskOutputType::Sink;
    output.SinkType = extSink.GetType();
    output.SinkSettings = extSink.GetSettings();
}

void TKqpTasksGraph::BuildInternalSinks(const NKqpProto::TKqpSink& sink, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) const {
    const auto& intSink = sink.GetInternalSink();
    auto& output = task.Outputs[sink.GetOutputIndex()];
    output.Type = TTaskOutputType::Sink;
    output.SinkType = intSink.GetType();

    if (intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
        NKikimrKqp::TKqpTableSinkSettings settings;
        if (!stageInfo.Meta.ResolvedSinkSettings) {
            YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
        } else {
            settings = *stageInfo.Meta.ResolvedSinkSettings;
        }

        auto& lockTxId = GetMeta().LockTxId;
        if (lockTxId) {
            settings.SetLockTxId(*lockTxId);
            settings.SetLockNodeId(GetMeta().ExecuterId.NodeId());
        }
        if (!settings.GetInconsistentTx() && !settings.GetIsOlap()) {
            ActorIdToProto(BufferActorId, settings.MutableBufferActorId());
        }
        if (!settings.GetInconsistentTx() && GetMeta().Snapshot.IsValid()) {
            settings.MutableMvccSnapshot()->SetStep(GetMeta().Snapshot.Step);
            settings.MutableMvccSnapshot()->SetTxId(GetMeta().Snapshot.TxId);
        }
        if (!settings.GetInconsistentTx() && GetMeta().LockMode) {
            settings.SetLockMode(*GetMeta().LockMode);
        }

        settings.SetPriority((task.StageId.TxId << PriorityTxShift) + settings.GetPriority());

        output.SinkSettings.ConstructInPlace();
        output.SinkSettings->PackFrom(settings);
    } else {
        output.SinkSettings = intSink.GetSettings();
    }
}

void TKqpTasksGraph::BuildSinks(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) const {
    if (stage.SinksSize() > 0) {
        YQL_ENSURE(stage.SinksSize() == 1, "multiple sinks are not supported");
        const auto& sink = stage.GetSinks(0);
        YQL_ENSURE(sink.GetOutputIndex() < task.Outputs.size());

        if (sink.HasInternalSink()) {
            BuildInternalSinks(sink, stageInfo, task);
        } else if (sink.HasExternalSink()) {
            BuildExternalSinks(sink, task);
        } else {
            YQL_ENSURE(false, "unknown sink type");
        }
    }
}

size_t TKqpTasksGraph::BuildAllTasks(bool isScan, bool limitTasksPerNode, std::optional<TLlvmSettings> llvmSettings,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions,
    const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot,
    bool collectProfileStats, TQueryExecutionStats* stats,
    size_t nodesCount, THashSet<ui64>* shardsWithEffects)
{
    size_t sourceScanPartitionsCount = 0;

    Y_ENSURE(stats);

    for (ui32 txIdx = 0; txIdx < transactions.size(); ++txIdx) {
        const auto& tx = transactions[txIdx];
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            // TODO:
            // if (EnableReadsMerge) {
            //     stageInfo.Introspections.push_back("Using tasks count limit because of enabled reads merge");
            // }

            // build task conditions
            const bool buildFromSourceTasks = stage.SourcesSize() > 0;
            const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
            const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!isScan && stage.SinksSize() > 0);
            const bool buildScanTasks = isScan
                ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                : (GetMeta().AllowOlapDataQuery || GetMeta().StreamResult) && stageInfo.Meta.IsOlap() && stage.SinksSize() == 0
                ;

            // TODO: doesn't work right now - multiple conditions are possible in tests
            // YQL_ENSURE(buildFromSourceTasks + buildSysViewTasks + buildComputeTasks + buildScanTasks <= 1,
            //     "Multiple task conditions: " <<
            //     "isScan = " << (isScan) << ", "
            //     "stage.SourcesSize() > 0 = " << (stage.SourcesSize() > 0) << ", "
            //     "stageInfo.Meta.IsSysView() = " << (stageInfo.Meta.IsSysView()) << ", "
            //     "stageInfo.Meta.ShardOperations.empty() = " << (stageInfo.Meta.ShardOperations.empty()) << ", "
            //     "stage.SinksSize() = " << (stage.SinksSize()) << ", "
            //     "stageInfo.Meta.IsOlap() = " << (stageInfo.Meta.IsOlap()) << ", "
            //     "stageInfo.Meta.IsDatashard() = " << (stageInfo.Meta.IsDatashard()) << ", "
            //     "AllowOlapDataQuery = " << (AllowOlapDataQuery) << ", "
            //     "StreamResult = " << (StreamResult)
            // );

            // TODO: LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            if (buildFromSourceTasks) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource: {
                        if (auto partitionsCount = BuildScanTasksFromSource(stageInfo, limitTasksPerNode, GetMeta().ShardIdToNodeId, stats)) {
                            sourceScanPartitionsCount += *partitionsCount;
                        } else {
                            GetMeta().UnknownAffectedShardCount = true;
                        }
                    } break;
                    case NKqpProto::TKqpSource::kExternalSource: {
                        YQL_ENSURE(!isScan);
                        auto it = scheduledTaskCount.find(stageIdx);
                        BuildReadTasksFromSource(stageInfo, resourcesSnapshot, it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
                    } break;
                    default:
                        YQL_ENSURE(false, "unknown source type");
                }
            } else if (buildSysViewTasks) {
                BuildSysViewScanTasks(stageInfo);
            } else if (buildComputeTasks) {
                GetMeta().UnknownAffectedShardCount |= BuildComputeTasks(stageInfo, nodesCount);
            } else if (buildScanTasks) {
                BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), GetMeta().ShardIdToNodeId,
                    collectProfileStats ? stats : nullptr);
            } else {
                if (!isScan) {
                    BuildDatashardTasks(stageInfo, shardsWithEffects);
                } else {
                    YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
                }
            }

            if (llvmSettings) {
                const bool useLlvm = llvmSettings->GetUseLlvm(stage.GetProgram().GetSettings());
                for (auto& taskId : stageInfo.Tasks) {
                    GetTask(taskId).SetUseLlvm(useLlvm);
                }
                if (collectProfileStats) {
                    stats->SetUseLlvm(stageInfo.Id.StageId, useLlvm);
                }
            }

            if (stage.GetIsSinglePartition()) {
                YQL_ENSURE(stageInfo.Tasks.size() <= 1, "Unexpected multiple tasks in single-partition stage");
            }

            for (const auto& taskId : stageInfo.Tasks) {
                auto& task = GetTask(taskId);

                task.Meta.ExecuterId = GetMeta().ExecuterId;
                FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                BuildSinks(stage, stageInfo, task);
            }

            // Not task-related
            GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
            if (!GetMeta().IsRestored) {
                BuildKqpStageChannels(stageInfo, GetMeta().TxId, GetMeta().AllowWithSpilling, tx.Body->EnableShuffleElimination());
            }
        }

        // Not task-related
        BuildKqpTaskGraphResultChannels(tx.Body, txIdx);
    }

    return sourceScanPartitionsCount;
}

void TKqpTasksGraph::UpdateRemoteTasksNodeId(const THashMap<ui64, TVector<ui64>>& remoteComputeTasks) {
    for (auto& [shardId, tasks] : remoteComputeTasks) {
        auto it = GetMeta().ShardIdToNodeId.find(shardId);
        YQL_ENSURE(it != GetMeta().ShardIdToNodeId.end());
        for (ui64 taskId : tasks) {
            auto& task = GetTask(taskId);
            task.Meta.NodeId = it->second;
        }
    }
}

TKqpTasksGraph::TKqpTasksGraph(const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
    const TPartitionPrunerConfig& partitionPrunerConfig,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
    const TKqpRequestCounters::TPtr& counters,
    TActorId bufferActorId)
    : PartitionPruner(MakeHolder<TPartitionPruner>(txAlloc->HolderFactory, txAlloc->TypeEnv, std::move(partitionPrunerConfig)))
    , TxAlloc(txAlloc)
    , AggregationSettings(aggregationSettings)
    , Counters(counters)
    , BufferActorId(bufferActorId)
{}

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

} // namespace NKikimr::NKqp
