#include "kqp_tasks_graph.h"

#include "kqp_partition_helper.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/control.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/program/resolver.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

struct TStageScheduleInfo {
    double StageCost = 0.0;
    ui32 TaskCount = 0;
};

void ParseColumnToProto(TString columnName,
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

std::map<ui32, TStageScheduleInfo> ScheduleByCost(const IKqpGateway::TPhysicalTxData& tx, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
    std::map<ui32, TStageScheduleInfo> result;
    if (!resourceSnapshot.empty()) { // can't schedule w/o node count
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

struct TShardRangesWithShardId {
    TMaybe<ui64> ShardId;
    const TShardKeyRanges* Ranges;
};

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

} // anonymous namespace

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

void TKqpTasksGraph::FillKqpTasksGraphStages() {
    for (size_t txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);

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

                    case NKqpProto::TKqpSource::kFullTextSource: {
                        YQL_ENSURE(source.GetInputIndex() == 0);
                        YQL_ENSURE(stage.SourcesSize() == 1);
                        meta.TableId = MakeTableId(source.GetFullTextSource().GetTable());
                        meta.TablePath = source.GetFullTextSource().GetTable().GetPath();
                        meta.ShardOperations.insert(TKeyDesc::ERowOperation::Read);
                        YQL_ENSURE(tx.Body->GetTableConstInfoById()->Map.find(meta.TableId) != tx.Body->GetTableConstInfoById()->Map.end(), "Cannot find table const info for table: " << meta.TableId);
                        meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                        stageSourcesCount++;
                        break;
                    }

                    case NKqpProto::TKqpSource::kSysViewSource: {
                        YQL_ENSURE(source.GetInputIndex() == 0);
                        YQL_ENSURE(stage.SourcesSize() == 1);
                        meta.TableId = MakeTableId(source.GetSysViewSource().GetTable());
                        meta.TablePath = source.GetSysViewSource().GetTable().GetPath();
                        meta.ShardOperations.insert(TKeyDesc::ERowOperation::Read);
                        meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                        stageSourcesCount++;
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

            auto fillMetaFromSinkSettings = [&tx, &meta](NKikimrKqp::TKqpTableSinkSettings& settings) {
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
            };

            for (auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    YQL_ENSURE(sink.GetOutputIndex() == 0);
                    YQL_ENSURE(stage.SinksSize() + stage.OutputTransformsSize() == 1);
                    fillMetaFromSinkSettings(settings);
                }
            }

            for (auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    YQL_ENSURE(transform.GetOutputIndex() == 0);
                    YQL_ENSURE(stage.SinksSize() + stage.OutputTransformsSize() == 1);
                    fillMetaFromSinkSettings(settings);
                }
            }

            bool stageAdded = AddStageInfo(TStageInfo(stageId, stage.InputsSize() + stageSourcesCount, stage.GetOutputsCount(), std::move(meta)));
            YQL_ENSURE(stageAdded);

            auto& stageInfo = GetStageInfo(stageId);
            LOG_D(stageInfo.DebugString());

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
        auto& originTask = GetTask(originTaskId);
        auto& taskOutput = originTask.Outputs[outputIdx];

        if (result.GetCanSkipChannel()) {
            taskOutput.Type = TTaskOutputType::Effects;
            continue;
        }

        auto& channel = AddChannel();
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIdx;
        channel.DstTask = 0;
        channel.DstInputIndex = i;
        channel.InMemory = true;

        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        LOG_D("Create result channelId: " << channel.Id << " from task: " << originTaskId << " with index: " << outputIdx);
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
    settings->MutableColumns()->CopyFrom(sequencer.GetColumns());

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

    settings->SetDatabase(GetMeta().Database);
    settings->MutableTable()->CopyFrom(streamLookup.GetTable());

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    for (const auto& keyColumn : tableInfo->KeyColumns) {
        auto columnIt = tableInfo->Columns.find(keyColumn);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << keyColumn);

        auto* keyColumnProto = settings->AddKeyColumns();
        ParseColumnToProto(keyColumn, columnIt, keyColumnProto);
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
        ParseColumnToProto(column, columnIt, columnProto);
    }

    settings->SetLookupStrategy(streamLookup.GetLookupStrategy());
    settings->SetKeepRowsOrder(streamLookup.GetKeepRowsOrder());
    settings->SetAllowNullKeysPrefixSize(streamLookup.GetAllowNullKeysPrefixSize());
    settings->SetIsolationLevel(GetMeta().RequestIsolationLevel);

    if (streamLookup.GetIsTableImmutable()
        && GetMeta().RequestIsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_STALE)
    {
        settings->SetAllowUseFollowers(true);
        settings->SetIsTableImmutable(true);
    }

    if (streamLookup.HasVectorTopK()) {
        const auto& in = streamLookup.GetVectorTopK();
        auto& out = *settings->MutableVectorTopK();
        out.SetColumn(in.GetColumn());
        *out.MutableSettings() = in.GetSettings();
        auto target = ExtractPhyValue(stageInfo, in.GetTargetVector(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        out.SetTargetVector(TString(target.AsStringRef()));
        out.SetLimit((ui32)ExtractPhyValue(stageInfo, in.GetLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod()).Get<ui64>());
        for (auto& colIdx: in.GetDistinctColumns()) {
            out.AddDistinctColumns(colIdx);
        }
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
    settings->SetOverlapClusters(vectorResolve.GetOverlapClusters());
    settings->SetOverlapRatio(vectorResolve.GetOverlapRatio());

    YQL_ENSURE(stageInfo.Meta.IndexMetas.size() == 1);
    const auto& levelTableInfo = stageInfo.Meta.IndexMetas.back().TableConstInfo;

    settings->SetDatabase(GetMeta().Database);
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
    settings->SetIsMultiMatches(dqSourceStreamLookup.GetIsMultiMatches());

    const auto& leftJointKeys = dqSourceStreamLookup.GetLeftJoinKeyNames();
    settings->MutableLeftJoinKeyNames()->Assign(leftJointKeys.begin(), leftJointKeys.end());

    const auto& rightJointKeys = dqSourceStreamLookup.GetRightJoinKeyNames();
    settings->MutableRightJoinKeyNames()->Assign(rightJointKeys.begin(), rightJointKeys.end());

    auto& streamLookupSource = *settings->MutableRightSource();
    streamLookupSource.SetSerializedRowType(dqSourceStreamLookup.GetLookupRowType());
    const auto& compiledSource = dqSourceStreamLookup.GetLookupSource();
    streamLookupSource.SetProviderName(compiledSource.GetType());
    *streamLookupSource.MutableLookupSource() = compiledSource.GetSettings();

    TString structuredToken;
    const auto& sourceName = compiledSource.GetSourceName();
    if (sourceName) {
        structuredToken = ReplaceStructuredTokenReferences(compiledSource.GetAuthInfo());
    }

    TTransform dqSourceStreamLookupTransform = {
        .Type = "StreamLookupInputTransform",
        .InputType = dqSourceStreamLookup.GetInputStageRowType(),
        .OutputType = dqSourceStreamLookup.GetOutputStageRowType(),
    };
    YQL_ENSURE(dqSourceStreamLookupTransform.Settings.PackFrom(*settings));

    for (const auto taskId : stageInfo.Tasks) {
        auto& task = GetTask(taskId);
        task.Inputs[inputIndex].Transform = dqSourceStreamLookupTransform;

        if (structuredToken) {
            task.Meta.SecureParams.emplace(sourceName, structuredToken);
        }
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
        LOG_D( "TxId: " << txId << ". "
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
                LOG_D( "Chose "
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

                        LOG_D( "Propagating columnhashv1 params to stage"
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
    if (channel.WatermarksIdleTimeoutUs) {
        channelDesc.SetWatermarksIdleTimeoutUs(*channel.WatermarksIdleTimeoutUs);
    }

    const auto& resultChannelProxies = GetMeta().ResultChannelProxies;

    YQL_ENSURE(channel.SrcTask);
    const auto& srcTask = GetTask(channel.SrcTask);
    FillEndpointDesc(*channelDesc.MutableSrcEndpoint(), srcTask);

    if (channel.DstTask) {
        FillEndpointDesc(*channelDesc.MutableDstEndpoint(), GetTask(channel.DstTask));
    } else if (!resultChannelProxies.empty()) {
        Y_ENSURE(GetMeta().DqChannelVersion <= 1u);
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
    }  else if (task.Meta.ScanTask || (stageInfo.Meta.IsSysView() && task.Meta.Reads.Defined())) {
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
                    LOG_D( "Filling columnshardhashv1 params for sending it to runtime "
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

    if (output.Transform) {
        auto* transformDesc = outputDesc.MutableTransform();
        auto& transform = output.Transform;

        transformDesc->SetType(transform->Type);
        transformDesc->SetInputType(transform->InputType);
        transformDesc->SetOutputType(transform->OutputType);

        *transformDesc->MutableSettings() = transform->Settings;
    }
}

void TKqpTasksGraph::FillInputDesc(NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input, bool serializeAsyncIoSettings, bool& enableMetering) const {
    const auto& snapshot = GetMeta().Snapshot;
    const auto& lockTxId = GetMeta().LockTxId;

    switch (input.Type()) {
        case NYql::NDq::TTaskInputType::Source:
            inputDesc.MutableSource()->SetType(input.SourceType);
            inputDesc.MutableSource()->SetWatermarksMode(input.WatermarksMode);
            if (input.WatermarksIdleTimeoutUs) {
                inputDesc.MutableSource()->SetWatermarksIdleTimeoutUs(*input.WatermarksIdleTimeoutUs);
            }
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

                if (isTableImmutable) {
                    input.Meta.SourceSettings->SetAllowInconsistentReads(true);
                }

                if (serializeAsyncIoSettings) {
                    inputDesc.MutableSource()->MutableSettings()->PackFrom(*input.Meta.SourceSettings);
                }
            } else if (input.Meta.FullTextSourceSettings) {

                if (snapshot.IsValid()) {
                    input.Meta.FullTextSourceSettings->MutableSnapshot()->SetStep(snapshot.Step);
                    input.Meta.FullTextSourceSettings->MutableSnapshot()->SetTxId(snapshot.TxId);
                }

                inputDesc.MutableSource()->MutableSettings()->PackFrom(*input.Meta.FullTextSourceSettings);
            } else if (input.Meta.SysViewSourceSettings) {
                inputDesc.MutableSource()->MutableSettings()->PackFrom(*input.Meta.SysViewSourceSettings);
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
                GetMeta().RequestIsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_STALE;

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

            if (lockTxId && GetMeta().LockMode && !isTableImmutable) {
                input.Meta.StreamLookupSettings->SetLockMode(
                    // Unique Index needs read lock even in snapshot isolation mode.
                    input.Meta.StreamLookupSettings->GetLookupStrategy() != NKqpProto::EStreamLookupStrategy::UNIQUE
                        ? *GetMeta().LockMode
                        : NKikimrDataEvents::OPTIMISTIC);
            }

            if (GetMeta().QuerySpanId && !isTableImmutable) {
                input.Meta.StreamLookupSettings->SetQuerySpanId(GetMeta().QuerySpanId);
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

            if (GetMeta().QuerySpanId) {
                input.Meta.VectorResolveSettings->SetQuerySpanId(GetMeta().QuerySpanId);
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

    if (const auto& infoAggregator = GetMeta().DqInfoAggregator) {
        NActorsProto::TActorId actorIdProto;
        ActorIdToProto(infoAggregator, &actorIdProto);
        (*result->MutableTaskParams())["dq_info_aggregator"] = actorIdProto.SerializeAsString();
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
        SerializeTaskToProto(task, taskInfo, /* serializeAsyncIoSettings */ true);

        taskInfo->ClearProgram();
        taskInfo->ClearSecureParams();
    }
}

void TKqpTasksGraph::RestoreTasksGraphInfo(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, const NKikimrKqp::TQueryPhysicalGraph& graphInfo) {
    GetMeta().IsRestored = true;

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

    const auto restoreDqInputTransform = [this, restoreDqTransform](const NDqProto::TTaskInput& protoInfo, NKikimr::NKqp::TTaskInputMeta& meta) -> TMaybe<TTransform> {
        auto info = restoreDqTransform(protoInfo);
        if (!info) {
            return Nothing();
        }

        const auto& settings = protoInfo.GetTransform().GetSettings();
        if (settings.Is<NKikimrKqp::TKqpStreamLookupSettings>()) {
            auto transformSettings = meta.StreamLookupSettings = GetMeta().Allocate<NKikimrKqp::TKqpStreamLookupSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse stream lookup settings");
            transformSettings->ClearSnapshot();
            transformSettings->ClearLockTxId();
            transformSettings->ClearLockNodeId();
        } else if (settings.Is<NKikimrKqp::TKqpSequencerSettings>()) {
            auto transformSettings = meta.SequencerSettings = GetMeta().Allocate<NKikimrKqp::TKqpSequencerSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse sequencer settings");
        } else if (settings.Is<NKikimrTxDataShard::TKqpVectorResolveSettings>()) {
            auto transformSettings = meta.VectorResolveSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpVectorResolveSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse vector resolve settings");
            transformSettings->ClearSnapshot();
            transformSettings->ClearLockTxId();
            transformSettings->ClearLockNodeId();
        }

        return info;
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
            channel.CheckpointingMode = protoInfo.GetCheckpointingMode();
            channel.WatermarksMode = protoInfo.GetWatermarksMode();
            if (protoInfo.HasWatermarksIdleTimeoutUs()) {
                channel.WatermarksIdleTimeoutUs = protoInfo.GetWatermarksIdleTimeoutUs();
            }
        }

        return channel;
    };

    const auto internalSinksOrder = BuildInternalSinksPriorityOrder();

    for (size_t taskIdx = 0; taskIdx < graphInfo.TasksSize(); ++taskIdx) {
        const auto& task = graphInfo.GetTasks(taskIdx);
        const auto txId = task.GetTxId();
        const auto& taskInfo = task.GetDqTask();
        const NYql::NDq::TStageId stageId(txId, taskInfo.GetStageId());

        auto& stageInfo = GetStageInfo(stageId);
        auto& newTask = AddTask(stageInfo, TTaskType::RESTORED);
        YQL_ENSURE(taskInfo.GetId() == newTask.Id);
        newTask.SetUseLlvm(taskInfo.GetUseLlvm());
        newTask.Meta.TaskParams.insert(taskInfo.GetTaskParams().begin(), taskInfo.GetTaskParams().end());
        newTask.Meta.ReadRanges.assign(taskInfo.GetReadRanges().begin(), taskInfo.GetReadRanges().end());
        newTask.Meta.Type = TTaskMeta::TTaskType::Compute;
        newTask.Meta.ExecuterId = GetMeta().ExecuterId;

        if (taskInfo.HasMetaId()) {
            newTask.SetMetaId(taskInfo.GetMetaId());
        }

        if (taskInfo.HasMeta()) {
            NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta meta;
            YQL_ENSURE(taskInfo.GetMeta().UnpackTo(&meta), "Failed to parse task meta");

            newTask.Meta.ScanTask = true;

            if (meta.HasEnableShardsSequentialScan()) {
                newTask.Meta.SetEnableShardsSequentialScan(meta.GetEnableShardsSequentialScan());
            }

            auto& readInfo = newTask.Meta.ReadInfo;
            readInfo.SetSorting(static_cast<ERequestSorting>(meta.GetOptionalSorting()));
            readInfo.ItemsLimit = meta.GetItemsLimit();
            readInfo.GroupByColumnNames.assign(meta.GetGroupByColumnNames().begin(), meta.GetGroupByColumnNames().end());
            readInfo.OlapProgram.Program = meta.GetOlapProgram().GetProgram();

            readInfo.ResultColumnsTypes.reserve(meta.ResultColumnsSize());
            for (const auto& column : meta.GetResultColumns()) {
                readInfo.ResultColumnsTypes.emplace_back(NScheme::TypeInfoModFromProtoColumnType(column.GetType(), &column.GetTypeInfo()).TypeInfo);
            }

            switch (meta.GetReadType()) {
                case NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::ROWS:
                    readInfo.ReadType = TTaskMeta::TReadInfo::EReadType::Rows;
                    break;
                case NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::BLOCKS:
                    readInfo.ReadType = TTaskMeta::TReadInfo::EReadType::Blocks;
                    break;
                default:
                    YQL_ENSURE(false, "Unknown read type");
            }

            TVector<TTaskMeta::TColumn> columns;
            columns.reserve(meta.ColumnsSize());
            for (const auto& columnProto : meta.GetColumns()) {
                auto& column = columns.emplace_back();
                column.Id = columnProto.GetId();
                column.Type = NScheme::TypeInfoModFromProtoColumnType(columnProto.GetType(), &columnProto.GetTypeInfo()).TypeInfo;
                column.Name = columnProto.GetName();
            }

            auto& reads = newTask.Meta.Reads.emplace();
            reads.reserve(meta.ReadsSize());
            for (const auto& readProto : meta.GetReads()) {
                auto& read = reads.emplace_back();
                read.Columns = columns;
                read.ShardId = readProto.GetShardId();
                read.Ranges.ParseFrom(readProto);
            }
        }

        for (size_t inputIdx = 0; inputIdx < taskInfo.InputsSize(); ++inputIdx) {
            const auto& inputInfo = taskInfo.GetInputs(inputIdx);
            auto& newInput = newTask.Inputs[inputIdx];
            newInput.Transform = restoreDqInputTransform(inputInfo, newInput.Meta);

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
                    newInput.WatermarksMode = sourceInfo.GetWatermarksMode();
                    if (sourceInfo.HasWatermarksIdleTimeoutUs()) {
                        newInput.WatermarksIdleTimeoutUs = sourceInfo.GetWatermarksIdleTimeoutUs();
                    }
                    if (sourceInfo.HasSettings()) {
                        const auto& settings = sourceInfo.GetSettings();
                        if (settings.Is<NKikimrTxDataShard::TKqpReadRangesSourceSettings>()) {
                            auto sourceSettings = newInput.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
                            YQL_ENSURE(settings.UnpackTo(sourceSettings), "Failed to parse source settings");
                            FillScanTaskLockTxId(*sourceSettings);
                            sourceSettings->ClearSnapshot();
                        } else if (settings.Is<NKikimrKqp::TKqpFullTextSourceSettings>()) {
                            auto sourceSettings = newInput.Meta.FullTextSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpFullTextSourceSettings>();
                            YQL_ENSURE(settings.UnpackTo(sourceSettings), "Failed to parse full text source settings");
                            sourceSettings->ClearSnapshot();
                        } else if (settings.Is<NKikimrKqp::TKqpSysViewSourceSettings>()) {
                            auto sourceSettings = newInput.Meta.SysViewSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpSysViewSourceSettings>();
                            YQL_ENSURE(settings.UnpackTo(sourceSettings), "Failed to parse sys view source settings");
                        } else {
                            newInput.SourceSettings = sourceInfo.GetSettings();
                        }
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
                    newOutput.Type = TKqpTaskOutputType::ShardRangePartition;

                    const auto& rangeInfo = outputInfo.GetRangePartition();
                    newOutput.KeyColumns.assign(rangeInfo.GetKeyColumns().begin(), rangeInfo.GetKeyColumns().end());
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

        const auto& stage = stageInfo.Meta.GetStage(stageId);
        FillSecureParamsFromStage(newTask.Meta.SecureParams, stage);
        BuildSinks(stage, stageInfo, internalSinksOrder, newTask);

        for (const auto& input : stage.GetInputs()) {
            if (input.GetTypeCase() != NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup) {
                continue;
            }

            if (const auto& compiledSource = input.GetDqSourceStreamLookup().GetLookupSource(); const auto& sourceName = compiledSource.GetSourceName()) {
                newTask.Meta.SecureParams.emplace(
                    sourceName,
                    ReplaceStructuredTokenReferences(compiledSource.GetAuthInfo())
                );
            }
        }
    }

    for (const auto& [id, channel] : channels) {
        auto& newChannel = AddChannel();
        newChannel = channel;
        YQL_ENSURE(id == newChannel.Id);
    }

    for (ui64 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        const auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);

        for (ui64 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo({txIdx, stageIdx});

            if (const auto& sources = stage.GetSources(); !sources.empty() && sources[0].GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                const auto it = scheduledTaskCount.find(stageIdx);
                BuildReadTasksFromSource(stageInfo, resourcesSnapshot, it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
            }

            GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
        }
    }
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

        auto& task = AddTask(stageInfo, TTaskType::SYSVIEW_COMPUTE);
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

        LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
    }
}

std::pair<ui32, TKqpTasksGraph::TTaskType::ECreateReason> TKqpTasksGraph::GetMaxTasksAggregation(TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount) {
    TTaskType::ECreateReason taskReason = TTaskType::MINIMUM_COMPUTE;
    ui32 result = 1;

    if (AggregationSettings.HasAggregationComputeThreads()) {
        auto threads = AggregationSettings.GetAggregationComputeThreads();
        if (result < threads) {
            taskReason = TTaskType::AGGREGATION_COMPUTE;
            result = threads;
        }
    } else if (nodesCount) {
        const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
        taskReason = TTaskType::LEVEL_PREDICTED; // TODO: need to store also params for predictor
        result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), previousTasksCount / nodesCount) * nodesCount;
    }

    return {result, taskReason};
}

bool TKqpTasksGraph::BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    TTaskType::ECreateReason tasksReason = TTaskType::MINIMUM_COMPUTE;
    bool unknownAffectedShardCount = false;
    ui32 partitionsCount = 1;
    ui32 inputTasks = 0;
    bool isShuffle = false;
    bool forceMapTasks = false;
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
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                forceMapTasks = true;
                ++mapConnectionCount;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                partitionsCount = std::max<ui64>(partitionsCount, originStageInfo.Tasks.size());
                break;
            }
            case NKqpProto::TKqpPhyConnection::kVectorResolve: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                break;
            }
            default:
                break;
        }
    }

    Y_ENSURE(mapConnectionCount <= 1, "Only a single map connection is allowed");

    if (isShuffle && !forceMapTasks) {
        if (stage.GetTaskCount()) {
            tasksReason = TTaskType::FORCED;
            partitionsCount = stage.GetTaskCount();
        } else {
            auto [newPartitionCount, minTasksReason] = GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount);
            if (newPartitionCount > partitionsCount) {
                tasksReason = minTasksReason;
                partitionsCount = newPartitionCount;
            }
        }
    }

    for (ui32 i = 0; i < partitionsCount; ++i) {
        auto& task = AddTask(stageInfo, tasksReason);
        task.Meta.Type = TTaskMeta::TTaskType::Compute;
        LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
    }

    return unknownAffectedShardCount;
}

void TKqpTasksGraph::BuildDatashardTasks(TStageInfo& stageInfo, THashSet<ui64>* shardsWithEffects) {
    THashMap<ui64, ui64> shardTasks; // shardId -> taskId
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    TTaskType::ECreateReason tasksReason = TTaskType::UPSERT_DELETE_DATASHARD;

    auto getShardTask = [&](ui64 shardId) -> TTask& {
        // TODO: YQL_ENSURE(!txManager);
        auto it  = shardTasks.find(shardId);
        if (it != shardTasks.end()) {
            return GetTask(it->second);
        }
        auto& task = AddTask(stageInfo, tasksReason);
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
                    if (shardsWithEffects) {
                        shardsWithEffects->insert(shardId);
                    }
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

    LOG_D("Stage " << stageInfo.Id << " will be executed on " << shardTasks.size() << " shards.");

    for (auto& shardTask : shardTasks) {
        auto& task = GetTask(shardTask.second);
        LOG_D( "Stage: " << stageInfo.Id << " create datashard task: " << shardTask.second
            << ", shard: " << shardTask.first
            << ", meta: " << task.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
    }
}

std::pair<ui32, TKqpTasksGraph::TTaskType::ECreateReason> TKqpTasksGraph::GetScanTasksPerNode(TStageInfo& stageInfo, const bool isOlapScan, const ui64 /* nodeId */, bool enableShuffleElimination) const {
    TTaskType::ECreateReason taskReason;
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    if (const auto taskCount = stage.GetTaskCount()) {
        taskReason = TTaskType::FORCED;
        return {taskCount, taskReason};
    }

    ui32 result = 0;
    if (isOlapScan) {
        if (AggregationSettings.HasCSScanThreadsPerNode()) {
            taskReason = TTaskType::OLAP_AGGREGATION_SCAN;
            result = AggregationSettings.GetCSScanThreadsPerNode();
        } else {
            const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
            taskReason = TTaskType::LEVEL_PREDICTED; // TODO: need to store also params for predictor
            result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), {});
        }
    } else {
        taskReason = TTaskType::OLTP_AGGREGATION_SCAN;
        result = AggregationSettings.GetDSScanMinimalThreads();
        if (stage.GetProgram().GetSettings().GetHasSort()) {
            if (result < AggregationSettings.GetDSBaseSortScanThreads()) {
                taskReason = TTaskType::OLTP_SORT_SCAN;
                result = AggregationSettings.GetDSBaseSortScanThreads();
            }
        }
        if (stage.GetProgram().GetSettings().GetHasMapJoin()) {
            if (result < AggregationSettings.GetDSBaseJoinScanThreads()) {
                taskReason = TTaskType::OLTP_MAP_JOIN_SCAN;
                result = AggregationSettings.GetDSBaseJoinScanThreads();
            }
        }
    }

    if (result < 1) {
        taskReason = TTaskType::MINIMUM_SCAN;
        result = 1;
    }

    // TODO: why?
    if (enableShuffleElimination) {
        result *= 2;
    }

    return {result, taskReason};
}

void TKqpTasksGraph::BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, TQueryExecutionStats* stats) {
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64, std::vector<TShardInfoWithId>> nodeShards;
    THashMap<ui64, ui64> assignedShardsCount;
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
            stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(), op.GetReadRange().GetSkipNullKeys().end());
            YQL_ENSURE(!readSettings.IsReverse(), "Not supported for scan queries");
        }

        for (auto&& [shardId, shardInfo]: partitions) {
            const ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
            nodeShards[nodeId].emplace_back(TShardInfoWithId(shardId, std::move(shardInfo)));
        }

        if (stats) {
            for (const auto& [nodeId, shardsInfo] : nodeShards) {
                stats->AddNodeShardsCount(stageInfo.Id.StageId, nodeId, shardsInfo.size());
            }
        }

        if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && readSettings.IsSorted())) {
            THashMap<ui64 /* nodeId */, ui64 /* tasks count */> olapAndSortedTasksCount;

            auto AssignScanTaskToShard = [&](const ui64 shardId, const bool sorted) -> TTask& {
                ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
                if (stageInfo.Meta.IsOlap() && sorted) {
                    auto& task = AddTask(stageInfo, TTaskType::OLAP_SORT_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    ++olapAndSortedTasksCount[nodeId];
                    return task;
                }

                auto& tasks = nodeTasks[nodeId];
                auto& cnt = assignedShardsCount[nodeId];
                const auto [maxScansPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                if (cnt < maxScansPerNode) {
                    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SHARD_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    tasks.push_back(task.Id);
                    ++cnt;

                    if (cnt == maxScansPerNode) {
                        for (const auto& taskId : tasks) {
                            GetTask(taskId).Reason = maxTasksReason;
                        }
                    }

                    return task;
                } else {
                    ui64 taskIdx = cnt % maxScansPerNode;
                    ++cnt;
                    return GetTask(tasks[taskIdx]);
                }
            };

            for (auto& [_, shardsInfo] : nodeShards) {
                for (auto& shardInfo : shardsInfo) {
                    auto& task = AssignScanTaskToShard(shardInfo.ShardId, readSettings.IsSorted());
                    MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                        columns, op, /*isPersistentScan*/ true);
                }
            }

            for (const auto& [nodeId, tasksId] : nodeTasks) {
                for (const auto& taskIdx : tasksId) {
                    auto& task = GetTask(taskIdx);
                    task.Meta.SetEnableShardsSequentialScan(readSettings.IsSorted());
                    PrepareScanMetaForUsage(task.Meta, keyTypes);
                }
            }
        } else if (shuffleEliminated /* save partitioning for shuffle elimination */) {
            std::size_t stageInternalTaskId = 0;
            columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>();
            columnShardHashV1Params.TaskIndexByHash->resize(columnShardHashV1Params.SourceShardCount);

            for (auto&& [nodeId, shardsInfo] : nodeShards) {
                auto tasksReason = TTaskType::SHUFFLE_ELIMINATE_SCAN;
                std::size_t tasksPerNode = shardsInfo.size();
                auto [maxTasksPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true);

                if (maxTasksPerNode < tasksPerNode) {
                    tasksReason = maxTasksReason;
                    tasksPerNode = maxTasksPerNode;
                }

                std::vector<TTaskMeta> metas(tasksPerNode, TTaskMeta());
                {
                    for (std::size_t i = 0; i < shardsInfo.size(); ++i) {
                        auto&& shardInfo = shardsInfo[i];
                        MergeReadInfoToTaskMeta(
                            metas[i % tasksPerNode],
                            shardInfo.ShardId,
                            shardInfo.KeyReadRanges,
                            readSettings,
                            columns, op,
                            /*isPersistentScan*/ true
                        );
                    }

                    for (auto& meta: metas) {
                        PrepareScanMetaForUsage(meta, keyTypes);
                        LOG_D( "Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                            << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
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

                for (ui32 t = 0; t < tasksPerNode; ++t, ++stageInternalTaskId) {
                    auto& task = AddTask(stageInfo, tasksReason);
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

            LOG_D( "Stage with scan " << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]"
                << " has keys: " << columnShardHashV1Params.KeyTypesToString() << " and task count: " << stageInternalTaskId;
            );
        } else {
            ui32 metaId = 0;
            for (auto&& [nodeId, shardsInfo] : nodeShards) {
                const ui32 metaGlueingId = ++metaId;
                TTaskMeta meta;
                {
                    for (auto&& shardInfo : shardsInfo) {
                        MergeReadInfoToTaskMeta(meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ true);
                    }
                    PrepareScanMetaForUsage(meta, keyTypes);
                    LOG_D( "Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                        << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                }

                const auto [maxTasksPerNode, tasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);

                for (ui32 t = 0; t < maxTasksPerNode; ++t) {
                    auto& task = AddTask(stageInfo, tasksReason);
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

    LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");
}

void TKqpTasksGraph::BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

    auto tasksReason = TTaskType::DEFAULT_SOURCE_READ;
    ui32 taskCount = externalSource.GetPartitionedTaskParams().size();

    auto tasksReasonHint = TTaskType::FORCED;
    auto taskCountHint = stage.GetTaskCount();

    if (taskCountHint == 0) {
        tasksReasonHint = TTaskType::SCHEDULED_SOURCE_READ;
        taskCountHint = scheduledTaskCount;
    }

    if (taskCountHint) {
        if (taskCount > taskCountHint) {
            tasksReason = tasksReasonHint;
            taskCount = taskCountHint;
        }
    } else if (!resourceSnapshot.empty()) {
        ui32 maxTaskCount = resourceSnapshot.size() * 2;
        if (taskCount > maxTaskCount) {
            tasksReason = TTaskType::SNAPSHOT_SOURCE_READ;
            taskCount = maxTaskCount;
        }
    }

    auto sourceName = externalSource.GetSourceName();
    TString structuredToken;
    if (sourceName) {
        structuredToken = ReplaceStructuredTokenReferences(externalSource.GetAuthInfo());
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
        auto& task = AddTask(stageInfo, tasksReason);

        if (!externalSource.GetEmbedded()) {
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            input.ConnectionInfo = NYql::NDq::TSourceInput{};
            input.SourceSettings = externalSource.GetSettings();
            input.SourceType = externalSource.GetType();
            if (externalSource.HasWatermarksSettings()) {
                const auto& watermarksSettings = externalSource.GetWatermarksSettings();
                input.WatermarksMode = NYql::NDqProto::EWatermarksMode::WATERMARKS_MODE_DEFAULT;
                if (watermarksSettings.HasIdleTimeoutUs()) {
                    input.WatermarksIdleTimeoutUs = watermarksSettings.GetIdleTimeoutUs();
                }
            }
        }

        FillReadTaskFromSource(task, sourceName, structuredToken, resourceSnapshot, nodeOffset++);

        if (GetMeta().UserRequestContext && GetMeta().UserRequestContext->StreamingQueryPath) {
            task.Meta.TaskParams.emplace("query_path", GetMeta().UserRequestContext->StreamingQueryPath);
        }

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

void TKqpTasksGraph::BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats) {
    YQL_ENSURE(stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource);
    Y_UNUSED(stats);

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& source = stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0);
    const auto& fullTextSource = source.GetFullTextSource();

    YQL_ENSURE(fullTextSource.GetIndex());

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::TTaskType::Scan;
    task.Meta.NodeId = GetMeta().ExecuterId.NodeId();
    const auto& stageSource = stage.GetSources(0);
    auto& input = task.Inputs.at(stageSource.GetInputIndex());
    input.SourceType = NYql::KqpFullTextSourceName;
    input.ConnectionInfo = NYql::NDq::TSourceInput{};

    input.Meta.FullTextSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpFullTextSourceSettings>();
    NKikimrKqp::TKqpFullTextSourceSettings* settings = input.Meta.FullTextSourceSettings;

    settings->SetIndex(fullTextSource.GetIndex());
    settings->SetDatabase(GetMeta().Database);
    settings->MutableTable()->CopyFrom(fullTextSource.GetTable());
    settings->MutableIndexDescription()->CopyFrom(fullTextSource.GetIndexDescription());

    auto guard = TxAlloc->TypeEnv.BindAllocator();
    {

        TStringBuilder queryBuilder;
        for (const auto& query : fullTextSource.GetQuerySettings().GetQueryValue()) {
            auto value = ExtractPhyValue(
                stageInfo, query,
                TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
            queryBuilder << TString(value.AsStringRef());
        }

        settings->MutableQuerySettings()->SetQuery(TString(queryBuilder));
    }


    if (fullTextSource.HasTakeLimit())
    {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetTakeLimit(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetLimit(value.Get<ui64>());
        }
    }

    if (fullTextSource.HasBFactor()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetBFactor(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());

        if (value.HasValue()) {
            settings->SetBFactor(value.Get<double>());
        }
    }

    if (fullTextSource.HasDefaultOperator()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetDefaultOperator(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetDefaultOperator(TString(value.AsStringRef()));
        }
    }

    if (fullTextSource.HasMinimumShouldMatch()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetMinimumShouldMatch(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetMinimumShouldMatch(TString(value.AsStringRef()));
        }
    }

    if (fullTextSource.HasK1Factor()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetK1Factor(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetK1Factor(value.Get<double>());
        }
    }

    settings->MutableQuerySettings()->MutableColumns()->CopyFrom(fullTextSource.GetQuerySettings().GetColumns());

    for(const auto& indexTable : fullTextSource.GetIndexTables()) {
        auto* indexTableProto = settings->AddIndexTables();
        indexTableProto->MutableTable()->CopyFrom(indexTable.GetTable());
        indexTableProto->MutableKeyColumns()->CopyFrom(indexTable.GetKeyColumns());
        indexTableProto->MutableColumns()->CopyFrom(indexTable.GetColumns());
    }

    settings->MutableKeyColumns()->CopyFrom(fullTextSource.GetKeyColumns());
    settings->MutableColumns()->CopyFrom(fullTextSource.GetColumns());
    if (GetMeta().Snapshot.IsValid()) {
        settings->MutableSnapshot()->SetStep(GetMeta().Snapshot.Step);
        settings->MutableSnapshot()->SetTxId(GetMeta().Snapshot.TxId);
    }
}

void TKqpTasksGraph::BuildSysViewTasksFromSource(TStageInfo& stageInfo) {
    YQL_ENSURE(stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kSysViewSource);

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& source = stage.GetSources(0);
    const auto& sysViewSource = source.GetSysViewSource();

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::TTaskType::Compute;
    task.Meta.NodeId = GetMeta().ExecuterId.NodeId();

    const auto& stageSource = stage.GetSources(0);
    auto& input = task.Inputs.at(stageSource.GetInputIndex());
    input.SourceType = NYql::KqpSysViewSourceName;
    input.ConnectionInfo = NYql::NDq::TSourceInput{};

    input.Meta.SysViewSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpSysViewSourceSettings>();
    NKikimrKqp::TKqpSysViewSourceSettings* settings = input.Meta.SysViewSourceSettings;

    settings->SetDatabase(GetMeta().Database);
    settings->MutableTable()->CopyFrom(sysViewSource.GetTable());
    settings->SetTablePath(sysViewSource.GetTable().GetPath());
    settings->SetSysViewInfo(sysViewSource.GetTable().GetSysView());
    settings->SetReverse(sysViewSource.GetReverse());

    // Fill SysViewDescription from table metadata
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    if (tableInfo && tableInfo->SysViewInfo) {
        *settings->MutableSysViewDescription() = *tableInfo->SysViewInfo;
    }

    // Fill columns: convert TKqpPhyColumnId to TKqpColumnMetadataProto using table metadata
    for (const auto& phyCol : sysViewSource.GetColumns()) {
        auto* col = settings->AddColumns();
        col->SetId(phyCol.GetId());
        col->SetName(phyCol.GetName());
        if (tableInfo) {
            auto it = tableInfo->Columns.find(phyCol.GetName());
            if (it != tableInfo->Columns.end()) {
                col->SetTypeId(it->second.Type.GetTypeId());
                if (NScheme::NTypeIds::IsParametrizedType(it->second.Type.GetTypeId())) {
                    NScheme::ProtoFromTypeInfo(it->second.Type, {}, *col->MutableTypeInfo());
                }
            }
        }
    }

    // Fill key columns from table info
    if (tableInfo) {
        for (size_t i = 0; i < tableInfo->KeyColumns.size(); ++i) {
            auto* kc = settings->AddKeyColumns();
            kc->SetName(tableInfo->KeyColumns[i]);
            if (i < tableInfo->KeyColumnTypes.size()) {
                kc->SetTypeId(tableInfo->KeyColumnTypes[i].GetTypeId());
            }
        }
    }

    // Fill key ranges from the source proto
    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& keyTypes = tableInfo ? tableInfo->KeyColumnTypes : TVector<NScheme::TTypeInfo>();

    auto guard = TxAlloc->TypeEnv.BindAllocator();

    switch (sysViewSource.GetRangesExprCase()) {
        case NKqpProto::TKqpSysViewSource::kKeyRange: {
            auto range = MakeKeyRange(keyTypes, sysViewSource.GetKeyRange(), stageInfo, holderFactory, typeEnv);
            range.Serialize(*settings->AddKeyRanges());
            break;
        }
        case NKqpProto::TKqpSysViewSource::kRanges: {
            auto ranges = FillRangesFromParameter(keyTypes, sysViewSource.GetRanges(), stageInfo, typeEnv);
            for (auto& pointOrRange : ranges) {
                if (auto* range = std::get_if<TSerializedTableRange>(&pointOrRange)) {
                    range->Serialize(*settings->AddKeyRanges());
                } else {
                    // Convert point to an inclusive range [point, point]
                    auto& point = std::get<TSerializedCellVec>(pointOrRange);
                    TSerializedTableRange rangeFromPoint(point.GetCells(), true, point.GetCells(), true);
                    rangeFromPoint.Point = true;
                    rangeFromPoint.Serialize(*settings->AddKeyRanges());
                }
            }
            break;
        }
        default: {
            // Full scan — construct a range from [NULL..NULL] (beginning) to [] (end), both inclusive.
            // Scan actors expect From to have at least one NULL cell for full scans;
            // a default-constructed TSerializedTableRange has empty From cells which some
            // scan actors interpret as +inf.
            TVector<TCell> fromCells(keyTypes.size()); // NULL cells = "from the very beginning"
            TSerializedTableRange fullRange(fromCells, true, TConstArrayRef<TCell>(), true);
            fullRange.Serialize(*settings->AddKeyRanges());
            break;
        }
    }

    // Pass user token for access control in scan actors (e.g., Auth* sys views)
    if (UserToken) {
        settings->SetUserToken(UserToken->SerializeAsString());
    }

    LOG_D("Stage " << stageInfo.Id << " create sys view source task: " << task.Id);
}

void TKqpTasksGraph::FillScanTaskLockTxId(NKikimrTxDataShard::TKqpReadRangesSourceSettings& settings) {
    if (const auto& lockTxId = GetMeta().LockTxId) {
        settings.SetLockTxId(*lockTxId);
        settings.SetLockNodeId(GetMeta().ExecuterId.NodeId());
    }
    if (GetMeta().QuerySpanId) {
        settings.SetQuerySpanId(GetMeta().QuerySpanId);
    }
}

TMaybe<size_t> TKqpTasksGraph::BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode, TQueryExecutionStats* stats) {
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
            ui64 taskShardId,
            bool hintShardId,
            TMaybe<ui64> maxInFlightShards,
            TTaskType::ECreateReason taskReason) -> TTask&
    {
        auto& task = AddTask(stageInfo, taskReason);
        task.Meta.Type = TTaskMeta::TTaskType::Scan; // TODO: can this scan task not have NodeId?
        if (nodeId) {
            task.Meta.NodeId = *nodeId;
        }

        if (!nodeId || !GetMeta().ShardsResolved) {
            YQL_ENSURE(!GetMeta().ShardsResolved);
            task.Meta.ShardId = taskShardId;
        }

        const auto& stageSource = stage.GetSources(0);
        auto& input = task.Inputs.at(stageSource.GetInputIndex());
        input.SourceType = NYql::KqpReadRangesSourceName;
        input.ConnectionInfo = NYql::NDq::TSourceInput{};

        // allocating source settings

        input.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;
        settings->SetDatabase(GetMeta().Database);
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

        if (GetMeta().RequestIsolationLevel == NKqpProto::ISOLATION_LEVEL_READ_UNCOMMITTED) {
            settings->SetAllowInconsistentReads(true);
        }

        settings->SetReverse(source.GetReverse());
        settings->SetSorted(source.GetSorted());

        if (maxInFlightShards) {
            settings->SetMaxInFlightShards(*maxInFlightShards);
        }

        if (hintShardId) {
            settings->SetShardIdHint(taskShardId);
        }

        if (GetMeta().MaxBatchSize) {
            settings->SetItemsLimit(*GetMeta().MaxBatchSize);
            settings->SetIsBatch(true);
        } else {
            ui64 itemsLimit = ExtractPhyValue(stageInfo, source.GetItemsLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod((ui32)0)).Get<ui64>();
            settings->SetItemsLimit(itemsLimit);
        }

        if (source.HasVectorTopK()) {
            const auto& in = source.GetVectorTopK();
            auto& out = *settings->MutableVectorTopK();
            out.SetColumn(in.GetColumn());
            *out.MutableSettings() = in.GetSettings();
            auto target = ExtractPhyValue(stageInfo, in.GetTargetVector(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
            out.SetTargetVector(TString(target.AsStringRef()));
            out.SetLimit((ui32)ExtractPhyValue(stageInfo, in.GetLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod()).Get<ui64>());
        }

        FillScanTaskLockTxId(*settings);

        if (GetMeta().LockMode) {
            settings->SetLockMode(*GetMeta().LockMode);
        }

        createdTasksIds.push_back(task.Id);
        return task;
    };

    THashMap<ui64, TVector<ui64>> nodeIdToTasks;
    THashMap<ui64, TVector<TShardRangesWithShardId>> nodeIdToShardKeyRanges;

    auto addPartition = [&](
        ui64 taskShardId,
        TMaybe<ui64> nodeId,
        bool hintShardId,
        const TShardInfo& shardInfo,
        TMaybe<ui64> maxInFlightShards,
        TTaskType::ECreateReason tasksReason)
    {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        if (!nodeId) {
            const auto nodeIdPtr = GetMeta().ShardIdToNodeId.FindPtr(taskShardId);
            nodeId = nodeIdPtr ? TMaybe<ui64>{*nodeIdPtr} : Nothing();
        }

        YQL_ENSURE(!GetMeta().ShardsResolved || nodeId);

        if (hintShardId && stats) {
            stats->AffectedShards.insert(taskShardId);
        }

        if (limitTasksPerNode && GetMeta().ShardsResolved) {
            const auto [maxScanTasksPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, *nodeId);
            auto& nodeTasks = nodeIdToTasks[*nodeId];
            if (nodeTasks.size() < maxScanTasksPerNode) {
                const auto& task = createNewTask(nodeId, taskShardId, false, maxInFlightShards, tasksReason);
                nodeTasks.push_back(task.Id);

                if (nodeTasks.size() == maxScanTasksPerNode) {
                    for (const auto& taskId : nodeTasks) {
                        GetTask(taskId).Reason = maxTasksReason;
                    }
                }
            }
            nodeIdToShardKeyRanges[*nodeId].push_back(TShardRangesWithShardId{hintShardId ? TMaybe<ui64>(taskShardId) : Nothing(), &*shardInfo.KeyReadRanges});
        } else {
            auto& task = createNewTask(nodeId, taskShardId, hintShardId, maxInFlightShards, tasksReason);
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
    const auto& partitions = PartitionPruner->Prune(source, stageInfo, isFullScan);

    if (isFullScan && !source.HasItemsLimit()) {
        Counters->Counters->FullScansExecuted->Inc();
    }

    bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();

    if (!partitions.empty() && (isSequentialInFlight || singlePartitionedStage)) {
        auto [startShard, shardInfo] = PartitionPruner->MakeVirtualTablePartition(source, stageInfo);

        if (stats) {
            for (const auto& [shardId, _] : partitions) {
                stats->AffectedShards.insert(shardId);
            }
        }

        TMaybe<ui64> inFlightShards = Nothing();
        if (isSequentialInFlight) {
            inFlightShards = source.GetSequentialInFlightShards();
        }

        if (shardInfo.KeyReadRanges) {
            const TMaybe<ui64> nodeId = singlePartitionedStage ? TMaybe<ui64>{GetMeta().ExecuterId.NodeId()} : Nothing();
            addPartition(startShard, nodeId, false, shardInfo, inFlightShards, TTaskType::SINGLE_SOURCE_SCAN);
            fillRangesForTasks();
            return singlePartitionedStage ? TMaybe<size_t>(partitions.size()) : Nothing();
        } else {
            return 0;
        }
    } else {
        for (auto& [shardId, shardInfo] : partitions) {
            addPartition(shardId, {}, true, shardInfo, {}, TTaskType::DEFAULT_SOURCE_SCAN);
        }
        fillRangesForTasks();
        return partitions.size();
    }
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
        auto structuredToken = ReplaceStructuredTokenReferences(extSink.GetAuthInfo());
        task.Meta.SecureParams.emplace(sinkName, structuredToken);
        if (GetMeta().UserRequestContext->TraceId) {
            task.Meta.TaskParams.emplace("fq.job_id", GetMeta().UserRequestContext->CustomerSuppliedId);
            // "fq.restart_count"
        }
    }

    if (GetMeta().UserRequestContext && GetMeta().UserRequestContext->StreamingQueryPath) {
        task.Meta.TaskParams.emplace("query_path", GetMeta().UserRequestContext->StreamingQueryPath);
    }

    auto& output = task.Outputs[sink.GetOutputIndex()];
    output.Type = TTaskOutputType::Sink;
    output.SinkType = extSink.GetType();
    output.SinkSettings = extSink.GetSettings();
}

void TKqpTasksGraph::FillKqpTableSinkSettings(NKikimrKqp::TKqpTableSinkSettings& settings, const std::vector<std::pair<ui64, i64>>& internalSinksOrder, const TKqpTasksGraph::TTaskType& task) const {
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
        // Use per-transaction QuerySpanId if available (for deferred effects),
        // otherwise fall back to global QuerySpanId
        ui64 querySpanId = GetMeta().GetTxQuerySpanId(task.StageId.TxId);
        if (querySpanId) {
            settings.SetQuerySpanId(querySpanId);
        }

    auto sinkPosition = std::lower_bound(
        internalSinksOrder.begin(),
        internalSinksOrder.end(),
        std::make_pair(task.StageId.TxId, settings.GetPriority()));
    AFL_ENSURE(sinkPosition != internalSinksOrder.end()
            && sinkPosition->first == task.StageId.TxId
            && sinkPosition->second == settings.GetPriority());

    settings.SetPriority(std::distance(internalSinksOrder.begin(), sinkPosition));
}

void TKqpTasksGraph::BuildInternalSinks(const NKqpProto::TKqpSink& sink, const TStageInfo& stageInfo, const std::vector<std::pair<ui64, i64>>& internalSinksOrder, TKqpTasksGraph::TTaskType& task) const {
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

        FillKqpTableSinkSettings(settings, internalSinksOrder, task);

        output.SinkSettings.ConstructInPlace();
        output.SinkSettings->PackFrom(settings);
    } else {
        output.SinkSettings = intSink.GetSettings();
    }
}

void TKqpTasksGraph::BuildInternalOutputTransform(const NKqpProto::TKqpOutputTransform& transform, const TStageInfo& stageInfo, const std::vector<std::pair<ui64, i64>>& internalSinksOrder, TKqpTasksGraph::TTaskType& task) const {
    const auto& intSink = transform.GetInternalSink();
    auto& output = task.Outputs[transform.GetOutputIndex()];
    output.Type = TTaskOutputType::Map;

    AFL_ENSURE(intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>());

    NKikimrKqp::TKqpTableSinkSettings settings;
    if (!stageInfo.Meta.ResolvedSinkSettings) {
        YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
    } else {
        settings = *stageInfo.Meta.ResolvedSinkSettings;
    }

    FillKqpTableSinkSettings(settings, internalSinksOrder, task);

    output.Transform.ConstructInPlace();
    output.Transform->Type = TString(NYql::KqpTableSinkName);
    output.Transform->InputType = transform.GetInputType();
    output.Transform->OutputType = transform.GetOutputType();
    output.Transform->Settings.PackFrom(settings);
}


void TKqpTasksGraph::BuildSinks(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, const std::vector<std::pair<ui64, i64>>& internalSinksOrder, TKqpTasksGraph::TTaskType& task) const {
    for (const auto& sink : stage.GetSinks()) {
        YQL_ENSURE(sink.GetOutputIndex() < task.Outputs.size());

        if (sink.HasInternalSink()) {
            BuildInternalSinks(sink, stageInfo, internalSinksOrder, task);
        } else if (sink.HasExternalSink()) {
            BuildExternalSinks(sink, task);
        } else {
            YQL_ENSURE(false, "unknown sink type");
        }
    }

    if (stage.OutputTransformsSize() > 0) {
        YQL_ENSURE(stage.OutputTransformsSize() == 1, "multiple output transforms are not supported");
        const auto& transform = stage.GetOutputTransforms(0);
        YQL_ENSURE(transform.GetOutputIndex() < task.Outputs.size());

        if (transform.HasInternalSink()) {
            BuildInternalOutputTransform(transform, stageInfo, internalSinksOrder, task);
        } else {
            YQL_ENSURE(false, "unknown sink type");
        }
    }
}

void TKqpTasksGraph::ResolveShards(TGraphMeta::TShardToNodeMap&& shardsToNodes) {
    GetMeta().ShardsResolved = true;
    GetMeta().ShardIdToNodeId = std::move(shardsToNodes);
    for (const auto& [shardId, nodeId] : GetMeta().ShardIdToNodeId) {
        GetMeta().ShardsOnNode[nodeId].push_back(shardId);
    }
}

size_t TKqpTasksGraph::BuildAllTasks(std::optional<TLlvmSettings> llvmSettings,
    const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats, THashSet<ui64>* shardsWithEffects)
{
    size_t sourceScanPartitionsCount = 0;

    bool limitTasksPerNode = IsEnabledReadsMerge();

    if (!GetMeta().IsScan) {
        limitTasksPerNode |= GetMeta().StreamResult;
    }

    const auto internalSinksOrder = BuildInternalSinksPriorityOrder();

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            // TODO:
            // if (EnableReadsMerge) {
            //     stageInfo.Introspections.push_back("Using tasks count limit because of enabled reads merge");
            // }

            const bool maybeOlapRead = (GetMeta().AllowOlapDataQuery || GetMeta().StreamResult) && stageInfo.Meta.IsOlap();

            // build task conditions
            const bool buildFromSourceTasks = stage.SourcesSize() > 0;
            const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
            const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!GetMeta().IsScan && stage.SinksSize() + stage.OutputTransformsSize() > 0 && !(maybeOlapRead && stageInfo.Meta.HasReads()));
            const bool buildScanTasks = GetMeta().IsScan
                ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                : maybeOlapRead && (stage.SinksSize() + stage.OutputTransformsSize() == 0 || stageInfo.Meta.HasReads())
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

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            if (buildFromSourceTasks) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource: {
                        if (auto partitionsCount = BuildScanTasksFromSource(stageInfo, limitTasksPerNode, stats)) {
                            sourceScanPartitionsCount += *partitionsCount;
                        } else {
                            GetMeta().UnknownAffectedShardCount = true;
                        }
                    } break;
                    case NKqpProto::TKqpSource::kFullTextSource: {
                        BuildFullTextScanTasksFromSource(stageInfo, stats);
                        GetMeta().UnknownAffectedShardCount = true;
                    } break;
                    case NKqpProto::TKqpSource::kSysViewSource: {
                        BuildSysViewTasksFromSource(stageInfo);
                        GetMeta().UnknownAffectedShardCount = true;
                    } break;
                    case NKqpProto::TKqpSource::kExternalSource: {
                        YQL_ENSURE(!GetMeta().IsScan);
                        auto it = scheduledTaskCount.find(stageIdx);
                        BuildReadTasksFromSource(stageInfo, resourcesSnapshot, it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
                    } break;
                    default:
                        YQL_ENSURE(false, "unknown source type");
                }
            } else if (buildSysViewTasks) {
                BuildSysViewScanTasks(stageInfo);
            } else if (buildComputeTasks) {
                auto nodesCount = GetMeta().ShardsOnNode.size();
                if (!GetMeta().IsScan) {
                    nodesCount = std::max<ui32>(resourcesSnapshot.size(), nodesCount);
                }
                GetMeta().UnknownAffectedShardCount |= BuildComputeTasks(stageInfo, nodesCount);
            } else if (buildScanTasks) {
                BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), CollectProfileStats(GetMeta().StatsMode) ? stats : nullptr);
            } else {
                if (!GetMeta().IsScan) {
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
                if (CollectProfileStats(GetMeta().StatsMode) && stats) {
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
                BuildSinks(stage, stageInfo, internalSinksOrder, task);
            }

            // Not task-related
            GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
            BuildKqpStageChannels(stageInfo, GetMeta().TxId, GetMeta().AllowWithSpilling, tx.Body->EnableShuffleElimination());
        }
        GetMeta().DqChannelVersion = tx.Body->DqChannelVersion();

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
            // TODO: YQL_ENSURE(task.Meta.Type == TTaskMeta::TTaskType::Scan);
        }
    }
}

TKqpTasksGraph::TKqpTasksGraph(
    const TString& database,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions,
    const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
    const TPartitionPrunerConfig& partitionPrunerConfig,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
    const TKqpRequestCounters::TPtr& counters,
    TActorId bufferActorId,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken)
    : PartitionPruner(MakeHolder<TPartitionPruner>(txAlloc->HolderFactory, txAlloc->TypeEnv, std::move(partitionPrunerConfig)))
    , Transactions(transactions)
    , TxAlloc(txAlloc)
    , AggregationSettings(aggregationSettings)
    , Counters(counters)
    , BufferActorId(bufferActorId)
    , UserToken(std::move(userToken))
{
    GetMeta().Arena = MakeIntrusive<NActors::TProtoArenaHolder>();
    GetMeta().Database = database;
    GetMeta().RequestIsolationLevel = NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SERIALIZABLE;

    if (Transactions.empty()) {
        return;
    }

    // Store per-transaction QuerySpanIds for deferred effects
    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        if (Transactions[txIdx].QuerySpanId != 0) {
            GetMeta().SetTxQuerySpanId(txIdx, Transactions[txIdx].QuerySpanId);
        }
    }

    TMaybe<NKqpProto::TKqpPhyTx::EType> txsType;
    for (const auto& tx : Transactions) {
        if (txsType) {
            YQL_ENSURE(*txsType == tx.Body->GetType(), "Mixed physical tx types in executer.");
            YQL_ENSURE((*txsType == NKqpProto::TKqpPhyTx::TYPE_DATA)
                || (*txsType == NKqpProto::TKqpPhyTx::TYPE_GENERIC),
                "Cannot execute multiple non-data physical txs.");
        } else {
            txsType = tx.Body->GetType();
        }
    }

    switch (*txsType) {
        case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
        case NKqpProto::TKqpPhyTx::TYPE_DATA:
            break;
        case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
            GetMeta().StreamResult = true;
            break;
        case NKqpProto::TKqpPhyTx::TYPE_SCAN: {
            size_t resultsSize = Transactions.at(0).Body->ResultsSize();
            YQL_ENSURE(resultsSize != 0);

            GetMeta().StreamResult = Transactions.at(0).Body->GetResults(0).GetIsStream();

            if (GetMeta().StreamResult) {
                YQL_ENSURE(resultsSize == 1);
            } else {
                for (size_t i = 1; i < resultsSize; ++i) {
                    YQL_ENSURE(Transactions.at(0).Body->GetResults(i).GetIsStream() == GetMeta().StreamResult);
                }
            }
            GetMeta().IsScan = true;
        }   break;
        default:
            YQL_ENSURE(false, "Unsupported physical tx type: " << (ui32)*txsType);
    }

    FillKqpTasksGraphStages();
}

std::vector<std::pair<ui64, i64>> TKqpTasksGraph::BuildInternalSinksPriorityOrder() {
    std::vector<std::pair<ui64, i64>> order;
    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            auto addSink = [&stageInfo, &order, txIdx](const NKqpProto::TKqpInternalSink& intSink) {
                AFL_ENSURE(intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>());
                if (!stageInfo.Meta.ResolvedSinkSettings) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    order.emplace_back(txIdx, settings.GetPriority());
                } else {
                    order.emplace_back(txIdx, stageInfo.Meta.ResolvedSinkSettings->GetPriority());
                }
            };

            if (stage.SinksSize() > 0) {
                AFL_ENSURE(stage.OutputTransformsSize() == 0);

                for (const auto& sink : stage.GetSinks()) {
                    if (!sink.HasInternalSink()) {
                        continue;
                    }

                    const auto& intSink = sink.GetInternalSink();
                    addSink(intSink);
                }
            }
            if (stage.OutputTransformsSize() > 0) {
                AFL_ENSURE(stage.OutputTransformsSize() == 1);
                AFL_ENSURE(stage.SinksSize() == 0);

                const auto& transform = stage.GetOutputTransforms(0);

                if (!transform.HasInternalSink()) {
                    continue;
                }

                const auto& intSink = transform.GetInternalSink();
                addSink(intSink);
            }
        }
    }
    std::sort(order.begin(), order.end());

    return order;
}

TString TKqpTasksGraph::ReplaceStructuredTokenReferences(const TString& token) const {
    const auto parser = NYql::CreateStructuredTokenParser(token);
    auto builder = parser.ToBuilder();
    if (!parser.HasTransientToken()) {
        builder.ReplaceReferences(GetMeta().SecureParams);
    } else if (UserToken && UserToken->GetSerializedToken()) {
        builder.SetTransientTokenAuth(UserToken->GetSerializedToken());
    }
    return builder.ToJson();
}

TVector<TString> TKqpTasksGraph::GetStageIntrospection(const TStageId& stageId) const {
    TVector<TString> introspections;
    THashMap<TTaskType::ECreateReason, ui64> tasksPerReason;

    for (const auto& taskId : GetStageInfo(stageId).Tasks) {
        const auto& task = GetTask(taskId);
        ++tasksPerReason[task.Reason];
    }

    for (const auto [reason, count] : tasksPerReason) {
        switch(reason) {
            case TTaskType::ECreateReason::UNKNOWN:
                introspections.push_back(ToString(count) + " tasks created for unknown reason");
                break;
            case TTaskType::ECreateReason::LITERAL:
                introspections.push_back(ToString(count) + " tasks for literal executer");
                break;
            case TTaskType::ECreateReason::RESTORED:
                introspections.push_back(ToString(count) + " tasks restored");
                break;
            case TTaskType::ECreateReason::FORCED:
                introspections.push_back(ToString(count) + " tasks forced by user override");
                break;
            case TTaskType::ECreateReason::LEVEL_PREDICTED:
                introspections.push_back(ToString(count) + " tasks by level prediction");
                break;
            case TTaskType::ECreateReason::MINIMUM_COMPUTE:
                introspections.push_back(ToString(count) + " minimum tasks for compute");
                break;
            case TTaskType::ECreateReason::SYSVIEW_COMPUTE:
                introspections.push_back(ToString(count) + " tasks for sysview");
                break;
            case TTaskType::ECreateReason::PREV_STAGE_COMPUTE:
                introspections.push_back(ToString(count) + " tasks same as previous stage");
                break;
            case TTaskType::ECreateReason::AGGREGATION_COMPUTE:
                introspections.push_back(ToString(count) + " tasks from AggregationComputeThreads setting");
                break;
            case TTaskType::ECreateReason::UPSERT_DELETE_DATASHARD:
                introspections.push_back(ToString(count) + " tasks for upsert/delete in datashard");
                break;
            case TTaskType::ECreateReason::DEFAULT_SOURCE_SCAN:
                introspections.push_back(ToString(count) + " tasks default for source scan");
                break;
            case TTaskType::ECreateReason::DEFAULT_SHARD_SCAN:
                introspections.push_back(ToString(count) + " tasks default for shard scan");
                break;
            case TTaskType::ECreateReason::SHUFFLE_ELIMINATE_SCAN:
                introspections.push_back(ToString(count) + " tasks for scan with shuffle elimination");
                break;
            case TTaskType::ECreateReason::SINGLE_SOURCE_SCAN:
                introspections.push_back(ToString(count) + " tasks for a single/sequential source scan");
                break;
            case TTaskType::ECreateReason::DEFAULT_SOURCE_READ:
                introspections.push_back(ToString(count) + " tasks default for source read");
                break;
            case TTaskType::ECreateReason::SCHEDULED_SOURCE_READ:
                introspections.push_back(ToString(count) + " tasks scheduled for source read");
                break;
            case TTaskType::ECreateReason::SNAPSHOT_SOURCE_READ:
                introspections.push_back(ToString(count) + " tasks by resource snapshot for source read");
                break;
            case TTaskType::ECreateReason::OLAP_AGGREGATION_SCAN:
                introspections.push_back(ToString(count) + " tasks from CSScanThreadsPerNode setting");
                break;
            case TTaskType::ECreateReason::OLTP_AGGREGATION_SCAN:
                introspections.push_back(ToString(count) + " tasks from DSScanMinimalThreads setting");
                break;
            case TTaskType::ECreateReason::OLAP_SORT_SCAN:
                introspections.push_back(ToString(count) + " tasks for OLAP and sort scan");
                break;
            case TTaskType::ECreateReason::OLTP_SORT_SCAN:
                introspections.push_back(ToString(count) + " tasks from DSBaseSortScanThreads setting");
                break;
            case TTaskType::ECreateReason::OLTP_MAP_JOIN_SCAN:
                introspections.push_back(ToString(count) + " tasks from DSBaseJoinScanThreads setting");
                break;
            case TTaskType::ECreateReason::MINIMUM_SCAN:
                introspections.push_back(ToString(count) + " tasks default for scan");
                break;
        }
    }

    return introspections;
}

TString TKqpTasksGraph::DumpToString() const {
    THashMap<TStageId, ui64> stageTasks;
    for (const auto& task : GetTasks()) {
        stageTasks[task.StageId]++;
    }

    TStringStream dump;
    for (const auto& [stageId, tasks] : stageTasks) {
        dump << "Stage " << stageId << " has " << tasks << " tasks: ";
        for (const auto& intro : GetStageIntrospection(stageId)) {
            dump << intro << ";";
        }
        dump << Endl;
    }

    return dump.Str();
}

TString TTaskMeta::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const {
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
