#include "kqp_tasks_graph.h"
#include "max_tasks_graph.h"

#include "kqp_partition_helper.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/json_index/json_index.h>
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
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/services/udf_store/wasm/query_compartment_scope.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

struct TStageScheduleInfo {
    double StageCost = 0.0;
    ui32 TaskCount = 0;
};

void ParseColumnToProto(const TString& columnName,
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
            const auto& stage = tx.Body->GetStages(stageIdx);
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
            ui32 maxStageTaskCount = ((TStagePredictor::GetUsableThreads() * 2) + 2) / 3;
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
        // TODO: why no check for ReadType?
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
            auto* memberType = resultStructType->GetMemberType(i);
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
    for (const auto& name: readOlapRange->GetOlapProgramParameterNames()) {
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

    if (!resourceSnapshot.empty()) {
        task.Meta.ExpectedNodeId = resourceSnapshot[nodeOffset % resourceSnapshot.size()].GetNodeId();
    }
}

template<typename Proto>
TVector<TTaskMeta::TColumn> BuildKqpColumns(const Proto& op, const TIntrusiveConstPtr<TTableConstInfo>& tableInfo) {
    TVector<TTaskMeta::TColumn> columns;
    columns.reserve(op.GetColumns().size());

    THashSet<TString> keyColumns;
    for (auto column : tableInfo->KeyColumns) {
        keyColumns.emplace(std::move(column));
    }

    for (const auto& column : op.GetColumns()) {
        TTaskMeta::TColumn c;

        const auto& tableColumn = tableInfo->Columns.at(column.GetName());
        c.Id = column.GetId();
        c.Type = tableColumn.Type;
        c.TypeMod = tableColumn.TypeMod;
        c.Name = column.GetName();
        c.NotNull = tableColumn.NotNull;
        c.IsPrimary = keyColumns.contains(c.Name);

        columns.emplace_back(std::move(c));
    }

    return columns;
}

struct TKqpTaskOutputType {
    enum : ui32 {
        ShardRangePartition = TTaskOutputType::COMMON_TASK_OUTPUT_TYPE_END
    };
};

NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::EReadType ReadTypeToProto(const TTaskMeta::TReadInfo::EReadType& type) {
    switch (type) {
        case TTaskMeta::TReadInfo::EReadType::Rows:
            return NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::ROWS;
        case TTaskMeta::TReadInfo::EReadType::Blocks:
            return NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::BLOCKS;
    }

    YQL_ENSURE(false, "Invalid read type in task meta.");
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

void FillEndpointDesc(NDqProto::TEndpoint& endpoint, const TTask& task) {
    if (task.ComputeActorId) {
        ActorIdToProto(task.ComputeActorId, endpoint.MutableActorId());
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
    if (task.Meta.ScanTask || (stageInfo.Meta.IsSysView() && task.Meta.Reads.Defined())) {
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

            for (const auto& columnType : task.Meta.ReadInfo.ResultColumnsTypes) {
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

            for (const auto& column : task.Meta.Reads->front().Columns) {
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

        for (const auto& read : *task.Meta.Reads) {
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

void AppendMKQLValueToToken(TString& token, NKikimr::NMiniKQL::TType* type, NUdf::TUnboxedValue value) {
    if (type->GetKind() != NKikimr::NMiniKQL::TType::EKind::Data) {
        YDB_LOG_WARN("Cannot append parameter value to token: unexpected type kind",
            {"typeKind", static_cast<int>(type->GetKind())});
        return;
    }

    auto dataSlot = static_cast<NKikimr::NMiniKQL::TDataType*>(type)->GetDataSlot();
    if (!dataSlot) {
        YDB_LOG_WARN("Cannot append parameter value to token: no data slot");
        return;
    }

    switch (*dataSlot) {
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::String, value.AsStringRef());
            break;

        case NUdf::EDataSlot::Bool:
            NJsonIndex::AppendJsonIndexLiteral(
                token, value.Get<bool>() ? NBinaryJson::EEntryType::BoolTrue : NBinaryJson::EEntryType::BoolFalse);
            break;

        case NUdf::EDataSlot::Int8: {
            double num = static_cast<double>(value.Get<i8>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Int16: {
            double num = static_cast<double>(value.Get<i16>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Int32: {
            double num = static_cast<double>(value.Get<i32>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Int64: {
            auto intValue = value.Get<i64>();
            if (intValue > NJsonIndex::MaxSupportedInt || intValue < -NJsonIndex::MaxSupportedInt) {
                break;
            }
            double num = static_cast<double>(intValue);
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Uint8: {
            double num = static_cast<double>(value.Get<ui8>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Uint16: {
            double num = static_cast<double>(value.Get<ui16>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Uint32: {
            double num = static_cast<double>(value.Get<ui32>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Uint64: {
            auto uintValue = value.Get<ui64>();
            if (uintValue > static_cast<ui64>(NJsonIndex::MaxSupportedInt)) {
                break;
            }
            double num = static_cast<double>(uintValue);
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Float: {
            double num = static_cast<double>(value.Get<float>());
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        case NUdf::EDataSlot::Double: {
            double num = value.Get<double>();
            NJsonIndex::AppendJsonIndexLiteral(token, NBinaryJson::EEntryType::Number, {}, &num);
            break;
        }

        default:
            YDB_LOG_WARN("Cannot append parameter value to token, unexpected data",
                {"slot", static_cast<int>(*dataSlot)});
    }
}

TString ResolveFullTextQueryToken(const NKqpProto::TKqpFullTextSource::TKqpQuerySettings::TQueryToken& token, const TStageInfo& stageInfo) {
    TString fullToken = token.GetToken();
    if (token.GetParamName().empty()) {
        return fullToken;
    }

    auto* paramPtr = stageInfo.Meta.Tx.Params->GetParameterUnboxedValuePtr(token.GetParamName());
    if (!paramPtr) {
        YDB_LOG_WARN("Failed to get parameter value for full-text query token",
            {"paramName", token.GetParamName()});
        return fullToken;
    }

    auto [type, value] = *paramPtr;
    AppendMKQLValueToToken(fullToken, type, std::move(value));
    return fullToken;
}

TVector<TString> ResolveFullTextQueryTokenExpanded(
    const NKqpProto::TKqpFullTextSource::TKqpQuerySettings::TQueryToken& token, const TStageInfo& stageInfo) {
    const TString baseToken = token.GetToken();
    if (token.GetParamName().empty()) {
        return { baseToken };
    }

    auto* paramPtr = stageInfo.Meta.Tx.Params->GetParameterUnboxedValuePtr(token.GetParamName());
    if (!paramPtr) {
        YDB_LOG_WARN("Failed to get parameter value for full-text query token",
            {"paramName", token.GetParamName()});
        return { baseToken };
    }

    auto [type, value] = *paramPtr;
    TVector<TString> result;

    if (type->GetKind() == NKikimr::NMiniKQL::TType::EKind::List) {
        NUdf::TUnboxedValue item;
        auto* itemType = static_cast<NKikimr::NMiniKQL::TListType*>(type)->GetItemType();
        auto iter = value.GetListIterator();
        while (iter.Next(item)) {
            TString currentToken = baseToken;
            AppendMKQLValueToToken(currentToken, itemType, std::move(item));
            result.emplace_back(std::move(currentToken));
        }
    } else if (type->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple) {
        auto* tupleType = static_cast<NKikimr::NMiniKQL::TTupleType*>(type);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            TString currentToken = baseToken;
            AppendMKQLValueToToken(currentToken, tupleType->GetElementType(i), value.GetElement(i));
            result.emplace_back(std::move(currentToken));
        }
    } else if (type->GetKind() == NKikimr::NMiniKQL::TType::EKind::Dict) {
        auto* dictType = static_cast<NKikimr::NMiniKQL::TDictType*>(type);
        auto* keyType = dictType->GetKeyType();
        NUdf::TUnboxedValue key;
        auto iter = value.GetKeysIterator();
        while (iter.Next(key)) {
            TString currentToken = baseToken;
            AppendMKQLValueToToken(currentToken, keyType, std::move(key));
            result.emplace_back(std::move(currentToken));
        }
    } else {
        return { ResolveFullTextQueryToken(token, stageInfo) };
    }

    return result.empty() ? TVector<TString>{ baseToken } : result;
}

void AddQueryPathParam(TKqpTasksGraph::TTaskType& task, const TIntrusivePtr<NKikimr::NKqp::TUserRequestContext>& userRequestContext) {
    if (!userRequestContext || !userRequestContext->IsStreamingQuery) {
        return;
    }

    const auto& queryPath = userRequestContext->StreamingQueryPath
        ? userRequestContext->StreamingQueryPath
        : "default";
    task.Meta.TaskParams.emplace("query_path", queryPath);
}

} // anonymous namespace

void TKqpTasksGraph::FillStages() {
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
                        YQL_ENSURE(tx.Body->GetTableConstInfoById()->Map.contains(meta.TableId),
                            "Cannot find table const info for table: " << meta.TableId);
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
                    meta.AccessCheckOperations.insert(TKeyDesc::ERowOperation::Read);
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                    YQL_ENSURE(meta.TableConstInfo);
                    meta.TableKind = meta.TableConstInfo->TableKind;
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorResolve) {
                    meta.TableId = MakeTableId(input.GetVectorResolve().GetTable());
                    meta.TablePath = input.GetVectorResolve().GetTable().GetPath();
                    meta.AccessCheckOperations.insert(TKeyDesc::ERowOperation::Read);
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                    YQL_ENSURE(meta.TableConstInfo);
                    meta.TableKind = meta.TableConstInfo->TableKind;

                    YQL_ENSURE(!meta.IndexMetas.size());
                    meta.IndexMetas.emplace_back();
                    meta.IndexMetas.back().TableId = MakeTableId(input.GetVectorResolve().GetLevelTable());
                    meta.IndexMetas.back().TablePath = input.GetVectorResolve().GetLevelTable().GetPath();
                    meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kVectorSearch) {
                    const auto& vectorSearch = input.GetVectorSearch();

                    meta.TableId = MakeTableId(vectorSearch.GetTable());
                    meta.TablePath = vectorSearch.GetTable().GetPath();
                    meta.AccessCheckOperations.insert(TKeyDesc::ERowOperation::Read);
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                    YQL_ENSURE(meta.TableConstInfo);
                    meta.TableKind = meta.TableConstInfo->TableKind;

                    YQL_ENSURE(!meta.IndexMetas.size());
                    // [0] = level table, [1] = posting table
                    meta.IndexMetas.emplace_back();
                    meta.IndexMetas.back().TableId = MakeTableId(vectorSearch.GetLevelTable());
                    meta.IndexMetas.back().TablePath = vectorSearch.GetLevelTable().GetPath();
                    meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                    meta.IndexMetas.emplace_back();
                    meta.IndexMetas.back().TableId = MakeTableId(vectorSearch.GetPostingTable());
                    meta.IndexMetas.back().TablePath = vectorSearch.GetPostingTable().GetPath();
                    meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                    meta.TableId = MakeTableId(input.GetSequencer().GetTable());
                    meta.TablePath = input.GetSequencer().GetTable().GetPath();
                    meta.TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.TableId);
                }
            }

            auto fillMetaFromSinkSettings = [&tx, &meta](NKikimrKqp::TKqpTableSinkSettings& settings) {
                // For MODE_FILL (CTAS): Table.Path is the actual write target (the TEMP table created
                // by RewriteCreateTableAs, e.g. /.tmp/sessions/.../Destination_uuid). This temp table
                // exists when the FILL runs and has the correct shards for affinity routing.
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
                        if (indexSettings.GetIndexType() == NKqpProto::EKqpFullTextIndexType::EKqpFullTextCompactRelevance) {
                            meta.IndexMetas.emplace_back();
                            meta.IndexMetas.back().TableId = MakeTableId(indexSettings.GetDocsTable());
                            meta.IndexMetas.back().TablePath = indexSettings.GetDocsTable().GetPath();
                            meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                            meta.IndexMetas.emplace_back();
                            meta.IndexMetas.back().TableId = MakeTableId(indexSettings.GetDictTable());
                            meta.IndexMetas.back().TablePath = indexSettings.GetDictTable().GetPath();
                            meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                            meta.IndexMetas.emplace_back();
                            meta.IndexMetas.back().TableId = MakeTableId(indexSettings.GetStatsTable());
                            meta.IndexMetas.back().TablePath = indexSettings.GetStatsTable().GetPath();
                            meta.IndexMetas.back().TableConstInfo = tx.Body->GetTableConstInfoById()->Map.at(meta.IndexMetas.back().TableId);
                        }
                    }
                }
            };

            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    YQL_ENSURE(sink.GetOutputIndex() == 0);
                    YQL_ENSURE(stage.SinksSize() + stage.OutputTransformsSize() == 1);
                    fillMetaFromSinkSettings(settings);
                }
            }

            for (const auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink && transform.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(transform.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    YQL_ENSURE(transform.GetOutputIndex() == 0);
                    YQL_ENSURE(stage.SinksSize() + stage.OutputTransformsSize() == 1);
                    fillMetaFromSinkSettings(settings);
                }
            }

            {
                const auto& programSettings = stage.GetProgram().GetSettings();
                TStageInfo stageInfo(stageId, stage.InputsSize() + stageSourcesCount, stage.GetOutputsCount(), std::move(meta));
                stageInfo.WatermarksMode = programSettings.GetHasWatermarkGenerator()
                    ? NDqProto::WATERMARKS_MODE_DEFAULT
                    : NDqProto::WATERMARKS_MODE_DISABLED;
                if (programSettings.HasWatermarkGeneratorIdleTimeoutUs()) {
                    stageInfo.WatermarksIdleTimeoutUs = programSettings.GetWatermarkGeneratorIdleTimeoutUs();
                }
                bool stageAdded = AddStageInfo(std::move(stageInfo));
                YQL_ENSURE(stageAdded);
            }

            auto& stageInfo = GetStageInfo(stageId);
            YDB_LOG_DEBUG("Built stage info for shard scan",
                {"stageInfo", stageInfo.DebugString()});

            THashSet<TTableId> tables;
            for (const auto& op : stage.GetTableOps()) {
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
                    default:
                        YQL_ENSURE(false, "Unexpected table operation: " << (ui32) op.GetTypeCase());
                }
            }

            YQL_ENSURE(tables.empty() || tables.size() == 1);
        }
    }
}

void TKqpTasksGraph::BuildResultChannels(const TKqpPhyTxHolder::TConstPtr& tx, ui64 txIdx) {
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

        YDB_LOG_DEBUG("Created result channel from task output",
            {"channelId", channel.Id},
            {"taskId", originTaskId},
            {"outputIndex", outputIdx});
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

    auto* settings = GetMeta().Allocate<NKikimrKqp::TKqpStreamLookupSettings>();

    settings->SetDatabase(GetMeta().Database);

    const auto& poolId = GetMeta().UserRequestContext->PoolId;
    if (!poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID) {
        settings->SetPoolId(poolId);
    }

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
        // For compatibility with old versions
        settings->AddLookupKeyColumns(keyColumn);
    }

    for (const auto& inputColumn : streamLookup.GetInputColumns()) {
        auto columnIt = tableInfo->Columns.find(inputColumn);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << inputColumn);
        auto* columnProto = settings->AddInputColumns();
        ParseColumnToProto(inputColumn, columnIt, columnProto);
    }

    for (const auto& column : streamLookup.GetColumns()) {
        auto columnIt = tableInfo->Columns.find(column);
        YQL_ENSURE(columnIt != tableInfo->Columns.end(), "Unknown column: " << column);

        auto* columnProto = settings->AddColumns();
        ParseColumnToProto(column, columnIt, columnProto);
    }

    settings->SetLookupStrategy(streamLookup.GetLookupStrategy());
    settings->SetKeepRowsOrder(streamLookup.GetKeepRowsOrder());
    settings->SetCookieFormatVersion(streamLookup.GetCookieFormatVersion());
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
        const auto guard = TxAlloc->TypeEnv.BindAllocator();
        // A parametric LIMIT can resolve to 0. The datashard rejects a VectorTopK with
        // limit 0, so skip the pushdown; the plan's own LIMIT still yields no rows.
        const ui64 limit = ExtractPhyValue(stageInfo, in.GetLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod()).Get<ui64>();
        if (limit) {
            auto& out = *settings->MutableVectorTopK();
            out.SetColumn(in.GetColumn());
            *out.MutableSettings() = in.GetSettings();
            auto target = ExtractPhyValue(stageInfo, in.GetTargetVector(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
            out.SetTargetVector(TString(target.AsStringRef()));
            out.SetLimit((ui32)limit);
            for (const auto& colIdx: in.GetDistinctColumns()) {
                out.AddDistinctColumns(colIdx);
            }
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

    const auto& poolId = GetMeta().UserRequestContext->PoolId;
    if (!poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID) {
        settings->SetPoolId(poolId);
    }

    auto* levelMeta = settings->MutableLevelTable();
    const auto& kqpMeta = vectorResolve.GetLevelTable();
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
    meta.TablePath = stageInfo.Meta.TablePath;
    BuildTransformChannels(vectorResolveTransform, meta, "VectorResolve/Map", stageInfo, inputIndex,
        inputStageInfo, outputIndex, enableSpilling, logFunc);
}

void TKqpTasksGraph::BuildVectorSearchChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo, ui32 outputIndex,
    const NKqpProto::TKqpPhyCnVectorSearch& vectorSearch, bool enableSpilling, const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == inputStageInfo.Tasks.size());

    auto* settings = GetMeta().Allocate<NKikimrTxDataShard::TKqpVectorSearchSettings>();

    *settings->MutableIndexSettings() = vectorSearch.GetIndexSettings();
    settings->SetOverlapClusters(vectorSearch.GetOverlapClusters());
    settings->SetIndexLevels(vectorSearch.GetLevels());
    {
        // TopK (LIMIT) may be a literal or a query parameter; resolve it to a value here.
        // Saturate to ui32: the pushdown chain is ui32, and a larger LIMIT just means "all".
        const auto guard = TxAlloc->TypeEnv.BindAllocator();
        const ui64 raw = ExtractPhyValue(stageInfo, vectorSearch.GetTopK(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod((ui32)0)).Get<ui64>();
        settings->SetTopK(static_cast<ui32>(std::min<ui64>(raw, Max<ui32>())));
    }
    settings->SetLevelTop(vectorSearch.GetLevelTop());
    settings->SetVectorColumnIndex(vectorSearch.GetVectorColumnIndex());
    settings->SetHasPrefix(vectorSearch.GetHasPrefix());

    YQL_ENSURE(stageInfo.Meta.IndexMetas.size() == 2);
    const auto& levelTableInfo = stageInfo.Meta.IndexMetas[0].TableConstInfo;
    const auto& postingTableInfo = stageInfo.Meta.IndexMetas[1].TableConstInfo;
    const auto& mainTableInfo = stageInfo.Meta.TableConstInfo;

    settings->SetDatabase(GetMeta().Database);

    const auto& poolId = GetMeta().UserRequestContext->PoolId;
    if (!poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID) {
        settings->SetPoolId(poolId);
    }

    auto fillTableMeta = [](NKikimrTxDataShard::TKqpTransaction::TTableMeta* meta,
        const NKqpProto::TKqpPhyTableId& kqpMeta, const auto& info) {
        meta->SetTablePath(kqpMeta.GetPath());
        meta->MutableTableId()->SetTableId(kqpMeta.GetTableId());
        meta->MutableTableId()->SetOwnerId(kqpMeta.GetOwnerId());
        meta->SetSchemaVersion(kqpMeta.GetVersion());
        meta->SetTableKind((ui32)info->TableKind);
    };

    auto fillColumnMeta = [](NKikimrTxDataShard::TKqpTransaction::TColumnMeta* meta,
        const TString& name, const auto& col) {
        meta->SetId(col.Id);
        meta->SetName(name);
        meta->SetType(col.Type.GetTypeId());
        if (NScheme::NTypeIds::IsParametrizedType(col.Type.GetTypeId())) {
            ProtoFromTypeInfo(col.Type, col.TypeMod, *meta->MutableTypeInfo());
        }
        meta->SetNotNull(col.NotNull);
    };

    // Level table
    fillTableMeta(settings->MutableLevelTable(), vectorSearch.GetLevelTable(), levelTableInfo);
    settings->SetLevelTableParentColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::ParentColumn).Id);
    settings->SetLevelTableClusterColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::IdColumn).Id);
    settings->SetLevelTableCentroidColumnId(levelTableInfo->Columns.at(NTableIndex::NKMeans::CentroidColumn).Id);

    // Posting table: key columns are (__ydb_parent, <main PK columns>)
    fillTableMeta(settings->MutablePostingTable(), vectorSearch.GetPostingTable(), postingTableInfo);
    for (const auto& keyColumn : postingTableInfo->KeyColumns) {
        settings->AddPostingTableKeyColumnIds(postingTableInfo->Columns.at(keyColumn).Id);
    }

    // Main table: PK columns, output columns
    fillTableMeta(settings->MutableMainTable(), vectorSearch.GetTable(), mainTableInfo);
    for (const auto& keyColumn : mainTableInfo->KeyColumns) {
        fillColumnMeta(settings->AddMainTableKeyColumns(), keyColumn, mainTableInfo->Columns.at(keyColumn));
    }
    for (const auto& column : vectorSearch.GetColumns()) {
        fillColumnMeta(settings->AddOutputColumns(), column, mainTableInfo->Columns.at(column));
    }

    // Covered index: if the posting table holds every output column, the actor
    // can build results straight from the posting scan and skip the main read.
    bool postingCovers = true;
    for (const auto& column : vectorSearch.GetColumns()) {
        if (!postingTableInfo->Columns.contains(column)) {
            postingCovers = false;
            break;
        }
    }
    if (postingCovers) {
        settings->SetPostingCovers(true);
        for (const auto& column : vectorSearch.GetColumns()) {
            settings->AddPostingOutputColumnIds(postingTableInfo->Columns.at(column).Id);
        }
    } else {
        // Partially-covered index: the output is not fully in the posting table
        // (e.g. a prefixed index whose posting table lacks the prefix key column),
        // but the posting table still holds the embedding column. Pass its posting
        // column id so the actor can push the final top-K down into the posting
        // scan (rank on the posting embedding) and then main-read only the
        // surviving PKs -- instead of streaming every candidate to the main read.
        const auto& embeddingColumn = vectorSearch.GetColumns(vectorSearch.GetVectorColumnIndex());
        if (auto it = postingTableInfo->Columns.find(embeddingColumn); it != postingTableInfo->Columns.end()) {
            settings->SetPostingEmbeddingColumnId(it->second.Id);
        }
    }

    TTransform vectorSearchTransform;
    vectorSearchTransform.Type = "VectorSearchInputTransformer";
    vectorSearchTransform.InputType = vectorSearch.GetInputType();
    vectorSearchTransform.OutputType = vectorSearch.GetOutputType();
    TTaskInputMeta meta;
    meta.VectorSearchSettings = settings;
    meta.TablePath = stageInfo.Meta.TablePath;
    BuildTransformChannels(vectorSearchTransform, meta, "VectorSearch/Map", stageInfo, inputIndex,
        inputStageInfo, outputIndex, enableSpilling, logFunc);
}

void TKqpTasksGraph::BuildDqSourceStreamLookupChannels(const TStageInfo& stageInfo, ui32 inputIndex, const TStageInfo& inputStageInfo,
    ui32 outputIndex, const NKqpProto::TKqpPhyCnDqSourceStreamLookup& dqSourceStreamLookup, const TChannelLogFunc& logFunc) {
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
    if (!AppData()->FeatureFlags.GetEnableDqSourceStreamLookupJoinFullscan()) {
        Y_ENSURE(
            !dqSourceStreamLookup.HasFullscanLimit() || dqSourceStreamLookup.GetFullscanLimit() == 0,
            TStringBuilder{} << "EnableDqSourceStreamLookupJoinFullscan disabled, but FullscanLimit is " << dqSourceStreamLookup.GetFullscanLimit()
        );
        settings->SetFullscanLimit(0);
    } else if (dqSourceStreamLookup.HasFullscanLimit()) {
        settings->SetFullscanLimit(dqSourceStreamLookup.GetFullscanLimit());
    }
    if (!AppData()->FeatureFlags.GetEnableDqSourceStreamLookupJoinShuffleMode()) {
        Y_ENSURE(
            dqSourceStreamLookup.GetShuffleMode() == NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_DEFAULT || dqSourceStreamLookup.GetShuffleMode() == NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_OFF,
            TStringBuilder{} << "EnableDqSourceStreamLookupJoinShuffleMode disabled, but ShuffleMode is " << static_cast<NDq::EShuffleMode>(dqSourceStreamLookup.GetShuffleMode()));
    }
    /* ShuffleMode intentionally omitted */

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
    switch (dqSourceStreamLookup.GetShuffleMode()) {
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_DEFAULT:
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_OFF:
            BuildUnionAllChannels(*this, stageInfo, inputIndex, inputStageInfo, outputIndex, /* enableSpilling */ false, logFunc);
            break;
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_MAP:
            BuildMapChannels(*this, stageInfo, inputIndex, inputStageInfo, outputIndex, /* enableSpilling */ false, logFunc);
            break;
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_HASH:
            BuildHashShuffleChannels(*this, stageInfo, inputIndex, inputStageInfo, outputIndex,
                dqSourceStreamLookup.GetLeftJoinKeyNames(),
                /* enableSpilling */false, logFunc);
            break;
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_TKqpPhyCnDqSourceStreamLookup_EShuffleMode_INT_MIN_SENTINEL_DO_NOT_USE_:
        case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_TKqpPhyCnDqSourceStreamLookup_EShuffleMode_INT_MAX_SENTINEL_DO_NOT_USE_:
            YQL_ENSURE(false, "Impossible");
            break;
    }
}

// Build ColumnShardHashV1 params for a CS write-affinity Sink Stage and return the
// key columns to pass to BuildHashShuffleChannels.
//
// This is the shared logic used by both:
//   - kColumnShardHashV1 with empty KeyColumns (CTAS: optimizer emits TDqCnHashShuffle)
//   - kBroadcast (INSERT/UPDATE/DELETE: optimizer emits TDqCnBroadcast, runtime converts)
//
// Returns std::nullopt if the params could not be built (caller should fall back to
// plain Broadcast in that case).
static std::optional<std::vector<TString>> BuildColumnShardHashV1ForWriteAffinity(
    TKqpTasksGraph& graph,
    const TStageInfo& stageInfo,
    const TStageInfo& inputStageInfo,
    ui32 outputIdx)
{
    // Check if we have shard info available (either via ColumnTableInfo or ShardKey).
    bool hasShardInfo = false;
    if (stageInfo.Meta.ColumnTableInfoPtr
            && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
        hasShardInfo = true;
    } else if (stageInfo.Meta.ShardKey
            && !stageInfo.Meta.ShardKey->GetPartitions().empty()) {
        hasShardInfo = true;
    }

    YDB_LOG_INFO("CS Write Affinity: BuildColumnShardHashV1ForWriteAffinity called",
        {"stageId", stageInfo.Id}
        , {"csShardingColumnsSize", stageInfo.Meta.CsShardingColumns.size()}
        , {"hasShardInfo", hasShardInfo}
        , {"shardsResolved", graph.GetMeta().ShardsResolved}
        , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
        , {"hasResolvedSinkSettings", stageInfo.Meta.ResolvedSinkSettings.has_value()}
        , {"tasksCount", stageInfo.Tasks.size()});

    if (stageInfo.Meta.CsShardingColumns.empty()
            || !hasShardInfo
            || !graph.GetMeta().ShardsResolved) {
        YDB_LOG_WARN("CS Write Affinity: BuildColumnShardHashV1ForWriteAffinity returning nullopt",
            {"stageId", stageInfo.Id}
            , {"csShardingColumnsEmpty", stageInfo.Meta.CsShardingColumns.empty()}
            , {"hasShardInfo", hasShardInfo}
            , {"shardsResolved", graph.GetMeta().ShardsResolved});
        return std::nullopt;
    }

    // Extract key columns and column definitions from either ResolvedSinkSettings
    // (set by table resolver) or the raw sink settings proto (for INSERT VALUES
    // where the resolver may not populate ResolvedSinkSettings).
    // Both KeyColumns and Columns are required: KeyColumns for hash types,
    // Columns for name-to-index mapping in wide channels.
    NKikimrKqp::TKqpTableSinkSettings sinkSettings;
    bool hasSettings = false;
    if (stageInfo.Meta.ResolvedSinkSettings) {
        sinkSettings = *stageInfo.Meta.ResolvedSinkSettings;
        hasSettings = true;
    } else {
        // Fallback: extract from raw sink settings proto.
        const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
        for (const auto& sink : stage.GetSinks()) {
            if (sink.HasInternalSink()
                    && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                if (sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings)) {
                    hasSettings = true;
                    break;
                }
            }
        }
    }

    if (!hasSettings || sinkSettings.GetKeyColumns().empty() || sinkSettings.GetColumns().empty()) {
        YDB_LOG_WARN("ColumnShardHashV1ForWriteAffinity: missing settings",
            {"stageId", stageInfo.Id}
            , {"hasSettings", hasSettings}
            , {"keyColumnsCount", sinkSettings.GetKeyColumns().size()}
            , {"columnsCount", sinkSettings.GetColumns().size()}
            , {"hasResolvedSinkSettings", stageInfo.Meta.ResolvedSinkSettings.has_value()});
        return std::nullopt;
    }

    // CRITICAL: For wide channels (compute/transform stages), the key columns
    // in the HashShuffle proto are column atoms (like "Col1") that the runtime
    // resolves against the output schema. If ResolvedSinkSettings is missing,
    // the column name-to-index mapping may not match the actual output schema,
    // causing GetColumnInfo() failures at runtime.
    // Only proceed if we have ResolvedSinkSettings (reliable column mapping)
    // OR the raw proto columns exactly match the sharding columns.
    if (!stageInfo.Meta.ResolvedSinkSettings) {
        // Verify that all CsShardingColumns exist in the raw proto Columns.
        THashSet<TString> availableColumns;
        for (const auto& col : sinkSettings.GetColumns()) {
            availableColumns.insert(col.GetName());
        }
        for (const auto& shardingCol : stageInfo.Meta.CsShardingColumns) {
            if (!availableColumns.contains(shardingCol)) {
                // Column mismatch — can't build correct routing.
                return std::nullopt;
            }
        }
    }

    // Build shardId to taskIdx map from task params (set by CountComputeTasks).
    // PlaceTasks reorders tasks by node, so we can't use positional index.
    THashMap<ui64 /* shardId */, ui32 /* taskIdx */> shardToTaskIdx;
    for (ui32 ti = 0; ti < stageInfo.Tasks.size(); ++ti) {
        const auto& task = graph.GetTask(stageInfo.Tasks[ti]);
        auto it = task.Meta.TaskParams.find("CsWriteAffinityShardId");
        if (it != task.Meta.TaskParams.end()) {
            ui64 shardId = 0;
            try {
                shardId = std::stoull(it->second);
            } catch (...) {
                continue;
            }
            if (shardId) {
                shardToTaskIdx[shardId] = ti;
            }
        }
    }

    // Verify: all tasks should have CsWriteAffinityShardId set.
    // If not, we can't build correct routing — return std::nullopt.
    if (shardToTaskIdx.empty()) {
        return std::nullopt;
    }
    AFL_VERIFY(shardToTaskIdx.size() == stageInfo.Tasks.size())
        ("stageId", stageInfo.Id)
        ("shardToTaskIdxSize", shardToTaskIdx.size())
        ("tasksCount", stageInfo.Tasks.size())
        ("msg", "Not all tasks have CsWriteAffinityShardId");

    // Canonical shard order = ColumnShard sharding bucket order.
    // CRITICAL: Use the SAME source as CountComputeTasks to get orderedShardIds.
    // CountComputeTasks iterates GetColumnShards() from ColumnTableInfoPtr,
    // or falls back to ShardKey->GetPartitions().
    // We must match that order exactly, because taskIndexByHash[i] maps
    // bucket i to the task that owns orderedShardIds[i].
    TVector<ui64> orderedShardIds;
    if (stageInfo.Meta.ColumnTableInfoPtr
            && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
        const auto& sharding = stageInfo.Meta.ColumnTableInfoPtr->Description.GetSharding();
        for (const auto& shardId : sharding.GetColumnShards()) {
            orderedShardIds.push_back(shardId);
        }
    } else if (stageInfo.Meta.ShardKey) {
        for (const auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
            orderedShardIds.push_back(partition.ShardId);
        }
    } else {
        return std::nullopt;
    }

    // Verify: orderedShardIds matches the shards from task params.
    // Both CountComputeTasks and this function must use the same shard order.
    {
        THashSet<ui64> taskShardIds;
        for (const auto& [shardId, taskIdx] : shardToTaskIdx) {
            taskShardIds.insert(shardId);
        }
        for (const auto& shardId : orderedShardIds) {
            AFL_VERIFY(taskShardIds.contains(shardId))
                ("shardId", shardId)
                ("stageId", stageInfo.Id)
                ("msg", "orderedShardIds contains shard not found in task params");
        }
        AFL_VERIFY(orderedShardIds.size() == taskShardIds.size())
            ("orderedShardIdsSize", orderedShardIds.size())
            ("taskShardIdsSize", taskShardIds.size())
            ("stageId", stageInfo.Id)
            ("msg", "orderedShardIds and task params have different shard counts");
    }

    const ui32 N = orderedShardIds.size();

    // Build TaskIndexByHash[0..N-1]: bucket i to taskIdx.
    // Bucket i corresponds to shardId = orderedShardIds[i] (ColumnShard contract).
    auto taskIndexByHash = std::make_shared<TVector<ui64>>(N, 0);
    bool allResolved = true;
    for (ui32 i = 0; i < N; ++i) {
        const ui64 shardId = orderedShardIds[i];
        auto itTask = shardToTaskIdx.find(shardId);
        if (itTask == shardToTaskIdx.end()) {
            // Shard has no dedicated task — can't build hash routing.
            allResolved = false;
            break;
        }
        (*taskIndexByHash)[i] = itTask->second;
    }

    if (!allResolved) {
        return std::nullopt;
    }

    // Cross-check: verify taskIndexByHash is consistent with shardToTaskIdx.
    for (ui32 i = 0; i < N; ++i) {
        const ui64 expectedShardId = orderedShardIds[i];
        const ui32 taskIdx = (*taskIndexByHash)[i];
        const auto& task = graph.GetTask(stageInfo.Tasks[taskIdx]);
        auto it = task.Meta.TaskParams.find("CsWriteAffinityShardId");
        AFL_VERIFY(it != task.Meta.TaskParams.end())
            ("stageId", stageInfo.Id)
            ("bucket", i)
            ("expectedShardId", expectedShardId)
            ("taskIdx", taskIdx)
            ("msg", "Task at taskIndexByHash[i] has no CsWriteAffinityShardId");
        ui64 actualShardId = 0;
        try {
            actualShardId = std::stoull(it->second);
        } catch (...) {
            AFL_VERIFY(false)
                ("stageId", stageInfo.Id)
                ("bucket", i)
                ("shardIdStr", it->second)
                ("msg", "Failed to parse CsWriteAffinityShardId");
        }
        AFL_VERIFY(actualShardId == expectedShardId)
            ("stageId", stageInfo.Id)
            ("bucket", i)
            ("expectedShardId", expectedShardId)
            ("actualShardId", actualShardId)
            ("taskIdx", taskIdx)
            ("msg", "taskIndexByHash[i] points to task with wrong shard ID");
    }

    // Derive key column types from sink settings Columns list, using CsShardingColumns
    // to find the right columns. This works even when KeyColumns has extra entries
    // (e.g., all table PK columns vs just hash sharding columns).
    THashMap<TString, ui32> columnNameToIndex;
    for (ui32 i = 0; i < static_cast<ui32>(sinkSettings.GetColumns().size()); ++i) {
        columnNameToIndex[sinkSettings.GetColumns(i).GetName()] = i;
    }

    auto keyTypes = std::make_shared<TVector<NScheme::TTypeInfo>>();
    for (const auto& shardingCol : stageInfo.Meta.CsShardingColumns) {
        auto it = columnNameToIndex.find(shardingCol);
        if (it == columnNameToIndex.end()) {
            YDB_LOG_WARN("ColumnShardHashV1ForWriteAffinity: sharding column not in Columns",
                {"stageId", stageInfo.Id}
                , {"shardingCol", shardingCol});
            return std::nullopt;
        }
        const auto& col = sinkSettings.GetColumns(it->second);
        keyTypes->push_back(NScheme::TypeInfoFromProto(col.GetTypeId(), col.GetTypeInfo()));
    }

    // Set ColumnShardHashV1Params on the upstream Transform Stage.
    // IMPORTANT: use GetColumnShardHashV1Params(outputIdx) to update the
    // per-output cache, because the shuffle elimination block may have already
    // created a stale cache entry in HashParamsByOutput[outputIdx] by calling
    // the same accessor. If we only update ColumnShardHashV1Params (the primary),
    // FillOutputDesc will read the stale cache entry instead of the updated value.
    auto& transformParams = const_cast<TStageInfo&>(inputStageInfo).Meta.GetColumnShardHashV1Params(outputIdx);
    transformParams.SourceShardCount = N;
    transformParams.TaskIndexByHash = std::move(taskIndexByHash);
    transformParams.SourceTableKeyColumnTypes = std::move(keyTypes);

    // Convert CsShardingColumns (column names) to numeric index strings
    // when the Transform stage uses wide channels (output type = Multi).
    // The runtime FindColumnInfo expects numeric indices for Multi types,
    // but column names for Struct types (narrow channels).
    //
    // The Transform stage's output type is available as the InputType
    // of the Sink stage's output transform (they are equal: the transform's
    // input type equals the stage's program output type).
    std::vector<TString> hashShuffleKeyColumns;
    // Determine whether the input (Transform) stage uses wide channels
    // (Multi output type) or narrow channels (Struct output type).
    //
    // Scan stages (with a table source) use narrow channels: the runtime
    // FindColumnInfo expects column names for Struct types.
    //
    // Compute/Transform stages (no table source, e.g. REPLACE INTO VALUES)
    // use wide channels: the runtime FindColumnInfo expects numeric index
    // strings for Multi types.
    //
    // We check TasksType: SCAN_TASKS means narrow (column names),
    // anything else means wide (numeric indices).
    bool useNumericIndices = (inputStageInfo.Meta.TasksType != TStageInfoMeta::SCAN_TASKS);

    if (useNumericIndices) {
        // Build column name to index map from sink settings Columns.
        // The Columns field lists columns in the same order as the
        // Transform stage's output struct (which maps to Multi elements).
        THashMap<TString, ui32> columnNameToIndex;
        for (ui32 i = 0; i < static_cast<ui32>(sinkSettings.GetColumns().size()); ++i) {
            columnNameToIndex[sinkSettings.GetColumns(i).GetName()] = i;
        }

        for (const auto& colName : stageInfo.Meta.CsShardingColumns) {
            auto it = columnNameToIndex.find(colName);
            if (it == columnNameToIndex.end()) {
                // Column not found in sink settings — can't build hash routing.
                // Fall back to Broadcast by returning std::nullopt.
                return std::nullopt;
            }
            hashShuffleKeyColumns.push_back(ToString(it->second));
        }
    } else {
        // Narrow channels (Struct type): column names work directly.
        hashShuffleKeyColumns = stageInfo.Meta.CsShardingColumns;
    }

    return hashShuffleKeyColumns;
}

void TKqpTasksGraph::BuildKqpStageChannels(TStageInfo& stageInfo, ui64 txId, bool enableSpilling, bool enableShuffleElimination) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    if (stage.GetIsEffectsStage() && stage.GetSinks().empty() && stage.GetOutputTransforms().empty()) {
        YQL_ENSURE(stageInfo.OutputsCount == 1);

        for (auto& taskId : stageInfo.Tasks) {
            auto& task = GetTask(taskId);
            auto& taskOutput = task.Outputs[0];
            taskOutput.Type = TTaskOutputType::Effects;
        }
    }

    auto log = [&stageInfo, txId](ui64 channel, ui64 from, ui64 to, TStringBuf type, bool spilling) {
        YDB_LOG_TRACE("Created stage channel between tasks",
            {"txId", txId},
            {"stageId", stageInfo.Id},
            {"channelId", channel},
            {"fromTask", from},
            {"toTask", to},
            {"type", type},
            {"enableSpilling", spilling});
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
                YDB_LOG_DEBUG("Propagating column shard hash params from input stage",
                    {"originStageTxId", originStageInfo.Id.TxId},
                    {"originStageId", originStageInfo.Id.StageId},
                    {"outputIdx", outputIdx},
                    {"stageTxId", stageInfo.Id.TxId},
                    {"stageId", stageInfo.Id.StageId},
                    {"columnShardHashKeyTypes", columnShardHashV1Params.KeyTypesToString()});
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

        for (const auto& input : stage.GetInputs()) {
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

            const auto& columnShardHashV1 = hashShuffle.GetColumnShardHashV1();
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

    for (const auto& input : stage.GetInputs()) {
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
                // Key columns resolved by BuildColumnShardHashV1ForWriteAffinity for
                // CTAS write affinity. Non-empty when the helper was used (CTAS case);
                // empty for shuffle elimination (use proto's KeyColumns directly).
                std::vector<TString> ctasKeyColumns;
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
                        // Two cases:
                        // 1. Shuffle elimination: columnShardHashV1Params already populated
                        //    from source scan stage (KeyColumns non-empty in proto, params
                        //    have SourceTableKeyColumnTypes set).
                        // 2. Write affinity (CTAS, Pure OLAP, UPDATE/DELETE): optimizer emits
                        //    TDqCnHashShuffle with ColumnShardHashV1 and non-empty KeyColumns
                        //    (from the target table's PK). columnShardHashV1Params must be
                        //    built from CsShardingColumns at runtime via the shared helper,
                        //    because shard info is not available at optimization time.
                        const bool isWriteAffinity = stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()
                            && !stageInfo.Meta.CsShardingColumns.empty();
                        const bool hasShuffleEliminationParams =
                            columnShardHashV1Params.SourceTableKeyColumnTypes
                            && !columnShardHashV1Params.SourceTableKeyColumnTypes->empty();

                        if (isWriteAffinity && !hasShuffleEliminationParams) {
                            // Write affinity: build params via shared helper.
                            auto csKeyColumns = BuildColumnShardHashV1ForWriteAffinity(
                                *this, stageInfo, inputStageInfo, outputIdx);
                            if (csKeyColumns.has_value()) {
                                // Store the resolved key columns for BuildHashShuffleChannels below.
                                // We can't use a reference here because the helper returns by value.
                                ctasKeyColumns = std::move(*csKeyColumns);
                                hashKind = EHashShuffleFuncType::ColumnShardHashV1;
                            } else {
                                // ColumnShardHashV1 params couldn't be built. This is an error
                                // because the optimizer emitted ColumnShardHashV1, meaning the
                                // sink stage expects per-shard routing. Falling back to HashV1
                                // would route rows to wrong tasks (equal-sized buckets vs actual
                                // shard intervals), causing data corruption.
                                Y_ENSURE(false, TStringBuilder{}
                                    << "ColumnShardHashV1 write affinity: params couldn't be built for stage "
                                    << stageInfo.Id.StageId);
                            }
                        } else {
                            // Shuffle elimination: params already populated from source scan stage.
                            Y_ENSURE(enableShuffleElimination, "OptShuffleElimination wasn't turned on, but ColumnShardHashV1 detected!");

                            YDB_LOG_DEBUG("Propagating column shard hash v1 params to input stage",
                                {"inputStageTxId", inputStageInfo.Id.TxId},
                                {"inputStageId", inputStageInfo.Id.StageId},
                                {"stageTxId", stageInfo.Id.TxId},
                                {"stageId", stageInfo.Id.StageId},
                                {"columnShardHashKeyTypes", columnShardHashV1Params.KeyTypesToString()},
                                {"keyColumns", JoinSeq(",", input.GetHashShuffle().GetKeyColumns())});

                            Y_ENSURE(
                                columnShardHashV1Params.SourceTableKeyColumnTypes->size() == input.GetHashShuffle().KeyColumnsSize(),
                                TStringBuilder{}
                                    << "Hashshuffle keycolumns and keytypes args count mismatch during executer stage, types: "
                                    << columnShardHashV1Params.KeyTypesToString() << " for the columns: "
                                    << "[" << JoinSeq(",", input.GetHashShuffle().GetKeyColumns()) << "]"
                            );

                            inputStageInfo.Meta.HashParamsByOutput[outputIdx] = columnShardHashV1Params;
                            hashKind = EHashShuffleFuncType::ColumnShardHashV1;
                        }
                        break;
                    }
                    default: {
                        Y_ENSURE(false, "undefined type of hash for shuffle");
                    }
                }

                Y_ENSURE(hashKind.has_value(), "HashKind wasn't set!");
                // For write affinity (CTAS, Pure OLAP, UPDATE/DELETE), use the key columns
                // resolved by the helper (which may have numeric indices for wide channels).
                // For shuffle elimination, use the proto's KeyColumns directly.
                if (!ctasKeyColumns.empty()) {
                    BuildHashShuffleChannels(
                        *this,
                        stageInfo,
                        inputIdx,
                        inputStageInfo,
                        outputIdx,
                        ctasKeyColumns,
                        enableSpilling,
                        log,
                        hashKind.value(),
                        forceSpilling
                    );
                } else {
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
                }
                break;
            }
            case NKqpProto::TKqpPhyConnection::kBroadcast: {
                // CsWriteAffinity: If this is an OLAP affinity Sink Stage (CsShardingColumns
                // populated by the table resolver) AND we have M>1 tasks (one per shard),
                // replace the Broadcast connection with ColumnShardHashV1 HashShuffle.
                //
                // This eliminates the Mx traffic overhead of Broadcast: each row is sent
                // only to the one Sink task that owns the target shard for that row's PK.
                //
                // The shared helper BuildColumnShardHashV1ForWriteAffinity builds the
                // ColumnShardHashV1 params (SourceShardCount, TaskIndexByHash,
                // SourceTableKeyColumnTypes) on the upstream Transform Stage and returns
                // the key columns to pass to BuildHashShuffleChannels.
#ifdef QP_FORCE_CS_WRITE_AFFINITY
                // Invariant: with the force flag, a kBroadcast connection into an OLAP
                // sink with >1 tasks MUST enter the ColumnShardHashV1 building block.
                if (stageInfo.Tasks.size() > 1) {
                    bool isOlapSink = false;
                    for (const auto& sink : stage.GetSinks()) {
                        if (sink.HasInternalSink()
                                && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                            NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                            if (sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings)
                                    && sinkSettings.GetIsOlap()) {
                                isOlapSink = true;
                                break;
                            }
                        }
                    }
                    if (isOlapSink) {
                        bool hasShardInfo = false;
                        if (stageInfo.Meta.ColumnTableInfoPtr
                                && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
                            hasShardInfo = true;
                        } else if (stageInfo.Meta.ShardKey
                                && !stageInfo.Meta.ShardKey->GetPartitions().empty()) {
                            hasShardInfo = true;
                        }
                        AFL_VERIFY(!stageInfo.Meta.CsShardingColumns.empty()
                                   && hasShardInfo
                                   && GetMeta().ShardsResolved)
                            ("stageId", stageInfo.Id)
                            ("hasCsShardingColumns", !stageInfo.Meta.CsShardingColumns.empty())
                            ("csShardingColumnsSize", stageInfo.Meta.CsShardingColumns.size())
                            ("hasShardKey", stageInfo.Meta.ShardKey != nullptr)
                            ("hasPartitions", stageInfo.Meta.ShardKey && !stageInfo.Meta.ShardKey->GetPartitions().empty())
                            ("partitionsCount", stageInfo.Meta.ShardKey ? stageInfo.Meta.ShardKey->GetPartitions().size() : 0)
                            ("shardsResolved", GetMeta().ShardsResolved)
                            ("tasksCount", stageInfo.Tasks.size())
                            ("msg", "QP_FORCE_CS_WRITE_AFFINITY: kBroadcast into multi-task OLAP sink:"
                                    " precondition for ColumnShardHashV1 is false;"
                                    " will fall through to plain Broadcast violating per-shard routing");
                    }
                }
#endif
                // Try to build ColumnShardHashV1 params via the shared helper.
                auto csKeyColumns = BuildColumnShardHashV1ForWriteAffinity(
                    *this, stageInfo, inputStageInfo, outputIdx);

                if (csKeyColumns.has_value()) {
                    // Use HashShuffle instead of Broadcast for correct per-shard routing.
                    BuildHashShuffleChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx,
                        *csKeyColumns, enableSpilling, log,
                        EHashShuffleFuncType::ColumnShardHashV1);
                    break;
                }

                if (stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()
                        && !stageInfo.Meta.CsShardingColumns.empty()) {
                    // ColumnShardHashV1 could not be built — this is an error when
                    // EnableCsWriteAffinity is set AND this stage has CsShardingColumns
                    // (indicating it's a CS write affinity sink). The optimizer already
                    // emitted a plan that expects per-shard routing. Falling back to
                    // Broadcast would violate the per-shard TargetShardIds invariant.
                    Y_ENSURE(false,
                        "ColumnShardHashV1 write affinity: kBroadcast fell back to plain Broadcast — "
                        "ColumnShardHashV1 could not be built (stageId=" << stageInfo.Id.StageId << "); "
                        "per-shard TargetShardIds will be violated");
                }
                // Fall back to plain Broadcast when affinity is disabled.
                BuildBroadcastChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                // For OLAP sink stages with per-shard affinity (multiple tasks),
                // replace Map with Broadcast + HashShuffle to avoid task count mismatch.
                // Map requires originTasks.size() == targetTasks.size(), but per-shard
                // affinity creates N sink tasks while the source may have fewer.
                bool isOlapSinkWithMultipleTasks = false;
                if (stageInfo.Tasks.size() > 1) {
                    // Use ResolvedSinkSettings first (has IsOlap set by table resolver for CTAS),
                    // falling back to raw proto for non-CTAS cases.
                    if (stageInfo.Meta.ResolvedSinkSettings
                            && stageInfo.Meta.ResolvedSinkSettings->GetIsOlap()) {
                        isOlapSinkWithMultipleTasks = true;
                    } else {
                        for (const auto& sink : stage.GetSinks()) {
                            if (sink.HasInternalSink()
                                    && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                                NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                                if (sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings)
                                        && sinkSettings.GetIsOlap()) {
                                    isOlapSinkWithMultipleTasks = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (isOlapSinkWithMultipleTasks
                        && stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()
                        && !stageInfo.Meta.CsShardingColumns.empty()) {
                    // For OLAP sinks with per-shard write affinity, use ColumnShardHashV1
                    // HashShuffle instead of Map to route each row to exactly one sink task.
                    auto csKeyColumns = BuildColumnShardHashV1ForWriteAffinity(
                        *this, stageInfo, inputStageInfo, outputIdx);

                    if (csKeyColumns.has_value()) {
                        BuildHashShuffleChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx,
                            *csKeyColumns, enableSpilling, log,
                            EHashShuffleFuncType::ColumnShardHashV1);
                    } else {
                        // ColumnShardHashV1 could not be built — this is an error when
                        // EnableCsWriteAffinity is set AND this stage has CsShardingColumns.
                        Y_ENSURE(false,
                            "ColumnShardHashV1 write affinity: kMap (OLAP multi-task) could not build "
                            "ColumnShardHashV1 (stageId=" << stageInfo.Id.StageId << "); "
                            "per-shard TargetShardIds will be violated");
                    }
                } else {
                    BuildMapChannels(*this, stageInfo, inputIdx, inputStageInfo, outputIdx, enableSpilling, log);
                }
                break;
            }
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

            case NKqpProto::TKqpPhyConnection::kVectorSearch: {
                BuildVectorSearchChannels(stageInfo, inputIdx, inputStageInfo, outputIdx, input.GetVectorSearch(), enableSpilling, log);
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

void TKqpTasksGraph::FillChannelDesc(NDqProto::TChannel& channelDesc, const TChannel& channel,
    const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion channelTransportVersion, bool enableSpilling) const
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
    if (channelTransportVersion == NKikimrConfig::TTableServiceConfig::CTV_OOB_PICKLE_1_0) {
        channelDesc.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0);
    } else {
        channelDesc.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0);
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
            for (const auto& column : output.KeyColumns) {
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
                    const auto& columnShardHashV1Params = stageInfo.Meta.GetColumnShardHashV1Params(outputIdx);
                    YDB_LOG_DEBUG("Filling column shard hash v1 params for runtime output",
                        {"stageTxId", stageInfo.Id.TxId},
                        {"stageId", stageInfo.Id.StageId},
                        {"columnShardHashKeyTypes", columnShardHashV1Params.KeyTypesToString()},
                        {"keyColumns", JoinSeq(",", output.KeyColumns)});

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
            for (const auto& column : output.KeyColumns) {
                *columns.Add() = column;
            }

            auto& partitionsDesc = *rangePartitionDesc.MutablePartitions();
            for (const auto& pair : output.Meta.ShardPartitions) {
                const auto& range = *pair.second->Range;
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

    for (const auto& channel : output.Channels) {
        auto& channelDesc = *outputDesc.AddChannels();
        FillChannelDesc(channelDesc, GetChannel(channel), GetMeta().ChannelTransportVersion, enableSpilling);
    }

    if (output.Transform) {
        auto* transformDesc = outputDesc.MutableTransform();
        const auto& transform = output.Transform;

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
            const auto& sortColumns = std::get<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo).SortColumns;
            for (const auto& sortColumn : sortColumns) {
                auto* newSortCol = mergeProto.AddSortColumns();
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
                if (input.Meta.StreamLookupSettings->GetLookupStrategy() == NKqpProto::EStreamLookupStrategy::LOCK_AND_LOOKUP) {
                    input.Meta.StreamLookupSettings->SetAllowInconsistentReads(true);
                }
            } else {
                YQL_ENSURE(GetMeta().AllowInconsistentReads || isTableImmutable, "Expected valid snapshot or enabled inconsistent read mode");
                input.Meta.StreamLookupSettings->SetAllowInconsistentReads(true);
            }

            if (lockTxId && !isTableImmutable) {
                input.Meta.StreamLookupSettings->SetLockTxId(*lockTxId);
                input.Meta.StreamLookupSettings->SetLockNodeId(GetMeta().LockNodeId);
            }

            if (lockTxId && GetMeta().LockMode && !isTableImmutable) {
                AFL_ENSURE(input.Meta.StreamLookupSettings->GetLookupStrategy() != NKqpProto::EStreamLookupStrategy::UNIQUE
                    || GetMeta().LockMode != NKikimrDataEvents::PESSIMISTIC_NONE);
                if (input.Meta.StreamLookupSettings->GetLookupStrategy() == NKqpProto::EStreamLookupStrategy::UNIQUE
                        && GetMeta().RequestIsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SNAPSHOT_RW) {
                    // Unique Index needs read lock even in snapshot isolation mode.
                    input.Meta.StreamLookupSettings->SetLockMode(NKikimrDataEvents::OPTIMISTIC);
                } else if (input.Meta.StreamLookupSettings->GetLookupStrategy() == NKqpProto::EStreamLookupStrategy::UNIQUE) {
                    input.Meta.StreamLookupSettings->SetLookupStrategy(NKqpProto::EStreamLookupStrategy::LOOKUP);
                    input.Meta.StreamLookupSettings->SetLockMode(*GetMeta().LockMode);
                } else {
                    input.Meta.StreamLookupSettings->SetLockMode(*GetMeta().LockMode);
                }
            } else if (input.Meta.StreamLookupSettings->GetLookupStrategy() == NKqpProto::EStreamLookupStrategy::UNIQUE
                    && GetMeta().RequestIsolationLevel != NKqpProto::EIsolationLevel::ISOLATION_LEVEL_SNAPSHOT_RW) {
                input.Meta.StreamLookupSettings->SetLookupStrategy(NKqpProto::EStreamLookupStrategy::LOOKUP);
            }

            if (!isTableImmutable) {
                const ui64 effectiveSpanId = GetMeta().GetEffectiveQuerySpanId(
                    GetMeta().QuerySpanId, input.Meta.StreamLookupSettings->GetTable().GetPath());
                if (effectiveSpanId) {
                    input.Meta.StreamLookupSettings->SetQuerySpanId(effectiveSpanId);
                }
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

            {
                const ui64 effectiveSpanId = GetMeta().GetEffectiveQuerySpanId(
                    GetMeta().QuerySpanId, input.Meta.TablePath);
                if (effectiveSpanId) {
                    input.Meta.VectorResolveSettings->SetQuerySpanId(effectiveSpanId);
                }
            }

            transformProto->MutableSettings()->PackFrom(*input.Meta.VectorResolveSettings);
        } else if (input.Meta.VectorSearchSettings) {
            enableMetering = true;

            // Unlike the write-path VectorResolve, the read path is not guaranteed an
            // MVCC snapshot (e.g. multi-phase data queries that compute the target
            // vector in an earlier phase). The read actor reads without a snapshot in
            // that case, so set it only when valid.
            if (snapshot.IsValid()) {
                input.Meta.VectorSearchSettings->MutableSnapshot()->SetStep(snapshot.Step);
                input.Meta.VectorSearchSettings->MutableSnapshot()->SetTxId(snapshot.TxId);
            }

            // Under stale-RO the index impl tables (level, posting) are read from
            // followers without a snapshot, regardless of whether the whole query forced
            // an MVCC snapshot for the main table. The level table is immutable; the
            // posting table is not, but stale-RO accepts reading a stale replica of it.
            input.Meta.VectorSearchSettings->SetUseFollowers(
                GetMeta().RequestIsolationLevel == NKqpProto::EIsolationLevel::ISOLATION_LEVEL_READ_STALE);

            if (lockTxId) {
                input.Meta.VectorSearchSettings->SetLockTxId(*lockTxId);
                input.Meta.VectorSearchSettings->SetLockNodeId(GetMeta().LockNodeId);
            }

            if (GetMeta().LockMode) {
                input.Meta.VectorSearchSettings->SetLockMode(*GetMeta().LockMode);
            }

            // Inconsistent online RO takes neither a snapshot nor a lock, so the reads
            // have to say so explicitly or the read actor rejects them.
            if (GetMeta().AllowInconsistentReads) {
                input.Meta.VectorSearchSettings->SetAllowInconsistentReads(true);
            }

            {
                const ui64 effectiveSpanId = GetMeta().GetEffectiveQuerySpanId(
                    GetMeta().QuerySpanId, input.Meta.TablePath);
                if (effectiveSpanId) {
                    input.Meta.VectorSearchSettings->SetQuerySpanId(effectiveSpanId);
                }
            }

            transformProto->MutableSettings()->PackFrom(*input.Meta.VectorSearchSettings);
        } else {
            *transformProto->MutableSettings() = input.Transform->Settings;
        }
    }
}

void TKqpTasksGraph::SerializeTaskToProto(const TTask& task, NYql::NDqProto::TDqTask* result, bool serializeAsyncIoSettings) const {
    const auto& stageInfo = GetStageInfo(task.StageId);
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

    if (stage.WasmUdfModulesSize() > 0) {
        TVector<TString> modules = NUdfStore::NWasm::WasmUdfModulesFromRepeated(stage.GetWasmUdfModules());
        (*result->MutableTaskParams())[TString(NUdfStore::NWasm::WasmUdfModulesTaskParam)] =
            NUdfStore::NWasm::SerializeWasmUdfModulesTaskParam(modules);
    }

    for (const auto& paramName : stage.GetProgramParameters()) {
        auto& dqParams = *result->MutableParameters();
        dqParams[paramName] = stageInfo.Meta.Tx.Params->SerializeParamValue(paramName);
    }

    for (const auto& [taskParam, actorId] : stageInfo.Meta.ControlPlaneActors) {
        NActorsProto::TActorId actorIdProto;
        ActorIdToProto(actorId, &actorIdProto);
        (*result->MutableTaskParams())[taskParam] = actorIdProto.SerializeAsString();
    }

    SerializeCtxToMap(*GetMeta().UserRequestContext, *result->MutableRequestContext());

    result->SetDisableMetering(!enableMetering);
    result->SetCreateSuspended(GetMeta().CreateSuspended);
    FillTaskMeta(stageInfo, task, *result);
}

NYql::NDqProto::TDqTask* TKqpTasksGraph::ArenaSerializeTaskToProto(const TTask& task, bool serializeAsyncIoSettings) {
    auto* result = GetMeta().Allocate<NYql::NDqProto::TDqTask>();
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
        taskInfo->ClearParameters();    // clear parameters to avoid bloating the saved cell
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
            auto* transformSettings = meta.StreamLookupSettings = GetMeta().Allocate<NKikimrKqp::TKqpStreamLookupSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse stream lookup settings");
            // TODO: should we setup Database and PoolId for settings?
            transformSettings->ClearSnapshot();
            transformSettings->ClearLockTxId();
            transformSettings->ClearLockNodeId();
        } else if (settings.Is<NKikimrKqp::TKqpSequencerSettings>()) {
            auto* transformSettings = meta.SequencerSettings = GetMeta().Allocate<NKikimrKqp::TKqpSequencerSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse sequencer settings");
        } else if (settings.Is<NKikimrTxDataShard::TKqpVectorResolveSettings>()) {
            auto* transformSettings = meta.VectorResolveSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpVectorResolveSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse vector resolve settings");
            // TODO: should we setup Database and PoolId for settings?
            transformSettings->ClearSnapshot();
            transformSettings->ClearLockTxId();
            transformSettings->ClearLockNodeId();
        } else if (settings.Is<NKikimrTxDataShard::TKqpVectorSearchSettings>()) {
            auto* transformSettings = meta.VectorSearchSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpVectorSearchSettings>();
            YQL_ENSURE(settings.UnpackTo(transformSettings), "Failed to parse vector search settings");
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
        newTask.Meta.ExecuterId = GetMeta().ExecuterId;

        if (taskInfo.HasMetaId()) {
            newTask.SetMetaId(taskInfo.GetMetaId());
        }

        if (taskInfo.HasMeta()) {
            NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta meta;
            YQL_ENSURE(taskInfo.GetMeta().UnpackTo(&meta), "Failed to parse task meta");

            // TODO: this affects setting proper metadata for Scan tasks,
            //       but can't be replaced with `Type = Scan` because it will run the logic of binding task to node,
            //       which is not implemented for restored graph.
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
                            auto* sourceSettings = newInput.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
                            YQL_ENSURE(settings.UnpackTo(sourceSettings), "Failed to parse source settings");
                            // TODO: should we setup Database and PoolId for settings?
                            FillScanTaskLockTxId(*sourceSettings);
                            sourceSettings->ClearSnapshot();
                        } else if (settings.Is<NKikimrKqp::TKqpFullTextSourceSettings>()) {
                            auto* sourceSettings = newInput.Meta.FullTextSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpFullTextSourceSettings>();
                            // TODO: should we setup Database and PoolId for settings?
                            YQL_ENSURE(settings.UnpackTo(sourceSettings), "Failed to parse full text source settings");
                            sourceSettings->ClearSnapshot();
                        } else if (settings.Is<NKikimrKqp::TKqpSysViewSourceSettings>()) {
                            auto* sourceSettings = newInput.Meta.SysViewSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpSysViewSourceSettings>();
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
        BuildSinks(stage, stageInfo, newTask);

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

        for (ui64 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo({txIdx, stageIdx});

            if (const auto& sources = stage.GetSources(); !sources.empty() && sources[0].GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                RestoreReadTasksFromSource(stageInfo, resourcesSnapshot);
            }

            GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
        }
    }
}

THashMap<ui64, TVector<ui64>> TKqpTasksGraph::GroupStageTasksByNode(const TStageInfo& stageInfo) const {
    THashMap<ui64, TVector<ui64>> tasksByNode;
    for (ui64 taskId : stageInfo.Tasks) {
        const auto& task = GetTask(taskId);
        Y_ENSURE(task.Meta.ExpectedNodeId, "Task " << taskId << " has no ExpectedNodeId after placement");
        tasksByNode[*task.Meta.ExpectedNodeId].push_back(taskId);
    }
    return tasksByNode;
}

void TKqpTasksGraph::BuildSysViewScanTasks(TStageInfo& stageInfo) {
    Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.IsSysView());

    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    YQL_ENSURE(stageInfo.Tasks.size() == stage.TableOpsSize());

    size_t taskIdx = 0;
    for (const auto& op : stage.GetTableOps()) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

        auto& task = GetTask(stageInfo.Tasks[taskIdx++]);
        task.Reason = TTaskType::SYSVIEW_COMPUTE;
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
    }
}

std::pair<ui32, TKqpTasksGraph::TTaskType::ECreateReason> TKqpTasksGraph::GetMaxTasksAggregation(const TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount) {
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

void TKqpTasksGraph::BuildComputeTasks(TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
        const auto& input = stage.GetInputs(inputIndex);
        GetMeta().UnknownAffectedShardCount |= input.HasStreamLookup() || input.HasVectorResolve() || input.HasVectorSearch();
    }
}

std::pair<ui32, TKqpTasksGraph::TTaskType::ECreateReason> TKqpTasksGraph::GetScanTasksPerNode(const TStageInfo& stageInfo, const bool isOlapScan, const ui64 /* nodeId */, bool enableShuffleElimination) const {
    TTaskType::ECreateReason taskReason = TTaskType::UNKNOWN;
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
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

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

    Y_ENSURE(stage.TableOpsSize() == 1);

    const auto& op = stage.GetTableOps(0);
    Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

    auto columns = BuildKqpColumns(op, tableInfo);
    auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
    auto readSettings = ExtractReadSettings(op, stageInfo, TxAlloc->HolderFactory, TxAlloc->TypeEnv);

    if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
        stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(), op.GetReadRange().GetSkipNullKeys().end());
        YQL_ENSURE(!readSettings.IsReverse(), "Not supported for scan queries");
    }

    THashMap<ui64 /* nodeId */, std::vector<TShardInfoWithId>> nodeShards;
    for (auto&& [shardId, shardInfo]: partitions) {
        const ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
        nodeShards[nodeId].emplace_back(shardId, std::move(shardInfo));
    }

    if (stats) {
        for (const auto& [nodeId, shardsInfo] : nodeShards) {
            stats->AddNodeShardsCount(stageId.StageId, nodeId, shardsInfo.size());
        }
    }

    // Tasks were already created, placed by node and laid into stageInfo.Tasks; here we just enrich them.
    auto tasksByNode = GroupStageTasksByNode(stageInfo);

    if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && readSettings.IsSorted())) {
        if (stageInfo.Meta.IsOlap() && readSettings.IsSorted()) {
            // OLAP + sorted: one task per shard (FIXED in MaxTasksGraph)
            for (auto& [nodeId, shardsInfo] : nodeShards) {
                auto& nodeTasks = tasksByNode.at(nodeId);
                for (size_t i = 0; i < shardsInfo.size(); ++i) {
                    auto& task = GetTask(nodeTasks[i]);
                    task.Reason = TTaskType::OLAP_SORT_SCAN;
                    task.Meta.ScanTask = true;
                    MergeReadInfoToTaskMeta(task.Meta, shardsInfo[i].ShardId, shardsInfo[i].KeyReadRanges,
                        readSettings, columns, op, /*isPersistentScan*/ true);
                }
            }
        } else {
            // Non-OLAP or non-sorted: pre-computed task count per node, shards distributed round-robin
            for (auto& [nodeId, shardsInfo] : nodeShards) {
                auto& taskIds = tasksByNode.at(nodeId);
                const ui32 tasksPerNode = taskIds.size();

                for (ui64 taskId : taskIds) {
                    auto& task = GetTask(taskId);
                    task.Reason = TTaskType::DEFAULT_SHARD_SCAN;
                    task.Meta.ScanTask = true;
                }

                for (size_t si = 0; si < shardsInfo.size(); ++si) {
                    auto& task = GetTask(taskIds[si % tasksPerNode]);
                    MergeReadInfoToTaskMeta(task.Meta, shardsInfo[si].ShardId, shardsInfo[si].KeyReadRanges,
                        readSettings, columns, op, /*isPersistentScan*/ true);
                }

                for (const auto& taskId : taskIds) {
                    auto& task = GetTask(taskId);
                    task.Meta.SetEnableShardsSequentialScan(readSettings.IsSorted());
                    PrepareScanMetaForUsage(task.Meta, keyTypes);
                }
            }
        }
    } else if (shuffleEliminated /* save partitioning for shuffle elimination */) {
        std::size_t stageInternalTaskId = 0;
        columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>();
        columnShardHashV1Params.TaskIndexByHash->resize(columnShardHashV1Params.SourceShardCount);

        for (auto&& [nodeId, shardsInfo] : nodeShards) {
            auto& nodeTasks = tasksByNode.at(nodeId);
            const ui32 tasksPerNode = nodeTasks.size();

            std::vector<TTaskMeta> metas(tasksPerNode, TTaskMeta());
            {
                for (std::size_t si = 0; si < shardsInfo.size(); ++si) {
                    MergeReadInfoToTaskMeta(
                        metas[si % tasksPerNode],
                        shardsInfo[si].ShardId,
                        shardsInfo[si].KeyReadRanges,
                        readSettings,
                        columns, op,
                        /*isPersistentScan*/ true
                    );
                }

                for (auto& meta: metas) {
                    PrepareScanMetaForUsage(meta, keyTypes);
                }
            }

            // in runtime we calc hash, which will be in [0; shardcount]
            // so we merge two mappings: hash -> shardID and shardID -> channelID for runtime
            THashMap<ui64, ui64> hashByShardId;
            Y_ENSURE(stageInfo.Meta.ColumnTableInfoPtr != nullptr, "ColumnTableInfoPtr is nullptr, maybe information about shards haven't been delivered yet.");
            const auto& tableDesc = stageInfo.Meta.ColumnTableInfoPtr->Description;
            const auto& sharding = tableDesc.GetSharding();
            for (std::size_t si = 0; si < sharding.ColumnShardsSize(); ++si) {
                hashByShardId.insert({sharding.GetColumnShards(si), si});
            }

            for (ui32 t = 0; t < tasksPerNode; ++t, ++stageInternalTaskId) {
                auto& task = GetTask(nodeTasks[t]);
                task.Reason = TTaskType::SHUFFLE_ELIMINATE_SCAN;
                task.Meta = metas[t];
                task.Meta.SetEnableShardsSequentialScan(false);
                task.Meta.ExpectedNodeId = nodeId;
                task.Meta.ScanTask = true;
                task.SetMetaId(t);

                for (const auto& readInfo: *task.Meta.Reads) {
                    Y_ENSURE(hashByShardId.contains(readInfo.ShardId));
                    (*columnShardHashV1Params.TaskIndexByHash)[hashByShardId[readInfo.ShardId]] = stageInternalTaskId;
                }
            }
        }
    } else {
        ui32 metaId = 0;
        for (auto& [nodeId, shardsInfo] : nodeShards) {
            const ui32 metaGlueingId = ++metaId;
            TTaskMeta meta;
            {
                for (auto& shardInfo : shardsInfo) {
                    MergeReadInfoToTaskMeta(meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                        columns, op, /*isPersistentScan*/ true);
                }
                PrepareScanMetaForUsage(meta, keyTypes);
            }

            for (ui64 taskId : tasksByNode.at(nodeId)) {
                auto& task = GetTask(taskId);
                task.Reason = TTaskType::DEFAULT_SHARD_SCAN;
                task.Meta = meta;
                task.Meta.SetEnableShardsSequentialScan(false);
                task.Meta.ExpectedNodeId = nodeId;
                task.Meta.ScanTask = true;
                task.SetMetaId(metaGlueingId);
            }
        }
    }
}

void TKqpTasksGraph::BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

    auto sourceName = externalSource.GetSourceName();
    TString structuredToken;
    if (sourceName) {
        structuredToken = ReplaceStructuredTokenReferences(externalSource.GetAuthInfo());
    }

    // Find offset so that the first task lands on the executer node.
    ui64 nodeOffset = 0;
    for (size_t i = 0; i < resourceSnapshot.size(); ++i) {
        if (resourceSnapshot[i].GetNodeId() == GetMeta().ExecuterId.NodeId()) {
            nodeOffset = i;
            break;
        }
    }

    const ui32 taskCount = stageInfo.Tasks.size();

    TVector<ui64> tasksIds;
    tasksIds.reserve(taskCount);

    for (ui32 i = 0; i < taskCount; i++) {
        auto& task = GetTask(stageInfo.Tasks[i]);
        task.Reason = TTaskType::DEFAULT_SOURCE_READ;

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

        AddQueryPathParam(task, GetMeta().UserRequestContext);
        if (externalSource.GetType() == NYql::PqSource && i == 0) {   // Only first task will check partition count.
            task.Meta.TaskParams.emplace("partition_count_check_enabled", "true");
        }
        tasksIds.push_back(task.Id);
    }

    // distribute read ranges between tasks
    ui32 currentTaskIndex = 0;
    for (const TString& partitionParam : externalSource.GetPartitionedTaskParams()) {
        GetTask(tasksIds[currentTaskIndex]).Meta.ReadRanges.push_back(partitionParam);
        if (++currentTaskIndex >= tasksIds.size()) {
            currentTaskIndex = 0;
        }
    }
}

void TKqpTasksGraph::RestoreReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

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

    for (const auto taskId : stageInfo.Tasks) {
        FillReadTaskFromSource(GetTask(taskId), sourceName, structuredToken, resourceSnapshot, nodeOffset++);
    }
}

void TKqpTasksGraph::BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* /*unused*/) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    YQL_ENSURE(stage.GetSources(0).HasFullTextSource());

    const auto& source = stage.GetSources(0);
    const auto& fullTextSource = source.GetFullTextSource();

    YQL_ENSURE(fullTextSource.GetIndex());
    YQL_ENSURE(stageInfo.Tasks.size() == 1);

    auto& task = GetTask(stageInfo.Tasks[0]);
    task.Reason = TTaskType::DEFAULT_SOURCE_READ;
    task.Meta.ExpectedNodeId = GetMeta().ExecuterId.NodeId();
    const auto& stageSource = stage.GetSources(0);
    auto& input = task.Inputs.at(stageSource.GetInputIndex());
    input.SourceType = NYql::KqpFullTextSourceName;
    input.ConnectionInfo = NYql::NDq::TSourceInput{};

    input.Meta.FullTextSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpFullTextSourceSettings>();
    NKikimrKqp::TKqpFullTextSourceSettings* settings = input.Meta.FullTextSourceSettings;

    settings->SetIndex(fullTextSource.GetIndex());
    settings->SetDatabase(GetMeta().Database);

    const auto& poolId = GetMeta().UserRequestContext->PoolId;
    if (!poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID) {
        settings->SetPoolId(poolId);
    }

    settings->MutableTable()->CopyFrom(fullTextSource.GetTable());
    settings->SetIndexType(fullTextSource.GetIndexType());
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

    for (const auto& token : fullTextSource.GetQuerySettings().GetTokens()) {
        for (const auto& resolved : ResolveFullTextQueryTokenExpanded(token, stageInfo)) {
            settings->MutableQuerySettings()->AddTokens(resolved);
        }
    }

    // Resolve prefix column equality values (literal or $param) into serialized key cells.
    for (const auto& prefixColumn : fullTextSource.GetQuerySettings().GetPrefixColumns()) {
        auto value = ExtractPhyValue(
            stageInfo, prefixColumn.GetValue(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        auto* out = settings->MutableQuerySettings()->AddPrefixColumns();
        out->MutableColumn()->CopyFrom(prefixColumn.GetColumn());
        auto typeInfo = NScheme::TypeInfoFromProto(
            prefixColumn.GetColumn().GetTypeId(), prefixColumn.GetColumn().GetTypeInfo());
        TCell cell = NMiniKQL::MakeCell(typeInfo, value, TxAlloc->TypeEnv, /* copy */ true);
        out->SetValue(TSerializedCellVec::Serialize(TConstArrayRef<TCell>(&cell, 1)));
    }

    if (fullTextSource.HasTakeLimit()) {
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

    if (fullTextSource.HasUniqueIndexImplTable()) {
        const auto& uniqueIdx = fullTextSource.GetUniqueIndexImplTable();
        auto* uniqueProto = settings->MutableUniqueIndexImplTable();
        uniqueProto->MutableTable()->CopyFrom(uniqueIdx.GetTable());
        uniqueProto->MutableKeyColumns()->CopyFrom(uniqueIdx.GetKeyColumns());
        uniqueProto->MutableColumns()->CopyFrom(uniqueIdx.GetColumns());
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

    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    const auto& source = stage.GetSources(0);
    const auto& sysViewSource = source.GetSysViewSource();

    YQL_ENSURE(stageInfo.Tasks.size() == 1);

    auto& task = GetTask(stageInfo.Tasks[0]);
    task.Reason = TTaskType::DEFAULT_SOURCE_READ;
    task.Meta.ExpectedNodeId = GetMeta().ExecuterId.NodeId();

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
}

void TKqpTasksGraph::FillScanTaskLockTxId(NKikimrTxDataShard::TKqpReadRangesSourceSettings& settings) {
    if (const auto& lockTxId = GetMeta().LockTxId) {
        settings.SetLockTxId(*lockTxId);
        settings.SetLockNodeId(GetMeta().ExecuterId.NodeId());
    }
    const ui64 effectiveSpanId = GetMeta().GetEffectiveQuerySpanId(
        GetMeta().QuerySpanId, settings.GetTable().GetTablePath());
    if (effectiveSpanId) {
        settings.SetQuerySpanId(effectiveSpanId);
    }
}

TMaybe<size_t> TKqpTasksGraph::BuildScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    const bool singlePartitionedStage = stage.GetIsSinglePartition();

    // TODO: describe in comments the exact expected stage state.
    YQL_ENSURE(stage.GetSources(0).HasReadRangesSource());
    YQL_ENSURE(stage.GetSources(0).GetInputIndex() == 0 && stage.SourcesSize() == 1);
    for (const auto& input : stage.GetInputs()) {
        YQL_ENSURE(input.HasBroadcast());
    }

    const auto& source = stage.GetSources(0).GetReadRangesSource();
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    // TODO: what is the difference with `stageInfo.Meta.IsOlap()`?
    YQL_ENSURE(tableInfo->TableKind != NKikimr::NKqp::ETableKind::Olap);

    auto columns = BuildKqpColumns(source, tableInfo);
    const auto& snapshot = GetMeta().Snapshot;
    
    if (stageInfo.Meta.PrunedPartitions.empty()) {
        return Nothing();
    }

    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    const bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0
        && partitions.size() > source.GetSequentialInFlightShards();

    auto tasksByNode = GroupStageTasksByNode(stageInfo);
    THashMap<ui64, size_t> nodeCursor; // next task index per node within tasksByNode

    auto createNewTask = [&](ui64 nodeId, TMaybe<ui64> maxInFlightShards) -> TTask& {
        auto& task = GetTask(tasksByNode.at(nodeId)[nodeCursor[nodeId]++]);
        task.Reason = TTaskType::UNKNOWN;
        task.Meta.ExpectedNodeId = nodeId;

        const auto& stageSource = stage.GetSources(0);
        auto& input = task.Inputs.at(stageSource.GetInputIndex());
        input.SourceType = NYql::KqpReadRangesSourceName;
        input.ConnectionInfo = NYql::NDq::TSourceInput{};

        // allocating source settings

        input.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;
        settings->SetDatabase(GetMeta().Database);

        auto* meta = settings->MutableTable();
        meta->SetTablePath(stageInfo.Meta.TablePath);
        meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
        meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
        meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
        meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
        meta->SetTableKind((ui32)stageInfo.Meta.TableKind);

        const auto& poolId = GetMeta().UserRequestContext->PoolId;
        if (!poolId.empty() && poolId != NResourcePool::DEFAULT_POOL_ID) {
            settings->SetPoolId(poolId);
        }

        settings->SetIsTableImmutable(source.GetIsTableImmutable());
        settings->SetIsolationLevel(GetMeta().RequestIsolationLevel);

        for (const auto& keyColumn : keyTypes) {
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
            for (const auto& colName : tableInfo->KeyColumns) {
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

        if (GetMeta().RequestIsolationLevel == NKqpProto::ISOLATION_LEVEL_INCONSISTENT_ONLINE_RO) {
            settings->SetAllowInconsistentReads(true);
        }

        settings->SetReverse(source.GetReverse());
        settings->SetSorted(source.GetSorted());

        if (maxInFlightShards) {
            settings->SetMaxInFlightShards(*maxInFlightShards);
        }

        if (GetMeta().MaxBatchSize) {
            settings->SetItemsLimit(*GetMeta().MaxBatchSize);
            settings->SetIsBatch(true);
        } else {
            const auto guard = TxAlloc->TypeEnv.BindAllocator();
            ui64 itemsLimit = ExtractPhyValue(stageInfo, source.GetItemsLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod((ui32)0)).Get<ui64>();
            settings->SetItemsLimit(itemsLimit);
        }

        if (source.HasVectorTopK()) {
            const auto& in = source.GetVectorTopK();
            const auto guard = TxAlloc->TypeEnv.BindAllocator();
            // A parametric LIMIT can resolve to 0. The datashard rejects a VectorTopK with
            // limit 0, so skip the pushdown; the plan's own LIMIT still yields no rows.
            const ui64 limit = ExtractPhyValue(stageInfo, in.GetLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod()).Get<ui64>();
            if (limit) {
                auto& out = *settings->MutableVectorTopK();
                out.SetColumn(in.GetColumn());
                *out.MutableSettings() = in.GetSettings();
                auto target = ExtractPhyValue(stageInfo, in.GetTargetVector(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
                out.SetTargetVector(TString(target.AsStringRef()));
                out.SetLimit((ui32)limit);
            }
        }

        FillScanTaskLockTxId(*settings);

        if (GetMeta().LockMode) {
            settings->SetLockMode(*GetMeta().LockMode);
        }

        return task;
    };

    if (stats) {
        for (const auto& [shardId, _] : partitions) {
            stats->AffectedShards.insert(shardId);
        }
    }

    if (!partitions.empty() && (isSequentialInFlight || singlePartitionedStage)) {
        Y_ENSURE(stageInfo.Meta.VirtualPartition);

        auto startShard = stageInfo.Meta.VirtualPartition->ShardId;
        const auto& shardInfo = *stageInfo.Meta.VirtualPartition;

        TMaybe<ui64> inFlightShards = Nothing();
        if (isSequentialInFlight) {
            inFlightShards = source.GetSequentialInFlightShards();
        }

        Y_ENSURE(shardInfo.KeyReadRanges); // TODO: redundant check - remove later.

        const ui64 nodeId = singlePartitionedStage ? GetMeta().ExecuterId.NodeId() : GetMeta().ShardIdToNodeId.at(startShard);

        YQL_ENSURE(!shardInfo.KeyWriteRanges);
        YQL_ENSURE(stageInfo.Tasks.size() == 1);

        auto& task = createNewTask(nodeId, inFlightShards);
        auto& input = task.Inputs[stage.GetSources(0).GetInputIndex()];
        const bool hasRanges = shardInfo.KeyReadRanges->HasRanges();

        shardInfo.KeyReadRanges->SerializeTo(input.Meta.SourceSettings, !hasRanges);

        return singlePartitionedStage ? TMaybe<size_t>(partitions.size()) : Nothing();
    }

    struct TShardRangesWithShardId {
        TMaybe<ui64> ShardId;
        const TShardKeyRanges* Ranges;
    };
    using TShardRangesVector = TVector<TShardRangesWithShardId>;

    THashMap<ui64, TShardRangesVector> nodeIdToShardKeyRanges;
    for (const auto& [shardId, shardInfo] : partitions) {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        const ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
        nodeIdToShardKeyRanges[nodeId].push_back(TShardRangesWithShardId{shardId, &*shardInfo.KeyReadRanges});
    }

    auto DistributeShardsToTasks = [&](TShardRangesVector& shardsRanges, const size_t tasksCount, const TVector<NScheme::TTypeInfo>& keyTypes) {
        std::sort(std::begin(shardsRanges), std::end(shardsRanges), [&](const TShardRangesWithShardId& lhs, const TShardRangesWithShardId& rhs) {
                return CompareBorders<true, true>(
                    lhs.Ranges->GetRightBorder().first->GetCells(),
                    rhs.Ranges->GetRightBorder().first->GetCells(),
                    lhs.Ranges->GetRightBorder().second,
                    rhs.Ranges->GetRightBorder().second,
                    keyTypes) < 0;
            });

        // One shard (ranges set) can be assigned only to one task. Otherwise, we can break some optimizations like removing unnecessary shuffle.
        TVector<TShardRangesVector> result(tasksCount);
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
    };

    for (auto& [nodeId, shardsRanges] : nodeIdToShardKeyRanges) {
        const ui32 tasksCount = tasksByNode.at(nodeId).size();

        TVector<ui64> taskIds;
        taskIds.reserve(tasksCount);

        for (size_t i = 0; i < tasksCount; ++i) {
            const auto& task = createNewTask(nodeId, Nothing());
            taskIds.push_back(task.Id);
        }

        const auto rangesDistribution = DistributeShardsToTasks(shardsRanges, tasksCount, keyTypes);
        YQL_ENSURE(rangesDistribution.size() == tasksCount);

        for (size_t taskIndex = 0; taskIndex < tasksCount; ++taskIndex) {
            auto& task = GetTask(taskIds[taskIndex]);
            auto& input = task.Inputs[stage.GetSources(0).GetInputIndex()];
            auto* settings = input.Meta.SourceSettings;

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

    return partitions.size();
}

void TKqpTasksGraph::FillSecureParamsFromStage(THashMap<TString, TString>& secureParams, const NKqpProto::TKqpPhyStage& stage) const {
    for (const auto& [secretName, authInfo] : stage.GetSecureParams()) {
        const auto& structuredToken = NYql::CreateStructuredTokenParser(authInfo).ToBuilder().ReplaceReferences(GetMeta().SecureParams).ToJson();
        const auto& structuredTokenParser = NYql::CreateStructuredTokenParser(structuredToken);
        YQL_ENSURE(structuredTokenParser.HasIAMToken(), "only token authentication supported for compute tasks");
        secureParams.emplace(secretName, structuredTokenParser.GetIAMToken());
    }
}

bool TKqpTasksGraph::StageNeedsLocalPlacement(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo) const {
    for (const auto& transform : stage.GetOutputTransforms()) {
        if (transform.HasInternalSink()) {
            // BuildInternalOutputTransform always sets Type = KqpTableSinkName for these, unconditionally.
            return true;
        }
    }

    for (const auto& sink : stage.GetSinks()) {
        if (!sink.HasInternalSink()) {
            continue;
        }

        const auto& intSink = sink.GetInternalSink();
        if (intSink.GetType() != NYql::KqpTableSinkName) {
            continue;
        }
        if (!intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            continue;
        }

        NKikimrKqp::TKqpTableSinkSettings settings;
        if (!stageInfo.Meta.ResolvedSinkSettings) {
            YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
        } else {
            settings = *stageInfo.Meta.ResolvedSinkSettings;
        }

        // Mirrors FillKqpTableSinkSettings: the buffer actor is attached (and thus the local-node requirement) exactly
        // for consistent-tx, non-OLAP writes.
        if (!settings.GetInconsistentTx() && !settings.GetIsOlap()) {
            return true;
        }
    }

    return false;
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
    AddQueryPathParam(task, GetMeta().UserRequestContext);

    auto& output = task.Outputs[sink.GetOutputIndex()];
    output.Type = TTaskOutputType::Sink;
    output.SinkType = extSink.GetType();
    output.SinkSettings = extSink.GetSettings();
}

void TKqpTasksGraph::FillKqpTableSinkSettings(NKikimrKqp::TKqpTableSinkSettings& settings, const std::vector<std::pair<ui64, i64>>& internalSinksOrder, const TKqpTasksGraph::TTaskType& task) const {
    const auto& lockTxId = GetMeta().LockTxId;
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
        // otherwise fall back to global QuerySpanId; apply per-table suppression.
        {
            const ui64 rawSpanId = GetMeta().GetTxQuerySpanId(task.StageId.TxId);
            const ui64 effectiveSpanId = GetMeta().GetEffectiveQuerySpanId(
                rawSpanId, settings.GetTable().GetPath());
            if (effectiveSpanId) {
                settings.SetQuerySpanId(effectiveSpanId);
            }
        }

    const auto *sinkPosition = std::lower_bound(
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

        // Per-shard affinity for OLAP writes (EnableCsWriteAffinity).
        //
        // Populate TargetShardIds with the target table shards that belong to this
        // task. Two cases:
        //
        //  A. ShardIdToNodeId contains the target shards (e.g. when the target table's
        //     shards were resolved and added to the global map):
        //     CountComputeTasks() created one task per shard, pinned to the shard's node.
        //     Each task owns exactly one shard — the shard at the task's index among the
        //     resolved shards (those present in ShardIdToNodeId). We assign that single
        //     shard to TargetShardIds.
        //
        //  B. ShardIdToNodeId does NOT contain the target shards (typical for OLAP writes
        //     where the resolver does not add the write-target shards to the global map):
        //     CountComputeTasks() fell through to the standard 1-task path. All target
        //     shards go into TargetShardIds for that single task.
        //
        // When TargetShardIds is populated, the WriteActor discards rows destined for
        // shards not in the list (which are handled by other tasks in case A, or are an
        // error in case B — but case B uses all shards so nothing is discarded).
        // Assign TargetShardIds for OLAP write tasks:
        //  - Multi-task path (N tasks, one per shard): each task gets exactly 1 shard.
        //    Rows are routed to the right task via ColumnShardHashV1 HashShuffle.
        //  - Single-task path (pure stage or fallback): the one task gets ALL shards.
        //    The WriteActor handles all shards without per-shard filtering.
        YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks decision",
            {"stageId", stageInfo.Id}
            , {"isOlap", settings.GetIsOlap()}
            , {"enableCsWriteAffinity", stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()}
            , {"csShardingColumnsSize", stageInfo.Meta.CsShardingColumns.size()}
            , {"tasksCount", stageInfo.Tasks.size()}
            , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
            , {"hasShardKey", stageInfo.Meta.ShardKey != nullptr}
            , {"taskCsWriteAffinityShardId", task.Meta.TaskParams.contains("CsWriteAffinityShardId") ? task.Meta.TaskParams.at("CsWriteAffinityShardId") : "N/A"});

        if (settings.GetIsOlap()) {
            // Collect all target shards from ColumnTableInfo (for OLAP) or ShardKey (for DataShards).
            TVector<ui64> resolvedShardIds;
            if (stageInfo.Meta.ColumnTableInfoPtr
                    && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
                const auto& sharding = stageInfo.Meta.ColumnTableInfoPtr->Description.GetSharding();
                for (const auto& shardId : sharding.GetColumnShards()) {
                    resolvedShardIds.push_back(shardId);
                }
                YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks using ColumnTableInfo",
                    {"stageId", stageInfo.Id}
                    , {"resolvedShardIdsCount", resolvedShardIds.size()});
            } else if (stageInfo.Meta.ShardKey) {
                // Fallback: use ShardKey partitions (for data shards)
                for (const auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    resolvedShardIds.push_back(partition.ShardId);
                }
                YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks using ShardKey",
                    {"stageId", stageInfo.Id}
                    , {"resolvedShardIdsCount", resolvedShardIds.size()});
            }

            if (!resolvedShardIds.empty()) {
                if (stageInfo.Tasks.size() > 1) {
                    YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks multi-task path",
                        {"stageId", stageInfo.Id}
                        , {"tasksCount", stageInfo.Tasks.size()});
                    // Multi-task per-shard path: assign exactly 1 shard per task.
                    // Read the shard ID stored on the task by CountComputeTasks.
                    // This is necessary because PlaceTasks reorders tasks by node,
                    // breaking the creation order assumption.
                    auto it = task.Meta.TaskParams.find("CsWriteAffinityShardId");
                    if (it != task.Meta.TaskParams.end()) {
                        ui64 shardId = 0;
                        try {
                            shardId = std::stoull(it->second);
                        } catch (...) {
                            AFL_ENSURE(false)("shardIdStr", it->second)("msg", "Failed to parse CsWriteAffinityShardId");
                        }
                        if (shardId && std::find(resolvedShardIds.begin(), resolvedShardIds.end(), shardId) != resolvedShardIds.end()) {
                            settings.AddTargetShardIds(shardId);
                        } else {
                            AFL_ENSURE(false)
                                ("shardId", shardId)
                                ("msg", "CsWriteAffinityShardId from task params not found in resolvedShardIds");
                        }
                    } else {
                        // Fallback: task doesn't have CsWriteAffinityShardId (shouldn't happen for affinity tasks).
                        // Try to find by node assignment.
                        const ui64 taskNodeId = task.Meta.ExpectedNodeId.value_or(GetMeta().ExecuterId.NodeId());
                        for (const auto& shardId : resolvedShardIds) {
                            auto sit = GetMeta().ShardIdToNodeId.find(shardId);
                            if (sit != GetMeta().ShardIdToNodeId.end() && sit->second == taskNodeId) {
                                settings.AddTargetShardIds(shardId);
                            }
                        }
                    }
                } else {
                    // Single-task path: assign all shards to the single task.
                    // This is used when CS Write Affinity is enabled but per-shard tasks
                    // were not created (e.g., ColumnTableInfoPtr is null).
                    // The runtime will handle all shards in this single task.
                    YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks single-task path",
                        {"stageId", stageInfo.Id}
                        , {"resolvedShardIdsCount", resolvedShardIds.size()});
                    for (const auto& shardId : resolvedShardIds) {
                        settings.AddTargetShardIds(shardId);
                    }
                }
            }
        } else {
            // Log when we skip the OLAP affinity path entirely.
            YDB_LOG_WARN("CS Write Affinity: BuildInternalSinks skipping OLAP affinity path",
                {"stageId", stageInfo.Id}
                , {"isOlap", settings.GetIsOlap()}
                , {"enableCsWriteAffinity", stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()}
                , {"msg", "TargetShardIds will remain empty!"});
        }
        // sink stages with enableCsWriteAffinity.

        // Final diagnostic: check if TargetShardIds is populated for OLAP with affinity.
        if (settings.GetIsOlap() && stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()) {
            YDB_LOG_INFO("CS Write Affinity: BuildInternalSinks final TargetShardIds",
                {"stageId", stageInfo.Id}
                , {"targetShardIdsSize", settings.TargetShardIdsSize()}
                , {"tasksCount", stageInfo.Tasks.size()});
            if (settings.TargetShardIdsSize() == 0) {
                YDB_LOG_WARN("CS Write Affinity: TargetShardIds is EMPTY for OLAP task!",
                    {"stageId", stageInfo.Id}
                    , {"tasksCount", stageInfo.Tasks.size()}
                    , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
                    , {"hasShardKey", stageInfo.Meta.ShardKey != nullptr}
                    , {"msg", "This will cause AFL_VERIFY failure in TColumnShardPayloadSerializer!"});
            }
        }

#ifdef QP_FORCE_CS_WRITE_AFFINITY
        if (settings.GetIsOlap()) {
            // Invariant: with the force flag, TargetShardIds must always be populated.
            AFL_VERIFY(settings.TargetShardIdsSize() > 0)
                ("stageId", stageInfo.Id)
                ("tasksCount", stageInfo.Tasks.size())
                ("msg", "QP_FORCE_CS_WRITE_AFFINITY requires TargetShardIds for OLAP task");

            if (stageInfo.Tasks.size() > 1) {
                // Multi-task per-shard invariant: each task must have exactly 1 shard,
                // and ALL input channels must use ColumnShardHashV1 routing.
                AFL_VERIFY(settings.TargetShardIdsSize() == 1)
                    ("stageId", stageInfo.Id)
                    ("targetShardIdsSize", settings.TargetShardIdsSize())
                    ("msg", "QP_FORCE_CS_WRITE_AFFINITY: multi-task OLAP task must have exactly 1 TargetShardId");

                // NOTE: Input channel verification (ColumnShardHashV1 / Broadcast) is done
                // AFTER BuildKqpStageChannels in BuildAllTasks, because channels are built
                // after BuildInternalSinks runs. See the QP_FORCE_CS_WRITE_AFFINITY check
                // following BuildKqpStageChannels.
            }
        }
#endif

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

    TTransform outputTransform;
    outputTransform.Type = NYql::KqpTableSinkName;
    outputTransform.InputType = transform.GetInputType();
    outputTransform.OutputType = transform.GetOutputType();
    outputTransform.Settings.PackFrom(settings);

    output.Transform = std::move(outputTransform);
}

void TKqpTasksGraph::BuildSinks(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) const {
    const auto internalSinksOrder = BuildInternalSinksPriorityOrder();

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
    const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats,
    const TPlacementParams& placementParams)
{
    // Counting tasks via MaxTasksGraph

    if (!resourcesSnapshot.empty()) {
        MaxTasksGraph->AddNodes(resourcesSnapshot);
    }

    // TODO: remove this part later. The nodes from snapshot should be sufficient.
    //       Right now the snapshot is not sufficient on cluster start.
    for (const auto [_, node] : GetMeta().ShardIdToNodeId) {
        MaxTasksGraph->AddNode(node);
    }

    // TODO: we always have at least current node.
    MaxTasksGraph->AddNode(GetMeta().ExecuterId.NodeId());

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot); // TODO: move inside ReadFromSource()
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            // TODO: move this check to FillStages() - after all necessary params are set in KqpTasksGraph ctor.

            // Check which type of tasks to build later
            const bool maybeOlapRead = (GetMeta().AllowOlapDataQuery || GetMeta().StreamResult) && stageInfo.Meta.IsOlap();

            const bool buildFromSourceTasks = stage.SourcesSize() > 0;
            const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
            const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!GetMeta().IsScan && stage.SinksSize() + stage.OutputTransformsSize() > 0 && !(maybeOlapRead && stageInfo.Meta.HasReads()));
            const bool buildScanTasks = GetMeta().IsScan
                ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                : maybeOlapRead && (stage.SinksSize() + stage.OutputTransformsSize() == 0 || stageInfo.Meta.HasReads())
                ;

            if (buildFromSourceTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SOURCE_TASKS;
            } else if (buildSysViewTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SYSVIEW_TASKS;
            } else if (buildComputeTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::COMPUTE_TASKS;
            } else if (buildScanTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SCAN_TASKS;
            }

            switch(stageInfo.Meta.TasksType) {
                case TStageInfoMeta::SOURCE_TASKS: {
                    switch (stage.GetSources(0).GetTypeCase()) {
                        case NKqpProto::TKqpSource::kReadRangesSource: {
                            bool limitTasksPerNode = IsEnabledReadsMerge();
                            if (!GetMeta().IsScan) {
                                limitTasksPerNode |= GetMeta().StreamResult;
                            }
                            CountScanTasksFromSource(stageInfo, limitTasksPerNode);
                        } break;
                        case NKqpProto::TKqpSource::kFullTextSource: {
                            CountFullTextScanTasksFromSource(stageInfo);
                        } break;
                        case NKqpProto::TKqpSource::kSysViewSource: {
                            CountSysViewTasksFromSource(stageInfo);
                        } break;
                        case NKqpProto::TKqpSource::kExternalSource: {
                            YQL_ENSURE(!GetMeta().IsScan);
                            auto it = scheduledTaskCount.find(stageIdx);
                            CountReadTasksFromSource(stageInfo, resourcesSnapshot.size(), it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
                        } break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } break;
                case TStageInfoMeta::SYSVIEW_TASKS: {
                    CountSysViewScanTasks(stageInfo); // TODO: remove after switching to SysView source
                } break;
                case TStageInfoMeta::COMPUTE_TASKS: {
                    // TODO: is it possible for "shards on node" to be greater than "resources snapshot"?
                    auto nodesCount = GetMeta().ShardsOnNode.size();
                    if (!GetMeta().IsScan) {
                        nodesCount = std::max<ui32>(resourcesSnapshot.size(), nodesCount);
                    }
                    CountComputeTasks(stageInfo, nodesCount);
                } break;
                case TStageInfoMeta::SCAN_TASKS: {
                    CountScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination());
                } break;
                case TStageInfoMeta::UNKNOWN_TASKS: {
                    AFL_ENSURE(false);
                } break;
            }
        }
    }

    if (UseKqpTasksGraphV2) {
        MaxTasksGraph->EstimateTasksResources();
        MaxTasksGraph->DistributeTasksToNodes(placementParams);
        MaxTasksGraph->Shrink();
    }

    MaxTasksGraph->PlaceTasks(*this);

    size_t sourceScanPartitionsCount = 0;

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            switch(stageInfo.Meta.TasksType) {
                case TStageInfoMeta::SOURCE_TASKS: {
                    switch (stage.GetSources(0).GetTypeCase()) {
                        case NKqpProto::TKqpSource::kReadRangesSource: {
                            if (auto partitionsCount = BuildScanTasksFromSource(stageInfo, stats)) {
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
                            BuildReadTasksFromSource(stageInfo, resourcesSnapshot);
                        } break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } break;
                case TStageInfoMeta::SYSVIEW_TASKS: {
                    BuildSysViewScanTasks(stageInfo); // TODO: remove after switching to SysView source
                } break;
                case TStageInfoMeta::COMPUTE_TASKS: {
                    BuildComputeTasks(stageInfo);
                } break;
                case TStageInfoMeta::SCAN_TASKS: {
                    BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), CollectProfileStats(GetMeta().StatsMode) ? stats : nullptr);
                } break;
                case TStageInfoMeta::UNKNOWN_TASKS: {
                    AFL_ENSURE(false);
                } break;
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
                BuildSinks(stage, stageInfo, task);
            }

            BuildKqpStageChannels(stageInfo, GetMeta().TxId, GetMeta().AllowWithSpilling, tx.Body->EnableShuffleElimination());

#ifdef QP_FORCE_CS_WRITE_AFFINITY
            // Invariant: with the force flag, a multi-task OLAP sink stage must have
            // input channels that are either ColumnShardHashV1 (per-shard routing) or
            // Broadcast (pure OLAP VALUES: all rows to every task, filtered by TargetShardIds).
            // This check runs AFTER BuildKqpStageChannels, when channels are populated.
            if (stageInfo.Tasks.size() > 1) {
                bool isOlapSink = false;
                for (const auto& sink : stage.GetSinks()) {
                    if (sink.HasInternalSink()
                            && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                        NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                        if (sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings)
                                && sinkSettings.GetIsOlap()) {
                            isOlapSink = true;
                            break;
                        }
                    }
                }
                if (isOlapSink) {
                    for (const auto& taskId : stageInfo.Tasks) {
                        const auto& task = GetTask(taskId);
                        bool hasNonHashShuffleInput = false;
                        bool hasAnyChannel = false;
                        TString nonHashShuffleInfo;
                        TString outputTypeInfo;
                        for (const auto& input : task.Inputs) {
                            for (const auto& channelId : input.Channels) {
                                hasAnyChannel = true;
                                const auto& channel = GetChannel(channelId);
                                const auto& srcTask = GetTask(channel.SrcTask);
                                const auto& srcOutput = srcTask.Outputs[channel.SrcOutputIndex];
                                outputTypeInfo = TStringBuilder()
                                    << "outputType=" << srcOutput.Type
                                    << " hashKind=" << (srcOutput.HashKind.has_value()
                                        ? ToString((int)*srcOutput.HashKind) : "none")
                                    << " partitionsCount=" << srcOutput.PartitionsCount;
                                if (srcOutput.Type == TTaskOutputType::Broadcast) {
                                    // Broadcast is valid for pure OLAP + affinity.
                                    continue;
                                }
                                if (srcOutput.Type != TTaskOutputType::HashPartition
                                        || srcOutput.HashKind != EHashShuffleFuncType::ColumnShardHashV1) {
                                    hasNonHashShuffleInput = true;
                                    nonHashShuffleInfo = TStringBuilder()
                                        << "srcTask=" << channel.SrcTask
                                        << " channelId=" << channelId
                                        << " outputType=" << srcOutput.Type
                                        << " hashKind=" << (srcOutput.HashKind.has_value()
                                            ? ToString((int)*srcOutput.HashKind) : "none");
                                    break;
                                }
                            }
                            if (hasNonHashShuffleInput) {
                                break;
                            }
                        }
                        AFL_VERIFY(hasAnyChannel && !hasNonHashShuffleInput)
                            ("stageId", stageInfo.Id)
                            ("taskId", taskId)
                            ("hasAnyChannel", hasAnyChannel)
                            ("outputTypeInfo", outputTypeInfo)
                            ("nonHashShuffleInfo", nonHashShuffleInfo)
                            ("inputsCount", task.Inputs.size())
                            ("msg", "QP_FORCE_CS_WRITE_AFFINITY: multi-task OLAP sink input channels"
                                    " are not ColumnShardHashV1 or Broadcast — per-shard routing violated");
                    }
                }
            }
#endif
        }

        GetMeta().DqChannelVersion = tx.Body->DqChannelVersion();
        BuildResultChannels(tx.Body, txIdx);
    }

    return sourceScanPartitionsCount;
}

void TKqpTasksGraph::BuildLiteralTasks() {
    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);

        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stageInfo = GetStageInfo(TStageId(txIdx, stageIdx));

            YQL_ENSURE(stageInfo.Meta.ShardOperations.empty());
            YQL_ENSURE(stageInfo.InputsCount == 0);

            AddTask(stageInfo, TKqpTasksGraph::TTaskType::LITERAL);
        }

        BuildResultChannels(tx.Body, txIdx);
    }
}

TKqpTasksGraph::~TKqpTasksGraph() = default;

TKqpTasksGraph::TKqpTasksGraph(
    const TString& database,
    const TVector<IKqpGateway::TPhysicalTxData>& transactions,
    const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
    const NKikimrConfig::TTableServiceConfig::TResourceManager& resourceManagerConfig,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
    const TKqpRequestCounters::TPtr& counters,
    TActorId bufferActorId,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    bool useKqpTasksGraphV2)
    : Transactions(transactions)
    , TxAlloc(txAlloc)
    , AggregationSettings(aggregationSettings)
    , Counters(counters)
    , BufferActorId(bufferActorId)
    , UserToken(std::move(userToken))
    , MaxTasksGraph(std::make_unique<TMaxTasksGraph>(resourceManagerConfig.GetMaxChannelCountPerNode(),
          TTaskResourceEstimationParams{
              .ChannelBufferSize = resourceManagerConfig.GetChannelBufferSize(),
              .MinChannelBufferSize = resourceManagerConfig.GetMinChannelBufferSize(),
              .MaxTotalChannelBuffersSize = resourceManagerConfig.GetMaxTotalChannelBuffersSize(),
              .MkqlHeavyProgramMemoryLimit = resourceManagerConfig.GetMkqlHeavyProgramMemoryLimit(),
              .MkqlLightProgramMemoryLimit = resourceManagerConfig.GetMkqlLightProgramMemoryLimit(),
          }))
    , UseKqpTasksGraphV2(useKqpTasksGraphV2)
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
        for (const auto& stage : tx.Body->GetStages()) {
            GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
        }

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

    FillStages();
}

std::vector<std::pair<ui64, i64>> TKqpTasksGraph::BuildInternalSinksPriorityOrder() const {
    std::vector<std::pair<ui64, i64>> order;
    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            const auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

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
    dump << MaxTasksGraph->DumpToString();

    return dump.Str();
}

void TKqpTasksGraph::CountScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }

    const auto stageType = stage.GetTaskCount() ? TMaxTasksGraph::FIXED : TMaxTasksGraph::ANY;
    MaxTasksGraph->AddStage(stageInfo, stageType, inputs);

    if (stageInfo.Meta.PrunedPartitions.empty() || stageInfo.Meta.PrunedPartitions.at(0).empty()) {
        return;
    }
    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    const auto& source = stage.GetSources(0).GetReadRangesSource();
    bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();
    bool singlePartitionedStage = stage.GetIsSinglePartition();

    if (isSequentialInFlight || singlePartitionedStage) {
        ui64 nodeId = 0;
        if (singlePartitionedStage) {
            nodeId = GetMeta().ExecuterId.NodeId();
        } else {
            Y_ENSURE(stageInfo.Meta.VirtualPartition);
            nodeId = GetMeta().ShardIdToNodeId.at(stageInfo.Meta.VirtualPartition->ShardId);
        }

        MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::SINGLE_SOURCE_SCAN), nodeId);

        return;
    }

    THashMap<ui64, ui64> tasksPerNode;
    THashMap<ui64, std::pair<ui64, TTask::ECreateReason>> maxTasksPerNode;

    for (const auto& [shardId, shardInfo] : partitions) {
        ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
        auto& nodeTasks = tasksPerNode[nodeId];

        if (limitTasksPerNode) {
            auto maxTasks = maxTasksPerNode.find(nodeId);
            if (maxTasks == maxTasksPerNode.end()) {
                maxTasks = maxTasksPerNode.emplace(nodeId, GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, nodeId)).first;
            }

            if (nodeTasks < maxTasks->second.first) {
                ++nodeTasks;
            }
        } else {
            // TODO: put the reason for task creation here.
            ++nodeTasks;
        }
    }

    for (const auto [nodeId, tasks] : tasksPerNode) {
        for (ui64 i = 0; i < tasks; ++i) {
            MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::DEFAULT_SOURCE_SCAN), nodeId);
        }
    }
}

void TKqpTasksGraph::CountFullTextScanTasksFromSource(TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }
    MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::ANY, inputs);

    MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::MINIMUM_SCAN), GetMeta().ExecuterId.NodeId());
}

void TKqpTasksGraph::CountSysViewTasksFromSource(TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }
    MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::ANY, inputs);

    MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::SYSVIEW_COMPUTE), GetMeta().ExecuterId.NodeId());
}

void TKqpTasksGraph::CountReadTasksFromSource(TStageInfo& stageInfo, size_t resourceSnapshotSize, ui32 scheduledTaskCount) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    const auto& externalSource = stage.GetSources(0).GetExternalSource();

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }
    MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::ANY, inputs);

    ui32 taskCountHint = stage.GetTaskCount();
    if (!taskCountHint) {
        taskCountHint = scheduledTaskCount;
    }

    ui32 taskCount = externalSource.GetPartitionedTaskParams().size();
    if (taskCountHint) {
        taskCount = std::min<ui32>(taskCount, taskCountHint);
    } else if (resourceSnapshotSize) {
        taskCount = std::min<ui32>(taskCount, resourceSnapshotSize * 2);
    }

    for (ui32 i = 0; i < taskCount; ++i) {
        MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::DEFAULT_SOURCE_READ), std::nullopt);
    }
}

void TKqpTasksGraph::CountSysViewScanTasks(TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }
    MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);

    for (int i = 0; i < stage.GetTableOps().size(); ++i) {
        MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), std::nullopt);
    }
}

void TKqpTasksGraph::CountComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    ui32 partitionsCount = 1;
    ui32 inputTasks = 0;
    bool isShuffle = false;
    bool forceMapTasks = false;
    ui32 mapConnectionCount = 0;

    std::list<TStageId> inputs;
    std::optional<TStageId> copyInput;
    TMaxTasksGraph::EStageType stageType = TMaxTasksGraph::ANY;
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

        const auto& inputStageId = NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex());
        inputs.push_back(inputStageId);

        auto inputTypeCase = input.GetTypeCase();
        if (inputTypeCase == NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup) {
            auto& dqSourceStreamLookup = input.GetDqSourceStreamLookup();
            switch (dqSourceStreamLookup.GetShuffleMode()) {
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_DEFAULT:
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_OFF:
                    inputTypeCase = NKqpProto::TKqpPhyConnection::kUnionAll;
                    break;
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_MAP:
                    inputTypeCase = NKqpProto::TKqpPhyConnection::kMap;
                    break;
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_HASH:
                    inputTypeCase = NKqpProto::TKqpPhyConnection::kHashShuffle;
                    break;
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_TKqpPhyCnDqSourceStreamLookup_EShuffleMode_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKqpProto::TKqpPhyCnDqSourceStreamLookup_EShuffleMode_TKqpPhyCnDqSourceStreamLookup_EShuffleMode_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ENSURE(false, "Impossible");
                    break;
            }
        }
        switch (inputTypeCase) {
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                inputTasks += MaxTasksGraph->GetStageTasksCount(inputStageId);
                isShuffle = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph->GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph->GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                forceMapTasks = true;
                ++mapConnectionCount;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                partitionsCount = std::max<ui64>(partitionsCount, MaxTasksGraph->GetStageTasksCount(inputStageId));
                break;
            }
            case NKqpProto::TKqpPhyConnection::kVectorResolve:
            case NKqpProto::TKqpPhyConnection::kSequencer: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph->GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                break;
            }
            default:
                break;
        }
    }

    Y_ENSURE(mapConnectionCount <= 1, "Only a single map connection is allowed");

    if (isShuffle && !forceMapTasks) {
        if (stage.GetTaskCount()) {
            stageType = TMaxTasksGraph::FIXED;
            partitionsCount = stage.GetTaskCount(); // TODO: is it allowed to have zero forced tasks?
        } else {
            auto [newPartitionCount, _] = GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount);
            partitionsCount = std::max(newPartitionCount, partitionsCount);
        }
    }

    // Per-Shard OLAP Write: if this stage is an OLAP write sink
    // (INSERT, FILL, etc.), create one task per target shard,
    // each pinned to the node that hosts that shard. The data arrives from the Transform
    // Stage via TDqCnBroadcast (all rows to all tasks); each task filters to its own
    // shard using TargetShardIds (a single shard) in TShardedWriteController::FlushSerializer.
    //
    // Conditions:
    //  - IsOlap sink
    //  - ShardKey resolved (table resolver has run before BuildAllTasks)
    //  - ShardIdToNodeId populated with target table's shards (from ResolveShards)
    //
    // NOTE: ShardIdToNodeId may only contain source table shards, not target table
    //       shards. When the mapping is unavailable, fall through to the standard
    //       single-task path (correctness preserved, node affinity benefit deferred).
    {
        bool isCsWriteAffinitySink = false;
        bool isModeFill = false;
        // Check for OLAP sink. Per-shard tasks are created for each shard of the
        // target table, pinned to the node hosting that shard.
        //
        // NOTE: For CTAS, the IsOlap flag is set by the table resolver into
        // ResolvedSinkSettings (not the raw proto). The raw sink settings proto does
        // NOT have IsOlap set for CTAS. So we must check ResolvedSinkSettings first,
        // falling back to the raw proto only if ResolvedSinkSettings is unavailable.
        if (stageInfo.Meta.ResolvedSinkSettings
                && stageInfo.Meta.ResolvedSinkSettings->GetIsOlap()) {
            isCsWriteAffinitySink = true;
            isModeFill = stageInfo.Meta.ResolvedSinkSettings->GetType()
                == NKikimrKqp::TKqpTableSinkSettings::MODE_FILL;
        } else {
            for (const auto& sink : stage.GetSinks()) {
                if (sink.HasInternalSink()
                        && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                    NKikimrKqp::TKqpTableSinkSettings sinkSettings;
                    if (sink.GetInternalSink().GetSettings().UnpackTo(&sinkSettings)
                            && sinkSettings.GetIsOlap()) {
                        isCsWriteAffinitySink = true;
                        isModeFill = sinkSettings.GetType()
                            == NKikimrKqp::TKqpTableSinkSettings::MODE_FILL;
                    }
                }
            }
        }

        // NOTE: Per-shard task creation is always enabled for OLAP sinks (regardless of
        // EnableCsWriteAffinity or isModeFill). CTAS uses Broadcast routing: all rows
        // are sent to all tasks, each task filters to its own shard via TargetShardIds.
        // ColumnShardHashV1 routing (affinity optimization) is only used when
        // EnableCsWriteAffinity=true AND isModeFill=false.

        YDB_LOG_INFO("CS Write Affinity: CountComputeTasks decision",
            {"stageId", stageInfo.Id}
            , {"isCsWriteAffinitySink", isCsWriteAffinitySink}
            , {"isModeFill", isModeFill}
            , {"enableCsWriteAffinity", stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()}
            , {"csShardingColumnsSize", stageInfo.Meta.CsShardingColumns.size()}
            , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
            , {"hasShardKey", stageInfo.Meta.ShardKey != nullptr}
            , {"hasResolvedSinkSettings", stageInfo.Meta.ResolvedSinkSettings.has_value()});

        if (isCsWriteAffinitySink) {
            // Build a list of (shardId, nodeId) for shards. One task is created per shard.
            // If nodeId is known (in ShardIdToNodeId), task is pinned to that node.
            // Otherwise, task is pinned to the executer node (no affinity benefit, but per-shard routing works).
            TVector<std::pair<ui64 /* shardId */, ui64 /* nodeId */>> shardNodes;
            const ui64 defaultNodeId = GetMeta().ExecuterId.NodeId();

            // For OLAP, use ColumnTableInfo to get column shard IDs
            if (stageInfo.Meta.ColumnTableInfoPtr
                    && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
                const auto& sharding = stageInfo.Meta.ColumnTableInfoPtr->Description.GetSharding();
                for (const auto& shardId : sharding.GetColumnShards()) {
                    auto it = GetMeta().ShardIdToNodeId.find(shardId);
                    ui64 nodeId = (it != GetMeta().ShardIdToNodeId.end()) ? it->second : defaultNodeId;
                    shardNodes.emplace_back(shardId, nodeId);
                }
                YDB_LOG_INFO("CS Write Affinity: Using ColumnTableInfo for shards",
                    {"stageId", stageInfo.Id}
                    , {"shardNodesCount", shardNodes.size()});
            } else if (stageInfo.Meta.ShardKey) {
                // Fallback: use ShardKey partitions (for data shards)
                for (const auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    const ui64 shardId = partition.ShardId;
                    auto it = GetMeta().ShardIdToNodeId.find(shardId);
                    ui64 nodeId = (it != GetMeta().ShardIdToNodeId.end()) ? it->second : defaultNodeId;
                    shardNodes.emplace_back(shardId, nodeId);
                }
                YDB_LOG_INFO("CS Write Affinity: Using ShardKey for shards",
                    {"stageId", stageInfo.Id}
                    , {"shardNodesCount", shardNodes.size()});
            } else {
                YDB_LOG_INFO("CS Write Affinity: No shard source available",
                    {"stageId", stageInfo.Id}
                    , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
                    , {"hasShardKey", stageInfo.Meta.ShardKey != nullptr});
            }

            if (!shardNodes.empty()) {
                YDB_LOG_INFO("CS Write Affinity: Creating per-shard tasks",
                    {"stageId", stageInfo.Id}
                    , {"shardNodesCount", shardNodes.size()});

                // FIXED: task count is determined here, independent of the upstream stage.
                // One task per shard, pinned to the node hosting that shard.
                MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);
                for (const auto& [shardId, nodeId] : shardNodes) {
                    auto& task = AddTask(stageInfo, TTask::UNKNOWN);
                    // Store the shard ID on the task so BuildInternalSinks can find it
                    // after PlaceTasks reorders tasks by node (breaking creation order).
                    task.Meta.TaskParams["CsWriteAffinityShardId"] = ToString(shardId);
                    MaxTasksGraph->AddTask(task, nodeId);
                }

#ifdef QP_FORCE_CS_WRITE_AFFINITY
                // Invariant: with the force flag, per-shard tasks must be created
                // (one task per shard).
                AFL_VERIFY(stageInfo.Tasks.size() == shardNodes.size())
                    ("stageId", stageInfo.Id)
                    ("tasksCount", stageInfo.Tasks.size())
                    ("shardNodesCount", shardNodes.size())
                    ("msg", "QP_FORCE_CS_WRITE_AFFINITY requires one task per shard");
#endif

                return; // Early-return: per-shard CTAS affinity path handled.
            }
            // Target table shards not in ShardIdToNodeId (e.g. OLAP CTAS where resolver
            // doesn't add target shards to the global map). Fall through to single-task
            // standard path: all shards handled by one task on the executer node.
            // TargetShardIds in BuildInternalSinks will be populated with all shards.
            YDB_LOG_WARN("CS Write Affinity: CountComputeTasks falling through to single-task path",
                {"stageId", stageInfo.Id}
                , {"shardNodesEmpty", shardNodes.empty()}
                , {"hasColumnTableInfo", stageInfo.Meta.ColumnTableInfoPtr != nullptr}
                , {"hasShardKey", stageInfo.Meta.ShardKey != nullptr}
                , {"isCsWriteAffinitySink", isCsWriteAffinitySink}
                , {"enableCsWriteAffinity", stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()}
                , {"msg", "This will cause TargetShardIds to be empty in runtime!"});
        }
    }

    // Tasks writing through the shared per-query buffer actor must run on the executer's own node (see
    // StageNeedsLocalPlacement, mirrors the old NeedToRunLocally). A write stage is not expected to be COPY, but if it
    // is, AddTask propagates this pin to the group's shared column (root) so the whole column stays co-located and local.
    std::optional<ui64> pinnedNode;
    if (StageNeedsLocalPlacement(stage, stageInfo)) {
        pinnedNode = GetMeta().ExecuterId.NodeId();
    }

    MaxTasksGraph->AddStage(stageInfo, stageType, inputs, copyInput);
    if (partitionsCount) {
        // It's possible to have zero partitions in case we COPY from input stage, which is empty because of non-intersecting param values:
        // i.e. "WHERE a > $1 AND a < $2", where $1 = $2 = 10
        for (ui32 i = 0; i < partitionsCount; ++i) {
            MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), pinnedNode);
        }
    } else {
        YQL_ENSURE(stageType == TMaxTasksGraph::COPY);
    }
}

void TKqpTasksGraph::CountScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    bool shuffleEliminated = enableShuffleElimination && stage.GetIsShuffleEliminated();

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.emplace_back(stageId.TxId, input.GetStageIndex());
    }

    THashMap<ui64 /* nodeId */, ui64> nodeShards;

    // TODO: clean-up all relevant code, where it's still expected to have multiple TableOps.
    Y_ENSURE(stage.TableOpsSize() == 1);

    const auto& op = stage.GetTableOps(0);
    const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
    const bool isSorted = ExtractReadSettings(op, stageInfo, TxAlloc->HolderFactory, TxAlloc->TypeEnv).IsSorted();

    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    for (const auto& [shardId, _]: partitions) {
        ++nodeShards[GetMeta().ShardIdToNodeId.at(shardId)];
    }

    if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && isSorted)) {
        if (stageInfo.Meta.IsOlap() && isSorted) {
            MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);
            for (const auto& [nodeId, shards] : nodeShards) {
                for (ui64 i = 0; i < shards; ++i) {
                    MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), nodeId);
                }
            }
        } else {
            const auto stageType = stage.GetTaskCount() ? TMaxTasksGraph::FIXED : TMaxTasksGraph::ANY;
            MaxTasksGraph->AddStage(stageInfo, stageType, inputs);
            for (const auto& [nodeId, shards] : nodeShards) {
                const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId).first;
                for (ui32 i = 0, count = std::min<ui32>(shards, maxTasksPerNode); i < count; ++i) {
                    MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), nodeId);
                }
            }
        }
    } else if (shuffleEliminated) {
        MaxTasksGraph->AddStage(stageInfo, TMaxTasksGraph::FIXED, inputs);

        for (const auto& [nodeId, shards] : nodeShards) {
            const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true).first;
            for (ui32 i = 0, count = std::min<ui32>(shards, maxTasksPerNode); i < count; ++i) {
                MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), nodeId);
            }
        }
    } else {
        const auto stageType = stage.GetTaskCount() ? TMaxTasksGraph::FIXED : TMaxTasksGraph::ANY;
        MaxTasksGraph->AddStage(stageInfo, stageType, inputs);

        // It's ok to create tasks only on nodes with shards - to prevent cross-network traffic.
        for (const auto& [nodeId, _] : nodeShards) {
            const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId).first;
            for (ui32 i = 0; i < maxTasksPerNode; ++i) {
                MaxTasksGraph->AddTask(AddTask(stageInfo, TTask::UNKNOWN), nodeId);
            }
        }
    }
}

TString TTaskMeta::ToString(const TVector<NScheme::TTypeInfo>& keyTypes, const NScheme::TTypeRegistry& typeRegistry) const {
    TStringBuilder sb;
    sb << "TTaskMeta{ Reads: { ";

    if (Reads) {
        for (ui64 i = 0; i < Reads->size(); ++i) {
            const auto& read = (*Reads)[i];
            sb << "[" << i << "]: { columns: [";
            for (const auto& x : read.Columns) {
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

    sb << " } }";

    return sb;
}

} // namespace NKikimr::NKqp
