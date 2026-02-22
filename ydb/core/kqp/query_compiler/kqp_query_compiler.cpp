#include "kqp_query_compiler.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/query_compiler/kqp_mkql_compiler.h>
#include <ydb/core/kqp/query_compiler/kqp_olap_compiler.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/kqp/query_data/kqp_request_predictor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/s3/statistics/yql_s3_statistics.h>

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NNodes;

namespace {

// Helper function to set VectorTopK metric from string
void SetVectorTopKMetric(Ydb::Table::VectorIndexSettings* indexSettings, const TString& metric) {
    if (metric == "CosineDistance") {
        indexSettings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
    } else if (metric == "CosineSimilarity") {
        indexSettings->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE);
    } else if (metric == "InnerProductSimilarity") {
        indexSettings->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
    } else if (metric == "ManhattanDistance") {
        indexSettings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN);
    } else if (metric == "EuclideanDistance") {
        indexSettings->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN);
    } else {
        YQL_ENSURE(false, "Unrecognized VectorTopK metric: " << metric);
    }
}

// Helper function to set VectorTopK target vector expression
void SetVectorTopKTarget(NKqpProto::TKqpPhyValue* targetProto, const TExprNode::TPtr& targetExpr) {
    TExprBase expr(targetExpr);
    if (expr.Maybe<TCoString>()) {
        FillLiteralProto(expr.Cast<TCoDataCtor>(), *targetProto->MutableLiteralValue());
    } else if (expr.Maybe<TCoParameter>()) {
        targetProto->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
    } else {
        YQL_ENSURE(false, "Unexpected VectorTopKTarget callable '" << expr.Ref().Content()
            << "'. Expected TCoString or TCoParameter.");
    }
}

// Helper function to set VectorTopK limit expression
void SetVectorTopKLimit(NKqpProto::TKqpPhyValue* limitProto, const TExprNode::TPtr& limitExpr) {
    TExprBase expr(limitExpr);
    if (expr.Maybe<TCoUint64>()) {
        FillLiteralProto(expr.Cast<TCoDataCtor>(), *limitProto->MutableLiteralValue());
    } else if (expr.Maybe<TCoParameter>()) {
        limitProto->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
    } else {
        YQL_ENSURE(false, "Unexpected VectorTopKLimit callable '" << expr.Ref().Content()
            << "'. Expected TCoUint64 or TCoParameter.");
    }
}

// Helper function to fill VectorTopK settings
template <typename TColumnsRange>
void FillVectorTopKSettings(
    NKqpProto::TKqpPhyVectorTopK& vectorTopK,
    const TKqpReadTableSettings& settings,
    const TColumnsRange& columns)
{
    // Find column index
    ui32 columnIdx = 0;
    bool columnFound = false;
    for (const auto& col : columns) {
        if (col.Value() == settings.VectorTopKColumn) {
            columnFound = true;
            break;
        }
        columnIdx++;
    }
    YQL_ENSURE(columnFound, "VectorTopK column " << settings.VectorTopKColumn << " not found in read columns");
    vectorTopK.SetColumn(columnIdx);

    // Set the metric settings
    auto* indexSettings = vectorTopK.MutableSettings();
    SetVectorTopKMetric(indexSettings, settings.VectorTopKMetric);

    // Default vector settings: when vector_dimension is 0, actual type and dimension
    // will be auto-detected from the target vector's format at runtime.
    indexSettings->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
    indexSettings->set_vector_dimension(0);

    // Set target vector
    SetVectorTopKTarget(vectorTopK.MutableTargetVector(), settings.VectorTopKTarget);

    // Set limit
    SetVectorTopKLimit(vectorTopK.MutableLimit(), settings.VectorTopKLimit);
}

NKqpProto::TKqpPhyTx::EType GetPhyTxType(const EPhysicalTxType& type) {
    switch (type) {
        case EPhysicalTxType::Compute: return NKqpProto::TKqpPhyTx::TYPE_COMPUTE;
        case EPhysicalTxType::Data: return NKqpProto::TKqpPhyTx::TYPE_DATA;
        case EPhysicalTxType::Scan: return NKqpProto::TKqpPhyTx::TYPE_SCAN;
        case EPhysicalTxType::Generic: return NKqpProto::TKqpPhyTx::TYPE_GENERIC;

        case EPhysicalTxType::Unspecified:
            break;
    }

    YQL_ENSURE(false, "Unexpected physical transaction type: " << type);
}

NKqpProto::TKqpPhyQuery::EType GetPhyQueryType(const EPhysicalQueryType& type) {
    switch (type) {
        case EPhysicalQueryType::Data: return NKqpProto::TKqpPhyQuery::TYPE_DATA;
        case EPhysicalQueryType::Scan: return NKqpProto::TKqpPhyQuery::TYPE_SCAN;
        case EPhysicalQueryType::GenericQuery: return NKqpProto::TKqpPhyQuery::TYPE_QUERY;
        case EPhysicalQueryType::GenericScript: return NKqpProto::TKqpPhyQuery::TYPE_SCRIPT;

        case EPhysicalQueryType::Unspecified:
            break;
    }

    YQL_ENSURE(false, "Unexpected physical query type: " << type);
}

NKqpProto::TKqpPhyInternalBinding::EType GetPhyInternalBindingType(const std::string_view type) {
    NKqpProto::TKqpPhyInternalBinding::EType bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_UNSPECIFIED;

    if (type == "Now"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_NOW;
    } else if (type == "CurrentUtcDate"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATE;
    } else if (type == "CurrentUtcDatetime"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATETIME;
    } else if (type == "CurrentUtcTimestamp"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_TIMESTAMP;
    } else if (type == "Random"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM;
    } else if (type == "RandomNumber"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_NUMBER;
    } else if (type == "RandomUuid"sv) {
        bindingType = NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_UUID;
    }

    YQL_ENSURE(bindingType != NKqpProto::TKqpPhyInternalBinding::PARAM_UNSPECIFIED,
        "Unexpected internal binding type: " << type);
    return bindingType;
}

NKqpProto::EStreamLookupStrategy GetStreamLookupStrategy(EStreamLookupStrategyType strategy) {
    switch (strategy) {
        case EStreamLookupStrategyType::Unspecified:
            break;
        case EStreamLookupStrategyType::LookupRows:
            return NKqpProto::EStreamLookupStrategy::LOOKUP;
        case EStreamLookupStrategyType::LookupUniqueRows:
            return NKqpProto::EStreamLookupStrategy::UNIQUE;
        case EStreamLookupStrategyType::LookupJoinRows:
            return NKqpProto::EStreamLookupStrategy::JOIN;
        case EStreamLookupStrategyType::LookupSemiJoinRows:
            return NKqpProto::EStreamLookupStrategy::SEMI_JOIN;
    }

    YQL_ENSURE(false, "Unspecified stream lookup strategy: " << strategy);
}

void FillTableId(const TKqpTable& table, NKqpProto::TKqpPhyTableId& tableProto) {
    auto pathId = TKikimrPathId::Parse(table.PathId());
    tableProto.SetPath(TString(table.Path()));
    tableProto.SetOwnerId(pathId.OwnerId());
    tableProto.SetTableId(pathId.TableId());
    tableProto.SetSysView(TString(table.SysView()));
    tableProto.SetVersion(FromString<ui64>(table.Version()));
}

void FillTableId(const TKikimrTableMetadata& tableMeta, NKqpProto::TKqpPhyTableId& tableProto) {
    tableProto.SetPath(tableMeta.Name);
    tableProto.SetOwnerId(tableMeta.PathId.OwnerId());
    tableProto.SetTableId(tableMeta.PathId.TableId());
    tableProto.SetSysView(tableMeta.SysView);
    tableProto.SetVersion(tableMeta.SchemaVersion);
}

NKqpProto::EKqpPhyTableKind GetPhyTableKind(EKikimrTableKind kind) {
    switch (kind) {
        case EKikimrTableKind::Datashard:
            return NKqpProto::TABLE_KIND_DS;
        case EKikimrTableKind::Olap:
            return NKqpProto::TABLE_KIND_OLAP;
        case EKikimrTableKind::SysView:
            return NKqpProto::TABLE_KIND_SYS_VIEW;
        case EKikimrTableKind::External:
            return NKqpProto::TABLE_KIND_EXTERNAL;
        default:
            return NKqpProto::TABLE_KIND_UNSPECIFIED;
    }
}

void FillTablesMap(const TStringBuf path, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap) {
    tablesMap.emplace(path, THashSet<TStringBuf>{});
}

void FillTablesMap(const TKqpTable& table, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap) {
    FillTablesMap(table.Path().Value(), tablesMap);
}

void FillTablesMap(const TStringBuf& path, const TVector<TStringBuf>& columns,
    THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
{
    FillTablesMap(path, tablesMap);

    for (const auto& column : columns) {
        tablesMap[path].emplace(column);
    }
}

void FillTablesMap(const TKqpTable& table, const TCoAtomList& columns,
    THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
{
    FillTablesMap(table, tablesMap);

    for (const auto& column : columns) {
        tablesMap[table.Path()].emplace(column);
    }
}

void FillTablesMap(const TKqpTable& table, const TVector<TStringBuf>& columns,
    THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
{
    FillTablesMap(table, tablesMap);

    for (const auto& column : columns) {
        tablesMap[table.Path()].emplace(column);
    }
}

void FillTable(const TKikimrTableMetadata& tableMeta, THashSet<TStringBuf>&& columns,
    NKqpProto::TKqpPhyTable& tableProto)
{
    FillTableId(tableMeta, *tableProto.MutableId());
    tableProto.SetKind(GetPhyTableKind(tableMeta.Kind));

    if (tableMeta.SysViewInfo) {
        *tableProto.MutableSysViewInfo() = *tableMeta.SysViewInfo;
    }

    for (const auto& keyColumnName : tableMeta.KeyColumnNames) {
        auto keyColumn = tableMeta.Columns.FindPtr(keyColumnName);
        YQL_ENSURE(keyColumn);

        auto& phyKeyColumn = *tableProto.MutableKeyColumns()->Add();
        phyKeyColumn.SetId(keyColumn->Id);
        phyKeyColumn.SetName(keyColumn->Name);

        columns.emplace(keyColumnName);
    }

    auto& phyColumns = *tableProto.MutableColumns();
    for (const auto& columnName : columns) {
        auto column = tableMeta.Columns.FindPtr(columnName);
        if (!column) {
            if (columnName == NTableIndex::NFulltext::FullTextRelevanceColumn) {
                continue;
            }

            YQL_ENSURE(GetSystemColumns().find(columnName) != GetSystemColumns().end());
            continue;
        }

        auto& phyColumn = phyColumns[column->Id];
        phyColumn.MutableId()->SetId(column->Id);
        phyColumn.MutableId()->SetName(column->Name);
        phyColumn.SetTypeId(column->TypeInfo.GetTypeId());
        phyColumn.SetIsBuildInProgress(column->IsBuildInProgress);
        if (column->IsDefaultFromSequence()) {
            phyColumn.SetDefaultFromSequence(column->DefaultFromSequence);
            phyColumn.MutableDefaultFromSequencePathId()->SetOwnerId(column->DefaultFromSequencePathId.OwnerId());
            phyColumn.MutableDefaultFromSequencePathId()->SetLocalPathId(column->DefaultFromSequencePathId.TableId());
        } else if (column->IsDefaultFromLiteral()) {
            phyColumn.MutableDefaultFromLiteral()->CopyFrom(column->DefaultFromLiteral);
        }
        phyColumn.SetNotNull(column->NotNull);
        switch (column->TypeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Pg: {
            phyColumn.MutableTypeParam()->SetPgTypeName(NPg::PgTypeNameFromTypeDesc(column->TypeInfo.GetPgTypeDesc()));
            break;
        }
        case NScheme::NTypeIds::Decimal: {
            ProtoFromDecimalType(column->TypeInfo.GetDecimalType(), *phyColumn.MutableTypeParam()->MutableDecimal());
            break;
        }
        }

    }
}

void FillExternalSource(const TKikimrTableMetadata& tableMeta, NKqpProto::TKqpPhyTable& tableProto) {
    THashSet<TStringBuf> columns;
    columns.reserve(tableMeta.Columns.size());
    for (const auto& [col, _] : tableMeta.Columns){
        columns.emplace(col);
    }

    FillTable(tableMeta, std::move(columns), tableProto);
}

template <typename TProto, typename TContainer>
void FillColumns(const TContainer& columns, const TKikimrTableMetadata& tableMeta,
    TProto& opProto, bool allowSystemColumns)
{
    for (const auto& columnNode : columns) {
        TString columnName(columnNode);

        ui32 columnId = 0;
        auto columnMeta = tableMeta.Columns.FindPtr(columnName);
        if (columnMeta) {
            columnId = columnMeta->Id;
        } else if (allowSystemColumns) {
            auto systemColumn = GetSystemColumns().find(columnName);
            YQL_ENSURE(systemColumn != GetSystemColumns().end());
            columnId = systemColumn->second.ColumnId;
        }


        if (columnName == NTableIndex::NFulltext::FullTextRelevanceColumn) {
            auto& columnProto = *opProto.AddColumns();
            // columnProto.SetId(columnId);
            columnProto.SetName(columnName);
            continue;
        }

        YQL_ENSURE(columnId, "Unknown column: " << columnName);
        auto& columnProto = *opProto.AddColumns();
        columnProto.SetId(columnId);
        columnProto.SetName(columnName);
    }
}

void FillNothingData(const TDataExprType& dataType, Ydb::TypedValue& value) {
    auto slot = dataType.GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NKikimr::NScheme::NTypeIds::IsYqlType(typeId) &&
        NKikimr::IsAllowedKeyType(NKikimr::NScheme::TTypeInfo(typeId)));

    if (slot == EDataSlot::Decimal) {
        const auto& paramsDataType = *dataType.Cast<TDataExprParamsType>();
        auto precision = FromString<ui8>(paramsDataType.GetParamOne());
        auto scale = FromString<ui8>(paramsDataType.GetParamTwo());
        value.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type()->set_precision(precision);
        value.mutable_type()->mutable_optional_type()->mutable_item()->mutable_decimal_type()->set_scale(scale);
    } else {
        auto& protoType = *value.mutable_type();
        protoType.mutable_optional_type()->mutable_item()->set_type_id((Ydb::Type::PrimitiveTypeId)typeId);
    }

    value.mutable_value()->set_null_flag_value(::google::protobuf::NullValue::NULL_VALUE);
}

void FillNothingPg(const TPgExprType& pgType, Ydb::TypedValue& value) {
    auto& protoType = *value.mutable_type();
    auto actualPgType = pgType.Cast<TPgExprType>();
    auto typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(actualPgType->GetId());

    protoType.mutable_pg_type()->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
    protoType.mutable_pg_type()->set_oid(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));

    value.mutable_value()->set_null_flag_value(::google::protobuf::NullValue::NULL_VALUE);
}

void FillNothing(TCoNothing expr, Ydb::TypedValue& value) {
    auto typeann = expr.Raw()->GetTypeAnn();
    switch (typeann->GetKind()) {
        case ETypeAnnotationKind::Optional: {
            typeann = typeann->Cast<TOptionalExprType>()->GetItemType();
            YQL_ENSURE(
                typeann->GetKind() == ETypeAnnotationKind::Data,
                "Unexpected type in Nothing.Optional: " << typeann->GetKind());
            FillNothingData(*typeann->Cast<TDataExprType>(), value);
            return;
        }
        case ETypeAnnotationKind::Pg: {
            FillNothingPg(*typeann->Cast<TPgExprType>(), value);
            return;
        }
        default:
            YQL_ENSURE(false, "Unexpected type in Nothing: " << typeann->GetKind());
    }
}

void FillKeyBound(const TVarArgCallable<TExprBase>& bound, NKqpProto::TKqpPhyKeyBound& boundProto) {
    if (bound.Maybe<TKqlKeyInc>()) {
        boundProto.SetIsInclusive(true);
    } else if (bound.Maybe<TKqlKeyExc>()) {
        boundProto.SetIsInclusive(false);
    } else {
        YQL_ENSURE(false, "Unexpected key bound type: " << bound.CallableName());
    }

    for (ui32 i = 0; i < bound.ArgCount(); ++i) {
        const auto& key = bound.Arg(i);

        auto& protoValue = *boundProto.AddValues();

        if (auto maybeParam = key.Maybe<TCoParameter>()) {
            auto& paramProto = *protoValue.MutableParamValue();
            paramProto.SetParamName(TString(maybeParam.Cast().Name()));
        } else if (auto maybeParam = key.Maybe<TCoNth>().Tuple().Maybe<TCoParameter>()) {
            auto& paramElementProto = *protoValue.MutableParamElementValue();
            paramElementProto.SetParamName(TString(maybeParam.Cast().Name()));
            paramElementProto.SetElementIndex(FromString<ui32>(key.Cast<TCoNth>().Index().Value()));
        } else if (auto maybeLiteral = key.Maybe<TCoDataCtor>()) {
            FillLiteralProto(maybeLiteral.Cast(), *protoValue.MutableLiteralValue());
        } else if (auto maybePgLiteral = key.Maybe<TCoPgConst>()) {
            FillLiteralProto(maybePgLiteral.Cast(), *protoValue.MutableLiteralValue());
        } else if (auto maybeNull = key.Maybe<TCoNothing>()) {
            FillNothing(maybeNull.Cast(), *protoValue.MutableLiteralValue());
        } else {
            YQL_ENSURE(false, "Unexpected key bound: " << key.Ref().Content());
        }
    }
}

void FillKeyRange(const TKqlKeyRange& range, NKqpProto::TKqpPhyKeyRange& rangeProto) {
    rangeProto.MutableFrom()->SetIsInclusive(true);
    rangeProto.MutableTo()->SetIsInclusive(true);

    FillKeyBound(range.From(), *rangeProto.MutableFrom());
    FillKeyBound(range.To(), *rangeProto.MutableTo());
    if (rangeProto.GetFrom().SerializeAsString() == rangeProto.GetTo().SerializeAsString()) {
        rangeProto.SetRangeIsPoint(true);
    }
}

void FillReadRange(const TKqpWideReadTable& read, const TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadRange& readProto)
{
    FillKeyRange(read.Range(), *readProto.MutableKeyRange());

    auto settings = TKqpReadTableSettings::Parse(read);

    readProto.MutableSkipNullKeys()->Resize(tableMeta.KeyColumnNames.size(), false);
    for (const auto& key : settings.SkipNullKeys) {
        size_t keyIndex = FindIndex(tableMeta.KeyColumnNames, key);
        YQL_ENSURE(keyIndex != NPOS);
        readProto.MutableSkipNullKeys()->Set(keyIndex, true);
    }

    if (settings.ItemsLimit) {
        TExprBase expr(settings.ItemsLimit);
        if (expr.Maybe<TCoUint64>()) {
            FillLiteralProto(expr.Cast<TCoDataCtor>(), *readProto.MutableItemsLimit()->MutableLiteralValue());
        } else if (expr.Maybe<TCoParameter>()) {
            readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
        }
    }

    readProto.SetReverse(settings.IsReverse());
}

template <typename TReader, typename TProto>
void FillReadRanges(const TReader& read, const TKikimrTableMetadata& /*tableMeta*/, TProto& readProto) {
    auto ranges = read.Ranges().template Maybe<TCoParameter>();

    if (ranges.IsValid()) {
        auto& rangesParam = *readProto.MutableKeyRanges();
        rangesParam.SetParamName(ranges.Cast().Name().StringValue());
    } else {
        YQL_ENSURE(
            TCoVoid::Match(read.Ranges().Raw()),
            "Read ranges should be parameter or void, got: " << read.Ranges().Ptr()->Content()
        );
    }

    auto settings = TKqpReadTableSettings::Parse(read);

    if (settings.ItemsLimit) {
        TExprBase expr(settings.ItemsLimit);
        if (expr.template Maybe<TCoUint64>()) {
            FillLiteralProto(expr.Cast<TCoDataCtor>(), *readProto.MutableItemsLimit()->MutableLiteralValue());
        } else if (expr.template Maybe<TCoParameter>()) {
            readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.template Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
        }
    }

    if constexpr (std::is_same_v<TProto, NKqpProto::TKqpPhyOpReadOlapRanges>) {
        readProto.SetSorted(settings.IsSorted());
        if (settings.TabletId) {
            readProto.SetTabletId(*settings.TabletId);
        }
    }

    // Handle VectorTopK settings for brute force vector search
    if constexpr (std::is_same_v<TProto, NKqpProto::TKqpPhyOpReadRanges>) {
        if (settings.VectorTopKColumn) {
            FillVectorTopKSettings(*readProto.MutableVectorTopK(), settings, read.Columns());
        }
    }

    readProto.SetReverse(settings.IsReverse());
}

template <typename TEffectCallable, typename TEffectProto>
void FillEffectRows(const TEffectCallable& callable, TEffectProto& proto, bool inplace) {
    if (auto maybeList = callable.Input().template Maybe<TCoIterator>().List()) {
        if (auto maybeParam = maybeList.Cast().template Maybe<TCoParameter>()) {
            const auto name = TString(maybeParam.Cast().Name());
            proto.MutableRowsValue()->MutableParamValue()->SetParamName(name);
        } else {
            YQL_ENSURE(false, "Unexpected effect input: " << maybeList.Cast().Ref().Content());
        }
    } else {
        YQL_ENSURE(inplace, "Expected iterator as effect input, got: " << callable.Input().Ref().Content());
    }
}

std::vector<std::string> GetResultColumnNames(const NKikimr::NMiniKQL::TType* resultType) {
    YQL_ENSURE(resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct
                || resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);

    auto* resultStructType = static_cast<const NKikimr::NMiniKQL::TStructType*>(resultType);
    ui32 resultColsCount = resultStructType->GetMembersCount();

    std::vector<std::string> resultColNames;
    resultColNames.reserve(resultColsCount);

    for (ui32 i = 0; i < resultColsCount; ++i) {
        resultColNames.emplace_back(resultStructType->GetMemberName(i));
    }
    return resultColNames;
}

template <class T>
void FillOlapProgram(const T& node, const NKikimr::NMiniKQL::TType* miniKqlResultType,
    const TKikimrTableMetadata& tableMeta, NKqpProto::TKqpPhyOpReadOlapRanges& readProto, TExprContext &ctx)
{
    if (NYql::HasSetting(node.Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames)) {
        auto groupByKeys = NYql::GetSetting(node.Settings().Ref(), TKqpReadTableSettings::GroupByFieldNames);
        if (!!groupByKeys) {
            auto keysList = (TCoNameValueTuple(groupByKeys).Value().Cast<TCoAtomList>());
            for (size_t i = 0; i < keysList.Size(); ++i) {
                readProto.AddGroupByColumnNames(keysList.Item(i).StringValue());
            }
        }
    }
    auto resultColNames = GetResultColumnNames(miniKqlResultType);
    CompileOlapProgram(node.Process(), tableMeta, readProto, resultColNames, ctx);
}

THashMap<TString, TString> FindSecureParams(const TExprNode::TPtr& node, const TTypeAnnotationContext& typesCtx, TSet<TString>& secretNames) {
    THashMap<TString, TString> secureParams;
    NYql::NCommon::FillSecureParams(node, typesCtx, secureParams);

    for (auto& [secretName, structuredToken] : secureParams) {
        const auto& tokenParser = CreateStructuredTokenParser(structuredToken);
        tokenParser.ListReferences(secretNames);
        structuredToken = tokenParser.ToBuilder().RemoveSecrets().ToJson();
    }

    return secureParams;
}

std::optional<std::pair<TString, TString>> FindOneSecureParam(const TExprNode::TPtr& node, const TTypeAnnotationContext& typesCtx, const TString& nodeName, TSet<TString>& secretNames) {
    const auto& secureParams = FindSecureParams(node, typesCtx, secretNames);
    if (secureParams.empty()) {
        return std::nullopt;
    }

    YQL_ENSURE(secureParams.size() == 1, "Only one SecureParams per " << nodeName << " allowed");
    return *secureParams.begin();
}

TIssues ApplyOverridePlannerSettings(const TString& overridePlannerJson, NKqpProto::TKqpPhyQuery& queryProto) {
    TIssues issues;
    NJson::TJsonValue jsonNode;
    try {
        NJson::TJsonReaderConfig jsonConfig;
        NJson::ReadJsonTree(overridePlannerJson, &jsonConfig, &jsonNode, true);
        if (!jsonNode.IsArray()) {
            issues.AddIssue("Expected array json value");
            return issues;
        }
    } catch (const std::exception& e) {
        issues.AddIssue(TStringBuilder() << "Failed to parse json: " << e.what());
        return issues;
    }

    const auto extractUint = [](const NJson::TJsonValue& node, ui32* result) -> TString {
        if (!node.IsUInteger()) {
            return "Expected non negative integer json value";
        }

        *result = node.GetUIntegerSafe();
        return "";
    };

    THashSet<std::pair<ui32, ui32>> updatedStages;
    const auto& jsonArray = jsonNode.GetArray();
    for (size_t i = 0; i < jsonArray.size(); ++i) {
        const auto& stageOverride = jsonArray[i];
        if (!stageOverride.IsMap()) {
            issues.AddIssue(TStringBuilder() << "Expected map json value for stage override " << i);
            continue;
        }

        ui32 txId = 0;
        ui32 stageId = 0;
        std::optional<ui32> tasks;
        bool optional = false;
        for (const auto& [key, value] : stageOverride.GetMap()) {
            ui32* result = nullptr;
            if (key == "tx") {
                result = &txId;
            } else if (key == "stage") {
                result = &stageId;
            } else if (key == "tasks") {
                tasks = 0;
                result = &(*tasks);
            } else if (key == "optional") {
                optional = value.GetBooleanRobust();
                continue;
            } else {
                issues.AddIssue(TStringBuilder() << "Unknown key '" << key << "' in stage override " << i);
                continue;
            }

            if (const auto& error = extractUint(value, result)) {
                issues.AddIssue(TStringBuilder() << error << " for key '" << key << "' in stage override " << i);
                continue;
            }
        }

        if (!updatedStages.emplace(txId, stageId).second) {
            issues.AddIssue(TStringBuilder() << "Duplicate stage override " << i << " for tx " << txId << " and stage " << stageId);
            continue;
        }

        if (!tasks) {
            issues.AddIssue(TStringBuilder() << "Missing stage settings for tx " << txId << " and stage " << stageId << " in stage override " << i);
            continue;
        }

        auto& txs = *queryProto.MutableTransactions();
        if (txId >= static_cast<ui32>(txs.size())) {
            if (!optional) {
                issues.AddIssue(TStringBuilder() << "Invalid tx id: " << txId << " in stage override " << i << ", number of transactions in query: " << txs.size());
            }
            continue;
        }

        auto& stages = *txs[txId].MutableStages();
        if (stageId >= static_cast<ui32>(stages.size())) {
            if (!optional) {
                issues.AddIssue(TStringBuilder() << "Invalid stage id: " << stageId << " in stage override " << i << ", number of stages in transaction " << txId << ": " << stages.size());
            }
            continue;
        }

        auto& stage = stages[stageId];
        if (tasks) {
            stage.SetTaskCount(*tasks);
        }
    }

    return issues;
}

TStringBuf RemoveJoinAliases(TStringBuf keyName) {
    if (const auto idx = keyName.find_last_of('.'); idx != TString::npos) {
        return keyName.substr(idx + 1);
    }

    return keyName;
}

void FillControlPlaneActors(NKqpProto::TKqpPhyStage& stageProto, const NDqProto::TDqIntegrationCommonSettings& settings) {
    for (const auto& [taskParamName, controlPlaneSettings] : settings.GetStageControlPlaneActors()) {
        NKqpProto::TKqpControlPlaneSettings protoSettings;
        const auto& type = controlPlaneSettings.GetType();
        protoSettings.SetType(controlPlaneSettings.GetType());

        const auto [it, inserted] = stageProto.MutableStageControlPlaneActors()->emplace(taskParamName, protoSettings);
        if (!inserted) {
            YQL_ENSURE(type == it->second.GetType(), "Duplicate control plane actor task param: " << taskParamName << " with different type: " << it->second.GetType() << " and " << type);
        }
    }
}

class TKqpQueryCompiler : public IKqpQueryCompiler {
public:
    TKqpQueryCompiler(const TString& cluster, const TString& database, const NMiniKQL::IFunctionRegistry& funcRegistry,
        TTypeAnnotationContext& typesCtx, NOpt::TKqpOptimizeContext& optimizeCtx, NYql::TKikimrConfiguration::TPtr config)
        : Cluster(cluster)
        , Database(database)
        , TablesData(optimizeCtx.Tables)
        , FuncRegistry(funcRegistry)
        , Alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
        , TypeEnv(Alloc)
        , KqlCtx(cluster, optimizeCtx.Tables, TypeEnv, FuncRegistry)
        , KqlCompiler(CreateKqlCompiler(KqlCtx, typesCtx))
        , TypesCtx(typesCtx)
        , OptimizeCtx(optimizeCtx)
        , Config(config)
    {
        Alloc.Release();
    }

    ~TKqpQueryCompiler() {
        Alloc.Acquire();
    }

    bool CompilePhysicalQuery(const TKqpPhysicalQuery& query, const TKiDataQueryBlocks& dataQueryBlocks,
        NKqpProto::TKqpPhyQuery& queryProto, TExprContext& ctx) final
    {
        TGuard<TScopedAlloc> allocGuard(Alloc);

        auto querySettings = TKqpPhyQuerySettings::Parse(query);
        YQL_ENSURE(querySettings.Type);
        queryProto.SetType(GetPhyQueryType(*querySettings.Type));

        queryProto.SetEnableOltpSink(Config->GetEnableOltpSink());
        queryProto.SetEnableOlapSink(Config->GetEnableOlapSink());
        queryProto.SetEnableHtapTx(Config->GetEnableHtapTx());
        queryProto.SetLangVer(Config->GetDefaultLangVer());

        queryProto.SetForceImmediateEffectsExecution(
            Config->KqpForceImmediateEffectsExecution.Get().GetOrElse(false));

        queryProto.SetDefaultTxMode(
            Config->DefaultTxMode.Get().GetOrElse(NKqpProto::ISOLATION_LEVEL_UNDEFINED));

        queryProto.SetDisableCheckpoints(Config->DisableCheckpoints.Get().GetOrElse(false));
        queryProto.SetEnableWatermarks(Config->GetEnableWatermarks());

        bool enableDiscardSelect = Config->GetEnableDiscardSelect();

        TDynBitMap resultDiscardFlags;
        ui32 resultCount = 0;
        for (const auto& queryBlock : dataQueryBlocks) {
            auto queryBlockSettings = TKiDataQueryBlockSettings::Parse(queryBlock);
            if (queryBlockSettings.HasUncommittedChangesRead) {
                queryProto.SetHasUncommittedChangesRead(true);
            }

            if (enableDiscardSelect) {
                for (const auto& kiResult : queryBlock.Results()) {
                    if (kiResult.Maybe<TKiResult>()) {
                        auto result = kiResult.Cast<TKiResult>();
                        bool discard = result.Discard().Value() == "1";
                        if (discard) {
                            resultDiscardFlags.Set(resultCount);
                        }
                        ++resultCount;
                    }
                }
            }

            auto ops = TableOperationsToProto(queryBlock.Operations(), ctx);
            for (auto& op : ops) {
                const auto tableName = op.GetTable();
                auto operation = static_cast<TYdbOperation>(op.GetOperation());

                *queryProto.AddTableOps() = std::move(op);

                const auto& desc = TablesData->GetTable(Cluster, tableName);
                TableDescriptionToTableInfo(desc, operation, *queryProto.MutableTableInfos());
            }
        }

        THashSet<std::pair<ui32, ui32>> shouldCreateChannel;
        std::vector<TResultBindingInfo> resultBindings;

        // if no discard flags are set, we can skip the dependency analysis
        bool hasDiscards = enableDiscardSelect && resultDiscardFlags.Count() > 0;

        if (enableDiscardSelect && hasDiscards) {
            YQL_ENSURE(resultCount <= query.Results().Size(),
                "resultCount=" << resultCount << " which exceeds query.Results().Size()=" << query.Results().Size());
            shouldCreateChannel = BuildChannelDependencies(query);
            resultBindings = BuildResultBindingsInfo(query, *querySettings.Type,
                                                     resultDiscardFlags, shouldCreateChannel);
        }

        for (ui32 txIdx = 0; txIdx < query.Transactions().Size(); ++txIdx) {
            const auto& tx = query.Transactions().Item(txIdx);
            CompileTransaction(tx, *queryProto.AddTransactions(), ctx, txIdx, shouldCreateChannel, hasDiscards);
        }

        if (const auto overridePlanner = Config->OverridePlanner.Get()) {
            if (const auto& issues = ApplyOverridePlannerSettings(*overridePlanner, queryProto)) {
                NYql::TIssue rootIssue("Invalid override planner settings");
                rootIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
                for (auto issue : issues) {
                    rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO)));
                }
                ctx.AddError(rootIssue);
                return false;
            }
        }

        ui32 resultIdx = 0;
        auto processResult = [&](ui32 originalIndex, ui32 txIndex, ui32 txResultIndex, bool skipBinding) {
            const auto& result = query.Results().Item(originalIndex);

            YQL_ENSURE(result.Maybe<TKqpTxResultBinding>());
            auto binding = result.Cast<TKqpTxResultBinding>();

            YQL_ENSURE(txIndex < queryProto.TransactionsSize());
            YQL_ENSURE(txResultIndex < queryProto.GetTransactions(txIndex).ResultsSize());
            auto& txResult = *queryProto.MutableTransactions(txIndex)->MutableResults(txResultIndex);

            YQL_ENSURE(txResult.GetIsStream());

            if (skipBinding) {
                return;
            }

            txResult.SetQueryResultIndex(resultIdx);
            auto& queryBindingProto = *queryProto.AddResultBindings();
            auto& txBindingProto = *queryBindingProto.MutableTxResultBinding();
            txBindingProto.SetTxIndex(txIndex);
            txBindingProto.SetResultIndex(txResultIndex);

            resultIdx++;

            auto type = binding.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            YQL_ENSURE(type);
            YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Struct);

            NKikimrMiniKQL::TType kikimrProto;
            auto typeBuilder = NKikimr::NMiniKQL::TTypeBuilder(TypeEnv);
            NKikimr::NMiniKQL::TType* resultType = NYql::NCommon::BuildType(result.Pos(), *type, typeBuilder);

            ExportTypeToProto(resultType, kikimrProto);

            auto resultMetaColumns = queryBindingProto.MutableResultSetMeta()->Mutablecolumns();
            for (size_t i = 0; i < kikimrProto.GetStruct().MemberSize(); i++) {
                resultMetaColumns->Add();
            }

            THashMap<TString, int> columnOrder;
            TColumnOrder order;
            columnOrder.reserve(kikimrProto.GetStruct().MemberSize());
            if (!txResult.GetColumnHints().empty()) {
                YQL_ENSURE(txResult.GetColumnHints().size() == (int)kikimrProto.GetStruct().MemberSize());
                for (int i = 0; i < txResult.GetColumnHints().size(); i++) {
                    const auto& hint = txResult.GetColumnHints().at(i);
                    columnOrder[order.AddColumn(TString(hint))] = i;
                }
            }

            int id = 0;
            for (const auto& column : kikimrProto.GetStruct().GetMember()) {
                auto it = columnOrder.find(column.GetName());
                int bindingColumnId = it != columnOrder.end() ? it->second : id++;
                auto& columnMeta = resultMetaColumns->at(bindingColumnId);
                columnMeta.Setname(it != columnOrder.end() ? order.at(it->second).LogicalName : column.GetName());
                ConvertMiniKQLTypeToYdbType(column.GetType(), *columnMeta.mutable_type());
            }
        };

        if (enableDiscardSelect && hasDiscards) {
            for (const auto& bindingInfo : resultBindings) {
                processResult(bindingInfo.OriginalIndex, bindingInfo.TxIndex,
                            bindingInfo.ResultIndex, bindingInfo.IsDiscard);
            }
        } else {
            for (ui32 i = 0; i < query.Results().Size(); ++i) {
                const auto& result = query.Results().Item(i);
                YQL_ENSURE(result.Maybe<TKqpTxResultBinding>());
                auto binding = result.Cast<TKqpTxResultBinding>();
                auto txIndex = FromString<ui32>(binding.TxIndex().Value());
                auto txResultIndex = FromString<ui32>(binding.ResultIndex());

                processResult(i, txIndex, txResultIndex, false);
            }
        }

        return true;
    }

    const TStructExprType* CollectParameters(const TDqPhyStage& stage, TExprContext& ctx) {
        TVector<const TItemExprType*> inputsParams;
        for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
            auto input = stage.Inputs().Item(i);
            if (input.Maybe<TDqSource>()) {
                VisitExpr(input.Ptr(), [&] (const TExprNode::TPtr& node) {
                  if (auto maybeParam = TMaybeNode<TCoParameter>(node)) {
                      auto param = maybeParam.Cast();

                      inputsParams.push_back(ctx.MakeType<TItemExprType>(param.Name(), param.Ref().GetTypeAnn()));
                  }

                  return true;
                });
            }
        }
        auto programParams = NDq::CollectParameters(stage.Program(), ctx);
        if (inputsParams.empty()) {
            return programParams;
        } else {
            for (auto member : programParams->GetItems()) {
                inputsParams.push_back(member);
            }

            std::sort(inputsParams.begin(), inputsParams.end(),
                [](const TItemExprType* first, const TItemExprType* second) {
                    return first->GetName() < second->GetName();
                });
            inputsParams.erase(std::unique(inputsParams.begin(), inputsParams.end(),
                [](const TItemExprType* first, const TItemExprType* second) {
                    return first->GetName() == second->GetName();
                }),
                inputsParams.end());

            return ctx.MakeType<TStructExprType>(inputsParams);
        }
    }

private:
    NKikimr::NMiniKQL::TType* CompileType(TProgramBuilder& pgmBuilder, const TTypeAnnotationNode& inputType) {
        TStringStream errorStream;
        auto type = NCommon::BuildType(inputType, pgmBuilder, errorStream);
        Y_ENSURE(type, "Failed to compile type: " << errorStream.Str());
        return type;
    }

    void CompileStage(
        const TDqPhyStage& stage,
        NKqpProto::TKqpPhyStage& stageProto,
        TExprContext& ctx,
        const TMap<ui64, ui32>& stagesMap,
        TRequestPredictor& rPredictor,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap,
        THashMap<ui64, NKqpProto::TKqpPhyStage*>& physicalStageByID
    ) {
        const bool hasEffects = NOpt::IsKqpEffectsStage(stage);

        TStagePredictor& stagePredictor = rPredictor.BuildForStage(stage, ctx);
        stagePredictor.Scan(stage.Program().Ptr());

        auto stageSettings = NDq::TDqStageSettings::Parse(stage);
        stageProto.SetIsShuffleEliminated(stageSettings.IsShuffleEliminated);

        for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
            const auto& input = stage.Inputs().Item(inputIndex);

            if (input.Maybe<TDqSource>()) {
                auto* protoSource = stageProto.AddSources();
                FillSource(input.Cast<TDqSource>(), stageProto, protoSource, true, tablesMap, ctx);
                protoSource->SetInputIndex(inputIndex);
            } else {
                YQL_ENSURE(input.Maybe<TDqConnection>());
                auto connection = input.Cast<TDqConnection>();

                auto& protoInput = *stageProto.AddInputs();
                FillConnection(connection, stagesMap, protoInput, ctx, tablesMap, physicalStageByID, &stage, inputIndex);
                protoInput.SetInputIndex(inputIndex);
            }
        }

        double stageCost = 0.0;
        VisitExpr(stage.Program().Ptr(), [&](const TExprNode::TPtr& exprNode) {

            TExprBase node(exprNode);

            if (auto maybeReadTable = node.Maybe<TKqpWideReadTable>()) {
                auto readTable = maybeReadTable.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTable.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(readTable.Table(), readTable.Columns(), tablesMap);
                FillTableId(readTable.Table(), *tableOp.MutableTable());
                FillColumns(readTable.Columns(), *tableMeta, tableOp, true);
                FillReadRange(readTable, *tableMeta, *tableOp.MutableReadRange());
            } else if (auto maybeUpsertRows = node.Maybe<TKqpUpsertRows>()) {
                auto upsertRows = maybeUpsertRows.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, upsertRows.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);
                YQL_ENSURE(hasEffects);

                auto settings = TKqpUpsertRowsSettings::Parse(upsertRows);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(upsertRows.Table(), upsertRows.Columns(), tablesMap);
                FillTableId(upsertRows.Table(), *tableOp.MutableTable());
                FillColumns(upsertRows.Columns(), *tableMeta, tableOp, false);
                FillEffectRows(upsertRows, *tableOp.MutableUpsertRows(), settings.Inplace);
            } else if (auto maybeDeleteRows = node.Maybe<TKqpDeleteRows>()) {
                auto deleteRows = maybeDeleteRows.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, deleteRows.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);
                YQL_ENSURE(hasEffects);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(deleteRows.Table(), tablesMap);
                FillTableId(deleteRows.Table(), *tableOp.MutableTable());
                FillEffectRows(deleteRows, *tableOp.MutableDeleteRows(), false);
            } else if (auto maybeWideReadTableRanges = node.Maybe<TKqpWideReadTableRanges>()) {
                auto readTableRanges = maybeWideReadTableRanges.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTableRanges.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(readTableRanges.Table(), readTableRanges.Columns(), tablesMap);
                FillTableId(readTableRanges.Table(), *tableOp.MutableTable());
                FillColumns(readTableRanges.Columns(), *tableMeta, tableOp, true);
                FillReadRanges(readTableRanges, *tableMeta, *tableOp.MutableReadRanges());
            } else if (auto maybeReadWideTableRanges = node.Maybe<TKqpWideReadOlapTableRanges>()) {
                auto readTableRanges = maybeReadWideTableRanges.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTableRanges.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(readTableRanges.Table(), readTableRanges.Columns(), tablesMap);
                FillTableId(readTableRanges.Table(), *tableOp.MutableTable());
                FillColumns(readTableRanges.Columns(), *tableMeta, tableOp, true);
                FillReadRanges(readTableRanges, *tableMeta, *tableOp.MutableReadOlapRange());
                auto miniKqlResultType = GetMKqlResultType(readTableRanges.Process().Ref().GetTypeAnn());
                FillOlapProgram(readTableRanges, miniKqlResultType, *tableMeta, *tableOp.MutableReadOlapRange(), ctx);
                FillResultType(miniKqlResultType, *tableOp.MutableReadOlapRange());
            } else if (auto maybeReadBlockTableRanges = node.Maybe<TKqpBlockReadOlapTableRanges>()) {
                auto readTableRanges = maybeReadBlockTableRanges.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTableRanges.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(readTableRanges.Table(), readTableRanges.Columns(), tablesMap);
                FillTableId(readTableRanges.Table(), *tableOp.MutableTable());
                FillColumns(readTableRanges.Columns(), *tableMeta, tableOp, true);
                FillReadRanges(readTableRanges, *tableMeta, *tableOp.MutableReadOlapRange());
                auto miniKqlResultType = GetMKqlResultType(readTableRanges.Process().Ref().GetTypeAnn());
                FillOlapProgram(readTableRanges, miniKqlResultType, *tableMeta, *tableOp.MutableReadOlapRange(), ctx);
                FillResultType(miniKqlResultType, *tableOp.MutableReadOlapRange());
                tableOp.MutableReadOlapRange()->SetReadType(NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS);
            } else if (auto maybeDqSourceWrapBase = node.Maybe<TDqSourceWrapBase>()) {
                stageCost += GetDqSourceWrapBaseCost(maybeDqSourceWrapBase.Cast(), TypesCtx);
            } else if (auto maybeDqReadWrapBase = node.Maybe<TDqReadWrapBase>()) {
                FillDqRead(maybeDqReadWrapBase.Cast(), stageProto, ctx);
            } else {
                YQL_ENSURE(!node.Maybe<TKqpReadTable>());
                YQL_ENSURE(!node.Maybe<TKqpLookupTable>());
            }
            return true;
        });

        stageProto.SetStageCost(stageCost);
        const auto& secureParams = FindSecureParams(stage.Program().Ptr(), TypesCtx, SecretNames);
        stageProto.MutableSecureParams()->insert(secureParams.begin(), secureParams.end());

        auto result = stage.Program().Body();
        auto resultType = result.Ref().GetTypeAnn();
        ui32 outputsCount = 0;
        if (resultType->GetKind() == ETypeAnnotationKind::Stream) {
            auto resultItemType = resultType->Cast<TStreamExprType>()->GetItemType();
            if (resultItemType->GetKind() == ETypeAnnotationKind::Variant) {
                auto underlyingType = resultItemType->Cast<TVariantExprType>()->GetUnderlyingType();
                YQL_ENSURE(underlyingType->GetKind() == ETypeAnnotationKind::Tuple);
                outputsCount = underlyingType->Cast<TTupleExprType>()->GetSize();
                YQL_ENSURE(outputsCount > 1);
            } else {
                outputsCount = 1;
            }
        } else {
            YQL_CLOG(TRACE, ProviderKqp) << "Stage " << stage.Ptr()->UniqueId() << " type ann kind " << resultType->GetKind();
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Void, "got " << *resultType);
        }

        stageProto.SetOutputsCount(outputsCount);

        // Dq sinks
        bool hasTxTableSink = false;
        if (auto maybeOutputsNode = stage.Outputs()) {
            auto outputsNode = maybeOutputsNode.Cast();
            for (size_t i = 0; i < outputsNode.Size(); ++i) {
                auto outputNode = outputsNode.Item(i);
                if (auto maybeSinkNode = outputNode.Maybe<TDqSink>(); maybeSinkNode) {
                    YQL_ENSURE(maybeSinkNode);
                    auto sinkNode = maybeSinkNode.Cast();
                    auto* sinkProto = stageProto.AddSinks();
                    FillSink(sinkNode, sinkProto, tablesMap, stage, ctx);
                    sinkProto->SetOutputIndex(FromString(TStringBuf(sinkNode.Index())));

                    if (IsTableSink(sinkNode.DataSink().Cast<TCoDataSink>().Category())) {
                        // Only sinks with transactions to ydb tables can be considered as effects.
                        // Inconsistent internal sinks and external sinks (like S3) aren't effects.
                        auto settings = sinkNode.Settings().Maybe<TKqpTableSinkSettings>();
                        YQL_ENSURE(settings);
                        hasTxTableSink |= settings.InconsistentWrite().Cast().StringValue() != "true";
                    }
                } else {
                    auto maybeTransformNode = outputNode.Maybe<TDqTransform>();
                    YQL_ENSURE(maybeTransformNode);
                    auto transformNode = maybeTransformNode.Cast();

                    AFL_ENSURE(IsTableSink(transformNode.DataSink().Cast<TCoDataSink>().Category()));

                    auto* transformProto = stageProto.AddOutputTransforms();
                    transformProto->MutableInternalSink()->SetType(TString(NYql::KqpTableSinkName));
                    
                    NKikimrKqp::TKqpTableSinkSettings settingsProto;
                    auto settings = transformNode.Settings().Maybe<TKqpTableSinkSettings>();
                    YQL_ENSURE(settings, "Unsupported sink type");

                    FillKqpSinkSettings(
                        settings.Cast(),
                        settingsProto,
                        tablesMap,
                        stage);
                    transformProto->MutableInternalSink()->MutableSettings()->PackFrom(settingsProto);
                    transformProto->SetOutputIndex(FromString(TStringBuf(transformNode.Index())));

                    const auto inputRowType = transformNode.InputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                    const auto outputRowType = transformNode.OutputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

                    const auto programOutputType = GetSeqItemType(stage.Program().Ref().GetTypeAnn());
                    AFL_ENSURE(inputRowType->Equals(*programOutputType));

                    TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
                    transformProto->SetInputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *inputRowType), TypeEnv));
                    transformProto->SetOutputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *outputRowType), TypeEnv));

                    hasTxTableSink = true;
                }
            }
        }

        stageProto.SetIsEffectsStage(hasEffects || hasTxTableSink);

        auto paramsType = CollectParameters(stage, ctx);
        NDq::TSpillingSettings spillingSettings{Config->GetEnabledSpillingNodes()};
        auto programBytecode = NDq::BuildProgram(stage.Program(), *paramsType, *KqlCompiler, TypeEnv, FuncRegistry,
            ctx, {}, spillingSettings);

        auto& programProto = *stageProto.MutableProgram();
        programProto.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        programProto.SetRaw(programBytecode);
        programProto.SetLangVer(Config->GetDefaultLangVer());

        stagePredictor.SerializeToKqpSettings(*programProto.MutableSettings());

        for (auto member : paramsType->GetItems()) {
            auto paramName = TString(member->GetName());
            stageProto.AddProgramParameters(paramName);
        }

        stageProto.SetProgramAst(KqpExprToPrettyString(stage.Program(), ctx));

        stageProto.SetStageGuid(stageSettings.Id);
        stageProto.SetIsSinglePartition(NDq::TDqStageSettings::EPartitionMode::Single == stageSettings.PartitionMode);
        stageProto.SetAllowWithSpilling(Config->GetEnableQueryServiceSpilling() && (OptimizeCtx.IsGenericQuery() || OptimizeCtx.IsScanQuery()) && Config->SpillingEnabled());
    }

    void CompileTransaction(const TKqpPhysicalTx& tx, NKqpProto::TKqpPhyTx& txProto, TExprContext& ctx,
                            ui32 txIdx, const THashSet<std::pair<ui32, ui32>>& shouldCreateChannel, bool hasDiscards) {
        auto txSettings = TKqpPhyTxSettings::Parse(tx);
        YQL_ENSURE(txSettings.Type);
        txProto.SetType(GetPhyTxType(*txSettings.Type));

        bool hasEffectStage = false;

        TMap<ui64, ui32> stagesMap;
        THashMap<ui64, NKqpProto::TKqpPhyStage*> physicalStageByID;
        THashMap<TStringBuf, THashSet<TStringBuf>> tablesMap;

        TRequestPredictor rPredictor;
        for (const auto& stage : tx.Stages()) {
            physicalStageByID[stage.Ref().UniqueId()] = txProto.AddStages();
            CompileStage(stage, *physicalStageByID[stage.Ref().UniqueId()], ctx, stagesMap, rPredictor, tablesMap, physicalStageByID);
            hasEffectStage |= physicalStageByID[stage.Ref().UniqueId()]->GetIsEffectsStage();
            stagesMap[stage.Ref().UniqueId()] = txProto.StagesSize() - 1;
        }
        for (auto&& i : *txProto.MutableStages()) {
            i.MutableProgram()->MutableSettings()->SetLevelDataPrediction(rPredictor.GetLevelDataVolume(i.GetProgram().GetSettings().GetStageLevel()));
        }

        txProto.SetEnableShuffleElimination(Config->OptShuffleElimination.Get().GetOrElse(Config->GetDefaultEnableShuffleElimination()));
        txProto.SetHasEffects(hasEffectStage);
        txProto.SetDqChannelVersion(Config->DqChannelVersion.Get().GetOrElse(Config->GetDqChannelVersion()));
        for (const auto& paramBinding : tx.ParamBindings()) {
            TString paramName(paramBinding.Name().Value());
            const auto& binding = paramBinding.Binding();

            auto& bindingProto = *txProto.AddParamBindings();
            bindingProto.SetName(paramName);

            if (!binding) {
                bindingProto.MutableExternalBinding();
            } else if (auto maybeResultBinding = binding.Maybe<TKqpTxResultBinding>()) {
                auto resultBinding = maybeResultBinding.Cast();
                auto txIndex = FromString<ui32>(resultBinding.TxIndex());
                auto resultIndex = FromString<ui32>(resultBinding.ResultIndex());

                auto& txResultProto = *bindingProto.MutableTxResultBinding();
                txResultProto.SetTxIndex(txIndex);
                txResultProto.SetResultIndex(resultIndex);
            } else if (auto maybeInternalBinding = binding.Maybe<TKqpTxInternalBinding>()) {
                auto internalBinding = maybeInternalBinding.Cast();
                auto& internalBindingProto = *bindingProto.MutableInternalBinding();
                internalBindingProto.SetType(GetPhyInternalBindingType(internalBinding.Kind().Value()));
            } else {
                YQL_ENSURE(false, "Unknown parameter binding type: " << binding.Cast().CallableName());
            }
        }

        TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
        for (ui32 resIdx = 0; resIdx < tx.Results().Size(); ++resIdx) {
            const auto& resultNode = tx.Results().Item(resIdx);
            YQL_ENSURE(resultNode.Maybe<TDqConnection>(), "" << NCommon::ExprToPrettyString(ctx, tx.Ref()));
            auto connection = resultNode.Cast<TDqConnection>();

            auto& resultProto = *txProto.AddResults();
            auto& connectionProto = *resultProto.MutableConnection();
            FillConnection(connection, stagesMap, connectionProto, ctx, tablesMap, physicalStageByID, nullptr, 0);

            const TTypeAnnotationNode* itemType = nullptr;
            switch (connectionProto.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kValue:
                    resultProto.SetIsStream(false);
                    itemType = resultNode.Ref().GetTypeAnn();
                    break;

                case NKqpProto::TKqpPhyConnection::kResult:
                    resultProto.SetIsStream(true);
                    itemType = resultNode.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                    break;

                default:
                    YQL_ENSURE(false, "Unexpected result connection type: " << (ui32)connectionProto.GetTypeCase());
            }

            YQL_ENSURE(itemType);
            ExportTypeToProto(CompileType(pgmBuilder, *itemType), *resultProto.MutableItemType());

            TMaybeNode<TCoAtomList> maybeColumnHints;
            if (connection.Maybe<TDqCnResult>()) {
                maybeColumnHints = connection.Cast<TDqCnResult>().ColumnHints();
            } else if (connection.Maybe<TDqCnValue>()) {
                // no column hints
            } else {
                YQL_ENSURE(false, "Unexpected tx result connection type " << connection.CallableName());
            }

            if (maybeColumnHints) {
                auto columnHints = maybeColumnHints.Cast();
                auto& columnHintsProto = *resultProto.MutableColumnHints();
                columnHintsProto.Reserve(columnHints.Size());
                for (const auto& columnHint : columnHints) {
                    columnHintsProto.Add(TString(columnHint.Value()));
                }
            }
            if (Config->GetEnableDiscardSelect() && hasDiscards) {
                bool canSkip = (shouldCreateChannel.find(std::make_pair(txIdx, resIdx)) == shouldCreateChannel.end());
                resultProto.SetCanSkipChannel(canSkip);
            }
        }

        for (auto& [tablePath, tableColumns] : tablesMap) {
            auto tableMeta = TablesData->ExistingTable(Cluster, tablePath).Metadata;
            YQL_ENSURE(tableMeta);

            FillTable(*tableMeta, std::move(tableColumns), *txProto.AddTables());
        }

        for (const auto& [path, desc] : TablesData->GetTables()) {
            const auto tableMeta = desc.Metadata;
            Y_ENSURE(tableMeta, path.first << ":" << path.second);

            if (tableMeta->Kind == NYql::EKikimrTableKind::External) {
                FillExternalSource(*tableMeta, *txProto.AddTables());

                if (const auto sourceMeta = tableMeta->ExternalSource.UnderlyingExternalSourceMetadata) {
                    FillExternalSource(*sourceMeta, *txProto.AddTables());
                }
            }
        }

        for (const auto& secretName : SecretNames) {
            txProto.AddSecretNames(secretName);
        }
    }

    void FillKqpSource(const TDqSource& source, NKqpProto::TKqpSource* protoSource, bool allowSystemColumns,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
    {
        if (auto settings = source.Settings().Maybe<TKqpReadRangesSourceSettings>()) {
            NKqpProto::TKqpReadRangesSource& readProto = *protoSource->MutableReadRangesSource();
            FillTablesMap(settings.Table().Cast(), settings.Columns().Cast(), tablesMap);
            FillTableId(settings.Table().Cast(), *readProto.MutableTable());

            auto tableMeta = TablesData->ExistingTable(Cluster, settings.Table().Cast().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            readProto.SetIsTableImmutable(TablesData->IsTableImmutable(Cluster, settings.Table().Cast().Path()));
            {

                THashMap<TString, const TExprNode*> columnsMap;
                for (auto item : settings.Columns().Cast()) {
                    columnsMap[item.StringValue()] = item.Raw();
                }
                TVector<TCoAtom> columns;
                auto type = settings.Raw()->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
                for (auto item : type->GetItems()) {
                    columns.push_back(TCoAtom(columnsMap.at(item->GetName())));
                }
                FillColumns(columns, *tableMeta, readProto, allowSystemColumns);
            }
            auto readSettings = TKqpReadTableSettings::Parse(settings.Settings().Cast());

            readProto.SetReverse(readSettings.IsReverse());
            readProto.SetSorted(readSettings.IsSorted());
            YQL_ENSURE(readSettings.SkipNullKeys.empty());

            if (readSettings.SequentialInFlight) {
                readProto.SetSequentialInFlightShards(*readSettings.SequentialInFlight);
            }

            auto ranges = settings.RangesExpr().template Maybe<TCoParameter>();
            if (ranges.IsValid()) {
                auto& rangesParam = *readProto.MutableRanges();
                rangesParam.SetParamName(ranges.Cast().Name().StringValue());
            } else if (!TCoVoid::Match(settings.RangesExpr().Raw())) {
                YQL_ENSURE(
                    TKqlKeyRange::Match(settings.RangesExpr().Raw()),
                    "Read ranges should be parameter or KqlKeyRange, got: " << settings.RangesExpr().Cast().Ptr()->Content()
                );

                FillKeyRange(settings.RangesExpr().Cast<TKqlKeyRange>(), *readProto.MutableKeyRange());
            }

            if (readSettings.ItemsLimit) {
                TExprBase expr(readSettings.ItemsLimit);
                if (expr.template Maybe<TCoUint64>()) {
                    FillLiteralProto(expr.Cast<TCoDataCtor>(), *readProto.MutableItemsLimit()->MutableLiteralValue());
                } else if (expr.template Maybe<TCoParameter>()) {
                    readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.template Cast<TCoParameter>().Name().StringValue());
                } else {
                    YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
                }
            }

            // Handle VectorTopK settings for brute force vector search
            if (readSettings.VectorTopKColumn) {
                FillVectorTopKSettings(*readProto.MutableVectorTopK(), readSettings, settings.Columns().Cast());
            }

        } else if (auto settings = source.Settings().Maybe<TKqpReadTableFullTextIndexSourceSettings>()) {
            NKqpProto::TKqpFullTextSource& fullTextProto = *protoSource->MutableFullTextSource();
            auto tableMeta = TablesData->ExistingTable(Cluster, settings.Table().Cast().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            FillTablesMap(settings.Table().Cast(), settings.Columns().Cast(), tablesMap);
            FillTableId(settings.Table().Cast(), *fullTextProto.MutableTable());

            TString indexName = TString(settings.Index().Cast().StringValue());
            fullTextProto.SetIndex(settings.Index().Cast().StringValue());

            auto type = settings.Raw()->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();

            auto [indexMeta, index] = tableMeta->GetIndex(indexName);

            auto* desc = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&index->SpecializedIndexDescription);
            YQL_ENSURE(desc, "unexpected index description type");
            fullTextProto.MutableIndexDescription()->MutableSettings()->CopyFrom(desc->GetSettings());

            auto fillCol = [&](const NYql::TKikimrColumnMetadata* columnMeta, NKikimrKqp::TKqpColumnMetadataProto* columnProto) {
                columnProto->SetName(columnMeta->Name);
                columnProto->SetId(columnMeta->Id);
                columnProto->SetTypeId(columnMeta->TypeInfo.GetTypeId());
                if (NScheme::NTypeIds::IsParametrizedType(columnMeta->TypeInfo.GetTypeId())) {
                    ProtoFromTypeInfo(columnMeta->TypeInfo, columnMeta->TypeMod, *columnProto->MutableTypeInfo());
                }
            };

            for(auto& implTable : index->GetImplTables()) {
                auto indexProto = fullTextProto.AddIndexTables();
                TVector<TString> pathParts = {TString(settings.Table().Cast().Path()), indexName, TString(implTable)};
                TString tablePath = NKikimr::JoinPath(pathParts);
                FillTablesMap(tablePath, tablesMap);

                auto implTableMeta = TablesData->ExistingTable(Cluster, tablePath).Metadata;
                indexProto->MutableTable()->SetOwnerId(implTableMeta->PathId.OwnerId());
                indexProto->MutableTable()->SetTableId(implTableMeta->PathId.TableId());
                indexProto->MutableTable()->SetPath(tablePath);
                indexProto->MutableTable()->SetSysView(implTableMeta->SysView);
                indexProto->MutableTable()->SetVersion(implTableMeta->SchemaVersion);

                for(auto& keyColumn: implTableMeta->KeyColumnNames) {
                    auto* columnPtr = implTableMeta->Columns.FindPtr(keyColumn);
                    YQL_ENSURE(columnPtr);
                    fillCol(columnPtr, indexProto->AddKeyColumns());
                }

                for(auto& column: implTableMeta->Columns) {
                    auto* columnPtr = implTableMeta->Columns.FindPtr(column.first);
                    YQL_ENSURE(columnPtr);
                    fillCol(columnPtr, indexProto->AddColumns());
                }
            }

            for (auto& keyColumn: tableMeta->KeyColumnNames) {
                auto* columnPtr = tableMeta->Columns.FindPtr(keyColumn);
                YQL_ENSURE(columnPtr);
                fillCol(columnPtr, fullTextProto.AddKeyColumns());
            }

            for (auto item : type->GetItems()) {
                auto* columnProto = fullTextProto.AddColumns();
                columnProto->SetName(TString(item->GetName()));
                if (item->GetName() == NTableIndex::NFulltext::FullTextRelevanceColumn) {
                    continue;
                }

                auto* columnPtr = tableMeta->Columns.FindPtr(item->GetName());
                YQL_ENSURE(columnPtr);
                fillCol(columnPtr, columnProto);
            }

            {
                for (const auto& expr: settings.Query().Maybe<TExprList>().Cast()) {
                    auto* value = fullTextProto.MutableQuerySettings()->AddQueryValue();
                    if (expr.Maybe<TCoParameter>()) {
                        value->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
                    } else {
                        FillLiteralProto(expr.Cast<TCoDataCtor>(), *value->MutableLiteralValue());
                    }
                }
            }

            TKqpReadTableFullTextIndexSettings settingsObj = TKqpReadTableFullTextIndexSettings::Parse(settings.Settings().Cast());
            if (settingsObj.ItemsLimit) {
                auto itemsLimit = TExprBase(settingsObj.ItemsLimit);
                if (itemsLimit.Maybe<TCoParameter>()) {
                    fullTextProto.MutableTakeLimit()->MutableParamValue()->SetParamName(itemsLimit.Cast<TCoParameter>().Name().StringValue());
                } else {
                    FillLiteralProto(itemsLimit.Cast<TCoDataCtor>(), *fullTextProto.MutableTakeLimit()->MutableLiteralValue());
                }
            }

            if (settingsObj.BFactor) {
                auto bFactor = TExprBase(settingsObj.BFactor);
                auto just = bFactor.Maybe<TCoJust>() ? bFactor.Maybe<TCoJust>().Cast().Input() : bFactor;
                if (just.Maybe<TCoParameter>()) {
                    fullTextProto.MutableBFactor()->MutableParamValue()->SetParamName(just.Cast<TCoParameter>().Name().StringValue());
                } else {
                    FillLiteralProto(just.Cast<TCoDataCtor>(), *fullTextProto.MutableBFactor()->MutableLiteralValue());
                }
            }

            if (settingsObj.DefaultOperator) {
                auto defaultOperator = TExprBase(settingsObj.DefaultOperator);
                auto just = defaultOperator.Maybe<TCoJust>() ? defaultOperator.Maybe<TCoJust>().Cast().Input() : defaultOperator;
                if (just.Maybe<TCoParameter>()) {
                    fullTextProto.MutableDefaultOperator()->MutableParamValue()->SetParamName(just.Cast<TCoParameter>().Name().StringValue());
                } else {
                    FillLiteralProto(just.Cast<TCoDataCtor>(), *fullTextProto.MutableDefaultOperator()->MutableLiteralValue());
                }
            }

            if (settingsObj.MinimumShouldMatch) {
                auto minimumShouldMatch = TExprBase(settingsObj.MinimumShouldMatch);
                auto just = minimumShouldMatch.Maybe<TCoJust>() ? minimumShouldMatch.Maybe<TCoJust>().Cast().Input() : minimumShouldMatch;
                if (just.Maybe<TCoParameter>()) {
                    fullTextProto.MutableMinimumShouldMatch()->MutableParamValue()->SetParamName(just.Cast<TCoParameter>().Name().StringValue());
                } else {
                    FillLiteralProto(just.Cast<TCoDataCtor>(), *fullTextProto.MutableMinimumShouldMatch()->MutableLiteralValue());
                }
            }

            if (settingsObj.K1Factor) {
                auto k1Factor = TExprBase(settingsObj.K1Factor);
                auto just = k1Factor.Maybe<TCoJust>() ? k1Factor.Maybe<TCoJust>().Cast().Input() : k1Factor;
                if (just.Maybe<TCoParameter>()) {
                    fullTextProto.MutableK1Factor()->MutableParamValue()->SetParamName(just.Cast<TCoParameter>().Name().StringValue());
                } else {
                    FillLiteralProto(just.Cast<TCoDataCtor>(), *fullTextProto.MutableK1Factor()->MutableLiteralValue());
                }
            }

            for(const auto& column: settings.QueryColumns().Cast()) {
                fillCol(tableMeta->Columns.FindPtr(column.StringValue()), fullTextProto.MutableQuerySettings()->AddColumns());
            }

        } else if (auto settings = source.Settings().Maybe<TKqpReadSysViewSourceSettings>()) {
            NKqpProto::TKqpSysViewSource& sysViewProto = *protoSource->MutableSysViewSource();
            FillTablesMap(settings.Table().Cast(), settings.Columns().Cast(), tablesMap);
            FillTableId(settings.Table().Cast(), *sysViewProto.MutableTable());

            auto tableMeta = TablesData->ExistingTable(Cluster, settings.Table().Cast().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            FillColumns(settings.Columns().Cast(), *tableMeta, sysViewProto, allowSystemColumns);

            auto readSettings = TKqpReadTableSettings::Parse(settings.Settings().Cast());
            sysViewProto.SetReverse(readSettings.IsReverse());

            auto ranges = settings.RangesExpr().template Maybe<TCoParameter>();
            if (ranges.IsValid()) {
                auto& rangesParam = *sysViewProto.MutableRanges();
                rangesParam.SetParamName(ranges.Cast().Name().StringValue());
            } else if (!TCoVoid::Match(settings.RangesExpr().Raw())) {
                YQL_ENSURE(
                    TKqlKeyRange::Match(settings.RangesExpr().Raw()),
                    "SysView read ranges should be parameter or KqlKeyRange, got: " << settings.RangesExpr().Cast().Ptr()->Content()
                );
                FillKeyRange(settings.RangesExpr().Cast<TKqlKeyRange>(), *sysViewProto.MutableKeyRange());
            }

            if (readSettings.ItemsLimit) {
                TExprBase expr(readSettings.ItemsLimit);
                if (expr.template Maybe<TCoUint64>()) {
                    FillLiteralProto(expr.Cast<TCoDataCtor>(), *sysViewProto.MutableItemsLimit()->MutableLiteralValue());
                } else if (expr.template Maybe<TCoParameter>()) {
                    sysViewProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.template Cast<TCoParameter>().Name().StringValue());
                } else {
                    YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
                }
            }

        } else {
            YQL_ENSURE(false, "Unsupported source type");
        }
    }

    void FillSource(const TDqSource& source, NKqpProto::TKqpPhyStage& stageProto, NKqpProto::TKqpSource* protoSource, bool allowSystemColumns,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap, TExprContext& ctx)
    {
        const TStringBuf dataSourceCategory = source.DataSource().Cast<TCoDataSource>().Category();
        if (IsIn({NYql::KikimrProviderName, NYql::YdbProviderName, NYql::KqpReadRangesSourceName, NYql::KqpFullTextSourceName, NYql::KqpSysViewSourceName}, dataSourceCategory)) {
            FillKqpSource(source, protoSource, allowSystemColumns, tablesMap);
        } else {
            FillDqInput(source.Ptr(), stageProto, protoSource, dataSourceCategory, ctx, true);
        }
    }

    void FillDqInput(TExprNode::TPtr source, NKqpProto::TKqpPhyStage& stageProto, NKqpProto::TKqpSource* protoSource, TStringBuf dataSourceCategory, TExprContext& ctx, bool isDqSource) {
        // Delegate source filling to dq integration of specific provider
        const auto provider = TypesCtx.DataSourceMap.find(dataSourceCategory);
        YQL_ENSURE(provider != TypesCtx.DataSourceMap.end(), "Unsupported data source category: \"" << dataSourceCategory << "\"");
        NYql::IDqIntegration* dqIntegration = provider->second->GetDqIntegration();
        YQL_ENSURE(dqIntegration, "Unsupported dq source for provider: \"" << dataSourceCategory << "\"");
        auto& externalSource = *protoSource->MutableExternalSource();

        // Partitioning
        TVector<TString> partitionParams;
        TString clusterName;
        // In runtime, number of tasks with Sources is limited by 2x of node count
        // We prepare a lot of partitions and distribute them between these tasks
        // Constraint of 1 task per partition is NOT valid anymore
        auto maxTasksPerStage = Config->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
        IDqIntegration::TPartitionSettings pSettings;
        pSettings.MaxPartitions = maxTasksPerStage;
        pSettings.CanFallback = false;
        pSettings.DataSizePerJob = Config->DataSizePerPartition.Get().GetOrElse(NYql::TDqSettings::TDefault::DataSizePerJob);
        dqIntegration->Partition(*source, partitionParams, &clusterName, ctx, pSettings);
        externalSource.SetTaskParamKey(TString(dataSourceCategory));
        for (const TString& partitionParam : partitionParams) {
            externalSource.AddPartitionedTaskParams(partitionParam);
        }

        if (isDqSource) {
            if (const auto& secureParams = FindOneSecureParam(source, TypesCtx, "source", SecretNames)) {
                externalSource.SetSourceName(secureParams->first);
                externalSource.SetAuthInfo(secureParams->second);
            }

            google::protobuf::Any& settings = *externalSource.MutableSettings();
            TString& sourceType = *externalSource.MutableType();
            dqIntegration->FillSourceSettings(*source, settings, sourceType, maxTasksPerStage, ctx);

            if (settings.Is<NDqProto::TDqIntegrationCommonSettings>()) {
                NDqProto::TDqIntegrationCommonSettings commonSettings;
                YQL_ENSURE(settings.UnpackTo(&commonSettings));
                settings.Swap(commonSettings.MutableSettings());
                FillControlPlaneActors(stageProto, commonSettings);
            }

            TMaybe<IDqIntegration::TSourceWatermarksSettings> watermarksSettings = dqIntegration->ExtractSourceWatermarksSettings(*source, settings, sourceType);
            if (watermarksSettings) {
                auto& protoWatermarksSettings = *externalSource.MutableWatermarksSettings();
                if (watermarksSettings->IdleTimeoutUs) {
                    protoWatermarksSettings.SetIdleTimeoutUs(*watermarksSettings->IdleTimeoutUs);
                }
            }
            YQL_ENSURE(!settings.type_url().empty(), "Data source provider \"" << dataSourceCategory << "\" didn't fill dq source settings for its dq source node");
            YQL_ENSURE(sourceType, "Data source provider \"" << dataSourceCategory << "\" didn't fill dq source settings type for its dq source node");
        } else {
            // Source is embedded into stage as lambda
            externalSource.SetType(TString(dataSourceCategory));
            externalSource.SetEmbedded(true);
        }
    }

    void FillDqRead(const TDqReadWrapBase& readWrapBase, NKqpProto::TKqpPhyStage& stageProto, TExprContext& ctx) {
        for (const auto& flag : readWrapBase.Flags()) {
            if (flag.Value() == "Solid") {
                return;
            }
        }

        const auto read = readWrapBase.Input();
        const ui32 dataSourceChildIndex = 1;
        YQL_ENSURE(read.Ref().ChildrenSize() > dataSourceChildIndex);
        YQL_ENSURE(read.Ref().Child(dataSourceChildIndex)->IsCallable("DataSource"));

        const TStringBuf dataSourceCategory = read.Ref().Child(dataSourceChildIndex)->Child(0)->Content();
        FillDqInput(read.Ptr(), stageProto, stageProto.AddSources(), dataSourceCategory, ctx, false);
    }

    THashMap<TStringBuf, ui32> CreateColumnToOrder(
            const TVector<TStringBuf>& columns,
            const TKikimrTableMetadataPtr& tableMeta,
            const THashSet<TStringBuf>& localDefaultColumnsIds,
            bool keysFirst) {
        THashSet<TStringBuf> usedColumns;
        for (const auto& columnName : columns) {
            usedColumns.insert(columnName);
        }

        THashMap<TStringBuf, ui32> columnToOrder;
        ui32 number = 0;
        if (keysFirst) {
            for (const auto& columnName : tableMeta->KeyColumnNames) {
                YQL_ENSURE(usedColumns.contains(columnName));
                columnToOrder[columnName] = number++;
            }
        }

        const auto isDefaultColumn = [&tableMeta](const TString& columnName) {
            const auto defaultKind = tableMeta->Columns.at(columnName).DefaultKind;
            return defaultKind == NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_SEQUENCE
                || defaultKind == NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_LITERAL;
        };

        // Not DEFAULT columns
        for (const auto& columnName : tableMeta->ColumnOrder) {
            if (usedColumns.contains(columnName)
                    && !columnToOrder.contains(columnName)
                    && !isDefaultColumn(columnName)) {
                columnToOrder[columnName] = number++;
            }
        }

        // Default columns without local processing
        for (const auto& columnName : tableMeta->ColumnOrder) {
            if (usedColumns.contains(columnName)
                    && !columnToOrder.contains(columnName)
                    && !localDefaultColumnsIds.contains(columnName)) {
                AFL_ENSURE(isDefaultColumn(columnName));
                columnToOrder[columnName] = number++;
            }
        }

        // Default columns with local processing
        for (const auto& columnName : tableMeta->ColumnOrder) {
            if (usedColumns.contains(columnName) && !columnToOrder.contains(columnName)) {
                AFL_ENSURE(isDefaultColumn(columnName));
                AFL_ENSURE(localDefaultColumnsIds.contains(columnName));
                columnToOrder[columnName] = number++;
            }
        }

        return columnToOrder;
    }

    void FillKqpSinkSettings(const TKqpTableSinkSettings& settings, NKikimrKqp::TKqpTableSinkSettings& settingsProto, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap, const TDqPhyStage& stage) {
        settingsProto.SetDatabase(Database);

        const auto* structType = GetSeqItemType(stage.Program().Ref().GetTypeAnn())->Cast<TStructExprType>();
        YQL_ENSURE(structType);

        TVector<TStringBuf> columns;
        columns.reserve(structType->GetSize());
        for (const auto& item : structType->GetItems()) {
            columns.emplace_back(item->GetName());
        }

        if (settings.Mode().StringValue() == "replace") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE);
        } else if (settings.Mode().StringValue() == "upsert" || settings.Mode().StringValue().empty() /* for compatibility, will be removed */) {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT);
        } else if (settings.Mode().StringValue() == "insert") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);
        } else if (settings.Mode().StringValue() == "delete") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE);
        } else if (settings.Mode().StringValue() == "update") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE);
        } else if (settings.Mode().StringValue() == "update_conditional") {
            AFL_ENSURE(Config->GetEnableIndexStreamWrite()); // Don't allow this mode for old versions.
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL);
        } else if (settings.Mode().StringValue() == "fill_table") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_FILL);
        } else if (settings.Mode().StringValue() == "upsert_increment") {
            settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT);
        } else {
            YQL_ENSURE(false, "Unsupported sink mode");
        }

        if (settings.Mode().StringValue() != "fill_table") {
            AFL_ENSURE(settings.Table().PathId() != "");
            FillTableId(settings.Table(), *settingsProto.MutableTable());
            FillTablesMap(settings.Table(), columns, tablesMap);

            const auto tableMeta = TablesData->ExistingTable(Cluster, settings.Table().Path()).Metadata;

            auto fillColumnProto = [] (TStringBuf columnName, const NYql::TKikimrColumnMetadata* column, NKikimrKqp::TKqpColumnMetadataProto* columnProto) {
                columnProto->SetId(column->Id);
                columnProto->SetName(TString(columnName));
                columnProto->SetTypeId(column->TypeInfo.GetTypeId());

                if(NScheme::NTypeIds::IsParametrizedType(column->TypeInfo.GetTypeId())) {
                    ProtoFromTypeInfo(column->TypeInfo, column->TypeMod, *columnProto->MutableTypeInfo());
                }
            };

            THashSet<ui32> defaultColumnsIds;
            THashSet<TStringBuf> localDefaultColumns; // for shards DefaultColumnsCount feature
            if (Config->GetEnableIndexStreamWrite()) {
                for (const auto& columnNameAtom : settings.DefaultColumns()) {
                    const auto columnName = TStringBuf(columnNameAtom);
                    const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                    YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + TString(columnName) + "\"");

                    const bool isDefault = columnMeta->DefaultKind == NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_SEQUENCE
                                    ||  columnMeta->DefaultKind == NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_LITERAL;
                    if (isDefault) {
                        defaultColumnsIds.insert(columnMeta->Id);
                        localDefaultColumns.insert(columnName);
                    }
                }
                AFL_ENSURE(tableMeta->Indexes.size() == tableMeta->ImplTables.size());

                std::vector<size_t> affectedIndexes;
                THashSet<size_t> affectedKeysIndexes;
                TVector<TStringBuf> lookupColumns;
                {
                    THashSet<TStringBuf> columnsSet;
                    for (const auto& columnName : columns) {
                        columnsSet.insert(columnName);
                    }
                    THashSet<TStringBuf> mainKeyColumnsSet;
                    for (const auto& columnName : tableMeta->KeyColumnNames) {
                        mainKeyColumnsSet.insert(columnName);
                        AFL_ENSURE(columnsSet.contains(columnName));
                    }

                    for (size_t index = 0; index < tableMeta->Indexes.size(); ++index) {
                        const auto& indexDescription = tableMeta->Indexes[index];

                        if (indexDescription.Type == TIndexDescription::EType::GlobalSync
                            || indexDescription.Type == TIndexDescription::EType::GlobalSyncUnique) {
                            const auto& implTable = tableMeta->ImplTables[index];

                            if (settingsProto.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE
                                    || settingsProto.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL) {
                                if (std::any_of(implTable->Columns.begin(), implTable->Columns.end(), [&](const auto& column) {
                                        return columnsSet.contains(column.first) && !mainKeyColumnsSet.contains(column.first);
                                    })) {
                                        affectedIndexes.push_back(index);
                                }

                                if (std::any_of(implTable->KeyColumnNames.begin(), implTable->KeyColumnNames.end(), [&](const auto& column) {
                                        return columnsSet.contains(column) && !mainKeyColumnsSet.contains(column);
                                    })) {
                                        affectedKeysIndexes.insert(index);
                                }
                            } else {
                                affectedIndexes.push_back(index);
                                affectedKeysIndexes.insert(index);
                            }
                        }
                    }

                    const bool needLookup = std::any_of(affectedIndexes.begin(), affectedIndexes.end(), [&](size_t index) {
                        const auto& indexDescription = tableMeta->Indexes[index];

                        if (indexDescription.Type != TIndexDescription::EType::GlobalSync
                            && indexDescription.Type != TIndexDescription::EType::GlobalSyncUnique) {
                            return false;
                        }
                        const auto& implTable = tableMeta->ImplTables[index];

                        AFL_ENSURE(implTable->Kind == EKikimrTableKind::Datashard);

                        for (const auto& columnName : implTable->KeyColumnNames) {
                            if (settingsProto.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT) {
                                AFL_ENSURE(columnsSet.contains(columnName));
                            } else if (!mainKeyColumnsSet.contains(columnName)) {
                                return true;
                            }
                        }

                        return false;
                    }) || std::any_of(settings.ReturningColumns().begin(), settings.ReturningColumns().end(), [&](const auto& columnName) {
                        return !columnsSet.contains(columnName);
                    });

                    if (needLookup) {
                        AFL_ENSURE(settingsProto.GetType() != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);

                        THashSet<TStringBuf> lookupColumnsSet;
                        for (size_t index = 0; index < tableMeta->Indexes.size(); ++index) {
                            const auto& indexDescription = tableMeta->Indexes[index];

                            if (indexDescription.Type == TIndexDescription::EType::GlobalSync
                                    || indexDescription.Type == TIndexDescription::EType::GlobalSyncUnique) {
                                const auto& implTable = tableMeta->ImplTables[index];

                                for (const auto& [columnName, columnMeta] : implTable->Columns) {
                                    lookupColumnsSet.insert(columnName);
                                }
                            }
                        }

                        for (const auto& columnName : settings.ReturningColumns()) {
                            lookupColumnsSet.insert(columnName);
                        }

                        for (const auto& [columnName, columnMeta] : tableMeta->Columns) {
                            if (!mainKeyColumnsSet.contains(columnName) && lookupColumnsSet.contains(columnName)) {
                                lookupColumns.push_back(columnName);
                                localDefaultColumns.erase(columnName);
                                auto columnProto = settingsProto.AddLookupColumns();
                                fillColumnProto(columnName, &columnMeta, columnProto);
                            }
                        }
                    }
                }

                // Fill indexes write settings
                for (size_t index : affectedIndexes) {
                    const auto& indexDescription = tableMeta->Indexes[index];
                    if (indexDescription.Type != TIndexDescription::EType::GlobalSync
                            && indexDescription.Type != TIndexDescription::EType::GlobalSyncUnique) {
                        continue;
                    }
                    const auto& implTable = tableMeta->ImplTables[index];

                    AFL_ENSURE(implTable->Kind == EKikimrTableKind::Datashard);

                    auto indexSettings = settingsProto.AddIndexes();
                    FillTableId(*implTable, *indexSettings->MutableTable());

                    indexSettings->SetIsUniq(
                        indexDescription.Type == TIndexDescription::EType::GlobalSyncUnique
                        && settingsProto.GetType() != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE
                        && affectedKeysIndexes.contains(index));

                    for (const auto& columnName : implTable->KeyColumnNames) {
                        const auto columnMeta = implTable->Columns.FindPtr(columnName);
                        YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + TString(columnName) + "\"");

                        auto keyColumnProto = indexSettings->AddKeyColumns();
                        fillColumnProto(columnName, columnMeta, keyColumnProto);
                    }

                    indexSettings->SetKeyPrefixSize(indexDescription.KeyColumns.size());

                    TVector<TStringBuf> indexColumns;
                    THashSet<TStringBuf> indexColumnsSet;
                    indexColumns.reserve(implTable->Columns.size());

                    for (const auto& columnsList : {columns, lookupColumns}) {
                        for (const auto& columnName : columnsList) {
                            const auto columnMeta = implTable->Columns.FindPtr(columnName);
                            if (columnMeta && indexColumnsSet.insert(columnName).second) {
                                indexColumns.emplace_back(columnName);

                                auto columnProto = indexSettings->AddColumns();
                                fillColumnProto(columnName, columnMeta, columnProto);
                            }
                        }
                    }

                    const auto indexColumnToOrder = CreateColumnToOrder(
                        indexColumns,
                        implTable,
                        localDefaultColumns,
                        true);
                    for (const auto& columnName: indexColumns) {
                        indexSettings->AddWriteIndexes(indexColumnToOrder.at(columnName));
                    }
                    FillTablesMap(
                        implTable->Name,
                        indexColumns,
                        tablesMap);
                }

                for (const auto& columnName : settings.ReturningColumns()) {
                    const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                    YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + TString(columnName) + "\"");
                    auto columnProto = settingsProto.AddReturningColumns();
                    fillColumnProto(columnName, columnMeta, columnProto);
                }
            } else {
                AFL_ENSURE(localDefaultColumns.empty());
                AFL_ENSURE(defaultColumnsIds.empty());
            }

            for (const auto& columnName : tableMeta->KeyColumnNames) {
                const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + TString(columnName) + "\"");

                auto keyColumnProto = settingsProto.AddKeyColumns();
                fillColumnProto(columnName, columnMeta, keyColumnProto);
            }

            for (const auto& columnName : columns) {
                const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + TString(columnName) + "\"");

                auto columnProto = settingsProto.AddColumns();
                fillColumnProto(columnName, columnMeta, columnProto);
            }

            AFL_ENSURE(tableMeta->Kind == EKikimrTableKind::Datashard || tableMeta->Kind == EKikimrTableKind::Olap);
            const auto columnToOrder = CreateColumnToOrder(
                columns,
                tableMeta,
                localDefaultColumns,
                tableMeta->Kind == EKikimrTableKind::Datashard);
            for (const auto& columnName : columns) {
                settingsProto.AddWriteIndexes(columnToOrder.at(columnName));
            }

            AFL_ENSURE(settingsProto.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT
                        || defaultColumnsIds.empty());
            for (const auto& defaultColumnId : defaultColumnsIds) { // from plan!!!
                settingsProto.AddDefaultColumnsIds(defaultColumnId);
            }

            settingsProto.SetIsOlap(tableMeta->Kind == EKikimrTableKind::Olap);

            const bool inconsistentWrite = settings.InconsistentWrite().Value() == "true"sv;
            AFL_ENSURE(!inconsistentWrite || (OptimizeCtx.UserRequestContext && OptimizeCtx.UserRequestContext->IsStreamingQuery));
            settingsProto.SetInconsistentTx(inconsistentWrite);
        } else {
            // Table info will be filled during execution after resolving table by name.
            settingsProto.MutableTable()->SetPath(TString(settings.Table().Path()));
            for (const auto& column : columns) {
                settingsProto.AddInputColumns(TString(column));
            }

            AFL_ENSURE(settings.InconsistentWrite().StringValue() == "true");
            settingsProto.SetInconsistentTx(true);

            AFL_ENSURE(settings.Priority().StringValue() == "0");
            AFL_ENSURE(settings.StreamWrite().StringValue() == "true");
        }

        settingsProto.SetPriority(FromString<i64>(settings.Priority().StringValue()));

        if (const auto streamWrite = settings.StreamWrite(); streamWrite.StringValue() == "true") {
            settingsProto.SetEnableStreamWrite(true);
        }

        if (const auto isBatch = settings.IsBatch(); isBatch.StringValue() == "true") {
            settingsProto.SetIsBatch(true);
        }

        if (const auto isIndexImplTable = settings.IsIndexImplTable(); isIndexImplTable.StringValue() == "true") {
            settingsProto.SetIsIndexImplTable(true);
        }
    }

    bool IsTableSink(const TStringBuf dataSinkCategory) const {
        return dataSinkCategory == NYql::KikimrProviderName
            || dataSinkCategory == NYql::YdbProviderName
            || dataSinkCategory == NYql::KqpTableSinkName;
    }

    void FillSink(const TDqSink& sink, NKqpProto::TKqpSink* protoSink, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap, const TDqPhyStage& stage, TExprContext& ctx) {
        Y_UNUSED(ctx);
        const TStringBuf dataSinkCategory = sink.DataSink().Cast<TCoDataSink>().Category();
        if (IsTableSink(dataSinkCategory)) {
            auto settings = sink.Settings().Maybe<TKqpTableSinkSettings>();
            YQL_ENSURE(settings, "Unsupported sink type");
            NKqpProto::TKqpInternalSink& internalSinkProto = *protoSink->MutableInternalSink();
            internalSinkProto.SetType(TString(NYql::KqpTableSinkName));

            NKikimrKqp::TKqpTableSinkSettings settingsProto;
            FillKqpSinkSettings(settings.Cast(), settingsProto, tablesMap, stage);
            internalSinkProto.MutableSettings()->PackFrom(settingsProto);
        } else {
            // Delegate sink filling to dq integration of specific provider
            const auto provider = TypesCtx.DataSinkMap.find(dataSinkCategory);
            YQL_ENSURE(provider != TypesCtx.DataSinkMap.end(), "Unsupported data sink category: \"" << dataSinkCategory << "\"");
            NYql::IDqIntegration* dqIntegration = provider->second->GetDqIntegration();
            YQL_ENSURE(dqIntegration, "Unsupported dq sink for provider: \"" << dataSinkCategory << "\"");
            auto& externalSink = *protoSink->MutableExternalSink();
            google::protobuf::Any& settings = *externalSink.MutableSettings();
            TString& sinkType = *externalSink.MutableType();
            dqIntegration->FillSinkSettings(sink.Ref(), settings, sinkType);
            YQL_ENSURE(!settings.type_url().empty(), "Data sink provider \"" << dataSinkCategory << "\" did't fill dq sink settings for its dq sink node");
            YQL_ENSURE(sinkType, "Data sink provider \"" << dataSinkCategory << "\" did't fill dq sink settings type for its dq sink node");

            if (const auto& secureParams = FindOneSecureParam(sink.Ptr(), TypesCtx, "sink", SecretNames)) {
                externalSink.SetSinkName(secureParams->first);
                externalSink.SetAuthInfo(secureParams->second);
            }
        }
    }

    void FillStreamLookupVectorTop(NKqpProto::TKqpPhyCnStreamLookup& streamLookupProto,
        const TKqpCnStreamLookup& streamLookup, const TKqpStreamLookupSettings& settings) {
        NKqpProto::TKqpPhyVectorTopK& vectorTopK = *streamLookupProto.MutableVectorTopK();
        const auto implTablePath = streamLookup.Table().Path();
        const auto mainTableFromImpl = TablesData->GetMainTableIfTableIsImplTableOfIndex(Cluster, implTablePath);
        const auto mainTable = mainTableFromImpl ? mainTableFromImpl : &TablesData->ExistingTable(Cluster, implTablePath);

        const TIndexDescription *indexDesc = nullptr;
        for (const auto& index: mainTable->Metadata->Indexes) {
            if (index.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree &&
                index.Name == settings.VectorTopIndex) {
                indexDesc = &index;
            }
        }
        YQL_ENSURE(indexDesc);

        // Index settings
        auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc->SpecializedIndexDescription);
        *vectorTopK.MutableSettings() = kmeansDesc.GetSettings().Getsettings();

        // Column index
        THashMap<TStringBuf, ui32> readColumnIndexes;
        ui32 columnIdx = 0;
        for (const auto& column: streamLookupProto.GetColumns()) {
            readColumnIndexes[column] = columnIdx++;
        }
        YQL_ENSURE(readColumnIndexes.contains(settings.VectorTopColumn));
        vectorTopK.SetColumn(readColumnIndexes.at(settings.VectorTopColumn));

        // Unique columns - required when we read from the index posting table and overlap is enabled
        if (settings.VectorTopDistinct) {
            for (const auto& keyColumn: mainTable->Metadata->KeyColumnNames) {
                YQL_ENSURE(readColumnIndexes.contains(keyColumn));
                vectorTopK.AddDistinctColumns(readColumnIndexes.at(keyColumn));
            }
        }

        // Limit - may be a parameter which will be linked later
        TExprBase expr(settings.VectorTopLimit);
        if (expr.Maybe<TCoUint64>()) {
            FillLiteralProto(expr.Cast<TCoDataCtor>(), *vectorTopK.MutableLimit()->MutableLiteralValue());
        } else if (expr.Maybe<TCoParameter>()) {
            vectorTopK.MutableLimit()->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected Limit callable " << expr.Ref().Content());
        }

        // Target vector - may be a parameter which will be linked later
        expr = TExprBase(settings.VectorTopTarget);
        if (expr.Maybe<TCoString>()) {
            FillLiteralProto(expr.Cast<TCoDataCtor>(), *vectorTopK.MutableTargetVector()->MutableLiteralValue());
        } else if (expr.Maybe<TCoParameter>()) {
            vectorTopK.MutableTargetVector()->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected TargetVector callable " << expr.Ref().Content());
        }
    }

    void FillConnection(
        const TDqConnection& connection,
        const TMap<ui64, ui32>& stagesMap,
        NKqpProto::TKqpPhyConnection& connectionProto,
        TExprContext& ctx,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap,
        THashMap<ui64, NKqpProto::TKqpPhyStage*>& physicalStageByID,
        const TDqPhyStage* stage,
        ui32 inputIndex
    ) {
        auto inputStageIndex = stagesMap.FindPtr(connection.Output().Stage().Ref().UniqueId());
        YQL_ENSURE(inputStageIndex, "stage #" << connection.Output().Stage().Ref().UniqueId() << " not found in stages map: "
            << PrintKqpStageOnly(connection.Output().Stage(), ctx));

        auto outputIndex = FromString<ui32>(connection.Output().Index().Value());

        connectionProto.SetStageIndex(*inputStageIndex);
        connectionProto.SetOutputIndex(outputIndex);

        if (connection.Maybe<TDqCnUnionAll>()) {
            connectionProto.MutableUnionAll();
            return;
        }

        if (connection.Maybe<TDqCnParallelUnionAll>()) {
            connectionProto.MutableParallelUnionAll();
            return;
        }

        if (auto maybeShuffle = connection.Maybe<TDqCnHashShuffle>()) {
            const auto& shuffle = maybeShuffle.Cast();
            auto& shuffleProto = *connectionProto.MutableHashShuffle();
            for (const auto& keyColumn : shuffle.KeyColumns()) {
                shuffleProto.AddKeyColumns(TString(keyColumn));
            }

            NDq::EHashShuffleFuncType hashFuncType = Config->GetDqDefaultHashShuffleFuncType();
            if (shuffle.HashFunc().IsValid()) {
                hashFuncType = FromString<NDq::EHashShuffleFuncType>(shuffle.HashFunc().Cast().StringValue());
            }

            switch (hashFuncType) {
                using enum NDq::EHashShuffleFuncType;
                case HashV1: {
                    shuffleProto.MutableHashV1();
                    break;
                }
                case HashV2: {
                    shuffleProto.MutableHashV2();
                    break;
                }
                case ColumnShardHashV1: {
                    auto& columnHashV1 = *shuffleProto.MutableColumnShardHashV1();

                    const auto& outputType = NYql::NDq::GetDqConnectionType(connection, ctx);
                    auto structType = outputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    for (const auto& column: shuffle.KeyColumns().Ptr()->Children()) {
                        auto ty = NYql::NDq::GetColumnType(connection, *structType, column->Content(), column->Pos(), ctx);
                        if (ty->GetKind() == ETypeAnnotationKind::List) {
                            ty = ty->Cast<TListExprType>()->GetItemType();
                        }
                        NYql::NUdf::EDataSlot slot;
                        switch (ty->GetKind()) {
                            case ETypeAnnotationKind::Data: {
                                slot = ty->Cast<TDataExprType>()->GetSlot();
                                break;
                            }
                            case ETypeAnnotationKind::Optional: {
                                auto optionalType = ty->Cast<TOptionalExprType>()->GetItemType();
                                if (optionalType->GetKind() == ETypeAnnotationKind::List) {
                                    optionalType = optionalType->Cast<TListExprType>()->GetItemType();
                                }
                                Y_ENSURE(
                                    optionalType->GetKind() == ETypeAnnotationKind::Data,
                                    TStringBuilder{} << "Can't retrieve type from optional" << static_cast<std::int64_t>(optionalType->GetKind()) << "for ColumnHashV1 Shuffling"
                                );
                                slot = optionalType->Cast<TDataExprType>()->GetSlot();
                                break;
                            }
                            default: {
                                Y_ENSURE(false, TStringBuilder{} << "Can't get type for ColumnHashV1 Shuffling: " << static_cast<std::int64_t>(ty->GetKind()));
                            }
                        }

                        auto typeId = GetDataTypeInfo(slot).TypeId;
                        columnHashV1.AddKeyColumnTypes(typeId);
                    }
                    break;
                }
            };

            if (Config->GetEnableSpillingInHashJoinShuffleConnections() && shuffle.UseSpilling()) {
                shuffleProto.SetUseSpilling(FromStringWithDefault<bool>(shuffle.UseSpilling().Cast().StringValue(), false));
            }

            return;
        }

        if (connection.Maybe<TDqCnMap>()) {
            auto stageID = connection.Output().Stage().Ref().UniqueId();
            auto physicalStage = physicalStageByID[stageID];
            Y_ENSURE(physicalStage != nullptr, TStringBuf{} << "stage#" << stageID);
            physicalStage->SetIsShuffleEliminated(true);
            connectionProto.MutableMap();
            return;
        }

        if (connection.Maybe<TDqCnBroadcast>()) {
            connectionProto.MutableBroadcast();
            return;
        }

        if (connection.Maybe<TDqCnResult>()) {
            connectionProto.MutableResult();
            return;
        }

        if (connection.Maybe<TDqCnValue>()) {
            connectionProto.MutableValue();
            return;
        }

        if (auto maybeMerge = connection.Maybe<TDqCnMerge>()) {
            auto& mergeProto = *connectionProto.MutableMerge();
            for (const auto& sortColumn : maybeMerge.Cast().SortColumns()) {
                auto newSortColumn = mergeProto.AddSortColumns();
                newSortColumn->SetColumn(sortColumn.Column().StringValue());
                newSortColumn->SetAscending(sortColumn.SortDirection().Value() == TTopSortSettings::AscendingSort);
            }
            return;
        }

        if (auto maybeSequencer = connection.Maybe<TKqpCnSequencer>()) {
            TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
            auto& sequencerProto = *connectionProto.MutableSequencer();

            auto sequencer = maybeSequencer.Cast();
            auto tableMeta = TablesData->ExistingTable(Cluster, sequencer.Table().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            FillTableId(sequencer.Table(), *sequencerProto.MutableTable());
            FillTablesMap(sequencer.Table(), sequencer.Columns(), tablesMap);

            const auto resultType = sequencer.Ref().GetTypeAnn();
            YQL_ENSURE(resultType, "Empty sequencer result type");
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Stream, "Unexpected sequencer result type");
            const auto resultItemType = resultType->Cast<TStreamExprType>()->GetItemType();
            sequencerProto.SetOutputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *resultItemType), TypeEnv));

            const auto inputNodeType = sequencer.InputItemType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(inputNodeType, "Empty sequencer input type");
            YQL_ENSURE(inputNodeType->GetKind() == ETypeAnnotationKind::List, "Unexpected input type");
            const auto inputItemType = inputNodeType->Cast<TListExprType>()->GetItemType();
            sequencerProto.SetInputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *inputItemType), TypeEnv));

            THashSet<TString> autoIncrementColumns;
            for(const auto& column : sequencer.DefaultConstraintColumns()) {
                autoIncrementColumns.insert(column.StringValue());
            }

            YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Struct);
            for(const auto* column: resultItemType->Cast<TStructExprType>()->GetItems()) {
                auto columnMeta = tableMeta->Columns.FindPtr(TString(column->GetName()));
                YQL_ENSURE(columnMeta);
                auto* columnProto = sequencerProto.AddColumns();
                columnProto->SetName(columnMeta->Name);
                columnProto->SetId(columnMeta->Id);
                columnProto->SetTypeId(columnMeta->TypeInfo.GetTypeId());
                if (NScheme::NTypeIds::IsParametrizedType(columnMeta->TypeInfo.GetTypeId())) {
                    ProtoFromTypeInfo(columnMeta->TypeInfo, columnMeta->TypeMod, *columnProto->MutableTypeInfo());
                }

                if (autoIncrementColumns.contains(columnMeta->Name)) {
                    // it means, that column is a generated column
                    // so it means we should set default value for this column
                    YQL_ENSURE(columnMeta->IsDefaultFromLiteral() || columnMeta->IsDefaultFromSequence());
                    columnProto->SetDefaultKind(columnMeta->DefaultKind);
                    if (columnMeta->IsDefaultFromLiteral()) {
                        columnProto->MutableDefaultFromLiteral()->CopyFrom(columnMeta->DefaultFromLiteral);
                    } else if (columnMeta->IsDefaultFromSequence()) {
                        columnProto->SetDefaultFromSequence(columnMeta->DefaultFromSequence);
                        columnMeta->DefaultFromSequencePathId.ToMessage(columnProto->MutableDefaultFromSequencePathId());
                    }
                }

            }

            return;
        }

        if (auto maybeStreamLookup = connection.Maybe<TKqpCnStreamLookup>()) {
            TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
            auto& streamLookupProto = *connectionProto.MutableStreamLookup();
            auto streamLookup = maybeStreamLookup.Cast();
            auto tableMeta = TablesData->ExistingTable(Cluster, streamLookup.Table().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            streamLookupProto.SetIsTableImmutable(TablesData->IsTableImmutable(Cluster, streamLookup.Table().Path()));

            FillTablesMap(streamLookup.Table(), streamLookup.Columns(), tablesMap);
            FillTableId(streamLookup.Table(), *streamLookupProto.MutableTable());

            const auto inputType = streamLookup.InputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(inputType, "Empty stream lookup input type");
            YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::List, "Unexpected stream lookup input type");
            const auto inputItemType = inputType->Cast<TListExprType>()->GetItemType();
            streamLookupProto.SetLookupKeysType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *inputItemType), TypeEnv));

            const auto resultType = streamLookup.Ref().GetTypeAnn();
            YQL_ENSURE(resultType, "Empty stream lookup result type");
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Stream, "Unexpected stream lookup result type");
            const auto resultItemType = resultType->Cast<TStreamExprType>()->GetItemType();
            streamLookupProto.SetResultType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *resultItemType), TypeEnv));

            auto settings = TKqpStreamLookupSettings::Parse(streamLookup);
            streamLookupProto.SetLookupStrategy(GetStreamLookupStrategy(settings.Strategy));
            streamLookupProto.SetKeepRowsOrder(Config->OrderPreservingLookupJoinEnabled());
            if (settings.AllowNullKeysPrefixSize) {
                streamLookupProto.SetAllowNullKeysPrefixSize(*settings.AllowNullKeysPrefixSize);
            }

            switch (streamLookupProto.GetLookupStrategy()) {
                case NKqpProto::EStreamLookupStrategy::LOOKUP:
                case NKqpProto::EStreamLookupStrategy::UNIQUE: {
                    YQL_ENSURE(inputItemType->GetKind() == ETypeAnnotationKind::Struct);
                    const auto& lookupKeyColumns = inputItemType->Cast<TStructExprType>()->GetItems();
                    for (const auto keyColumn : lookupKeyColumns) {
                        YQL_ENSURE(tableMeta->Columns.FindPtr(keyColumn->GetName()),
                            "Unknown column: " << keyColumn->GetName());
                        streamLookupProto.AddKeyColumns(TString(keyColumn->GetName()));
                    }

                    YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Struct);
                    const auto& resultColumns = resultItemType->Cast<TStructExprType>()->GetItems();
                    for (const auto column : resultColumns) {
                        const auto &systemColumns = GetSystemColumns();
                        YQL_ENSURE(tableMeta->Columns.FindPtr(column->GetName())
                            || systemColumns.find(column->GetName()) != systemColumns.end(),
                            "Unknown column: " << column->GetName());
                        streamLookupProto.AddColumns(TString(column->GetName()));
                    }

                    break;
                }
                case NKqpProto::EStreamLookupStrategy::JOIN:
                case NKqpProto::EStreamLookupStrategy::SEMI_JOIN: {
                    YQL_ENSURE(inputItemType->GetKind() == ETypeAnnotationKind::Tuple);
                    const auto inputTupleType = inputItemType->Cast<TTupleExprType>();
                    YQL_ENSURE(inputTupleType->GetSize() == 2 || inputTupleType->GetSize() == 3);

                    YQL_ENSURE(inputTupleType->GetItems()[1]->GetKind() == ETypeAnnotationKind::Optional);
                    const auto joinKeyType = inputTupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
                    YQL_ENSURE(joinKeyType->GetKind() == ETypeAnnotationKind::Struct);
                    const auto& joinKeyColumns = joinKeyType->Cast<TStructExprType>()->GetItems();
                    for (const auto keyColumn : joinKeyColumns) {
                        YQL_ENSURE(tableMeta->Columns.FindPtr(keyColumn->GetName()),
                            "Unknown column: " << keyColumn->GetName());
                        streamLookupProto.AddKeyColumns(TString(keyColumn->GetName()));
                    }

                    YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Tuple);
                    const auto resultTupleType = resultItemType->Cast<TTupleExprType>();
                    YQL_ENSURE(resultTupleType->GetSize() == 3);

                    YQL_ENSURE(resultTupleType->GetItems()[1]->GetKind() == ETypeAnnotationKind::Optional);
                    auto rightRowOptionalType = resultTupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
                    YQL_ENSURE(rightRowOptionalType->GetKind() == ETypeAnnotationKind::Struct);
                    const auto& rightColumns = rightRowOptionalType->Cast<TStructExprType>()->GetItems();
                    for (const auto column : rightColumns) {
                        const auto& systemColumns = GetSystemColumns();
                        YQL_ENSURE(tableMeta->Columns.FindPtr(column->GetName())
                            || systemColumns.find(column->GetName()) != systemColumns.end(),
                            "Unknown column: " << column->GetName());
                        streamLookupProto.AddColumns(TString(column->GetName()));
                    }

                    break;
                }
                default:
                    YQL_ENSURE(false, "Unexpected lookup strategy for stream lookup: " << settings.Strategy);
            }

            if (settings.VectorTopColumn) {
                FillStreamLookupVectorTop(streamLookupProto, streamLookup, settings);
            }

            return;
        }

        if (auto maybeVectorResolve = connection.Maybe<TKqpCnVectorResolve>()) {
            TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
            auto& vectorResolveProto = *connectionProto.MutableVectorResolve();
            auto vectorResolve = maybeVectorResolve.Cast();

            auto tableMeta = TablesData->ExistingTable(Cluster, vectorResolve.Table().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            TIndexDescription *indexDesc = nullptr;
            for (auto& index: tableMeta->Indexes) {
                if (index.Name == vectorResolve.Index().Value()) {
                    indexDesc = &index;
                }
            }
            YQL_ENSURE(indexDesc);

            // Index settings
            auto& kmeansDesc = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(indexDesc->SpecializedIndexDescription);
            *vectorResolveProto.MutableIndexSettings() = kmeansDesc.GetSettings().Getsettings();
            vectorResolveProto.SetOverlapClusters(kmeansDesc.GetSettings().overlap_clusters());
            vectorResolveProto.SetOverlapRatio(kmeansDesc.GetSettings().overlap_ratio());

            // Main table
            FillTablesMap(vectorResolve.Table(), tablesMap);
            FillTableId(vectorResolve.Table(), *vectorResolveProto.MutableTable());

            // Index level table
            TString levelTablePath = TStringBuilder()
                << vectorResolve.Table().Path().Value()
                << "/" << vectorResolve.Index().Value()
                << "/" << NTableIndex::NKMeans::LevelTable;
            auto levelTableMeta = TablesData->ExistingTable(Cluster, levelTablePath).Metadata;
            YQL_ENSURE(levelTableMeta);

            tablesMap.emplace(levelTablePath, THashSet<TStringBuf>{});
            tablesMap[levelTablePath].emplace(NTableIndex::NKMeans::ParentColumn);
            tablesMap[levelTablePath].emplace(NTableIndex::NKMeans::IdColumn);
            tablesMap[levelTablePath].emplace(NTableIndex::NKMeans::CentroidColumn);

            vectorResolveProto.MutableLevelTable()->SetPath(levelTablePath);
            vectorResolveProto.MutableLevelTable()->SetOwnerId(levelTableMeta->PathId.OwnerId());
            vectorResolveProto.MutableLevelTable()->SetTableId(levelTableMeta->PathId.TableId());
            vectorResolveProto.MutableLevelTable()->SetVersion(levelTableMeta->SchemaVersion);

            // Input and output types

            const auto inputType = vectorResolve.InputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(inputType, "Empty vector resolve input type");
            YQL_ENSURE(inputType->GetKind() == ETypeAnnotationKind::List, "Unexpected vector resolve input type");
            const auto inputItemType = inputType->Cast<TListExprType>()->GetItemType();
            vectorResolveProto.SetInputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *inputItemType), TypeEnv));

            const auto outputType = vectorResolve.Ref().GetTypeAnn();
            YQL_ENSURE(outputType, "Empty vector resolve output type");
            YQL_ENSURE(outputType->GetKind() == ETypeAnnotationKind::Stream, "Unexpected vector resolve output type");
            const auto outputItemType = outputType->Cast<TStreamExprType>()->GetItemType();
            vectorResolveProto.SetOutputType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *outputItemType), TypeEnv));

            // Input columns. Input is strictly mapped to main table's columns to ease passing their types through PhyCnVectorResolve.
            // For prefixed indexes, input must contain the __ydb_parent column referring the root cluster ID of the row's prefix.

            TMap<TString, ui32> columnIndexes;
            YQL_ENSURE(inputItemType->GetKind() == ETypeAnnotationKind::Struct);
            const auto& inputColumns = inputItemType->Cast<TStructExprType>()->GetItems();
            ui32 n = 0;
            for (const auto inputColumn : inputColumns) {
                auto name = TString(inputColumn->GetName());
                vectorResolveProto.AddColumns(name);
                if (name != NTableIndex::NKMeans::ParentColumn) {
                    YQL_ENSURE(tableMeta->Columns.FindPtr(name), "Unknown column: " << name);
                    tablesMap[vectorResolve.Table().Path()].emplace(name);
                }
                columnIndexes[name] = n++;
            }

            // Copy columns & vector column index

            auto vectorColumn = indexDesc->KeyColumns.back();
            YQL_ENSURE(columnIndexes.contains(vectorColumn));
            vectorResolveProto.SetVectorColumnIndex(columnIndexes.at(vectorColumn));

            if (indexDesc->KeyColumns.size() > 1) {
                // Prefixed index
                YQL_ENSURE(columnIndexes.contains(NTableIndex::NKMeans::ParentColumn));
                vectorResolveProto.SetRootClusterColumnIndex(columnIndexes.at(NTableIndex::NKMeans::ParentColumn));
            }

            TSet<TString> copyColumns;
            copyColumns.insert(NTableIndex::NKMeans::ParentColumn);
            for (const auto& keyColumn : tableMeta->KeyColumnNames) {
                copyColumns.insert(keyColumn);
            }
            if (vectorResolve.WithData() == "true") {
                for (const auto& dataColumn : indexDesc->DataColumns) {
                    copyColumns.insert(dataColumn);
                }
            }

            // Maintain alphabetical output column order

            ui32 pos = 0;
            for (const auto& copyCol : copyColumns) {
                if (copyCol == NTableIndex::NKMeans::ParentColumn) {
                    vectorResolveProto.SetClusterColumnOutPos(pos);
                } else {
                    YQL_ENSURE(columnIndexes.contains(copyCol));
                    vectorResolveProto.AddCopyColumnIndexes(columnIndexes.at(copyCol));
                    pos++;
                }
            }

            return;
        }

        if (auto maybeDqSourceStreamLookup = connection.Maybe<TDqCnStreamLookup>()) {
            const auto streamLookup = maybeDqSourceStreamLookup.Cast();
            const auto lookupSourceWrap = streamLookup.RightInput().Cast<TDqLookupSourceWrap>();

            const TStringBuf dataSourceCategory = lookupSourceWrap.DataSource().Category();
            const auto provider = TypesCtx.DataSourceMap.find(dataSourceCategory);
            YQL_ENSURE(provider != TypesCtx.DataSourceMap.end(), "Unsupported data source category: \"" << dataSourceCategory << "\"");
            NYql::IDqIntegration* dqIntegration = provider->second->GetDqIntegration();
            YQL_ENSURE(dqIntegration, "Unsupported dq source for provider: \"" << dataSourceCategory << "\"");

            auto& dqSourceLookupCn = *connectionProto.MutableDqSourceStreamLookup();
            auto& lookupSource = *dqSourceLookupCn.MutableLookupSource();
            auto& lookupSourceSettings = *lookupSource.MutableSettings();
            auto& lookupSourceType = *lookupSource.MutableType();
            dqIntegration->FillLookupSourceSettings(lookupSourceWrap.Ref(), lookupSourceSettings, lookupSourceType);
            YQL_ENSURE(!lookupSourceSettings.type_url().empty(), "Data source provider \"" << dataSourceCategory << "\" did't fill dq source settings for its dq source node");
            YQL_ENSURE(lookupSourceType, "Data source provider \"" << dataSourceCategory << "\" did't fill dq source settings type for its dq source node");

            if (const auto& secureParams = FindOneSecureParam(lookupSourceWrap.Ptr(), TypesCtx, "streamLookupSource", SecretNames)) {
                lookupSource.SetSourceName(secureParams->first);
                lookupSource.SetAuthInfo(secureParams->second);
            }

            const auto& streamLookupOutput = streamLookup.Output();
            const auto connectionInputRowType = GetSeqItemType(streamLookupOutput.Ref().GetTypeAnn());
            YQL_ENSURE(connectionInputRowType->GetKind() == ETypeAnnotationKind::Struct);
            const auto connectionOutputRowType = GetSeqItemType(streamLookup.Ref().GetTypeAnn());
            YQL_ENSURE(connectionOutputRowType->GetKind() == ETypeAnnotationKind::Struct);
            YQL_ENSURE(stage);
            dqSourceLookupCn.SetConnectionInputRowType(NYql::NCommon::GetSerializedTypeAnnotation(connectionInputRowType));
            dqSourceLookupCn.SetConnectionOutputRowType(NYql::NCommon::GetSerializedTypeAnnotation(connectionOutputRowType));
            dqSourceLookupCn.SetLookupRowType(NYql::NCommon::GetSerializedTypeAnnotation(lookupSourceWrap.RowType().Ref().GetTypeAnn()));
            dqSourceLookupCn.SetInputStageRowType(NYql::NCommon::GetSerializedTypeAnnotation(GetSeqItemType(streamLookupOutput.Stage().Program().Ref().GetTypeAnn())));
            dqSourceLookupCn.SetOutputStageRowType(NYql::NCommon::GetSerializedTypeAnnotation(GetSeqItemType(stage->Program().Args().Arg(inputIndex).Ref().GetTypeAnn())));

            const TString leftLabel(streamLookup.LeftLabel());
            dqSourceLookupCn.SetLeftLabel(leftLabel);
            dqSourceLookupCn.SetRightLabel(streamLookup.RightLabel().StringValue());
            dqSourceLookupCn.SetJoinType(streamLookup.JoinType().StringValue());
            dqSourceLookupCn.SetCacheLimit(FromString<ui64>(streamLookup.MaxCachedRows()));
            dqSourceLookupCn.SetCacheTtlSeconds(FromString<ui64>(streamLookup.TTL()));
            dqSourceLookupCn.SetMaxDelayedRows(FromString<ui64>(streamLookup.MaxDelayedRows()));

            if (const auto maybeMultiget = streamLookup.IsMultiget()) {
                dqSourceLookupCn.SetIsMultiGet(FromString<bool>(maybeMultiget.Cast()));
            }

            if (const auto maybeIsMultiMatches = streamLookup.IsMultiMatches()) {
                dqSourceLookupCn.SetIsMultiMatches(FromString<bool>(maybeIsMultiMatches.Cast()));
            }

            for (const auto& key : streamLookup.LeftJoinKeyNames()) {
                *dqSourceLookupCn.AddLeftJoinKeyNames() = leftLabel ? RemoveJoinAliases(key) : key;
            }

            for (const auto& key : streamLookup.RightJoinKeyNames()) {
                *dqSourceLookupCn.AddRightJoinKeyNames() = RemoveJoinAliases(key);
            }

            return;
        }

        YQL_ENSURE(false, "Unexpected connection type: " << connection.CallableName());
    }

    void FillResultType(NKikimr::NMiniKQL::TType* miniKqlResultType, NKqpProto::TKqpPhyOpReadOlapRanges& opProto)
    {
        ExportTypeToProto(miniKqlResultType, *opProto.MutableResultType());
    }

    NKikimr::NMiniKQL::TType* GetMKqlResultType(const TTypeAnnotationNode* resultType)
    {
        YQL_ENSURE(resultType->GetKind() == NYql::ETypeAnnotationKind::Flow, "Unexpected type: " << NYql::FormatType(resultType));
        TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
        const auto resultItemType = resultType->Cast<TFlowExprType>()->GetItemType();
        return CompileType(pgmBuilder, *resultItemType);
    }

private:
    struct TResultBindingInfo {
        ui32 OriginalIndex;
        ui32 TxIndex;
        ui32 ResultIndex;
        bool IsDiscard;
    };

    THashSet<std::pair<ui32, ui32>> BuildChannelDependencies(const TKqpPhysicalQuery& query) {
        THashSet<std::pair<ui32, ui32>> shouldCreateChannel;

        for (ui32 txIdx = 0; txIdx < query.Transactions().Size(); ++txIdx) {
            const auto& tx = query.Transactions().Item(txIdx);
            for (const auto& paramBinding : tx.ParamBindings()) {
                if (auto maybeResultBinding = paramBinding.Binding().Maybe<TKqpTxResultBinding>()) {
                    auto resultBinding = maybeResultBinding.Cast();
                    auto refTxIndex = FromString<ui32>(resultBinding.TxIndex());
                    auto refResultIndex = FromString<ui32>(resultBinding.ResultIndex());
                    shouldCreateChannel.insert({refTxIndex, refResultIndex});
                }
            }
        }

        return shouldCreateChannel;
    }

    std::vector<TResultBindingInfo> BuildResultBindingsInfo(
        const TKqpPhysicalQuery& query,
        EPhysicalQueryType queryType,
        const TDynBitMap& resultDiscardFlags,
        THashSet<std::pair<ui32, ui32>>& shouldCreateChannel)
    {
        std::vector<TResultBindingInfo> resultBindings;
        resultBindings.reserve(query.Results().Size());

        for (ui32 i = 0; i < query.Results().Size(); ++i) {
            const auto& result = query.Results().Item(i);
            if (result.Maybe<TKqpTxResultBinding>()) {
                auto binding = result.Cast<TKqpTxResultBinding>();
                auto txIndex = FromString<ui32>(binding.TxIndex().Value());
                auto resultIndex = FromString<ui32>(binding.ResultIndex());

                YQL_ENSURE(i < resultDiscardFlags.Size(),
                    "Index " << i << " is out of bounds for resultDiscardFlags with size " << resultDiscardFlags.Size());

                bool markedDiscard = (queryType == EPhysicalQueryType::GenericQuery) && resultDiscardFlags[i];
                bool canDiscard = markedDiscard && !shouldCreateChannel.contains(std::pair{txIndex, resultIndex});

                resultBindings.push_back({i, txIndex, resultIndex, canDiscard});

                if (!canDiscard) {
                    shouldCreateChannel.insert({txIndex, resultIndex});
                }
            }
        }
        return resultBindings;
    }

    const TString Cluster;
    const TString Database;
    const TIntrusivePtr<TKikimrTablesData> TablesData;
    const IFunctionRegistry& FuncRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;
    TKqlCompileContext KqlCtx;
    TIntrusivePtr<NCommon::IMkqlCallableCompiler> KqlCompiler;
    TTypeAnnotationContext& TypesCtx;
    NOpt::TKqpOptimizeContext& OptimizeCtx;
    TKikimrConfiguration::TPtr Config;
    TSet<TString> SecretNames;
};

} // namespace

TIntrusivePtr<IKqpQueryCompiler> CreateKqpQueryCompiler(const TString& cluster, const TString& database,
    const IFunctionRegistry& funcRegistry, TTypeAnnotationContext& typesCtx,
    NOpt::TKqpOptimizeContext& optimizeCtx, NYql::TKikimrConfiguration::TPtr config)
{
    return MakeIntrusive<TKqpQueryCompiler>(cluster, database, funcRegistry, typesCtx, optimizeCtx, config);
}

} // namespace NKqp
} // namespace NKikimr
