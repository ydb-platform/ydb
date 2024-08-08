#include "kqp_query_compiler.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/query_data/kqp_request_predictor.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/kqp/query_compiler/kqp_mkql_compiler.h>
#include <ydb/core/kqp/query_compiler/kqp_olap_compiler.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NNodes;

namespace {

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

NKqpProto::EStreamLookupStrategy GetStreamLookupStrategy(const std::string_view strategy) {
    NKqpProto::EStreamLookupStrategy lookupStrategy = NKqpProto::EStreamLookupStrategy::UNSPECIFIED;

    if (strategy == "LookupRows"sv) {
        lookupStrategy = NKqpProto::EStreamLookupStrategy::LOOKUP;
    } else if (strategy == "LookupJoinRows"sv) {
        lookupStrategy = NKqpProto::EStreamLookupStrategy::JOIN;
    } else if (strategy == "LookupSemiJoinRows"sv) {
        lookupStrategy = NKqpProto::EStreamLookupStrategy::SEMI_JOIN;
    }

    YQL_ENSURE(lookupStrategy != NKqpProto::EStreamLookupStrategy::UNSPECIFIED,
        "Unexpected stream lookup strategy: " << strategy);
    return lookupStrategy;
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

void FillTablesMap(const TKqpTable& table, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap) {
    tablesMap.emplace(table.Path().Value(), THashSet<TStringBuf>{});
}

void FillTablesMap(const TKqpTable& table, const TCoAtomList& columns,
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
            YQL_ENSURE(GetSystemColumns().find(columnName) != GetSystemColumns().end());
            continue;
        }

        auto& phyColumn = phyColumns[column->Id];
        phyColumn.MutableId()->SetId(column->Id);
        phyColumn.MutableId()->SetName(column->Name);
        phyColumn.SetTypeId(column->TypeInfo.GetTypeId());
        phyColumn.SetIsBuildInProgress(column->IsBuildInProgress);
        phyColumn.SetIsCheckingNotNullInProgress(column->IsCheckingNotNullInProgress);

        if (column->IsDefaultFromSequence()) {
            phyColumn.SetDefaultFromSequence(column->DefaultFromSequence);
        } else if (column->IsDefaultFromLiteral()) {
            phyColumn.MutableDefaultFromLiteral()->CopyFrom(column->DefaultFromLiteral);
        }
        phyColumn.SetNotNull(column->NotNull);
        if (column->TypeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
            phyColumn.SetPgTypeName(NPg::PgTypeNameFromTypeDesc(column->TypeInfo.GetTypeDesc()));
        }
    }
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

        YQL_ENSURE(columnId, "Unknown column: " << columnName);
        auto& columnProto = *opProto.AddColumns();
        columnProto.SetId(columnId);
        columnProto.SetName(columnName);
    }
}

void FillNothing(TCoNothing expr, NKqpProto::TKqpPhyLiteralValue& value) {
    auto* typeann = expr.Raw()->GetTypeAnn();
    YQL_ENSURE(typeann->GetKind() == ETypeAnnotationKind::Optional);
    typeann = typeann->Cast<TOptionalExprType>()->GetItemType();
    YQL_ENSURE(typeann->GetKind() == ETypeAnnotationKind::Data);
    auto slot = typeann->Cast<TDataExprType>()->GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NKikimr::NScheme::NTypeIds::IsYqlType(typeId) &&
        NKikimr::NSchemeShard::IsAllowedKeyType(NKikimr::NScheme::TTypeInfo(typeId)));

    value.MutableType()->SetKind(NKikimrMiniKQL::Optional);
    auto* toFill = value.MutableType()->MutableOptional()->MutableItem();

    toFill->SetKind(NKikimrMiniKQL::ETypeKind::Data);
    toFill->MutableData()->SetScheme(typeId);

    if (slot == EDataSlot::Decimal) {
        const auto paramsDataType = typeann->Cast<TDataExprParamsType>();
        auto precision = FromString<ui8>(paramsDataType->GetParamOne());
        auto scale = FromString<ui8>(paramsDataType->GetParamTwo());
        toFill->MutableData()->MutableDecimalParams()->SetPrecision(precision);
        toFill->MutableData()->MutableDecimalParams()->SetScale(scale);
    }

    value.MutableValue()->SetNullFlagValue(::google::protobuf::NullValue::NULL_VALUE);
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
            auto* literal = readProto.MutableItemsLimit()->MutableLiteralValue();

            literal->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            literal->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);

            literal->MutableValue()->SetUint64(FromString<ui64>(expr.Cast<TCoUint64>().Literal().Value()));
        } else if (expr.Maybe<TCoParameter>()) {
            readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
        }
    }

    readProto.SetReverse(settings.Reverse);
}

template <typename TReader, typename TProto>
void FillReadRanges(const TReader& read, const TKikimrTableMetadata&, TProto& readProto) {
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
            auto* literal = readProto.MutableItemsLimit()->MutableLiteralValue();

            literal->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            literal->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);

            literal->MutableValue()->SetUint64(FromString<ui64>(expr.Cast<TCoUint64>().Literal().Value()));
        } else if (expr.template Maybe<TCoParameter>()) {
            readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.template Cast<TCoParameter>().Name().StringValue());
        } else {
            YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
        }
    }

    if constexpr (std::is_same_v<TProto, NKqpProto::TKqpPhyOpReadOlapRanges>) {
        readProto.SetSorted(settings.Sorted);
    }

    readProto.SetReverse(settings.Reverse);
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

void FillLookup(const TKqpLookupTable& lookup, NKqpProto::TKqpPhyOpLookup& lookupProto, TExprContext& ctx) {
    auto maybeList = lookup.LookupKeys().Maybe<TCoIterator>().List();
    YQL_ENSURE(maybeList, "Expected iterator as lookup input, got: " << lookup.LookupKeys().Ref().Content());

    if (auto maybeParam = maybeList.Cast().Maybe<TCoParameter>()) {
         lookupProto.MutableKeysValue()->MutableParamValue()->SetParamName(maybeParam.Cast().Name().StringValue());
    } else if (auto maybeAsList = maybeList.Cast().Maybe<TCoAsList>()) {
        auto asList = maybeAsList.Cast();
        auto proto = lookupProto.MutableKeysValue()->MutableRowsList();

        for (auto row : asList) {
            YQL_ENSURE(row.Maybe<TCoAsStruct>(), "" << row.Ref().Dump());
            auto asStruct = row.Cast<TCoAsStruct>();
            auto protoRow = proto->AddRows();
            auto& protoRowColumns = *protoRow->MutableColumns();

            for (auto item : asStruct) {
                auto tuple = item.Cast<TCoNameValueTuple>();
                auto columnName = tuple.Name().StringValue();
                auto& protoColumn = protoRowColumns[columnName];

                if (auto maybeParam = tuple.Value().Maybe<TCoParameter>()) {
                    protoColumn.MutableParamValue()->SetParamName(maybeParam.Cast().Name().StringValue());
                } else if (auto maybeNothing = tuple.Value().Maybe<TCoNothing>()) {
                    FillNothing(maybeNothing.Cast(), *protoColumn.MutableLiteralValue());
                } else {
                    YQL_ENSURE(tuple.Value().Maybe<TCoDataCtor>(), "" << tuple.Value().Ref().Dump());
                    FillLiteralProto(tuple.Value().Cast<TCoDataCtor>(), *protoColumn.MutableLiteralValue());
                }
            }
        }
    } else {
        auto brokenLookup =  KqpExprToPrettyString(lookup, ctx);
        YQL_ENSURE(false, "Unexpected lookup input: " << maybeList.Cast().Ref().Content()
            << "lookup: " << brokenLookup);
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

THashMap<TString, TString> FindSecureParams(const TExprNode::TPtr& node, const TTypeAnnotationContext& typesCtx, TSet<TString>& SecretNames) {
    THashMap<TString, TString> secureParams;
    NYql::NCommon::FillSecureParams(node, typesCtx, secureParams);

    for (auto& [secretName, structuredToken] : secureParams) {
        const auto& tokenParser = CreateStructuredTokenParser(structuredToken);
        tokenParser.ListReferences(SecretNames);
        structuredToken = tokenParser.ToBuilder().RemoveSecrets().ToJson();
    }

    return secureParams;
}

std::optional<std::pair<TString, TString>> FindOneSecureParam(const TExprNode::TPtr& node, const TTypeAnnotationContext& typesCtx, const TString& nodeName, TSet<TString>& SecretNames) {
    const auto& secureParams = FindSecureParams(node, typesCtx, SecretNames);
    if (secureParams.empty()) {
        return std::nullopt;
    }

    YQL_ENSURE(secureParams.size() == 1, "Only one SecureParams per " << nodeName << " allowed");
    return *secureParams.begin();
}

class TKqpQueryCompiler : public IKqpQueryCompiler {
public:
    TKqpQueryCompiler(const TString& cluster, const TIntrusivePtr<TKikimrTablesData> tablesData,
        const NMiniKQL::IFunctionRegistry& funcRegistry, TTypeAnnotationContext& typesCtx, NYql::TKikimrConfiguration::TPtr config)
        : Cluster(cluster)
        , TablesData(tablesData)
        , FuncRegistry(funcRegistry)
        , Alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
        , TypeEnv(Alloc)
        , KqlCtx(cluster, tablesData, TypeEnv, FuncRegistry)
        , KqlCompiler(CreateKqlCompiler(KqlCtx, typesCtx))
        , TypesCtx(typesCtx)
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

        for (const auto& queryBlock : dataQueryBlocks) {
            auto queryBlockSettings = TKiDataQueryBlockSettings::Parse(queryBlock);
            if (queryBlockSettings.HasUncommittedChangesRead) {
                queryProto.SetHasUncommittedChangesRead(true);
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

        for (const auto& tx : query.Transactions()) {
            CompileTransaction(tx, *queryProto.AddTransactions(), ctx);
        }

        auto overridePlanner = Config->OverridePlanner.Get();
        if (overridePlanner) {
            NJson::TJsonReaderConfig jsonConfig;
            NJson::TJsonValue jsonNode;
            if (NJson::ReadJsonTree(*overridePlanner, &jsonConfig, &jsonNode)) {
                for (auto& stageOverride : jsonNode.GetArray()) {
                    ui32 txId = 0;
                    if (auto* txNode = stageOverride.GetValueByPath("tx")) {
                        txId = txNode->GetIntegerSafe();
                    }
                    if (txId < static_cast<ui32>(queryProto.GetTransactions().size())) {
                        auto& tx = *queryProto.MutableTransactions(txId);
                        ui32 stageId = 0;
                        if (auto* stageNode = stageOverride.GetValueByPath("stage")) {
                            stageId = stageNode->GetIntegerSafe();
                        }
                        if (stageId < static_cast<ui32>(tx.GetStages().size())) {
                            auto& stage = *tx.MutableStages(stageId);
                            if (auto* tasksNode = stageOverride.GetValueByPath("tasks")) {
                                stage.SetTaskCount(tasksNode->GetIntegerSafe());
                            }
                        }
                    }
                }
            }
        }

        for (ui32 i = 0; i < query.Results().Size(); ++i) {
            const auto& result = query.Results().Item(i);

            YQL_ENSURE(result.Maybe<TKqpTxResultBinding>());
            auto binding = result.Cast<TKqpTxResultBinding>();
            auto txIndex = FromString<ui32>(binding.TxIndex().Value());
            auto txResultIndex = FromString<ui32>(binding.ResultIndex());

            YQL_ENSURE(txIndex < queryProto.TransactionsSize());
            YQL_ENSURE(txResultIndex < queryProto.GetTransactions(txIndex).ResultsSize());
            auto& txResult = *queryProto.MutableTransactions(txIndex)->MutableResults(txResultIndex);

            YQL_ENSURE(txResult.GetIsStream());
            txResult.SetQueryResultIndex(i);

            auto& queryBindingProto = *queryProto.AddResultBindings();
            auto& txBindingProto = *queryBindingProto.MutableTxResultBinding();
            txBindingProto.SetTxIndex(txIndex);
            txBindingProto.SetResultIndex(txResultIndex);

            auto type = binding.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            YQL_ENSURE(type);
            YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Struct);

            NKikimrMiniKQL::TType kikimrProto;

            if (!NYql::ExportTypeToKikimrProto(*type, kikimrProto, ctx)) {
                return false;
            }

            auto resultMetaColumns = queryBindingProto.MutableResultSetMeta()->Mutablecolumns();
            for (size_t i = 0; i < kikimrProto.GetStruct().MemberSize(); i++) {
                resultMetaColumns->Add();
            }

            THashMap<TString, int> columnOrder;
            columnOrder.reserve(kikimrProto.GetStruct().MemberSize());
            if (!txResult.GetColumnHints().empty()) {
                YQL_ENSURE(txResult.GetColumnHints().size() == (int)kikimrProto.GetStruct().MemberSize());
                for (int i = 0; i < txResult.GetColumnHints().size(); i++) {
                    const auto& hint = txResult.GetColumnHints().at(i);
                    columnOrder[TString(hint)] = i;
                }
            }

            int id = 0;
            for (const auto& column : kikimrProto.GetStruct().GetMember()) {
                int bindingColumnId = columnOrder.count(column.GetName()) ? columnOrder.at(column.GetName()) : id++;
                auto& columnMeta = resultMetaColumns->at(bindingColumnId);
                columnMeta.Setname(column.GetName());
                ConvertMiniKQLTypeToYdbType(column.GetType(), *columnMeta.mutable_type());
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

    void CompileStage(const TDqPhyStage& stage, NKqpProto::TKqpPhyStage& stageProto, TExprContext& ctx,
        const TMap<ui64, ui32>& stagesMap, TRequestPredictor& rPredictor, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
    {
        const bool hasEffects = NOpt::IsKqpEffectsStage(stage);

        TStagePredictor& stagePredictor = rPredictor.BuildForStage(stage, ctx);
        stagePredictor.Scan(stage.Program().Ptr());

        for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
            const auto& input = stage.Inputs().Item(inputIndex);

            if (input.Maybe<TDqSource>()) {
                auto* protoSource = stageProto.AddSources();
                FillSource(input.Cast<TDqSource>(), protoSource, true, tablesMap, ctx);
                protoSource->SetInputIndex(inputIndex);
            } else {
                YQL_ENSURE(input.Maybe<TDqConnection>());
                auto connection = input.Cast<TDqConnection>();

                auto& protoInput = *stageProto.AddInputs();
                FillConnection(connection, stagesMap, protoInput, ctx, tablesMap);
                protoInput.SetInputIndex(inputIndex);
            }
        }

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
            } else if (auto maybeLookupTable = node.Maybe<TKqpLookupTable>()) {
                auto lookupTable = maybeLookupTable.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, lookupTable.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTablesMap(lookupTable.Table(), lookupTable.Columns(), tablesMap);
                FillTableId(lookupTable.Table(), *tableOp.MutableTable());
                FillColumns(lookupTable.Columns(), *tableMeta, tableOp, true);
                FillLookup(lookupTable, *tableOp.MutableLookup(), ctx);
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
            } else {
                YQL_ENSURE(!node.Maybe<TKqpReadTable>());
            }
            return true;
        });

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
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Void, "got " << *resultType);
        }

        stageProto.SetOutputsCount(outputsCount);

        // Dq sinks
        bool hasTxTableSink = false;
        if (auto maybeOutputsNode = stage.Outputs()) {
            auto outputsNode = maybeOutputsNode.Cast();
            for (size_t i = 0; i < outputsNode.Size(); ++i) {
                auto outputNode = outputsNode.Item(i);
                auto maybeSinkNode = outputNode.Maybe<TDqSink>();
                YQL_ENSURE(maybeSinkNode);
                auto sinkNode = maybeSinkNode.Cast();
                auto* sinkProto = stageProto.AddSinks();
                FillSink(sinkNode, sinkProto, tablesMap, ctx);
                sinkProto->SetOutputIndex(FromString(TStringBuf(sinkNode.Index())));

                if (IsTableSink(sinkNode.DataSink().Cast<TCoDataSink>().Category())) {
                    // Only sinks with transactions to ydb tables can be considered as effects.
                    // Inconsistent internal sinks and external sinks (like S3) aren't effects.
                    auto settings = sinkNode.Settings().Maybe<TKqpTableSinkSettings>();
                    YQL_ENSURE(settings);
                    hasTxTableSink |= settings.InconsistentWrite().Cast().StringValue() != "true";
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

        stagePredictor.SerializeToKqpSettings(*programProto.MutableSettings());

        for (auto member : paramsType->GetItems()) {
            auto paramName = TString(member->GetName());
            stageProto.AddProgramParameters(paramName);
        }

        stageProto.SetProgramAst(KqpExprToPrettyString(stage.Program(), ctx));

        auto stageSettings = NDq::TDqStageSettings::Parse(stage);
        stageProto.SetStageGuid(stageSettings.Id);
        stageProto.SetIsSinglePartition(NDq::TDqStageSettings::EPartitionMode::Single == stageSettings.PartitionMode);
        stageProto.SetAllowWithSpilling(Config->EnableSpillingGenericQuery);
    }

    void CompileTransaction(const TKqpPhysicalTx& tx, NKqpProto::TKqpPhyTx& txProto, TExprContext& ctx) {
        auto txSettings = TKqpPhyTxSettings::Parse(tx);
        YQL_ENSURE(txSettings.Type);
        txProto.SetType(GetPhyTxType(*txSettings.Type));

        bool hasEffectStage = false;

        TMap<ui64, ui32> stagesMap;
        THashMap<TStringBuf, THashSet<TStringBuf>> tablesMap;

        TRequestPredictor rPredictor;
        for (const auto& stage : tx.Stages()) {
            auto* protoStage = txProto.AddStages();
            CompileStage(stage, *protoStage, ctx, stagesMap, rPredictor, tablesMap);
            hasEffectStage |= protoStage->GetIsEffectsStage();
            stagesMap[stage.Ref().UniqueId()] = txProto.StagesSize() - 1;
        }
        for (auto&& i : *txProto.MutableStages()) {
            i.MutableProgram()->MutableSettings()->SetLevelDataPrediction(rPredictor.GetLevelDataVolume(i.GetProgram().GetSettings().GetStageLevel()));
        }

        txProto.SetHasEffects(hasEffectStage);

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
        for (const auto& resultNode : tx.Results()) {
            YQL_ENSURE(resultNode.Maybe<TDqConnection>(), "" << NCommon::ExprToPrettyString(ctx, tx.Ref()));
            auto connection = resultNode.Cast<TDqConnection>();

            auto& resultProto = *txProto.AddResults();
            auto& connectionProto = *resultProto.MutableConnection();
            FillConnection(connection, stagesMap, connectionProto, ctx, tablesMap);

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
        }

        for (auto& [tablePath, tableColumns] : tablesMap) {
            auto tableMeta = TablesData->ExistingTable(Cluster, tablePath).Metadata;
            YQL_ENSURE(tableMeta);

            FillTable(*tableMeta, std::move(tableColumns), *txProto.AddTables());
        }

        for (const auto& [a, desc] : TablesData->GetTables()) {
            auto tableMeta = desc.Metadata;
            YQL_ENSURE(tableMeta);
            if (desc.Metadata->Kind == NYql::EKikimrTableKind::External) {
                THashSet<TStringBuf> columns;
                for (const auto& [col, _]: tableMeta->Columns){
                    columns.emplace(col);
                }
                FillTable(*tableMeta, std::move(columns), *txProto.AddTables());
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

            readProto.SetReverse(readSettings.Reverse);
            readProto.SetSorted(readSettings.Sorted);
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
                    auto* literal = readProto.MutableItemsLimit()->MutableLiteralValue();

                    literal->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    literal->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);

                    literal->MutableValue()->SetUint64(FromString<ui64>(expr.Cast<TCoUint64>().Literal().Value()));
                } else if (expr.template Maybe<TCoParameter>()) {
                    readProto.MutableItemsLimit()->MutableParamValue()->SetParamName(expr.template Cast<TCoParameter>().Name().StringValue());
                } else {
                    YQL_ENSURE(false, "Unexpected ItemsLimit callable " << expr.Ref().Content());
                }
            }
        } else {
            YQL_ENSURE(false, "Unsupported source type");
        }
    }

    void FillSource(const TDqSource& source, NKqpProto::TKqpSource* protoSource, bool allowSystemColumns,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap, TExprContext& ctx)
    {
        const TStringBuf dataSourceCategory = source.DataSource().Cast<TCoDataSource>().Category();
        if (dataSourceCategory == NYql::KikimrProviderName || dataSourceCategory == NYql::YdbProviderName || dataSourceCategory == NYql::KqpReadRangesSourceName) {
            FillKqpSource(source, protoSource, allowSystemColumns, tablesMap);
        } else {
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
            dqIntegration->Partition(NYql::TDqSettings(), maxTasksPerStage, source.Ref(), partitionParams, &clusterName, ctx, false);
            externalSource.SetTaskParamKey(TString(dataSourceCategory));
            for (const TString& partitionParam : partitionParams) {
                externalSource.AddPartitionedTaskParams(partitionParam);
            }

            if (const auto& secureParams = FindOneSecureParam(source.Ptr(), TypesCtx, "source", SecretNames)) {
                externalSource.SetSourceName(secureParams->first);
                externalSource.SetAuthInfo(secureParams->second);
            }

            google::protobuf::Any& settings = *externalSource.MutableSettings();
            TString& sourceType = *externalSource.MutableType();
            dqIntegration->FillSourceSettings(source.Ref(), settings, sourceType, maxTasksPerStage);
            YQL_ENSURE(!settings.type_url().empty(), "Data source provider \"" << dataSourceCategory << "\" didn't fill dq source settings for its dq source node");
            YQL_ENSURE(sourceType, "Data source provider \"" << dataSourceCategory << "\" didn't fill dq source settings type for its dq source node");
        }
    }

    void FillKqpSink(const TDqSink& sink, NKqpProto::TKqpSink* protoSink, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap) {
        if (auto settings = sink.Settings().Maybe<TKqpTableSinkSettings>()) {
            NKqpProto::TKqpInternalSink& internalSinkProto = *protoSink->MutableInternalSink();
            internalSinkProto.SetType(TString(NYql::KqpTableSinkName));
            NKikimrKqp::TKqpTableSinkSettings settingsProto;
            FillTablesMap(settings.Table().Cast(), settings.Columns().Cast(), tablesMap);
            FillTableId(settings.Table().Cast(), *settingsProto.MutableTable());

            const auto tableMeta = TablesData->ExistingTable(Cluster, settings.Table().Cast().Path()).Metadata;

            for (const auto& columnName : tableMeta->KeyColumnNames) {
                const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + columnName + "\"");

                auto keyColumnProto = settingsProto.AddKeyColumns();
                keyColumnProto->SetId(columnMeta->Id);
                keyColumnProto->SetName(columnName);
                keyColumnProto->SetTypeId(columnMeta->TypeInfo.GetTypeId());

                if (columnMeta->TypeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
                    auto& typeInfo = *keyColumnProto->MutableTypeInfo();
                    typeInfo.SetPgTypeId(NPg::PgTypeIdFromTypeDesc(columnMeta->TypeInfo.GetTypeDesc()));
                    typeInfo.SetPgTypeMod(columnMeta->TypeMod);
                }
            }

            for (const auto& column : settings.Columns().Cast()) {
                const auto columnName = column.StringValue();
                const auto columnMeta = tableMeta->Columns.FindPtr(columnName);
                YQL_ENSURE(columnMeta != nullptr, "Unknown column in sink: \"" + columnName + "\"");

                auto columnProto = settingsProto.AddColumns();
                columnProto->SetId(columnMeta->Id);
                columnProto->SetName(columnName);
                columnProto->SetTypeId(columnMeta->TypeInfo.GetTypeId());

                if (columnMeta->TypeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
                    auto& typeInfo = *columnProto->MutableTypeInfo();
                    typeInfo.SetPgTypeId(NPg::PgTypeIdFromTypeDesc(columnMeta->TypeInfo.GetTypeDesc()));
                    typeInfo.SetPgTypeMod(columnMeta->TypeMod);
                }
            }

            if (const auto inconsistentWrite = settings.InconsistentWrite().Cast(); inconsistentWrite.StringValue() == "true") {
                settingsProto.SetInconsistentTx(true);
            }

            if (settings.Mode().Cast().StringValue() == "replace") {
                settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE);
            } else if (settings.Mode().Cast().StringValue() == "upsert" || settings.Mode().Cast().StringValue().empty() /* for compatibility, will be removed */) {
                settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT);
            } else if (settings.Mode().Cast().StringValue() == "insert") {
                settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);
            } else if (settings.Mode().Cast().StringValue() == "delete") {
                settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE);
            } else if (settings.Mode().Cast().StringValue() == "update") {
                settingsProto.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE);
            } else {
                YQL_ENSURE(false, "Unsupported sink mode");
            }

            internalSinkProto.MutableSettings()->PackFrom(settingsProto);
        } else {
            YQL_ENSURE(false, "Unsupported sink type");
        }
    }

    bool IsTableSink(const TStringBuf dataSinkCategory) const {
        return dataSinkCategory == NYql::KikimrProviderName
            || dataSinkCategory == NYql::YdbProviderName
            || dataSinkCategory == NYql::KqpTableSinkName;
    }

    void FillSink(const TDqSink& sink, NKqpProto::TKqpSink* protoSink, THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap, TExprContext& ctx) {
        Y_UNUSED(ctx);
        const TStringBuf dataSinkCategory = sink.DataSink().Cast<TCoDataSink>().Category();
        if (IsTableSink(dataSinkCategory)) {
            FillKqpSink(sink, protoSink, tablesMap);
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

    void FillConnection(const TDqConnection& connection, const TMap<ui64, ui32>& stagesMap,
        NKqpProto::TKqpPhyConnection& connectionProto, TExprContext& ctx,
        THashMap<TStringBuf, THashSet<TStringBuf>>& tablesMap)
    {
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

        if (auto maybeShuffle = connection.Maybe<TDqCnHashShuffle>()) {
            auto& shuffleProto = *connectionProto.MutableHashShuffle();
            for (const auto& keyColumn : maybeShuffle.Cast().KeyColumns()) {
                shuffleProto.AddKeyColumns(TString(keyColumn));
            }
            return;
        }

        if (connection.Maybe<TDqCnMap>()) {
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

        if (connection.Maybe<TKqpCnMapShard>()) {
            connectionProto.MutableMapShard();
            return;
        }

        if (connection.Maybe<TKqpCnShuffleShard>()) {
            connectionProto.MutableShuffleShard();
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

            auto autoIncrementColumns = sequencer.DefaultConstraintColumns();
            for(const auto& column : autoIncrementColumns) {
                sequencerProto.AddAutoIncrementColumns(column.StringValue());
            }

            YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Struct);
            for(const auto* column: resultItemType->Cast<TStructExprType>()->GetItems()) {
                sequencerProto.AddColumns(TString(column->GetName()));
            }

            return;
        }

        if (auto maybeStreamLookup = connection.Maybe<TKqpCnStreamLookup>()) {
            TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
            auto& streamLookupProto = *connectionProto.MutableStreamLookup();
            auto streamLookup = maybeStreamLookup.Cast();
            auto tableMeta = TablesData->ExistingTable(Cluster, streamLookup.Table().Path()).Metadata;
            YQL_ENSURE(tableMeta);

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

            YQL_ENSURE(streamLookup.LookupStrategy().Maybe<TCoAtom>());
            TString lookupStrategy = streamLookup.LookupStrategy().Maybe<TCoAtom>().Cast().StringValue();
            streamLookupProto.SetLookupStrategy(GetStreamLookupStrategy(lookupStrategy));

            switch (streamLookupProto.GetLookupStrategy()) {
                case NKqpProto::EStreamLookupStrategy::LOOKUP: {
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
                    YQL_ENSURE(inputTupleType->GetSize() == 2);

                    YQL_ENSURE(inputTupleType->GetItems()[0]->GetKind() == ETypeAnnotationKind::Optional);
                    const auto joinKeyType = inputTupleType->GetItems()[0]->Cast<TOptionalExprType>()->GetItemType();
                    YQL_ENSURE(joinKeyType->GetKind() == ETypeAnnotationKind::Struct);
                    const auto& joinKeyColumns = joinKeyType->Cast<TStructExprType>()->GetItems();
                    for (const auto keyColumn : joinKeyColumns) {
                        YQL_ENSURE(tableMeta->Columns.FindPtr(keyColumn->GetName()),
                            "Unknown column: " << keyColumn->GetName());
                        streamLookupProto.AddKeyColumns(TString(keyColumn->GetName()));
                    }

                    YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Tuple);
                    const auto resultTupleType = resultItemType->Cast<TTupleExprType>();
                    YQL_ENSURE(resultTupleType->GetSize() == 2);

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
                    YQL_ENSURE(false, "Unexpected lookup strategy for stream lookup: " << lookupStrategy);
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
    TString Cluster;
    const TIntrusivePtr<TKikimrTablesData> TablesData;
    const IFunctionRegistry& FuncRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;
    TKqlCompileContext KqlCtx;
    TIntrusivePtr<NCommon::IMkqlCallableCompiler> KqlCompiler;
    TTypeAnnotationContext& TypesCtx;
    TKikimrConfiguration::TPtr Config;
    TSet<TString> SecretNames;
};

} // namespace

TIntrusivePtr<IKqpQueryCompiler> CreateKqpQueryCompiler(const TString& cluster,
    const TIntrusivePtr<TKikimrTablesData> tablesData, const IFunctionRegistry& funcRegistry,
    TTypeAnnotationContext& typesCtx, NYql::TKikimrConfiguration::TPtr config)
{
    return MakeIntrusive<TKqpQueryCompiler>(cluster, tablesData, funcRegistry, typesCtx, config);
}

} // namespace NKqp
} // namespace NKikimr
