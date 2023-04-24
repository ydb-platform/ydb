#include "kqp_query_compiler.h"

#include <ydb/core/kqp/common/kqp_yql.h>
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
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

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
        case EPhysicalQueryType::Query: return NKqpProto::TKqpPhyQuery::TYPE_QUERY;
        case EPhysicalQueryType::FederatedQuery: return NKqpProto::TKqpPhyQuery::TYPE_FEDERATED_QUERY;

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

void FillLiteralKeyBound(const TCoDataCtor& literal, NKqpProto::TKqpPhyLiteralValue& proto) {
    auto type = literal.Ref().GetTypeAnn();

    // TODO: support pg types
    YQL_ENSURE(type->GetKind() != ETypeAnnotationKind::Pg, "pg types are not supported");

    auto slot = type->Cast<TDataExprType>()->GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NScheme::NTypeIds::IsYqlType(typeId) && NSchemeShard::IsAllowedKeyType(NScheme::TTypeInfo(typeId)));

    auto& protoType = *proto.MutableType();
    auto& protoValue = *proto.MutableValue();

    protoType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    protoType.MutableData()->SetScheme(typeId);

    auto value = literal.Literal().Value();

    switch (slot) {
        case EDataSlot::Bool:
            protoValue.SetBool(FromString<bool>(value));
            break;
        case EDataSlot::Uint8:
        case EDataSlot::Uint32:
        case EDataSlot::Date:
        case EDataSlot::Datetime:
            protoValue.SetUint32(FromString<ui32>(value));
            break;
        case EDataSlot::Int32:
            protoValue.SetInt32(FromString<i32>(value));
            break;
        case EDataSlot::Int64:
        case EDataSlot::Interval:
            protoValue.SetInt64(FromString<i64>(value));
            break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            protoValue.SetUint64(FromString<ui64>(value));
            break;
        case EDataSlot::String:
        case EDataSlot::DyNumber:
            protoValue.SetBytes(value.Data(), value.Size());
            break;
        case EDataSlot::Utf8:
            protoValue.SetText(ToString(value));
            break;
        case EDataSlot::Decimal: {
            const auto paramsDataType = type->Cast<TDataExprParamsType>();
            auto precision = FromString<ui8>(paramsDataType->GetParamOne());
            auto scale = FromString<ui8>(paramsDataType->GetParamTwo());
            protoType.MutableData()->MutableDecimalParams()->SetPrecision(precision);
            protoType.MutableData()->MutableDecimalParams()->SetScale(scale);

            auto v = NDecimal::FromString(literal.Cast<TCoDecimal>().Literal().Value(), precision, scale);
            const auto p = reinterpret_cast<ui8*>(&v);
            protoValue.SetLow128(*reinterpret_cast<ui64*>(p));
            protoValue.SetHi128(*reinterpret_cast<ui64*>(p + 8));
            break;
        }

        default:
            YQL_ENSURE(false, "Unexpected type slot " << slot);
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
            FillLiteralKeyBound(maybeLiteral.Cast(), *protoValue.MutableLiteralValue());
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

void FillLookup(const TKqpLookupTable& lookup, NKqpProto::TKqpPhyOpLookup& lookupProto) {
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
                } else {
                    YQL_ENSURE(tuple.Value().Maybe<TCoDataCtor>(), "" << tuple.Value().Ref().Dump());
                    FillLiteralKeyBound(tuple.Value().Cast<TCoDataCtor>(), *protoColumn.MutableLiteralValue());
                }
            }
        }
    } else {
        YQL_ENSURE(false, "Unexpected lookup input: " << maybeList.Cast().Ref().Content());
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

void FillOlapProgram(const TCoLambda& process, const NKikimr::NMiniKQL::TType* miniKqlResultType,
    const TKikimrTableMetadata& tableMeta, NKqpProto::TKqpPhyOpReadOlapRanges& readProto)
{
    auto resultColNames = GetResultColumnNames(miniKqlResultType);
    CompileOlapProgram(process, tableMeta, readProto, resultColNames);
}

class TKqpQueryCompiler : public IKqpQueryCompiler {
public:
    TKqpQueryCompiler(const TString& cluster, const TIntrusivePtr<TKikimrTablesData> tablesData,
        const NMiniKQL::IFunctionRegistry& funcRegistry, TTypeAnnotationContext& typesCtx)
        : Cluster(cluster)
        , TablesData(tablesData)
        , FuncRegistry(funcRegistry)
        , Alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
        , TypeEnv(Alloc)
        , KqlCtx(cluster, tablesData, TypeEnv, FuncRegistry)
        , KqlCompiler(CreateKqlCompiler(KqlCtx, typesCtx))
        , TypesCtx(typesCtx)
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

        for (const auto& result : query.Results()) {
            YQL_ENSURE(result.Maybe<TKqpTxResultBinding>());
            auto binding = result.Cast<TKqpTxResultBinding>();

            auto txIndex = FromString<ui32>(binding.TxIndex().Value());
            auto resultIndex = FromString<ui32>(binding.ResultIndex());

            YQL_ENSURE(txIndex < queryProto.TransactionsSize());
            YQL_ENSURE(resultIndex < queryProto.GetTransactions(txIndex).ResultsSize());
            YQL_ENSURE(queryProto.GetTransactions(txIndex).GetResults(resultIndex).GetIsStream());

            auto& queryBindingProto = *queryProto.AddResultBindings();
            auto& txBindingProto = *queryBindingProto.MutableTxResultBinding();
            txBindingProto.SetTxIndex(txIndex);
            txBindingProto.SetResultIndex(resultIndex);
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
        stageProto.SetIsEffectsStage(NOpt::IsKqpEffectsStage(stage));

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
                FillLookup(lookupTable, *tableOp.MutableLookup());
            } else if (auto maybeUpsertRows = node.Maybe<TKqpUpsertRows>()) {
                auto upsertRows = maybeUpsertRows.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, upsertRows.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);
                YQL_ENSURE(stageProto.GetIsEffectsStage());

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

                YQL_ENSURE(stageProto.GetIsEffectsStage());

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
                FillOlapProgram(readTableRanges.Process(), miniKqlResultType, *tableMeta, *tableOp.MutableReadOlapRange());
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
                FillOlapProgram(readTableRanges.Process(), miniKqlResultType, *tableMeta, *tableOp.MutableReadOlapRange());
                FillResultType(miniKqlResultType, *tableOp.MutableReadOlapRange());
                tableOp.MutableReadOlapRange()->SetReadType(NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS);
            } else {
                YQL_ENSURE(!node.Maybe<TKqpReadTable>());
            }
            return true;
        });

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

        auto paramsType = CollectParameters(stage, ctx);
        auto programBytecode = NDq::BuildProgram(stage.Program(), *paramsType, *KqlCompiler, TypeEnv, FuncRegistry,
            ctx, {});

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
        stageProto.SetIsSinglePartition(stageSettings.SinglePartition);
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


        YQL_ENSURE(hasEffectStage == txSettings.WithEffects);

        txProto.SetHasEffects(txSettings.WithEffects);

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
        const TStringBuf dataSourceCategory = source.DataSource().Cast<TCoDataSource>().Category().Value();
        if (dataSourceCategory == NYql::KikimrProviderName || dataSourceCategory == NYql::YdbProviderName || dataSourceCategory == NYql::KqpReadRangesSourceName) {
            FillKqpSource(source, protoSource, allowSystemColumns, tablesMap);
        } else {
            // Delegate source filling to dq integration of specific provider
            const auto provider = TypesCtx.DataSourceMap.find(dataSourceCategory);
            YQL_ENSURE(provider != TypesCtx.DataSourceMap.end(), "Unsupported data source category: \"" << dataSourceCategory << "\"");
            NYql::IDqIntegration* dqIntegration = provider->second->GetDqIntegration();
            YQL_ENSURE(dqIntegration, "Unsupported dq source for provider: \"" << dataSourceCategory << "\"");
            auto& externalSource = *protoSource->MutableExternalSource();
            google::protobuf::Any& settings = *externalSource.MutableSettings();
            TString& sourceType = *externalSource.MutableType();
            dqIntegration->FillSourceSettings(source.Ref(), settings, sourceType);
            YQL_ENSURE(!settings.type_url().empty(), "Data source provider \"" << dataSourceCategory << "\" did't fill dq source settings for its dq source node");
            YQL_ENSURE(sourceType, "Data source provider \"" << dataSourceCategory << "\" did't fill dq source settings type for its dq source node");

            // Partitioning
            TVector<TString> partitionParams;
            TString clusterName;
            dqIntegration->Partition(NYql::TDqSettings(), NYql::TDqSettings::TDefault::MaxTasksPerStage, source.Ref(), partitionParams, &clusterName, ctx, false);
            externalSource.SetTaskParamKey(TString(dataSourceCategory));
            for (const TString& partitionParam : partitionParams) {
                externalSource.AddPartitionedTaskParams(partitionParam);
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

        if (auto maybeStreamLookup = connection.Maybe<TKqpCnStreamLookup>()) {
            TProgramBuilder pgmBuilder(TypeEnv, FuncRegistry);
            auto& streamLookupProto = *connectionProto.MutableStreamLookup();
            auto streamLookup = maybeStreamLookup.Cast();
            auto tableMeta = TablesData->ExistingTable(Cluster, streamLookup.Table().Path()).Metadata;
            YQL_ENSURE(tableMeta);

            FillTablesMap(streamLookup.Table(), streamLookup.Columns(), tablesMap);
            FillTableId(streamLookup.Table(), *streamLookupProto.MutableTable());

            const auto lookupKeysType = streamLookup.LookupKeysType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(lookupKeysType, "Empty stream lookup keys type");
            YQL_ENSURE(lookupKeysType->GetKind() == ETypeAnnotationKind::List, "Unexpected stream lookup keys type");
            const auto lookupKeysItemType = lookupKeysType->Cast<TListExprType>()->GetItemType();
            streamLookupProto.SetLookupKeysType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *lookupKeysItemType), TypeEnv));

            YQL_ENSURE(lookupKeysItemType->GetKind() == ETypeAnnotationKind::Struct);
            const auto& lookupKeyColumns = lookupKeysItemType->Cast<TStructExprType>()->GetItems();
            for (const auto keyColumn : lookupKeyColumns) {
                YQL_ENSURE(tableMeta->Columns.FindPtr(keyColumn->GetName()), "Unknown column: " << keyColumn->GetName());
                streamLookupProto.AddKeyColumns(TString(keyColumn->GetName()));
            }

            const auto resultType = streamLookup.Ref().GetTypeAnn();
            YQL_ENSURE(resultType, "Empty stream lookup result type");
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Stream, "Unexpected stream lookup result type");
            const auto resultItemType = resultType->Cast<TStreamExprType>()->GetItemType();
            streamLookupProto.SetResultType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *resultItemType), TypeEnv));

            YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Struct);
            const auto& resultColumns = resultItemType->Cast<TStructExprType>()->GetItems();
            for (const auto column : resultColumns) {
                const auto& systemColumns = GetSystemColumns();
                YQL_ENSURE(tableMeta->Columns.FindPtr(column->GetName()) || systemColumns.find(column->GetName()) != systemColumns.end(),
                    "Unknown column: " << column->GetName());
                streamLookupProto.AddColumns(TString(column->GetName()));
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
};

} // namespace

TIntrusivePtr<IKqpQueryCompiler> CreateKqpQueryCompiler(const TString& cluster,
    const TIntrusivePtr<TKikimrTablesData> tablesData, const IFunctionRegistry& funcRegistry,
    TTypeAnnotationContext& typesCtx)
{
    return MakeIntrusive<TKqpQueryCompiler>(cluster, tablesData, funcRegistry, typesCtx);
}

} // namespace NKqp
} // namespace NKikimr
