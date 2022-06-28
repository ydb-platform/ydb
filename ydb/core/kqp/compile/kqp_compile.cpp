#include "kqp_compile.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compile/kqp_mkql_compiler.h>
#include <ydb/core/kqp/compile/kqp_olap_compiler.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/core/tx/schemeshard/schemeshard_utils.h>
#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

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

        case EPhysicalTxType::Unspecified:
            break;
    }

    YQL_ENSURE(false, "Unexpected physical transaction type: " << type);
}

NKqpProto::TKqpPhyQuery::EType GetPhyQueryType(const EPhysicalQueryType& type) {
    switch (type) {
        case EPhysicalQueryType::Data: return NKqpProto::TKqpPhyQuery::TYPE_DATA;
        case EPhysicalQueryType::Scan: return NKqpProto::TKqpPhyQuery::TYPE_SCAN;

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

void FillTable(const TKqpTable& table, NKqpProto::TKqpPhyTable& tableProto) {
    auto pathId = TKikimrPathId::Parse(table.PathId());

    tableProto.SetPath(TString(table.Path()));
    tableProto.SetOwnerId(pathId.OwnerId());
    tableProto.SetTableId(pathId.TableId());
    tableProto.SetSysView(TString(table.SysView()));
    tableProto.SetVersion(FromString<ui64>(table.Version()));
}

template <typename TProto>
void FillColumns(const TCoAtomList& columns, const TKikimrTableMetadata& tableMeta,
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
    auto slot = type->Cast<TDataExprType>()->GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NScheme::NTypeIds::IsYqlType(typeId) && NSchemeShard::IsAllowedKeyType(typeId));

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

void FillOlapProgram(const TCoLambda& process, const TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto)
{
    CompileOlapProgram(process, tableMeta, readProto);
}

class TKqpQueryCompiler : public IKqpQueryCompiler {
public:
    TKqpQueryCompiler(const TString& cluster, const TIntrusivePtr<TKikimrTablesData> tablesData,
        const NMiniKQL::IFunctionRegistry& funcRegistry)
        : Cluster(cluster)
        , TablesData(tablesData)
        , FuncRegistry(funcRegistry)
        , Alloc(TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
        , TypeEnv(Alloc)
        , KqlCtx(cluster, tablesData, TypeEnv, FuncRegistry)
        , KqlCompiler(CreateKqlCompiler(KqlCtx))
    {
        Alloc.Release();
    }

    ~TKqpQueryCompiler() {
        Alloc.Acquire();
    }

    bool CompilePhysicalQuery(const TKqpPhysicalQuery& query, const TKiOperationList& tableOps,
        NKqpProto::TKqpPhyQuery& queryProto, TExprContext& ctx) final
    {
        TGuard<TScopedAlloc> allocGuard(Alloc);

        auto querySettings = TKqpPhyQuerySettings::Parse(query);
        YQL_ENSURE(querySettings.Type);
        queryProto.SetType(GetPhyQueryType(*querySettings.Type));

        auto ops = TableOperationsToProto(tableOps, ctx);
        for (auto& op : ops) {
            const auto tableName = op.GetTable();
            auto operation = static_cast<TYdbOperation>(op.GetOperation());

            *queryProto.AddTableOps() = std::move(op);

            const auto& desc = TablesData->GetTable(Cluster, tableName);
            TableDescriptionToTableInfo(desc, operation, *queryProto.MutableTableInfos());
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

            auto& bindingProto = *queryProto.AddResultBindings();
            auto& txResultProto = *bindingProto.MutableTxResultBinding();
            txResultProto.SetTxIndex(txIndex);
            txResultProto.SetResultIndex(resultIndex);
        }

        return true;
    }

private:
    NKikimr::NMiniKQL::TType* CompileType(TProgramBuilder& pgmBuilder, const TTypeAnnotationNode& inputType) {
        TStringStream errorStream;

        auto type = NCommon::BuildType(inputType, pgmBuilder, errorStream);
        Y_ENSURE(type, "Failed to compile type: " << errorStream.Str());
        return type;
    }

    void CompileStage(const TDqPhyStage& stage, NKqpProto::TKqpPhyStage& stageProto, TExprContext& ctx,
        const TMap<ui64, ui32>& stagesMap)
    {
        stageProto.SetIsEffectsStage(NOpt::IsKqpEffectsStage(stage));

        for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
            const auto& input = stage.Inputs().Item(inputIndex);
            YQL_ENSURE(input.Maybe<TDqConnection>());
            auto connection = input.Cast<TDqConnection>();

            auto& protoInput = *stageProto.AddInputs();
            FillConnection(connection, stagesMap, protoInput, ctx);
        }

        bool hasSort = false;
        bool hasMapJoin = false;
        bool hasUdf = false;
        VisitExpr(stage.Program().Ptr(), [&](const TExprNode::TPtr& exprNode) {
            TExprBase node(exprNode);
            if (auto maybeReadTable = node.Maybe<TKqpWideReadTable>()) {
                auto readTable = maybeReadTable.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTable.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(readTable.Table(), *tableOp.MutableTable());
                FillColumns(readTable.Columns(), *tableMeta, tableOp, true);
                FillReadRange(readTable, *tableMeta, *tableOp.MutableReadRange());
            } else if (auto maybeLookupTable = node.Maybe<TKqpLookupTable>()) {
                auto lookupTable = maybeLookupTable.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, lookupTable.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(lookupTable.Table(), *tableOp.MutableTable());
                FillColumns(lookupTable.Columns(), *tableMeta, tableOp, true);
                FillLookup(lookupTable, *tableOp.MutableLookup());
            } else if (auto maybeUpsertRows = node.Maybe<TKqpUpsertRows>()) {
                auto upsertRows = maybeUpsertRows.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, upsertRows.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);
                YQL_ENSURE(stageProto.GetIsEffectsStage());

                auto settings = TKqpUpsertRowsSettings::Parse(upsertRows);

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(upsertRows.Table(), *tableOp.MutableTable());
                FillColumns(upsertRows.Columns(), *tableMeta, tableOp, false);
                FillEffectRows(upsertRows, *tableOp.MutableUpsertRows(), settings.Inplace);
            } else if (auto maybeDeleteRows = node.Maybe<TKqpDeleteRows>()) {
                auto deleteRows = maybeDeleteRows.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, deleteRows.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                YQL_ENSURE(stageProto.GetIsEffectsStage());

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(deleteRows.Table(), *tableOp.MutableTable());
                FillEffectRows(deleteRows, *tableOp.MutableDeleteRows(), false);
            } else if (auto maybeWideReadTableRanges = node.Maybe<TKqpWideReadTableRanges>()) {
                auto readTableRanges = maybeWideReadTableRanges.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTableRanges.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(readTableRanges.Table(), *tableOp.MutableTable());
                FillColumns(readTableRanges.Columns(), *tableMeta, tableOp, true);
                FillReadRanges(readTableRanges, *tableMeta, *tableOp.MutableReadRanges());
            } else if (auto maybeReadWideTableRanges = node.Maybe<TKqpWideReadOlapTableRanges>()) {
                auto readTableRanges = maybeReadWideTableRanges.Cast();
                auto tableMeta = TablesData->ExistingTable(Cluster, readTableRanges.Table().Path()).Metadata;
                YQL_ENSURE(tableMeta);

                auto& tableOp = *stageProto.AddTableOps();
                FillTable(readTableRanges.Table(), *tableOp.MutableTable());
                FillColumns(readTableRanges.Columns(), *tableMeta, tableOp, true);
                FillReadRanges(readTableRanges, *tableMeta, *tableOp.MutableReadOlapRange());
                FillOlapProgram(readTableRanges.Process(), *tableMeta, *tableOp.MutableReadOlapRange());
            } else if (node.Maybe<TCoSort>()) {
                hasSort = true;
            } else if (node.Maybe<TCoMapJoinCore>()) {
                hasMapJoin = true;
            } else if (node.Maybe<TCoUdf>()) {
                hasUdf = true;
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

        auto paramsType = NDq::CollectParameters(stage.Program(), ctx);
        auto programBytecode = NDq::BuildProgram(stage.Program(), *paramsType, *KqlCompiler, TypeEnv, FuncRegistry,
            ctx, {});

        auto& programProto = *stageProto.MutableProgram();
        programProto.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        programProto.SetRaw(programBytecode);
        programProto.MutableSettings()->SetHasMapJoin(hasMapJoin);
        programProto.MutableSettings()->SetHasSort(hasSort);
        programProto.MutableSettings()->SetHasUdf(hasUdf);

        for (auto member : paramsType->GetItems()) {
            auto paramName = TString(member->GetName());
            stageProto.AddProgramParameters(paramName);
        }

        stageProto.SetProgramAst(KqpExprToPrettyString(stage.Program(), ctx));
        stageProto.SetStageGuid(NDq::TDqStageSettings::Parse(stage).Id);
    }

    void CompileTransaction(const TKqpPhysicalTx& tx, NKqpProto::TKqpPhyTx& txProto, TExprContext& ctx) {
        auto txSettings = TKqpPhyTxSettings::Parse(tx);
        YQL_ENSURE(txSettings.Type);
        txProto.SetType(GetPhyTxType(*txSettings.Type));

        bool hasEffectStage = false;

        TMap<ui64, ui32> stagesMap;
        for (const auto& stage : tx.Stages()) {
            auto* protoStage = txProto.AddStages();
            CompileStage(stage, *protoStage, ctx, stagesMap);
            hasEffectStage |= protoStage->GetIsEffectsStage();
            stagesMap[stage.Ref().UniqueId()] = txProto.StagesSize() - 1;
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
            FillConnection(connection, stagesMap, connectionProto, ctx);

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
    }

    void FillConnection(const TDqConnection& connection, const TMap<ui64, ui32>& stagesMap,
        NKqpProto::TKqpPhyConnection& connectionProto, TExprContext& ctx)
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

            FillTable(streamLookup.Table(), *streamLookupProto.MutableTable());
            FillColumns(streamLookup.Columns(), *tableMeta, streamLookupProto, true);

            const auto lookupKeysType = streamLookup.LookupKeysType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            YQL_ENSURE(lookupKeysType, "Empty stream lookup keys type");
            YQL_ENSURE(lookupKeysType->GetKind() == ETypeAnnotationKind::List, "Unexpected stream lookup keys type");
            const auto lookupKeysItemType = lookupKeysType->Cast<TListExprType>()->GetItemType();
            streamLookupProto.SetLookupKeysType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *lookupKeysItemType), TypeEnv));

            const auto resultType = streamLookup.Ref().GetTypeAnn();
            YQL_ENSURE(resultType, "Empty stream lookup result type");
            YQL_ENSURE(resultType->GetKind() == ETypeAnnotationKind::Stream, "Unexpected stream lookup result type");
            const auto resultItemType = resultType->Cast<TStreamExprType>()->GetItemType();
            streamLookupProto.SetResultType(NMiniKQL::SerializeNode(CompileType(pgmBuilder, *resultItemType), TypeEnv));

            return;
        }

        YQL_ENSURE(false, "Unexpected connection type: " << connection.CallableName());
    }

private:
    TString Cluster;
    const TIntrusivePtr<TKikimrTablesData> TablesData;
    const IFunctionRegistry& FuncRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;
    TKqlCompileContext KqlCtx;
    TIntrusivePtr<NCommon::IMkqlCallableCompiler> KqlCompiler;
};

} // namespace

TIntrusivePtr<IKqpQueryCompiler> CreateKqpQueryCompiler(const TString& cluster,
    const TIntrusivePtr<TKikimrTablesData> tablesData, const IFunctionRegistry& funcRegistry)
{
    return MakeIntrusive<TKqpQueryCompiler>(cluster, tablesData, funcRegistry);
}

} // namespace NKqp
} // namespace NKikimr
