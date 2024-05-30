#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_type_ann_pg.h"

#include <ydb/core/docapi/traits.h>

#include <ydb/library/yql/core/type_ann/type_ann_impl.h>
#include <ydb/library/yql/core/type_ann/type_ann_list.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_datasource_type_ann.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <util/generic/is_in.h>

namespace NYql {
namespace {

using namespace NCommon;
using namespace NNodes;

const TTypeAnnotationNode* GetExpectedRowType(const TKikimrTableDescription& tableDesc,
    const TVector<TString>& columns, const TPosition& pos, TExprContext& ctx)
{
    TVector<const TItemExprType*> expectedRowTypeItems;
    for (auto& column : columns) {
        auto columnType = tableDesc.GetColumnType(column);

        if (!columnType) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "No such column: " << column << ", table: "
                << FullTableName(tableDesc.Metadata->Cluster, tableDesc.Metadata->Name)));
            return nullptr;
        }

        expectedRowTypeItems.push_back(ctx.MakeType<TItemExprType>(column, columnType));
    }

    const TTypeAnnotationNode* expectedRowType = ctx.MakeType<TStructExprType>(expectedRowTypeItems);
    return expectedRowType;
}

const TTypeAnnotationNode* GetExpectedRowType(const TKikimrTableDescription& tableDesc,
    const TStructExprType& structType, const TPosition& pos, TExprContext& ctx)
{
    TVector<TString> columns;
    for (auto& item : structType.GetItems()) {
        columns.push_back(TString(item->GetName()));
    }

    return GetExpectedRowType(tableDesc, columns, pos, ctx);
}

IGraphTransformer::TStatus ConvertTableRowType(TExprNode::TPtr& input, const TKikimrTableDescription& tableDesc,
    TExprContext& ctx)
{
    YQL_ENSURE(input->GetTypeAnn());

    const TTypeAnnotationNode* actualType;
    switch (input->GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::List:
            actualType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            break;
        case ETypeAnnotationKind::Stream:
            actualType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
            break;
        default:
            actualType = input->GetTypeAnn();
            break;
    }

    YQL_ENSURE(actualType->GetKind() == ETypeAnnotationKind::Struct);
    auto rowType = actualType->Cast<TStructExprType>();

    auto pos = ctx.GetPosition(input->Pos());
    auto expectedType = GetExpectedRowType(tableDesc, *rowType, pos, ctx);
    if (!expectedType) {
        return IGraphTransformer::TStatus::Error;
    }

    switch (input->GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::List:
            expectedType = ctx.MakeType<TListExprType>(expectedType);
            break;
        case ETypeAnnotationKind::Stream:
            expectedType = ctx.MakeType<TStreamExprType>(expectedType);
            break;
        default:
            break;
    }

    auto convertStatus = TryConvertTo(input, *expectedType, ctx);

    if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
        ctx.AddError(TIssue(pos, TStringBuilder()
            << "Row type mismatch for table: "
            << FullTableName(tableDesc.Metadata->Cluster, tableDesc.Metadata->Name)));
        return IGraphTransformer::TStatus::Error;
    }

    return convertStatus;
}

class TKiSourceTypeAnnotationTransformer : public TKiSourceVisitorTransformer {
public:
    TKiSourceTypeAnnotationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx, TTypeAnnotationContext& types)
        : SessionCtx(sessionCtx)
        , Types(types)
        , DqsTypeAnn(IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, SessionCtx->Query().Type) ? CreateDqsDataSourceTypeAnnotationTransformer(false) : nullptr)
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        if (DqsTypeAnn && DqsTypeAnn->CanParse(*input)) {
            TStatus status = DqsTypeAnn->DoTransform(input, output, ctx);
            if (input->GetTypeAnn()) {
                return status;
            }
        }
        return TKiSourceVisitorTransformer::DoTransform(input, output, ctx);
    }

private:
    TStatus HandleKiRead(TKiReadBase node, TExprContext& ctx) override {
        auto cluster = TString(node.DataSource().Cluster());

        TKikimrKey key(ctx);
        if (!key.Extract(node.TableKey().Ref())) {
            return TStatus::Error;
        }

        switch (key.GetKeyType()) {
            case TKikimrKey::Type::Table:
            {
                auto readTable = node.Cast<TKiReadTable>();

                const TKikimrTableDescription* tableDesc;
                if ((tableDesc = SessionCtx->Tables().EnsureTableExists(cluster, key.GetTablePath(), node.Pos(), ctx)) == nullptr) {
                    return TStatus::Error;
                }

                const auto& view = key.GetView();
                if (view && view->Name) {
                    if (!ValidateTableHasIndex(tableDesc->Metadata, ctx, node.Pos())) {
                        return TStatus::Error;
                    }
                    if (tableDesc->Metadata->GetIndexMetadata(view->Name).first == nullptr) {
                        ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder()
                            << "Required global index not found, index name: " << view->Name));
                        return TStatus::Error;
                    }
                }
                bool sysColumnsEnabled = SessionCtx->Config().SystemColumnsEnabled();
                auto selectType = GetReadTableRowType(
                    ctx, SessionCtx->Tables(), TString(readTable.DataSource().Cluster()), key.GetTablePath(),
                    readTable.GetSelectColumns(ctx, SessionCtx->Tables(), sysColumnsEnabled), sysColumnsEnabled
                );

                if (!selectType) {
                    return TStatus::Error;
                }

                auto listSelectType = ctx.MakeType<TListExprType>(selectType);

                TTypeAnnotationNode::TListType children;
                children.push_back(node.World().Ref().GetTypeAnn());
                children.push_back(listSelectType);
                auto tupleAnn = ctx.MakeType<TTupleExprType>(children);
                node.Ptr()->SetTypeAnn(tupleAnn);

                YQL_ENSURE(tableDesc->Metadata->ColumnOrder.size() == tableDesc->Metadata->Columns.size());
                return Types.SetColumnOrder(node.Ref(), tableDesc->Metadata->ColumnOrder, ctx);
            }

            case TKikimrKey::Type::TableList:
            {
                auto tableListAnnotation = BuildCommonTableListType(ctx);
                TTypeAnnotationNode::TListType children;
                children.push_back(node.World().Ref().GetTypeAnn());
                children.push_back(tableListAnnotation);
                node.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(children));
                return TStatus::Ok;
            }

            case TKikimrKey::Type::TableScheme:
            {
                auto tableDesc = SessionCtx->Tables().EnsureTableExists(cluster, key.GetTablePath(), node.Pos(), ctx);
                if (!tableDesc) {
                    return TStatus::Error;
                }

                TTypeAnnotationNode::TListType children;
                children.push_back(node.World().Ref().GetTypeAnn());
                children.push_back(ctx.MakeType<TDataExprType>(EDataSlot::Yson));
                node.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(children));
                return TStatus::Ok;
            }

            case TKikimrKey::Type::Role:
            {
                return TStatus::Ok;
            }

            case TKikimrKey::Type::Object:
            {
                return TStatus::Ok;
            }
            case TKikimrKey::Type::Topic:
            {
                return TStatus::Ok;
            }
            case TKikimrKey::Type::Permission:
            {
                return TStatus::Ok;
            }
            case TKikimrKey::Type::PGObject:
            {
                return TStatus::Ok;
            }
            case TKikimrKey::Type::Replication:
            {
                return TStatus::Ok;
            }
        }

        return TStatus::Error;
    }

    TStatus HandleRead(TExprBase node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Failed to annotate Read!, IO rewrite should handle this"));
        return TStatus::Error;
    }

    TStatus HandleLength(TExprBase node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);
        return TStatus::Error;
    }

    TStatus HandleConfigure(TExprBase node, TExprContext& ctx) override {
        if (!EnsureWorldType(*node.Ref().Child(0), ctx)) {
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.Ref().Child(0)->GetTypeAnn());
        return TStatus::Ok;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TTypeAnnotationContext& Types;
    THolder<TVisitorTransformerBase> DqsTypeAnn;
};

namespace {
    std::function<void(TPositionHandle pos, const TString& column, const TString& message)> GetColumnTypeErrorFn(TExprContext& ctx) {
        auto columnTypeError = [&ctx](TPositionHandle pos, const TString& column, const TString& message) {
            ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE,
                TStringBuilder() << "Invalid type for column: " << column << ". " << message));
        };
        return columnTypeError;
    }

    TStringBuf GetColumnTypeName(const TTypeAnnotationNode* type) {
        if (type->GetKind() == ETypeAnnotationKind::Data) {
            return type->Cast<TDataExprType>()->GetName();
        } else {
            auto pgTypeId = type->Cast<TPgExprType>()->GetId();
            auto typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(pgTypeId);
            return NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc);
        }
    }

    bool ValidateColumnDataType(const TDataExprType* type, const TExprBase& typeNode, const TString& columnName,
            TExprContext& ctx) {
        auto columnTypeError = GetColumnTypeErrorFn(ctx);
        switch (type->GetSlot()) {
        case EDataSlot::Decimal:
            if (const auto dataExprParamsType = dynamic_cast<const TDataExprParamsType*>(type)) {
                if (dataExprParamsType->GetParamOne() != "22") {
                    columnTypeError(typeNode.Pos(), columnName, TStringBuilder() << "Bad decimal precision \""
                        << dataExprParamsType->GetParamOne() << "\". Only Decimal(22,9) is supported for table columns");
                    return false;
                }
                if (dataExprParamsType->GetParamTwo() != "9") {
                    columnTypeError(typeNode.Pos(), columnName, TStringBuilder() << "Bad decimal scale \""
                        << dataExprParamsType->GetParamTwo() << "\". Only Decimal(22,9) is supported for table columns");
                    return false;
                }
            }
            break;

        default:
            break;
        }
        return true;
    }

    bool ParseConstraintNode(TExprContext& ctx, TKikimrColumnMetadata& columnMeta, const TExprList& columnTuple, TCoNameValueTuple constraint,  TKikimrConfiguration& config, bool& needEval, bool isAlter = false) {
        auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
        auto typeNode = columnTuple.Item(1);

        auto columnName = TString(nameNode.Value());
        auto columnType = typeNode.Ref().GetTypeAnn();
        auto type = columnType->Cast<TTypeExprType>()->GetType();
        auto isOptional = type->GetKind() == ETypeAnnotationKind::Optional;
        auto actualType = !isOptional ? type : type->Cast<TOptionalExprType>()->GetItemType();
        if (constraint.Name() == "default") {
            auto defaultType = constraint.Value().Ref().GetTypeAnn();
            YQL_ENSURE(constraint.Value().IsValid());
            TExprBase constrValue = constraint.Value().Cast();

            if (!config.EnableColumnsWithDefault) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()),
                    "Columns with default values are not supported yet."));
                return false;
            }

            const bool isNull = IsPgNullExprNode(constrValue) || defaultType->HasNull();
            if (isNull && columnMeta.NotNull) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), TStringBuilder() << "Default expr " << columnName
                    << " is nullable or optional, but column has not null constraint. "));
                return false;
            }

            // unwrap optional
            if (defaultType->GetKind() == ETypeAnnotationKind::Optional) {
                defaultType = defaultType->Cast<TOptionalExprType>()->GetItemType();
            }

            if (defaultType->GetKind() != actualType->GetKind()) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), TStringBuilder() << "Default expr " << columnName
                    << " type mismatch, expected: " << (*actualType) << ", actual: " << *(defaultType)));
                return false;
            }

            bool skipAnnotationValidation = false;
            if (defaultType->GetKind() == ETypeAnnotationKind::Pg) {
                auto defaultPgType = defaultType->Cast<TPgExprType>();
                if (defaultPgType->GetName() == "unknown") {
                    skipAnnotationValidation = true;
                }
            }

            if (!skipAnnotationValidation && !IsSameAnnotation(*defaultType, *actualType)) {
                auto constrPtr = constraint.Value().Cast().Ptr();
                auto status = TryConvertTo(constrPtr, *type, ctx);
                if (status == IGraphTransformer::TStatus::Error) {
                    ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), TStringBuilder() << "Default expr " << columnName
                        << " type mismatch, expected: " << (*actualType) << ", actual: " << *(defaultType)));
                    return false;
                } else if (status == IGraphTransformer::TStatus::Repeat) {
                    auto evaluatedExpr = ctx.Builder(constrPtr->Pos())
                        .Callable("EvaluateExpr")
                        .Add(0, constrPtr)
                        .Seal()
                        .Build();

                    constraint.Ptr()->ChildRef(TCoNameValueTuple::idx_Value) = evaluatedExpr;
                    needEval = true;
                    return true;
                }
            }

            if (columnMeta.IsDefaultKindDefined()) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), TStringBuilder() << "Default setting for " << columnName
                    << " column is already set: "
                    << NKikimrKqp::TKqpColumnMetadataProto::EDefaultKind_Name(columnMeta.DefaultKind)));
                return false;
            }

            columnMeta.SetDefaultFromLiteral();
            auto err = FillLiteralProto(constrValue, actualType, columnMeta.DefaultFromLiteral);
            if (err) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), err.value()));
                return false;
            }

        } else if (constraint.Name().Value() == "serial") {
            if (isAlter) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()),
                     "Column addition with serial data type is unsupported"));
                return false;
            }

            if (columnMeta.IsDefaultKindDefined()) {
                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), TStringBuilder() << "Default setting for "
                    << columnName << " column is already set: "
                    << NKikimrKqp::TKqpColumnMetadataProto::EDefaultKind_Name(columnMeta.DefaultKind)));
                return false;
            }

            columnMeta.DefaultFromSequence = "_serial_column_" + columnMeta.Name;
            columnMeta.SetDefaultFromSequence();
            columnMeta.NotNull = true;
        } else if (constraint.Name().Value() == "not_null") {
            columnMeta.NotNull = true;
        }

        return true;
    }
}

class TKiSinkTypeAnnotationTransformer : public TKiSinkVisitorTransformer
{
public:
    TKiSinkTypeAnnotationTransformer(TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx, TTypeAnnotationContext& types)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , Types(types) {}

private:
    virtual TStatus HandleWriteTable(TKiWriteTable node, TExprContext& ctx) override {
        if (!EnsureWorldType(node.World().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(node.DataSink().Ref(), KikimrProviderName, ctx)) {
            return TStatus::Error;
        }

        auto table = SessionCtx->Tables().EnsureTableExists(TString(node.DataSink().Cluster()),
            TString(node.Table().Value()), node.Pos(), ctx);

        if (!table) {
            return TStatus::Error;
        }

        if (!CheckDocApiModifiation(*table->Metadata, node.Pos(), ctx)) {
            return TStatus::Error;
        }

        auto pos = ctx.GetPosition(node.Pos());
        if (auto maybeTuple = node.Input().Maybe<TExprList>()) {
            auto tuple = maybeTuple.Cast();

            TVector<TExprBase> convertedValues;
            for (const auto& value : tuple) {
                auto valueType = value.Ref().GetTypeAnn();
                if (valueType->GetKind() != ETypeAnnotationKind::Struct) {
                    ctx.AddError(TIssue(pos, TStringBuilder()
                        << "Expected structs as input, but got: " << *valueType));
                    return TStatus::Error;
                }

                auto expectedType = GetExpectedRowType(*table, *valueType->Cast<TStructExprType>(), pos, ctx);
                if (!expectedType) {
                    return TStatus::Error;
                }

                TExprNode::TPtr node = value.Ptr();
                if (TryConvertTo(node, *expectedType, ctx) == TStatus::Error) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                        << "Failed to convert input columns types to scheme types"));
                    return TStatus::Error;
                }

                convertedValues.push_back(TExprBase(node));
            }

            auto list = Build<TCoAsList>(ctx, node.Pos())
                .Add(convertedValues)
                .Done();


            node.Ptr()->ChildRef(TKiWriteTable::idx_Input) = list.Ptr();
            return TStatus::Repeat;
        }

        const TStructExprType* rowType = nullptr;

        auto inputType = node.Input().Ref().GetTypeAnn();
        if (inputType->GetKind() == ETypeAnnotationKind::List) {
            auto listType = inputType->Cast<TListExprType>();
            auto itemType = listType->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                rowType = itemType->Cast<TStructExprType>();
            }
        } else if (inputType->GetKind() == ETypeAnnotationKind::Stream) {
            auto streamType = inputType->Cast<TStreamExprType>();
            auto itemType = streamType->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
                rowType = itemType->Cast<TStructExprType>();
            }
        }

        auto op = GetTableOp(node);
        if (NPgTypeAnn::IsPgInsert(node, op)) {
            TExprNode::TPtr newInput;
            auto ok = NCommon::RenamePgSelectColumns(node.Input().Cast<TCoPgSelect>(), newInput, table->Metadata->ColumnOrder, ctx, Types);
            if (!ok) {
                return TStatus::Error;
            }
            if (newInput != node.Input().Ptr()) {
                node.Ptr()->ChildRef(TKiWriteTable::idx_Input) = newInput;
                return TStatus::Repeat;
            }
        }

        if (!rowType) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "Expected list or stream of structs as input, but got: " << *inputType));
            return TStatus::Error;
        }

        THashSet<TString> defaultConstraintColumnsSet;
        for (auto& keyColumnName : table->Metadata->KeyColumnNames) {
            const auto& columnInfo = table->Metadata->Columns.at(keyColumnName);
            if (rowType->FindItem(keyColumnName)) {
                continue;
            }

            if (!columnInfo.IsDefaultKindDefined())  {
                ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Missing key column in input: " << keyColumnName
                    << " for table: " << table->Metadata->Name));
                return TStatus::Error;
            }

            defaultConstraintColumnsSet.emplace(keyColumnName);
        }

        THashSet<TString> generateColumnsIfInsertColumnsSet;

        for(const auto& [name, info] : table->Metadata->Columns) {
            if (info.IsBuildInProgress && rowType->FindItem(name)) {
                ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
                    << "Column is under build operation, write operation is not allowed to column: " << name
                    << " for table: " << table->Metadata->Name));
                return TStatus::Error;
            }

            if (rowType->FindItem(name)) {
                continue;
            }

            if (op == TYdbOperation::UpdateOn || op == TYdbOperation::DeleteOn) {
                continue;
            }

            if (defaultConstraintColumnsSet.find(name) != defaultConstraintColumnsSet.end()) {
                continue;
            }

            if (info.IsDefaultKindDefined()) {
                if (op == TYdbOperation::Upsert && !info.IsBuildInProgress) {
                    generateColumnsIfInsertColumnsSet.emplace(name);
                }

                defaultConstraintColumnsSet.emplace(name);
            }
        }

        if (op == TYdbOperation::InsertAbort || op == TYdbOperation::InsertRevert ||
            op == TYdbOperation::Upsert || op == TYdbOperation::Replace) {
            for (const auto& [name, meta] : table->Metadata->Columns) {
                if (meta.NotNull) {
                    if (!rowType->FindItem(name) && !meta.IsDefaultKindDefined()) {
                        ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE, TStringBuilder()
                            << "Missing not null column in input: " << name
                            << ". All not null columns should be initialized"));
                        return TStatus::Error;
                    }

                    const auto* itemType = rowType->FindItemType(name);
                    if (itemType && itemType->GetKind() == ETypeAnnotationKind::Pg) {
                        //no type-level notnull check for pg types.
                        continue;
                    }

                    if (itemType && itemType->HasOptionalOrNull()) {
                        ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                            << "Can't set NULL or optional value to not null column: " << name
                            << ". All not null columns should be initialized"));
                        return TStatus::Error;
                    }
                }
            }
        } else if (op == TYdbOperation::UpdateOn) {
            if (TCoPgSelect::Match(node.Input().Ptr().Get())) {
                if (!NPgTypeAnn::ValidatePgUpdateKeys(node, table, ctx)) {
                    return TStatus::Error;
                }
            }
            for (const auto& item : rowType->GetItems()) {
                auto column = table->Metadata->Columns.FindPtr(TString(item->GetName()));
                YQL_ENSURE(column);
                if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Pg) {
                    if (column->NotNull && item->HasOptionalOrNull()) {
                        ctx.AddError(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                            << "Can't set NULL or optional value to not null column: " << column->Name));
                        return TStatus::Error;
                    }
                }
            }
        }

        auto inputColumns = GetSetting(node.Settings().Ref(), "input_columns");
        if (!inputColumns) {
            TExprNode::TListType columns;
            for (auto& item : rowType->GetItems()) {
                columns.push_back(ctx.NewAtom(node.Pos(), item->GetName()));
            }

            TExprNode::TListType defaultConstraintColumns;
            for(auto& generatedColumn: defaultConstraintColumnsSet) {
                defaultConstraintColumns.push_back(ctx.NewAtom(node.Pos(), generatedColumn));
            }

            TExprNode::TListType generateColumnsIfInsert;
            for(auto& generatedColumn: generateColumnsIfInsertColumnsSet) {
                generateColumnsIfInsert.push_back(ctx.NewAtom(node.Pos(), generatedColumn));
            }

            node.Ptr()->ChildRef(TKiWriteTable::idx_Settings) = Build<TCoNameValueTupleList>(ctx, node.Pos())
                .Add(node.Settings())
                .Add()
                    .Name().Build("input_columns")
                    .Value<TCoAtomList>()
                        .Add(columns)
                        .Build()
                    .Build()
                .Add()
                    .Name().Build("default_constraint_columns")
                    .Value<TCoAtomList>()
                        .Add(defaultConstraintColumns)
                        .Build()
                    .Build()
                .Add()
                    .Name().Build("generate_columns_if_insert")
                    .Value<TCoAtomList>()
                        .Add(generateColumnsIfInsert)
                        .Build()
                    .Build()
                .Done()
                .Ptr();

            return TStatus::Repeat;
        } else {
            for (const auto& atom : TCoNameValueTuple(inputColumns).Value().Cast<TCoAtomList>()) {
                YQL_ENSURE(rowType->FindItem(atom.Value()));
            }
        }

        auto status = ConvertTableRowType(node.Ptr()->ChildRef(TKiWriteTable::idx_Input), *table, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    template<typename KiCallable>
    TMaybe<TStatus> DoHandleReturningList(TKiReturningList node, TExprContext& ctx) {
        if (auto maybeWrite = node.Update().Maybe<KiCallable>()) {
            auto write = maybeWrite.Cast();

            bool sysColumnsEnabled = SessionCtx->Config().SystemColumnsEnabled();
            auto selectType = GetReadTableRowType(
                ctx, SessionCtx->Tables(), TString(write.DataSink().Cluster()), TString(write.Table().Value()),
                write.ReturningColumns(), sysColumnsEnabled
            );
            if (!selectType) {
                return TStatus::Error;
            }

            node.Ptr()->SetTypeAnn(ctx.MakeType<TListExprType>(selectType));

            return TStatus::Ok;
        }
        return {};
    }

    virtual TStatus HandleReturningList(TKiReturningList node, TExprContext& ctx) override {
        if (auto status = DoHandleReturningList<TKiWriteTable>(node, ctx)) {
            return *status;
        }
        if (auto status = DoHandleReturningList<TKiUpdateTable>(node, ctx)) {
            return *status;
        }
        if (auto status = DoHandleReturningList<TKiDeleteTable>(node, ctx)) {
            return *status;
        }
        return TStatus::Error;
    }

    virtual TStatus HandleUpdateTable(TKiUpdateTable node, TExprContext& ctx) override {
        auto table = SessionCtx->Tables().EnsureTableExists(TString(node.DataSink().Cluster()), TString(node.Table().Value()), node.Pos(), ctx);
        if (!table) {
            return TStatus::Error;
        }

        if (!CheckDocApiModifiation(*table->Metadata, node.Pos(), ctx)) {
            return TStatus::Error;
        }

        auto rowType = table->SchemeNode;
        auto& filterLambda = node.Ptr()->ChildRef(TKiUpdateTable::idx_Filter);
        if (!UpdateLambdaAllArgumentsTypes(filterLambda, {rowType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!filterLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*filterLambda, EDataSlot::Bool, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& updateLambda = node.Ptr()->ChildRef(TKiUpdateTable::idx_Update);
        if (!UpdateLambdaAllArgumentsTypes(updateLambda, {rowType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!updateLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureStructType(*updateLambda, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto updateResultType = updateLambda->GetTypeAnn()->Cast<TStructExprType>();
        for (auto& item : updateResultType->GetItems()) {
            const auto& name = item->GetName();

            if (table->GetKeyColumnIndex(TString(name))) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                    << "Cannot update primary key column: " << name));
                return IGraphTransformer::TStatus::Error;
            }
        }

        for (const auto& item : updateResultType->GetItems()) {
            auto column = table->Metadata->Columns.FindPtr(TString(item->GetName()));
            if (!column) {
                ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
                    << "Column '" << item->GetName() << "' does not exist in table '" << node.Table().Value() << "'."));
                return TStatus::Error;
            }

            if (column->IsBuildInProgress) {
                ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
                    << "Column '" << item->GetName() << "' is under the build operation '" << node.Table().Value() << "'."));
                return TStatus::Error;
            }

            if (column->NotNull && item->HasOptionalOrNull()) {
                if (item->GetItemType()->GetKind() == ETypeAnnotationKind::Pg) {
                    //no type-level notnull check for pg types.
                    continue;
                }
                ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                    << "Can't set NULL or optional value to not null column: " << column->Name));
                return TStatus::Error;
            }
        }


        auto updateBody = node.Update().Body().Ptr();
        auto status = ConvertTableRowType(updateBody, *table, ctx);
        if (status != IGraphTransformer::TStatus::Ok) {
            if (status == IGraphTransformer::TStatus::Repeat) {
                updateLambda = Build<TCoLambda>(ctx, node.Update().Pos())
                    .Args(node.Update().Args())
                    .Body(updateBody)
                    .Done()
                    .Ptr();
            }

            return status;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDeleteTable(TKiDeleteTable node, TExprContext& ctx) override {
        auto table = SessionCtx->Tables().EnsureTableExists(TString(node.DataSink().Cluster()), TString(node.Table().Value()), node.Pos(), ctx);
        if (!table) {
            return TStatus::Error;
        }

        if (!CheckDocApiModifiation(*table->Metadata, node.Pos(), ctx)) {
            return TStatus::Error;
        }

        auto rowType = table->SchemeNode;
        auto& filterLambda = node.Ptr()->ChildRef(TKiUpdateTable::idx_Filter);
        if (!UpdateLambdaAllArgumentsTypes(filterLambda, {rowType}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!filterLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        if (!EnsureSpecificDataType(*filterLambda, EDataSlot::Bool, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

virtual TStatus HandleCreateTable(TKiCreateTable create, TExprContext& ctx) override {
        TString cluster = TString(create.DataSink().Cluster());
        TString table = TString(create.Table());
        TString tableType = TString(create.TableType());

        auto columnTypeError = GetColumnTypeErrorFn(ctx);

        TKikimrTableMetadataPtr meta = new TKikimrTableMetadata(cluster, table);
        meta->DoesExist = true;
        meta->ColumnOrder.reserve(create.Columns().Size());

        auto tableTypeEnum = GetTableTypeFromString(tableType);
        if (tableTypeEnum == ETableType::Unknown) {
            ctx.AddError(TIssue(ctx.GetPosition(create.Pos()), TStringBuilder()
                << "Unknown table type: " << tableType << "."));
            return TStatus::Error;
        }
        meta->TableType = tableTypeEnum;

        meta->Temporary = TString(create.Temporary()) == "true" ? true : false;

        for (auto atom : create.PrimaryKey()) {
            meta->KeyColumnNames.emplace_back(atom.Value());
        }

        for (const auto& column : create.PartitionBy()) {
            meta->TableSettings.PartitionBy.emplace_back(column.Value());
        }

        for (auto item : create.Columns()) {
            auto columnTuple = item.Cast<TExprList>();
            auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
            auto typeNode = columnTuple.Item(1);

            auto columnName = TString(nameNode.Value());
            auto columnType = typeNode.Ref().GetTypeAnn();
            YQL_ENSURE(columnType && columnType->GetKind() == ETypeAnnotationKind::Type);

            auto type = columnType->Cast<TTypeExprType>()->GetType();

            auto isOptional = type->GetKind() == ETypeAnnotationKind::Optional;
            auto actualType = !isOptional ? type : type->Cast<TOptionalExprType>()->GetItemType();

            if (actualType->GetKind() != ETypeAnnotationKind::Data
                && actualType->GetKind() != ETypeAnnotationKind::Pg
            ) {
                columnTypeError(typeNode.Pos(), columnName, "Only YQL data types and PG types are currently supported");
                return TStatus::Error;
            }

            if (actualType->GetKind() == ETypeAnnotationKind::Data) {
                if (!ValidateColumnDataType(actualType->Cast<TDataExprType>(), typeNode, columnName, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                //TODO: Validate pg modifiers
            }

            TKikimrColumnMetadata columnMeta;
            columnMeta.Name = columnName;
            columnMeta.Type = GetColumnTypeName(actualType);

            if (actualType->GetKind() == ETypeAnnotationKind::Pg) {
                auto pgTypeId = actualType->Cast<TPgExprType>()->GetId();
                columnMeta.TypeInfo = NKikimr::NScheme::TTypeInfo(
                    NKikimr::NScheme::NTypeIds::Pg,
                    NKikimr::NPg::TypeDescFromPgTypeId(pgTypeId)
                );
            }

            if (columnTuple.Size() > 2) {
                const auto& columnConstraints = columnTuple.Item(2).Cast<TCoNameValueTuple>();
                for(const auto& constraint: columnConstraints.Value().Cast<TCoNameValueTupleList>()) {
                    bool needEval = false;
                    if (!ParseConstraintNode(ctx, columnMeta, columnTuple, constraint, SessionCtx->Config(), needEval)) {
                        return TStatus::Error;
                    }

                    if (needEval) {
                        ctx.Step.Repeat(TExprStep::ExprEval);
                        return TStatus(TStatus::Repeat, true);
                    }
                }
            }

            if (columnTuple.Size() > 3) {
                auto families = columnTuple.Item(3).Cast<TCoAtomList>();
                for (auto family : families) {
                    columnMeta.Families.push_back(TString(family.Value()));
                }
            }

            meta->ColumnOrder.push_back(columnName);
            auto insertRes = meta->Columns.insert(std::make_pair(columnName, columnMeta));
            if (!insertRes.second) {
                ctx.AddError(TIssue(ctx.GetPosition(create.Pos()), TStringBuilder()
                    << "Duplicate column: " << columnName << "."));
                return TStatus::Error;
            }
        }

        for (const auto& index : create.Indexes()) {
            const auto type = index.Type().Value();
            TIndexDescription::EType indexType;

            if (type == "syncGlobal") {
                indexType = TIndexDescription::EType::GlobalSync;
            } else if (type == "asyncGlobal") {
                indexType = TIndexDescription::EType::GlobalAsync;
            } else if (type == "syncGlobalUnique") {
                indexType = TIndexDescription::EType::GlobalSyncUnique;
            } else {
                YQL_ENSURE(false, "Unknown index type: " << type);
            }

            TVector<TString> indexColums;
            TVector<TString> dataColums;

            for (const auto& indexCol : index.Columns()) {
                if (!meta->Columns.contains(TString(indexCol.Value()))) {
                    ctx.AddError(TIssue(ctx.GetPosition(indexCol.Pos()), TStringBuilder()
                        << "Index column: " << indexCol.Value() << " was not found in the index table"));
                    return IGraphTransformer::TStatus::Error;
                }
                indexColums.emplace_back(TString(indexCol.Value()));
            }

            for (const auto& dataCol : index.DataColumns()) {
                if (!meta->Columns.contains(TString(dataCol.Value()))) {
                    ctx.AddError(TIssue(ctx.GetPosition(dataCol.Pos()), TStringBuilder()
                        << "Data column: " << dataCol.Value() << " was not found in the index table"));
                    return IGraphTransformer::TStatus::Error;
                }
                dataColums.emplace_back(TString(dataCol.Value()));
            }

            // IndexState and version, pathId are ignored for create table with index request
            TIndexDescription indexDesc(
                TString(index.Name().Value()),
                indexColums,
                dataColums,
                indexType,
                TIndexDescription::EIndexState::Ready,
                0,
                0,
                0
            );

            meta->Indexes.push_back(indexDesc);
        }

        for (const auto& changefeed : create.Changefeeds()) {
            Y_UNUSED(changefeed);
            ctx.AddError(TIssue(ctx.GetPosition(changefeed.Pos()), TStringBuilder()
                << "Cannot create table with changefeed"));
            return TStatus::Error;
        }

        for (auto columnFamily : create.ColumnFamilies()) {
            if (auto maybeTupleList = columnFamily.Maybe<TCoNameValueTupleList>()) {
                TColumnFamily family;
                for (auto familySetting : maybeTupleList.Cast()) {
                    auto name = familySetting.Name().Value();
                    if (name == "name") {
                        family.Name = TString(familySetting.Value().Cast<TCoAtom>().Value());
                    } else if (name == "data") {
                        family.Data = TString(
                            familySetting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                        );
                    } else if (name == "compression") {
                        family.Compression = TString(
                            familySetting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                        );
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(familySetting.Name().Pos()),
                            TStringBuilder() << "Unknown column family setting name: " << name));
                        return TStatus::Error;
                    }
                }
                meta->ColumnFamilies.push_back(family);
            }
        }

        switch (meta->TableType) {
            case ETableType::Unknown:
            case ETableType::TableStore:
            case ETableType::Table: {
                auto status = FillTableSettings(create, ctx, meta);
                if (status != TStatus::Ok) {
                    return status;
                }
                break;
            }
            case ETableType::ExternalTable: {
                auto status = FillExternalTableSettings(create, ctx, meta);
                if (status != TStatus::Ok) {
                    return status;
                }
                break;
            }
        };

        if (meta->TableType == ETableType::TableStore && meta->StoreType != EStoreType::Column) {
            ctx.AddError(TIssue(ctx.GetPosition(create.Pos()),
                    TStringBuilder() << "TABLESTORE recuires STORE = COLUMN setting now"));
            return TStatus::Error;
        }

        auto& tableDesc = SessionCtx->Tables().GetTable(cluster, table);

        auto existingOk = (create.ExistingOk().Value() == "1");
        if (!existingOk && meta->TableType == ETableType::Table && tableDesc.DoesExist() && !tableDesc.Metadata->IsSameTable(*meta)) {
            ctx.AddError(TIssue(ctx.GetPosition(create.Pos()), TStringBuilder()
                << "Table name conflict: " << NCommon::FullTableName(cluster, table)
                << " is used to reference multiple tables."));
            return TStatus::Error;
        }

        tableDesc.Metadata = meta;
        bool sysColumnsEnabled = SessionCtx->Config().SystemColumnsEnabled();
        YQL_ENSURE(tableDesc.Load(ctx, sysColumnsEnabled));

        create.Ptr()->SetTypeAnn(create.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus FillExternalTableSettings(TKiCreateTable create, TExprContext& ctx, TKikimrTableMetadataPtr meta) {
        for (const auto& setting : create.TableSettings()) {
            auto name = setting.Name().Value();
            if (name == "data_source_path") {
                meta->TableSettings.DataSourcePath = TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else if (name == "location") {
                meta->TableSettings.Location = TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else {
                meta->TableSettings.ExternalSourceParameters.emplace_back(name, TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                ));
            }
        }
        if (!meta->TableSettings.DataSourcePath) {
            ctx.AddError(TIssue(ctx.GetPosition(create.Pos()),
                "DATA_SOURCE parameter is required for external table"));
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus FillTableSettings(TKiCreateTable create, TExprContext& ctx, TKikimrTableMetadataPtr meta) {
        for (const auto& setting : create.TableSettings()) {
            auto name = setting.Name().Value();
            if (name == "compactionPolicy") {
                meta->TableSettings.CompactionPolicy = TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else if (name == "autoPartitioningBySize") {
                meta->TableSettings.AutoPartitioningBySize = TString(setting.Value().Cast<TCoAtom>().Value());
            }  else if (name == "partitionSizeMb") {
                ui64 value = FromString<ui64>(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                    );
                if (value) {
                    meta->TableSettings.PartitionSizeMb = value;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                        "Can't set preferred partition size to 0. "
                        "To disable auto partitioning by size use 'SET AUTO_PARTITIONING_BY_SIZE DISABLED'"));
                    return TStatus::Error;
                }
            } else if (name == "autoPartitioningByLoad") {
                meta->TableSettings.AutoPartitioningByLoad = TString(setting.Value().Cast<TCoAtom>().Value());
            } else if (name == "minPartitions") {
                ui64 value = FromString<ui64>(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                    );
                if (value) {
                    meta->TableSettings.MinPartitions = value;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                        "Can't set min partition count to 0"));
                    return TStatus::Error;
                }
            } else if (name == "maxPartitions") {
                ui64 value = FromString<ui64>(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                    );
                if (value) {
                    meta->TableSettings.MaxPartitions = value;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                        "Can't set max partition count to 0"));
                    return TStatus::Error;
                }
            } else if (name == "uniformPartitions") {
                meta->TableSettings.UniformPartitions = FromString<ui64>(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else if (name == "partitionAtKeys") {
                TVector<const TDataExprType*> keyTypes;
                keyTypes.reserve(meta->KeyColumnNames.size() + 1);

                // Getting key column types
                for (const auto& key : meta->KeyColumnNames) {
                    for (auto item : create.Columns()) {
                        auto columnTuple = item.Cast<TExprList>();
                        auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                        auto columnName = TString(nameNode.Value());
                        if (columnName == key) {
                            auto typeNode = columnTuple.Item(1);
                            auto keyType = typeNode.Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                            YQL_ENSURE(
                                keyType->GetKind() != ETypeAnnotationKind::Pg,
                                "pg types are not supported for partition at keys"
                            );
                            if (keyType->HasOptional()) {
                                keyType = keyType->Cast<TOptionalExprType>()->GetItemType();
                            }
                            keyTypes.push_back(keyType->Cast<TDataExprType>());
                        }
                    }
                }
                if (keyTypes.size() != create.PrimaryKey().Size()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Can't get all key column types"));
                    return IGraphTransformer::TStatus::Error;
                }
                auto listNode = setting.Value().Cast<TExprList>();
                for (size_t i = 0; i < listNode.Size(); ++i) {
                    auto partitionNode = listNode.Item(i);
                    TVector<std::pair<EDataSlot, TString>> keys;
                    auto boundaries = partitionNode.Cast<TExprList>();
                    if (boundaries.Size() > keyTypes.size()) {
                        ctx.AddError(TIssue(ctx.GetPosition(partitionNode.Pos()), TStringBuilder()
                            << "Partition at keys has " << boundaries.Size() << " key values while there are only "
                            << keyTypes.size() << " key columns"));
                        return IGraphTransformer::TStatus::Error;
                    }
                    for (size_t j = 0; j < boundaries.Size(); ++j) {
                        TExprNode::TPtr keyNode = boundaries.Item(j).Ptr();
                        TString content(keyNode->Child(0)->Content());
                        if (keyNode->GetTypeAnn()->Cast<TDataExprType>()->GetSlot() != keyTypes[j]->GetSlot()) {
                            if (TryConvertTo(keyNode, *keyTypes[j], ctx) == TStatus::Error) {
                                ctx.AddError(TIssue(ctx.GetPosition(keyNode->Pos()), TStringBuilder()
                                    << "Failed to convert value \"" << content
                                    << "\" to a corresponding key column type"));
                                return TStatus::Error;
                            }
                            auto newTypeAnn = ctx.MakeType<TDataExprType>(keyTypes[j]->GetSlot());
                            keyNode->SetTypeAnn(newTypeAnn);
                        }

                        keys.emplace_back(keyTypes[j]->GetSlot(), content);
                    }

                    meta->TableSettings.PartitionAtKeys.push_back(keys);
                }
            } else if (name == "keyBloomFilter") {
                meta->TableSettings.KeyBloomFilter = TString(setting.Value().Cast<TCoAtom>().Value());
            } else if (name == "readReplicasSettings") {
                meta->TableSettings.ReadReplicasSettings = TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else if (name == "setTtlSettings") {
                TTtlSettings ttlSettings;
                TString error;

                YQL_ENSURE(setting.Value().Maybe<TCoNameValueTupleList>());
                if (!TTtlSettings::TryParse(setting.Value().Cast<TCoNameValueTupleList>(), ttlSettings, error)) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                        TStringBuilder() << "Invalid TTL settings: " << error));
                    return TStatus::Error;
                }

                meta->TableSettings.TtlSettings.Set(ttlSettings);
            } else if (name == "resetTtlSettings") {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                    "Can't reset TTL settings"));
                return TStatus::Error;
            } else if (name == "setTiering") {
                meta->TableSettings.Tiering.Set(TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                ));
            } else if (name == "resetTiering") {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                    "Can't reset TIERING"));
                return TStatus::Error;
            } else if (name == "storeType") {
                TMaybe<TString> storeType = TString(setting.Value().Cast<TCoAtom>().Value());
                if (storeType && to_lower(storeType.GetRef()) == "column") {
                    meta->StoreType = EStoreType::Column;
                }
            } else if (name == "partitionByHashFunction") {
                meta->TableSettings.PartitionByHashFunction = TString(
                    setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                );
            } else if (name == "storeExternalBlobs") {
                meta->TableSettings.StoreExternalBlobs = TString(setting.Value().Cast<TCoAtom>().Value());
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                    TStringBuilder() << "Unknown table profile setting: " << name));
                return TStatus::Error;
            }
        }
        return TStatus::Ok;
    }


    virtual TStatus HandleDropTable(TKiDropTable node, TExprContext& ctx) override {
        bool missingOk = (node.MissingOk().Value() == "1");
        if (!missingOk) {
            auto table = SessionCtx->Tables().EnsureTableExists(TString(node.DataSink().Cluster()), TString(node.Table().Value()), node.Pos(), ctx);
            if (!table) {
                return TStatus::Error;
            }

            if (table->GetTableType() == ETableType::Table) {
                if (!CheckDocApiModifiation(*table->Metadata, node.Pos(), ctx)) {
                    return TStatus::Error;
                }
            }
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterTable(TKiAlterTable node, TExprContext& ctx) override {
        auto table = SessionCtx->Tables().EnsureTableExists(TString(node.DataSink().Cluster()), TString(node.Table().Value()), node.Pos(), ctx);
        if (!table) {
            return TStatus::Error;
        }

        if (!table->Metadata) {
            return TStatus::Error;
        }

        if (!CheckDocApiModifiation(*table->Metadata, node.Pos(), ctx)) {
            return TStatus::Error;
        }

        YQL_ENSURE(!node.Actions().Empty());

        for (const auto& action : node.Actions()) {
            auto name = action.Name().Value();
            if (name == "renameTo") {
                YQL_ENSURE(action.Value().Cast<TCoAtom>().Value());
            } else if (name == "addColumns") {
                auto listNode = action.Value().Cast<TExprList>();
                for (size_t i = 0; i < listNode.Size(); ++i) {
                    auto item = listNode.Item(i);
                    auto columnTuple = item.Cast<TExprList>();
                    auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                    auto name = TString(nameNode.Value());
                    if (table->Metadata->Columns.FindPtr(name)) {
                        ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()), TStringBuilder()
                            << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                            << " Column: \"" << name << "\" already exists"));
                        return TStatus::Error;
                    }

                }
                auto columnTypeError = GetColumnTypeErrorFn(ctx);
                for (size_t i = 0; i < listNode.Size(); ++i) {
                    auto item = listNode.Item(i);
                    auto columnTuple = item.Cast<TExprList>();
                    auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                    auto name = TString(nameNode.Value());
                    columnTuple.Item(0).Cast<TCoAtom>();
                    auto typeNode = columnTuple.Item(1);
                    auto columnType = typeNode.Ref().GetTypeAnn();
                    YQL_ENSURE(columnType && columnType->GetKind() == ETypeAnnotationKind::Type);
                    auto type = columnType->Cast<TTypeExprType>()->GetType();
                    auto actualType = (type->GetKind() == ETypeAnnotationKind::Optional) ?
                        type->Cast<TOptionalExprType>()->GetItemType() : type;

                    if (actualType->GetKind() != ETypeAnnotationKind::Data) {
                        columnTypeError(typeNode.Pos(), name, "Only core YQL data types are currently supported");
                        return TStatus::Error;
                    }

                    auto dataType = actualType->Cast<TDataExprType>();

                    if (!ValidateColumnDataType(dataType, typeNode, name, ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    TKikimrColumnMetadata columnMeta;
                    // columnMeta.Name = columnName;
                    columnMeta.Type = GetColumnTypeName(actualType);
                    if (columnTuple.Size() > 2) {
                        const auto& columnConstraints = columnTuple.Item(2).Cast<TCoNameValueTuple>();
                        for(const auto& constraint: columnConstraints.Value().Cast<TCoNameValueTupleList>()) {
                            bool needEval = false;
                            if (!ParseConstraintNode(ctx, columnMeta, columnTuple, constraint, SessionCtx->Config(), needEval, true)) {
                                return TStatus::Error;
                            }

                            if (needEval) {
                                ctx.Step.Repeat(TExprStep::ExprEval);
                                return TStatus(TStatus::Repeat, true);
                            }
                        }
                    }

                    if (columnTuple.Size() > 3) {
                        auto families = columnTuple.Item(3);
                        if (families.Cast<TCoAtomList>().Size() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()), TStringBuilder()
                                << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                                << " Column: \"" << name
                                << "\". Several column families for a single column are not yet supported"));
                            return TStatus::Error;
                        }
                    }
                }
            } else if (name == "dropColumns") {
                auto listNode = action.Value().Cast<TCoAtomList>();
                THashSet<TString> keyColumns;
                for (const auto& keyColumnName : table->Metadata->KeyColumnNames) {
                    keyColumns.insert(keyColumnName);
                }
                for (auto dropColumn : listNode) {
                    TString name(dropColumn.Value());

                    if (!table->Metadata->Columns.FindPtr(name)) {
                        ctx.AddError(TIssue(ctx.GetPosition(dropColumn.Pos()), TStringBuilder()
                            << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                            << " Column \"" << name << "\" does not exist"));
                        return TStatus::Error;
                    }

                    if (keyColumns.find(name) != keyColumns.end()) {
                        ctx.AddError(TIssue(ctx.GetPosition(dropColumn.Pos()), TStringBuilder()
                            << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                            << " Column: \"" << name << "\" is a key column. Key column drop is not supported"));
                        return TStatus::Error;
                    }
                }
            } else if (name == "alterColumns") {
                auto listNode = action.Value().Cast<TExprList>();
                for (size_t i = 0; i < listNode.Size(); ++i) {
                    auto item = listNode.Item(i);
                    auto columnTuple = item.Cast<TExprList>();
                    auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();;
                    auto name = TString(nameNode.Value());
                    if (!table->Metadata->Columns.FindPtr(name)) {
                        ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()), TStringBuilder()
                            << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                            << " Column: \"" << name << "\" does not exist"));
                        return TStatus::Error;
                    }
                    auto alterColumnList = columnTuple.Item(1).Cast<TExprList>();
                    auto alterColumnAction = TString(alterColumnList.Item(0).Cast<TCoAtom>());
                    if (alterColumnAction == "setDefault") {
                        auto setDefault = alterColumnList.Item(1).Cast<TCoAtomList>();
                        auto func = TString(setDefault.Item(0).Cast<TCoAtom>());
                        auto arg = TString(setDefault.Item(1).Cast<TCoAtom>());
                        if (func != "nextval") {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                TStringBuilder() << "Unsupported function to set default: " << func));
                            return TStatus::Error;
                        }
                        if (setDefault.Size() > 2) {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                TStringBuilder() << "Function nextval has exactly one argument"));
                            return TStatus::Error;
                        }
                    } else if (alterColumnAction == "setFamily") {
                        auto families = alterColumnList.Item(1).Cast<TCoAtomList>();
                        if (families.Size() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()), TStringBuilder()
                                << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                                << " Column: \"" << name
                                << "\". Several column families for a single column are not yet supported"));
                            return TStatus::Error;
                        }
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                TStringBuilder() << "Unsupported action to alter column"));
                        return TStatus::Error;
                    }
                }
            } else if (name == "addIndex") {
                auto listNode = action.Value().Cast<TExprList>();
                for (size_t i = 0; i < listNode.Size(); ++i) {
                    auto item = listNode.Item(i);
                    auto columnTuple = item.Cast<TExprList>();
                    auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                    auto name = TString(nameNode.Value());
                    if (name == "indexColumns" || name == "dataColumns") {
                        auto columnList = columnTuple.Item(1).Cast<TCoAtomList>();
                        for (auto column : columnList) {
                            TString columnName(column.Value());
                            if (!table->Metadata->Columns.FindPtr(columnName)) {
                                ctx.AddError(TIssue(ctx.GetPosition(column.Pos()), TStringBuilder()
                                    << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                                    << " Column: \"" << columnName << "\" does not exist"));
                                return TStatus::Error;
                            }
                        }
                    }
                }
            } else if (name == "dropIndex") {
                auto nameNode = action.Value().Cast<TCoAtom>();
                auto name = TString(nameNode.Value());

                const auto& indexes = table->Metadata->Indexes;

                auto cmp = [name](const TIndexDescription& desc) {
                    return name == desc.Name;
                };

                if (std::find_if(indexes.begin(), indexes.end(), cmp) == indexes.end()) {
                    ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()), TStringBuilder()
                        << "AlterTable : " << NCommon::FullTableName(table->Metadata->Cluster, table->Metadata->Name)
                        << " Index: \"" << name << "\" does not exist"));
                    return TStatus::Error;
                }
            } else if (name != "addColumnFamilies"
                    && name != "alterColumnFamilies"
                    && name != "setTableSettings"
                    && name != "addChangefeed"
                    && name != "dropChangefeed"
                    && name != "renameIndexTo"
                    && name != "alterIndex")
            {
                ctx.AddError(TIssue(ctx.GetPosition(action.Name().Pos()),
                    TStringBuilder() << "Unknown alter table action: " << name));
                return TStatus::Error;
            }
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    static bool CheckTopicSettings(const TCoNameValueTupleList& settings, TExprContext& ctx) {
        ui32 minParts = 0, partsLimit = 0;
        TPosition errorPos;
        for (const auto& setting : settings) {
            auto name = setting.Name().Value();
            if (name == "setMeteringMode") {
                if (!EnsureAtom(setting.Value().Ref(), ctx)) {
                    return false;
                }
                auto val = to_lower(TString(setting.Value().template Cast<TCoAtom>().Value()));
                Ydb::Topic::MeteringMode meteringMode;
                auto result = GetTopicMeteringModeFromString(val, meteringMode);
                if (!result) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                                        TStringBuilder() << "unknown metering_mode: " << val));
                }

            } else if (name == "setMinPartitions") {
                ui32 value = FromString<ui32>(
                        setting.Value().Cast<TCoDataCtor>().Literal().template Cast<TCoAtom>().Value()
                );
                minParts = value;
                errorPos = ctx.GetPosition(setting.Value().Ref().Pos());
            } else if (name == "setPartitionsLimit") {
                ui32 value = FromString<ui32>(
                        setting.Value().Cast<TCoDataCtor>().Literal().template Cast<TCoAtom>().Value()
                );
                partsLimit = value;
                errorPos = ctx.GetPosition(setting.Value().Ref().Pos());
            } else if (name.StartsWith("reset")) {
                ctx.AddError(TIssue(
                        errorPos,
                        TStringBuilder() << "RESET is currently not supported for topic options")
                );
                return false;
            }
            if (minParts && partsLimit && partsLimit < minParts) {
                ctx.AddError(TIssue(
                        errorPos,
                        TStringBuilder() << "partitions_limit cannot be less than min_partitions")
                );
                return false;
            }
        }
        return true;
    }
    static bool CheckConsumerSettings(const TCoNameValueTupleList& settings, TExprContext& ctx) {
        for (const auto& setting : settings) {
            auto name = setting.Name().Value();
            auto val = TString(setting.Value().Cast<TCoDataCtor>().Literal().template Cast<TCoAtom>().Value());
            if (name == "setSupportedCodecs") {
                auto codecsList = GetTopicCodecsFromString(val);
                if (codecsList.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                                        TStringBuilder() << "unknown codec found or codecs list is malformed : " << val));
                    return false;
                }
            }
        }
        return true;
    }

    static bool CheckSequenceSettings(const TCoNameValueTupleList& settings, TExprContext& ctx) {
        const static std::unordered_set<TString> sequenceSettingNames =
            {"start", "increment", "cache", "minvalue", "maxvalue", "cycle"};
        for (const auto& setting : settings) {
            auto name = setting.Name().Value();
            if (!sequenceSettingNames.contains(TString(name))) {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                    TStringBuilder() << "unsupported setting with name: " << name));
                return false;
            }
        }
        return true;
    }

    virtual TStatus HandleCreateTopic(TKiCreateTopic node, TExprContext& ctx) override {
        if (!CheckTopicSettings(node.Settings(), ctx)) {
            return TStatus::Error;
        }

        for (const auto& consumer : node.Consumers()) {
            if(!CheckConsumerSettings(consumer.Settings(), ctx)) {
                return TStatus::Error;
            }
        }
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleCreateSequence(TKiCreateSequence node, TExprContext& ctx) override {
        if(!CheckSequenceSettings(node.SequenceSettings(), ctx)) {
            return TStatus::Error;
        }

        if (!node.Settings().Empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Unsupported sequence settings"));
            return TStatus::Error;
        }

        TString valueType = TString(node.ValueType());
        if (valueType != "int8") {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Unsupported value type: " << valueType));
            return TStatus::Error;
        }

        if (TString(node.Temporary()) == "true") {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Temporary sequences is currently not supported"));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropSequence(TKiDropSequence node, TExprContext& ctx) override {
        for (const auto& setting : node.Settings()) {
            auto name = setting.Name().Value();
            ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                TStringBuilder() << "Unknown drop sequence setting: " << name));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterSequence(TKiAlterSequence node, TExprContext& ctx) override {
        if(!CheckSequenceSettings(node.SequenceSettings(), ctx)) {
            return TStatus::Error;
        }

        if (!node.Settings().Empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Unsupported sequence settings"));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterTopic(TKiAlterTopic node, TExprContext& ctx) override {
        if (!CheckTopicSettings(node.Settings(), ctx)) {
            return TStatus::Error;
        }
       THashSet<TString> allConsumers;
        auto CheckConsumerIsUnique = [&] (const auto& consumer) {
            auto res = allConsumers.insert(consumer.Name().StringValue());
            if (!res.second) {
                ctx.AddError(TIssue(
                        ctx.GetPosition(consumer.Name().Ref().Pos()),
                        TStringBuilder() << "consumer '" << consumer.Name().StringValue() << "' referenced more than once"
                ));
                return false;
            }
            return true;
        };
        for (const auto& consumer : node.AddConsumers()) {
            if (!CheckConsumerIsUnique(consumer)) {
                return TStatus::Error;
            }
            if(!CheckConsumerSettings(consumer.Settings(), ctx)) {
                return TStatus::Error;
            }
        }
        for (const auto& consumer : node.AlterConsumers()) {
            if (!CheckConsumerIsUnique(consumer)) {
                return TStatus::Error;
            }
            if(!CheckConsumerSettings(consumer.Settings(), ctx)) {
                return TStatus::Error;
            }
        }
        for (const auto& consumer : node.DropConsumers()) {
            auto res = allConsumers.insert(consumer.StringValue());
            if (!res.second) {
                ctx.AddError(TIssue(
                        ctx.GetPosition(consumer.Ref().Pos()),
                        TStringBuilder() << "consumer '" << consumer.StringValue() << "' referenced more than once"
                ));
                return TStatus::Error;
            }
        }
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropTopic(TKiDropTopic node, TExprContext&) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    static bool CheckReplicationSettings(const TCoNameValueTupleList& settings, const THashSet<TString>& supported, TExprContext& ctx) {
        for (const auto& setting : settings) {
            auto name = setting.Name().Value();
            if (!supported.contains(TString(name))) {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()), TStringBuilder() << "Unsupported setting"
                    << ": " << name));
                return false;
            }
        }

        return true;
    }

    virtual TStatus HandleCreateReplication(TKiCreateReplication node, TExprContext& ctx) override {
        const THashSet<TString> supportedSettings = {
            "connection_string",
            "endpoint",
            "database",
            "token",
            "token_secret_name",
            "user",
            "password",
            "password_secret_name",
        };

        if (!CheckReplicationSettings(node.ReplicationSettings(), supportedSettings, ctx)) {
            return TStatus::Error;
        }

        if (!node.Settings().Empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unsupported settings"));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterReplication(TKiAlterReplication node, TExprContext& ctx) override {
        const THashSet<TString> supportedSettings = {
            "state",
            "failover_mode",
        };

        if (!CheckReplicationSettings(node.ReplicationSettings(), supportedSettings, ctx)) {
            return TStatus::Error;
        }

        if (!node.Settings().Empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Unsupported settings"));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropReplication(TKiDropReplication node, TExprContext&) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleModifyPermissions(TKiModifyPermissions node, TExprContext&) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleCreateUser(TKiCreateUser node, TExprContext& ctx) override {
        for (const auto& setting : node.Settings()) {
            auto name = setting.Name().Value();
            if (name == "password") {
                if (!EnsureAtom(setting.Value().Ref(), ctx)) {
                    return TStatus::Error;
                }
            } else if (name == "passwordEncrypted") {
                if (setting.Value()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                        TStringBuilder() << "passwordEncrypted node shouldn't have value" << name));
                }
            } else if (name == "nullPassword") {
                if (setting.Value()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                        TStringBuilder() << "nullPassword node shouldn't have value" << name));
                }
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                    TStringBuilder() << "Unknown create user setting: " << name));
                return TStatus::Error;
            }
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterUser(TKiAlterUser node, TExprContext& ctx) override {
        for (const auto& setting : node.Settings()) {
            auto name = setting.Name().Value();
            if (name == "password") {
                if (!EnsureAtom(setting.Value().Ref(), ctx)) {
                    return TStatus::Error;
                }
            } else if (name == "passwordEncrypted") {
                if (setting.Value()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                        TStringBuilder() << "passwordEncrypted node shouldn't have value" << name));
                }
            } else if (name == "nullPassword") {
                if (setting.Value()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Ref().Pos()),
                        TStringBuilder() << "nullPassword node shouldn't have value" << name));
                }
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                    TStringBuilder() << "Unknown alter user setting: " << name));
                return TStatus::Error;
            }
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropUser(TKiDropUser node, TExprContext& ctx) override {
        for (const auto& setting : node.Settings()) {
            auto name = setting.Name().Value();
            ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                TStringBuilder() << "Unknown drop user setting: " << name));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleUpsertObject(TKiUpsertObject node, TExprContext& /*ctx*/) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleCreateObject(TKiCreateObject node, TExprContext& /*ctx*/) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterObject(TKiAlterObject node, TExprContext& /*ctx*/) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropObject(TKiDropObject node, TExprContext& /*ctx*/) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleCreateGroup(TKiCreateGroup node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleAlterGroup(TKiAlterGroup node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleRenameGroup(TKiRenameGroup node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleDropGroup(TKiDropGroup node, TExprContext& ctx) override {
        for (const auto& setting : node.Settings()) {
            auto name = setting.Name().Value();
            ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                TStringBuilder() << "Unknown drop group setting: " << name));
            return TStatus::Error;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandlePgDropObject(TPgDropObject node, TExprContext& /*ctx*/) override {
        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleWrite(TExprBase node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Failed to annotate Write!, IO rewrite should handle this"));
        return TStatus::Error;
    }

    virtual TStatus HandleCommit(NNodes::TCoCommit node, TExprContext& ctx) override {
        auto settings = NCommon::ParseCommitSettings(node, ctx);

        bool isFlushCommit = false;
        bool isRollback = false;
        if (settings.Mode) {
            auto mode = settings.Mode.Cast().Value();

            if (!KikimrCommitModes().contains(mode)) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                    << "Unsupported Kikimr commit mode: " << mode));
                return TStatus::Error;
            }

            isFlushCommit = (mode == KikimrCommitModeFlush());
            isRollback = (mode == KikimrCommitModeRollback());
        }

        if (!settings.EnsureEpochEmpty(ctx)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!settings.EnsureOtherEmpty(ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (SessionCtx->Query().Type) {
            case EKikimrQueryType::YqlScript:
            case EKikimrQueryType::YqlScriptStreaming:
            case EKikimrQueryType::YqlInternal:
                break;

            default:
                if (!isFlushCommit) {
                    auto opName = isRollback ? "ROLLBACK" : "COMMIT";
                    ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::KIKIMR_BAD_OPERATION, TStringBuilder()
                        << opName << " not supported inside YDB query"));

                    return TStatus::Error;
                }
                break;
        }

        node.Ptr()->SetTypeAnn(node.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    virtual TStatus HandleEffects(NNodes::TKiEffects node, TExprContext& ctx) override {
        for (const auto& effect : node) {
            if (!EnsureWorldType(effect.Ref(), ctx)) {
                return TStatus::Error;
            }

            if (!KikimrSupportedEffects().contains(effect.CallableName())) {
                bool supported = false;
                if (effect.Ref().ChildrenSize() > 1) {
                    TExprBase dataSinkArg(effect.Ref().Child(1));
                    if (auto maybeDataSink = dataSinkArg.Maybe<TCoDataSink>()) {
                        TStringBuf dataSinkCategory = maybeDataSink.Cast().Category();
                        auto dataSinkProviderIt = Types.DataSinkMap.find(dataSinkCategory);
                        if (dataSinkProviderIt != Types.DataSinkMap.end()) {
                            if (auto* dqIntegration = dataSinkProviderIt->second->GetDqIntegration()) {
                                auto canWrite = dqIntegration->CanWrite(*effect.Raw(), ctx);
                                if (canWrite) {
                                    supported = *canWrite; // if false, we will exit this function a few lines later
                                }
                            }
                        }
                    }
                }

                if (!supported) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Unsupported Kikimr data query effect: " << effect.CallableName()));
                    return TStatus::Error;
                }
            }
        }

        node.Ptr()->SetTypeAnn(ctx.MakeType<TWorldExprType>());
        return TStatus::Ok;
    }

    virtual TStatus HandleDataQueryBlock(NNodes::TKiDataQueryBlock node, TExprContext& ctx) override {
        if (!EnsureWorldType(node.Effects().Ref(), ctx)) {
            return TStatus::Error;
        }

        TTypeAnnotationNode::TListType resultTypes;
        for (const auto& result : node.Results()) {
            auto resultType = result.Value().Ref().GetTypeAnn();
            if (!EnsureListType(node.Pos(), *resultType, ctx)) {
                return TStatus::Error;
            }
            auto itemType = resultType->Cast<TListExprType>()->GetItemType();
            if (!EnsureStructType(node.Pos(), *itemType, ctx)) {
                return TStatus::Error;
            }
            auto structType = itemType->Cast<TStructExprType>();

            for (const auto& column : result.Columns()) {
                if (!structType->FindItem(column)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Invalid column in result: " << column.Value()));
                    return TStatus::Error;
                }
            }

            resultTypes.push_back(resultType);
        }

        node.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(resultTypes));
        return TStatus::Ok;
    }

    virtual TStatus HandleDataQueryBlocks(NNodes::TKiDataQueryBlocks node, TExprContext& ctx) override {
        TTypeAnnotationNode::TListType resultTypes;
        for (const auto& block : node) {
            auto blockType = block.Ref().GetTypeAnn();
            if (!EnsureTupleType(block.Pos(), *blockType, ctx)) {
                return TStatus::Error;
            }

            for (const auto& resultType : blockType->Cast<TTupleExprType>()->GetItems()) {
                resultTypes.push_back(resultType);
            }
        }

        node.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(resultTypes));
        return TStatus::Ok;
    }

    virtual TStatus HandleExecDataQuery(NNodes::TKiExecDataQuery node, TExprContext& ctx) override {
        if (!EnsureWorldType(node.World().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureDataSink(node.DataSink().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTupleType(node.QueryBlocks().Ref(), ctx)) {
            return TStatus::Error;
        }

        TTypeAnnotationNode::TListType children;
        children.push_back(node.World().Ref().GetTypeAnn());
        children.push_back(node.QueryBlocks().Ref().GetTypeAnn());
        auto tupleAnn = ctx.MakeType<TTupleExprType>(children);
        node.Ptr()->SetTypeAnn(tupleAnn);

        return TStatus::Ok;
    }

    bool CheckDocApiModifiation(const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) {
        if (!SessionCtx->Query().DocumentApiRestricted) {
            return true;
        }

        if (!meta.Attributes.FindPtr(NKikimr::NDocApi::VersionAttribute)) {
            return true;
        }

        ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_BAD_OPERATION, TStringBuilder()
            << "Document API table cannot be modified from YQL query: " << meta.Name));
        return false;
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TTypeAnnotationContext& Types;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKiSourceTypeAnnotationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types)
{
    return new TKiSourceTypeAnnotationTransformer(sessionCtx, types);
}

TAutoPtr<IGraphTransformer> CreateKiSinkTypeAnnotationTransformer(TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx, TTypeAnnotationContext& types)
{
    return new TKiSinkTypeAnnotationTransformer(gateway, sessionCtx, types);
}

const TTypeAnnotationNode* GetReadTableRowType(TExprContext& ctx, const TKikimrTablesData& tablesData,
    const TString& cluster, const TString& table, TPositionHandle pos, bool withSystemColumns)
{
    auto tableDesc = tablesData.EnsureTableExists(cluster, table, pos, ctx);
    if (!tableDesc) {
        return nullptr;
    }
    TVector<TCoAtom> columns;
    for (auto&& [column, _] : tableDesc->Metadata->Columns) {
        columns.push_back(Build<TCoAtom>(ctx, pos).Value(column).Done());
    }
    return GetReadTableRowType(ctx, tablesData, cluster, table, Build<TCoAtomList>(ctx, pos).Add(columns).Done(), withSystemColumns);
}

const TTypeAnnotationNode* GetReadTableRowType(TExprContext& ctx, const TKikimrTablesData& tablesData,
    const TString& cluster, const TString& table, TCoAtomList select, bool withSystemColumns)
{
    auto tableDesc = tablesData.EnsureTableExists(cluster, table, select.Pos(), ctx);
    if (!tableDesc) {
        return nullptr;
    }

    TVector<const TItemExprType*> resultItems;
    for (auto item : select) {
        auto column = tableDesc->Metadata->Columns.FindPtr(item.Value());
        TString columnName;
        if (column) {
            columnName = column->Name;
        } else {
            if (withSystemColumns && IsKikimrSystemColumn(item.Value())) {
                columnName = TString(item.Value());
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(select.Pos()), TStringBuilder()
                    << "Column not found: " << item.Value()));
                return nullptr;
            }
        }

        auto type = tableDesc->GetColumnType(columnName);
        YQL_ENSURE(type, "No such column: " << columnName);

        auto itemType = ctx.MakeType<TItemExprType>(columnName, type);
        if (!itemType->Validate(select.Pos(), ctx)) {
            return nullptr;
        }
        resultItems.push_back(itemType);
    }

    auto resultType = ctx.MakeType<TStructExprType>(resultItems);
    if (!resultType->Validate(select.Pos(), ctx)) {
        return nullptr;
    }

    return resultType;
}

} // namespace NYql
