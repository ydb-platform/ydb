#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

#include <algorithm>
#include <functional>
#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <tuple>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;
using namespace NYql::NNodes;

struct TColumnSpec {
    TString Name;
    TString Type;
    bool NotNull;
};

struct TExportTestContext {
    TExportTestContext()
        : FuncRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryCtx(MakeIntrusive<TKikimrQueryContext>(
              FuncRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , UserRequestContext(MakeIntrusive<TUserRequestContext>())
        , KqpCtx("ut", Config, QueryCtx, Tables, UserRequestContext)
        , RboCtx(KqpCtx, ExprCtx, TypeCtx, TypeAnnTransformer, *FuncRegistry)
    {
    }

    TExprContext ExprCtx;
    TTypeAnnotationContext TypeCtx;
    TNullTransformer TypeAnnTransformer;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry;
    TIntrusivePtr<TKikimrConfiguration> Config;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NOpt::TKqpOptimizeContext KqpCtx;
    TRBOContext RboCtx;
    TPlanProps ExpressionProps;
};

const TKikimrTableDescription& AddTable(
    TExportTestContext& ctx,
    const TString& path,
    const TVector<TColumnSpec>& columns,
    const TVector<TString>& keyColumns = {})
{
    auto& table = ctx.Tables->GetOrAddTable("ut", "/Root", path);
    table.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", path);
    table.Metadata->DoesExist = true;
    table.Metadata->PathId = TKikimrPathId(1, ctx.Tables->GetTables().size());
    table.Metadata->SchemaVersion = 1;
    table.Metadata->Kind = EKikimrTableKind::Datashard;
    table.Metadata->KeyColumnNames = keyColumns;

    ui32 id = 1;
    for (const auto& column : columns) {
        TKikimrColumnMetadata metadata(
            column.Name,
            id++,
            column.Type,
            column.NotNull);
        if (const auto decimal = NScheme::TDecimalType::ParseTypeName(column.Type)) {
            metadata.Type = "Decimal";
            metadata.TypeInfo = NScheme::TTypeInfo(*decimal);
        }
        table.Metadata->Columns.emplace(
            column.Name,
            std::move(metadata));
        table.Metadata->ColumnOrder.push_back(column.Name);
    }
    UNIT_ASSERT(table.Load(ctx.ExprCtx));
    return table;
}

TIntrusivePtr<TOpRead> MakeRead(
    TExportTestContext& ctx,
    const TKikimrTableDescription& table,
    const TString& alias,
    const TVector<TString>& columns,
    NYql::EStorageType storage = NYql::EStorageType::RowStorage)
{
    const auto pos = TPositionHandle();
    TVector<TInfoUnit> outputs;
    outputs.reserve(columns.size());
    for (const auto& column : columns) {
        outputs.emplace_back(alias, column);
    }

    return MakeIntrusive<TOpRead>(
        alias,
        columns,
        outputs,
        storage,
        NOpt::BuildTableMeta(table, pos, ctx.ExprCtx).Ptr(),
        nullptr,
        nullptr,
        std::nullopt,
        std::nullopt,
        ESortDir::None,
        TPhysicalOpProps{},
        pos);
}

struct TOutputTypeSpec {
    TString Name;
    NUdf::EDataSlot Slot;
    bool Nullable = false;
};

void SetOutputType(
    TExportTestContext& ctx,
    IOperator& op,
    const TVector<TOutputTypeSpec>& outputs)
{
    TVector<const TItemExprType*> items;
    for (const auto& output : outputs) {
        const TTypeAnnotationNode* type = ctx.ExprCtx.MakeType<TDataExprType>(output.Slot);
        if (output.Nullable) {
            type = ctx.ExprCtx.MakeType<TOptionalExprType>(type);
        }
        items.push_back(ctx.ExprCtx.MakeType<TItemExprType>(output.Name, type));
    }
    op.Type = ctx.ExprCtx.MakeType<TListExprType>(
        ctx.ExprCtx.MakeType<TStructExprType>(std::move(items)));
}

void SetExactOutputType(
    TExportTestContext& ctx,
    IOperator& op,
    const TVector<std::pair<TString, const TTypeAnnotationNode*>>& outputs)
{
    TVector<const TItemExprType*> items;
    for (const auto& [name, type] : outputs) {
        items.push_back(ctx.ExprCtx.MakeType<TItemExprType>(name, type));
    }
    op.Type = ctx.ExprCtx.MakeType<TListExprType>(
        ctx.ExprCtx.MakeType<TStructExprType>(std::move(items)));
}

const NJson::TJsonValue& FindNode(const NJson::TJsonValue& snapshot, TStringBuf operation) {
    for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
        if (node["op"].GetStringSafe() == operation) {
            return node;
        }
    }
    UNIT_FAIL(TStringBuilder() << "missing plan node " << operation);
    return snapshot;
}

NJson::TJsonValue ParseSupported(const TSemanticSnapshotExportResult& result) {
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    NJson::TJsonValue snapshot;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.Json, &snapshot, true), result.Json);
    return snapshot;
}

NJson::TJsonValue ParseSupported(const TRBOSemanticSnapshotBoundaryResultV1& result) {
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    NJson::TJsonValue snapshot;
    UNIT_ASSERT_C(NJson::ReadJsonTree(result.Json, &snapshot, true), result.Json);
    return snapshot;
}

class TRecordingSemanticSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        Results.push_back(std::move(result));
    }

    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

class TThrowOnceSemanticSnapshotSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        if (ThrowNext) {
            ThrowNext = false;
            throw std::runtime_error("test sink failure");
        }
        Results.push_back(std::move(result));
    }

    bool ThrowNext = true;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

class TTransformationPrefixRecordingSink final : public IRBOSemanticSnapshotSink {
public:
    explicit TTransformationPrefixRecordingSink(ui64 target)
        : Target(target)
    {
    }

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        Results.push_back(std::move(result));
    }

    std::optional<ui64> GetTransformationPrefixTarget() const override {
        return Target;
    }

    ui64 Target;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

class TThrowingTransformationPrefixConfigurationSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1) override {
    }

    std::optional<ui64> GetTransformationPrefixTarget() const override {
        throw std::runtime_error("test prefix configuration failure");
    }
};

class TFixedApplicationRule final : public IRule {
public:
    TFixedApplicationRule(TString name, ui32 applications, ui32& attempts)
        : IRule(std::move(name))
        , Remaining(applications)
        , Attempts(attempts)
    {
    }

    bool MatchAndApply(
        TIntrusivePtr<IOperator>& input,
        TRBOContext& ctx,
        TPlanProps& props) override
    {
        Y_UNUSED(input);
        Y_UNUSED(ctx);
        Y_UNUSED(props);
        ++Attempts;
        if (Remaining == 0) {
            return false;
        }
        --Remaining;
        return true;
    }

private:
    ui32 Remaining;
    ui32& Attempts;
};

class TWrapReadRule final : public ISimplifiedRule {
public:
    explicit TWrapReadRule(ui32& applications)
        : ISimplifiedRule("Wrap read", ERuleProperties::RequireParents)
        , Applications(applications)
    {
    }

    bool QuickMatch(const TIntrusivePtr<IOperator>& input) const override {
        return !Applied && input->Kind == EOperator::Source;
    }

    TIntrusivePtr<IOperator> SimpleMatchAndApply(
        const TIntrusivePtr<IOperator>& input,
        TRBOContext& ctx,
        TPlanProps& props) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(props);
        Applied = true;
        ++Applications;
        return MakeIntrusive<TOpMap>(
            input,
            input->Pos,
            TVector<TMapElement>{});
    }

private:
    ui32& Applications;
    bool Applied = false;
};

class TCountingStage final : public IRBOStage {
public:
    explicit TCountingStage(ui32& runs)
        : IRBOStage(
            TString("Must not run"),
            ERBOStageTransformationMode::NoSemanticMutation)
        , Runs(runs)
    {
    }

    void RunStage(TOpRoot& root, TRBOContext& ctx) override {
        Y_UNUSED(root);
        Y_UNUSED(ctx);
        ++Runs;
    }

private:
    ui32& Runs;
};

class TAtomicWrapStage final : public IRBOStage {
public:
    explicit TAtomicWrapStage(ui32& runs)
        : IRBOStage(
            TString("Atomic stage"),
            ERBOStageTransformationMode::AtomicStageCommit,
            TString("Wrap root"))
        , Runs(runs)
    {
    }

    void RunStage(TOpRoot& root, TRBOContext& ctx) override {
        Y_UNUSED(ctx);
        ++Runs;
        root.SetInput(MakeIntrusive<TOpMap>(
            root.GetInput(),
            root.Pos,
            TVector<TMapElement>{}));
    }

private:
    ui32& Runs;
};

class TNoOpAtomicStage final : public IRBOStage {
public:
    explicit TNoOpAtomicStage(ui32& runs)
        : IRBOStage(
            TString("No-op atomic stage"),
            ERBOStageTransformationMode::AtomicStageCommit,
            TString("Commit no-op checkpoint"))
        , Runs(runs)
    {
    }

    void RunStage(TOpRoot& root, TRBOContext& ctx) override {
        Y_UNUSED(root);
        Y_UNUSED(ctx);
        ++Runs;
    }

private:
    ui32& Runs;
};

TVector<TString> Strings(const NJson::TJsonValue& array) {
    TVector<TString> result;
    for (const auto& value : array.GetArraySafe()) {
        result.push_back(value.GetStringSafe());
    }
    return result;
}

TVector<TString> ProjectionOutputs(const NJson::TJsonValue& project) {
    TVector<TString> result;
    for (const auto& column : project["columns"].GetArraySafe()) {
        result.push_back(column["output"].GetStringSafe());
    }
    return result;
}

std::pair<TString, TString> EqualityColumns(const NJson::TJsonValue& expression) {
    UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "eq");
    return {
        expression["left"]["column"].GetStringSafe(),
        expression["right"]["column"].GetStringSafe(),
    };
}

TIntrusivePtr<TOpMap> MakeCopyMap(
    TExportTestContext& ctx,
    TIntrusivePtr<IOperator> input,
    const TString& output,
    const TString& source)
{
    const auto pos = TPositionHandle();
    return MakeIntrusive<TOpMap>(input, pos, TVector<TMapElement>{TMapElement(
        TInfoUnit(output),
        TInfoUnit(source),
        pos,
        &ctx.ExprCtx,
        &ctx.ExpressionProps)});
}

TSemanticSnapshotExportResult ExportSharedInputJoin(
    TStringBuf joinKind,
    bool withFilter = false)
{
    TExportTestContext ctx;
    AddTable(ctx, "/Root/Left", {{"k", "Int32", true}});
    AddTable(ctx, "/Root/Right", {{"k", "Int32", true}});
    auto left = MakeRead(
        ctx,
        ctx.Tables->ExistingTable("ut", "/Root/Left"),
        "shared",
        {"k"});
    auto right = MakeRead(
        ctx,
        ctx.Tables->ExistingTable("ut", "/Root/Right"),
        "shared",
        {"k"});
    SetOutputType(ctx, *left, {
        {"shared.k", NUdf::EDataSlot::Int32},
    });
    SetOutputType(ctx, *right, {
        {"shared.k", NUdf::EDataSlot::Int32},
    });

    const auto pos = TPositionHandle();
    TVector<TExpression> filters;
    if (withFilter) {
        filters.push_back(MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                TInfoUnit("shared.k"),
                pos,
                &ctx.ExprCtx,
                &ctx.ExpressionProps),
            MakeColumnAccess(
                TInfoUnit("shared.k"),
                pos,
                &ctx.ExprCtx,
                &ctx.ExpressionProps)));
    }
    auto join = MakeIntrusive<TOpJoin>(
        left,
        right,
        pos,
        TString(joinKind),
        TVector<std::pair<TInfoUnit, TInfoUnit>>{{
            TInfoUnit("shared.k"),
            TInfoUnit("shared.k"),
        }},
        std::move(filters));
    SetOutputType(ctx, *join, {
        {"shared.k", NUdf::EDataSlot::Int32},
    });
    TOpRoot root(join, pos, {"shared.k"});
    return ExportSemanticSnapshotV1(root, ctx.RboCtx);
}

const TTypeAnnotationNode* ScalarType(
    TExportTestContext& ctx,
    NUdf::EDataSlot slot,
    bool nullable = false)
{
    const TTypeAnnotationNode* result = ctx.ExprCtx.MakeType<TDataExprType>(slot);
    return nullable ? ctx.ExprCtx.MakeType<TOptionalExprType>(result) : result;
}

const TTypeAnnotationNode* DecimalType(
    TExportTestContext& ctx,
    TStringBuf precision,
    TStringBuf scale,
    bool nullable = false)
{
    const TTypeAnnotationNode* result = ctx.ExprCtx.MakeType<TDataExprParamsType>(
        NUdf::EDataSlot::Decimal,
        precision,
        scale);
    return nullable ? ctx.ExprCtx.MakeType<TOptionalExprType>(result) : result;
}

TExprNode::TPtr TypedMember(
    TExportTestContext& ctx,
    TStringBuf name,
    const TTypeAnnotationNode* type)
{
    auto member = MakeColumnAccess(
        TInfoUnit(TString(name)),
        TPositionHandle(),
        &ctx.ExprCtx,
        &ctx.ExpressionProps).GetExpressionBody();
    member->SetTypeAnn(type);
    return member;
}

TExprNode::TPtr TypedLiteral(
    TExportTestContext& ctx,
    TStringBuf callable,
    TStringBuf value,
    const TTypeAnnotationNode* type)
{
    auto literal = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        callable,
        {ctx.ExprCtx.NewAtom(TPositionHandle(), value)});
    literal->SetTypeAnn(type);
    return literal;
}

TExprNode::TPtr TypedDecimalLiteral(
    TExportTestContext& ctx,
    TStringBuf value,
    TStringBuf precision,
    TStringBuf scale,
    const TTypeAnnotationNode* type)
{
    auto literal = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        "Decimal",
        {
            ctx.ExprCtx.NewAtom(TPositionHandle(), value),
            ctx.ExprCtx.NewAtom(TPositionHandle(), precision),
            ctx.ExprCtx.NewAtom(TPositionHandle(), scale),
        });
    literal->SetTypeAnn(type);
    return literal;
}

TExprNode::TPtr TypedCallable(
    TExportTestContext& ctx,
    TStringBuf callable,
    TExprNode::TListType children,
    const TTypeAnnotationNode* type)
{
    auto result = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        callable,
        std::move(children));
    result->SetTypeAnn(type);
    return result;
}

void AnnotateExpression(
    TExpression& expression,
    const TTypeAnnotationNode* type)
{
    expression.GetExpressionBody()->SetTypeAnn(type);
    expression.Node->SetTypeAnn(type);
}

void AnnotateBinaryExpression(
    TExpression& expression,
    const TTypeAnnotationNode* leftType,
    const TTypeAnnotationNode* rightType,
    const TTypeAnnotationNode* resultType)
{
    auto body = expression.GetExpressionBody();
    UNIT_ASSERT_VALUES_EQUAL(body->ChildrenSize(), 2);
    body->Child(0)->SetTypeAnn(leftType);
    body->Child(1)->SetTypeAnn(rightType);
    AnnotateExpression(expression, resultType);
}

TExprNode::TPtr TypedUnaryLambda(
    TExportTestContext& ctx,
    const TExprNode::TPtr& argument,
    TExprNode::TPtr body)
{
    return ctx.ExprCtx.NewLambda(
        TPositionHandle(),
        ctx.ExprCtx.NewArguments(TPositionHandle(), {argument}),
        std::move(body));
}

TExprNode::TPtr SqlInOptions(
    TExportTestContext& ctx,
    std::initializer_list<TStringBuf> names)
{
    TExprNode::TListType options;
    for (const TStringBuf name : names) {
        options.push_back(ctx.ExprCtx.NewList(
            TPositionHandle(),
            {ctx.ExprCtx.NewAtom(TPositionHandle(), name)}));
    }
    return ctx.ExprCtx.NewList(TPositionHandle(), std::move(options));
}

TExprNode::TPtr TypedStaticTuple(
    TExportTestContext& ctx,
    TExprNode::TListType items,
    const TTypeAnnotationNode* itemType)
{
    TTypeAnnotationNode::TListType itemTypes(items.size(), itemType);
    auto result = ctx.ExprCtx.NewList(TPositionHandle(), std::move(items));
    result->SetTypeAnn(ctx.ExprCtx.MakeType<TTupleExprType>(std::move(itemTypes)));
    return result;
}

TExprNode::TPtr TypedStaticAsList(
    TExportTestContext& ctx,
    TExprNode::TListType items,
    const TTypeAnnotationNode* itemType)
{
    return TypedCallable(
        ctx,
        "AsList",
        std::move(items),
        ctx.ExprCtx.MakeType<TListExprType>(itemType));
}

TExprNode::TPtr TypedSqlIn(
    TExportTestContext& ctx,
    TExprNode::TPtr collection,
    TExprNode::TPtr lookup,
    TExprNode::TPtr options,
    const TTypeAnnotationNode* resultType)
{
    return TypedCallable(
        ctx,
        "SqlIn",
        {std::move(collection), std::move(lookup), std::move(options)},
        resultType);
}

TExprNode::TPtr DataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf typeName,
    const TTypeAnnotationNode* type);

TExprNode::TPtr OptionalDataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf typeName,
    const TTypeAnnotationNode* itemType,
    const TTypeAnnotationNode* optionalType);

TExprNode::TPtr DecimalDataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf precision,
    TStringBuf scale,
    const TTypeAnnotationNode* type);

enum class EStaticSetIfPresentShape {
    Exact,
    NonIdentityKey,
    NonVoidPayload,
    ReversedSettings,
    DecimalItems,
};

TExprNode::TPtr TypedStaticSetIfPresent(
    TExportTestContext& ctx,
    EStaticSetIfPresentShape shape)
{
    const bool decimalItems = shape == EStaticSetIfPresentShape::DecimalItems;
    const auto* itemType = decimalItems
        ? ctx.ExprCtx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal,
            "5",
            "2")
        : ScalarType(ctx, NUdf::EDataSlot::String);
    const auto* optionalItemType = ctx.ExprCtx.MakeType<TOptionalExprType>(itemType);
    const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
    const auto* voidType = ctx.ExprCtx.MakeType<TVoidExprType>();
    const auto* listType = ctx.ExprCtx.MakeType<TListExprType>(itemType);
    const auto* dictType = ctx.ExprCtx.MakeType<TDictExprType>(itemType, voidType);

    auto itemDescriptor = decimalItems
        ? DecimalDataTypeDescriptor(ctx, "5", "2", itemType)
        : DataTypeDescriptor(ctx, "String", itemType);
    auto listDescriptor = TypedCallable(
        ctx,
        "ListType",
        {std::move(itemDescriptor)},
        ctx.ExprCtx.MakeType<TTypeExprType>(listType));
    TExprNode::TListType valuesChildren;
    valuesChildren.push_back(std::move(listDescriptor));
    if (decimalItems) {
        valuesChildren.push_back(TypedDecimalLiteral(
            ctx, "1.00", "5", "2", itemType));
        valuesChildren.push_back(TypedDecimalLiteral(
            ctx, "2.00", "5", "2", itemType));
    } else {
        valuesChildren.push_back(TypedLiteral(ctx, "String", "AIR", itemType));
        valuesChildren.push_back(TypedLiteral(ctx, "String", "AIR REG", itemType));
    }
    auto values = TypedCallable(
        ctx,
        "List",
        std::move(valuesChildren),
        listType);

    auto keyArgument = ctx.ExprCtx.NewArgument(TPositionHandle(), "key");
    keyArgument->SetTypeAnn(itemType);
    TExprNode::TPtr keyBody = keyArgument;
    if (shape == EStaticSetIfPresentShape::NonIdentityKey) {
        keyBody = TypedLiteral(ctx, "String", "constant-key", itemType);
    }

    auto payloadArgument = ctx.ExprCtx.NewArgument(TPositionHandle(), "payload");
    payloadArgument->SetTypeAnn(itemType);
    TExprNode::TPtr payload = TypedCallable(ctx, "Void", {}, voidType);
    if (shape == EStaticSetIfPresentShape::NonVoidPayload) {
        payload = TypedLiteral(ctx, "String", "payload", itemType);
    }

    const bool reversedSettings =
        shape == EStaticSetIfPresentShape::ReversedSettings;
    auto settings = ctx.ExprCtx.NewList(
        TPositionHandle(),
        {
            ctx.ExprCtx.NewAtom(
                TPositionHandle(),
                reversedSettings ? "Auto" : "One"),
            ctx.ExprCtx.NewAtom(
                TPositionHandle(),
                reversedSettings ? "One" : "Auto"),
        });
    auto dict = TypedCallable(
        ctx,
        "ToDict",
        {
            std::move(values),
            TypedUnaryLambda(ctx, keyArgument, std::move(keyBody)),
            TypedUnaryLambda(ctx, payloadArgument, std::move(payload)),
            std::move(settings),
        },
        dictType);

    auto lookup = ctx.ExprCtx.NewArgument(TPositionHandle(), "lookup");
    lookup->SetTypeAnn(itemType);
    auto contains = TypedCallable(
        ctx,
        "Contains",
        {std::move(dict), lookup},
        boolType);
    return TypedCallable(
        ctx,
        "IfPresent",
        {
            TypedMember(ctx, "a.x", optionalItemType),
            TypedUnaryLambda(ctx, lookup, std::move(contains)),
            TypedLiteral(ctx, "Bool", "false", boolType),
        },
        boolType);
}

TExprNode::TPtr DataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf typeName,
    const TTypeAnnotationNode* type)
{
    auto result = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        "DataType",
        {ctx.ExprCtx.NewAtom(TPositionHandle(), typeName)});
    result->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(type));
    return result;
}

TExprNode::TPtr OptionalDataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf typeName,
    const TTypeAnnotationNode* itemType,
    const TTypeAnnotationNode* optionalType)
{
    return TypedCallable(
        ctx,
        "OptionalType",
        {DataTypeDescriptor(ctx, typeName, itemType)},
        ctx.ExprCtx.MakeType<TTypeExprType>(optionalType));
}

TExprNode::TPtr TypedNothing(
    TExportTestContext& ctx,
    TStringBuf typeName,
    const TTypeAnnotationNode* itemType,
    const TTypeAnnotationNode* optionalType)
{
    auto descriptor = OptionalDataTypeDescriptor(
        ctx,
        typeName,
        itemType,
        optionalType);
    return TypedCallable(ctx, "Nothing", {std::move(descriptor)}, optionalType);
}

TExprNode::TPtr DecimalDataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf precision,
    TStringBuf scale,
    const TTypeAnnotationNode* type)
{
    auto result = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        "DataType",
        {
            ctx.ExprCtx.NewAtom(TPositionHandle(), "Decimal"),
            ctx.ExprCtx.NewAtom(TPositionHandle(), precision),
            ctx.ExprCtx.NewAtom(TPositionHandle(), scale),
        });
    result->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(type));
    return result;
}

TExprNode::TPtr OptionalDecimalDataTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf precision,
    TStringBuf scale,
    const TTypeAnnotationNode* itemType,
    const TTypeAnnotationNode* optionalType)
{
    return TypedCallable(
        ctx,
        "OptionalType",
        {DecimalDataTypeDescriptor(ctx, precision, scale, itemType)},
        ctx.ExprCtx.MakeType<TTypeExprType>(optionalType));
}

TExprNode::TPtr TypedTextLiteralDecimalCast(
    TExportTestContext& ctx,
    TStringBuf castCallable,
    TStringBuf sourceCallable,
    TStringBuf text,
    TStringBuf precision,
    TStringBuf scale)
{
    UNIT_ASSERT(sourceCallable == "String" || sourceCallable == "Utf8");
    const auto sourceSlot = sourceCallable == "String"
        ? NUdf::EDataSlot::String
        : NUdf::EDataSlot::Utf8;
    const auto* sourceType = ScalarType(ctx, sourceSlot);
    const auto* decimalType = DecimalType(ctx, precision, scale);
    const auto* optionalDecimalType = DecimalType(
        ctx,
        precision,
        scale,
        true);
    return TypedCallable(
        ctx,
        castCallable,
        {
            TypedLiteral(ctx, sourceCallable, text, sourceType),
            OptionalDecimalDataTypeDescriptor(
                ctx,
                precision,
                scale,
                decimalType,
                optionalDecimalType),
        },
        optionalDecimalType);
}

TExprNode::TPtr TypedTextLiteralDateCast(
    TExportTestContext& ctx,
    TStringBuf castCallable,
    TStringBuf sourceCallable,
    TStringBuf text)
{
    UNIT_ASSERT(sourceCallable == "String" || sourceCallable == "Utf8");
    const auto sourceSlot = sourceCallable == "String"
        ? NUdf::EDataSlot::String
        : NUdf::EDataSlot::Utf8;
    const auto* sourceType = ScalarType(ctx, sourceSlot);
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType = ScalarType(
        ctx,
        NUdf::EDataSlot::Date,
        true);
    return TypedCallable(
        ctx,
        castCallable,
        {
            TypedLiteral(ctx, sourceCallable, text, sourceType),
            OptionalDataTypeDescriptor(
                ctx,
                "Date",
                dateType,
                optionalDateType),
        },
        optionalDateType);
}

enum class EDateIntervalShape {
    Exact,
    NonOptionalDateTarget,
    MismatchedDateTargetAnnotation,
    NonOptionalDateCastResult,
    NonOptionalArithmeticResult,
    WrongUdfName,
    NonVoidRunConfig,
    NonVoidUserType,
    NonEmptyTypeConfig,
    WrongCachedArgumentFlags,
    MismatchedCachedCallableAnnotation,
    WrongCachedReturnDescriptor,
    NonVoidCachedRunConfigType,
    NonEmptyFileAlias,
    WrongCallableAnnotation,
    ReversedUdfSettings,
    MissingUdfSetting,
    WrongApplyResult,
    WrongDaysType,
    NullableDays,
};

enum class EDirectDateIntervalShape {
    Exact,
    NonOptionalResult,
    WrongResultType,
    WrongArithmeticCallable,
    UnaryArithmetic,
    TernaryArithmetic,
    WrongDateCallable,
    EmptyDate,
    BinaryDate,
    WrongDateType,
    NullableDate,
    WrongIntervalCallable,
    EmptyInterval,
    BinaryInterval,
    WrongIntervalType,
    NullableInterval,
};

enum class EDateTime2ShiftShape {
    Exact,
    WrongSplitUserType,
    WrongShiftUserType,
    WrongMakeDateUserType,
    WrongSplitReturnDescriptor,
    WrongShiftReturnDescriptor,
    WrongMakeDateArgumentDescriptor,
    WrongSplitSettings,
    WrongShiftSettings,
    WrongMakeDateSettings,
    WrongSplitFlags,
    WrongShiftFlags,
    WrongMakeDateFlags,
    WrongLambdaBinder,
};

enum class EDateTime2YearShape {
    Exact,
    NonOptionalResult,
    WrongResultType,
    NonMemberSource,
    InvisibleSource,
    NonOptionalSource,
    WrongSourceType,
    WrongCastCallable,
    WrongCastTarget,
    NonOptionalCastResult,
    NonUnaryLambda,
    WrongLambdaType,
    WrongLambdaBinder,
    WrongSplitName,
    WrongSplitUserType,
    WrongSplitReturnDescriptor,
    WrongSplitSettings,
    WrongSplitFlags,
    WrongGetYearName,
    WrongGetYearUserType,
    WrongGetYearReturnDescriptor,
    WrongGetYearSettings,
    WrongGetYearFlags,
};

TExprNode::TPtr VoidValue(TExportTestContext& ctx) {
    return TypedCallable(
        ctx,
        "Void",
        {},
        ctx.ExprCtx.MakeType<TVoidExprType>());
}

TExprNode::TPtr VoidTypeDescriptor(TExportTestContext& ctx) {
    const auto* voidType = ctx.ExprCtx.MakeType<TVoidExprType>();
    return TypedCallable(
        ctx,
        "VoidType",
        {},
        ctx.ExprCtx.MakeType<TTypeExprType>(voidType));
}

TExprNode::TPtr ResourceTypeDescriptor(
    TExportTestContext& ctx,
    TStringBuf tag,
    const TTypeAnnotationNode* resourceType)
{
    return TypedCallable(
        ctx,
        "ResourceType",
        {ctx.ExprCtx.NewAtom(TPositionHandle(), tag)},
        ctx.ExprCtx.MakeType<TTypeExprType>(resourceType));
}

TExprNode::TPtr OptionalTypeDescriptor(
    TExportTestContext& ctx,
    TExprNode::TPtr item,
    const TTypeAnnotationNode* optionalType)
{
    return TypedCallable(
        ctx,
        "OptionalType",
        {std::move(item)},
        ctx.ExprCtx.MakeType<TTypeExprType>(optionalType));
}

TExprNode::TPtr TupleTypeDescriptor(
    TExportTestContext& ctx,
    TExprNode::TListType items,
    const TTypeAnnotationNode* tupleType)
{
    return TypedCallable(
        ctx,
        "TupleType",
        std::move(items),
        ctx.ExprCtx.MakeType<TTypeExprType>(tupleType));
}

TExprNode::TPtr EmptyStructTypeDescriptor(
    TExportTestContext& ctx,
    const TTypeAnnotationNode* structType)
{
    return TypedCallable(
        ctx,
        "StructType",
        {},
        ctx.ExprCtx.MakeType<TTypeExprType>(structType));
}

const TCallableExprType* UdfCallableType(
    TExportTestContext& ctx,
    const TTypeAnnotationNode* resultType,
    std::initializer_list<std::pair<const TTypeAnnotationNode*, ui64>> arguments)
{
    TVector<TCallableExprType::TArgumentInfo> infos;
    infos.reserve(arguments.size());
    for (const auto& [type, flags] : arguments) {
        TCallableExprType::TArgumentInfo info;
        info.Type = type;
        info.Flags = flags;
        infos.push_back(std::move(info));
    }
    return ctx.ExprCtx.MakeType<TCallableExprType>(
        resultType,
        std::move(infos),
        0,
        TStringBuf());
}

struct TUdfArgumentDescriptor {
    TExprNode::TPtr Type;
    std::optional<ui64> Flags;
};

TExprNode::TPtr CallableTypeDescriptor(
    TExportTestContext& ctx,
    TExprNode::TPtr resultType,
    TVector<TUdfArgumentDescriptor> arguments,
    const TTypeAnnotationNode* callableType)
{
    TExprNode::TListType children = {
        ctx.ExprCtx.NewList(TPositionHandle(), {}),
        ctx.ExprCtx.NewList(TPositionHandle(), {std::move(resultType)}),
    };
    for (auto& argument : arguments) {
        TExprNode::TListType settings = {std::move(argument.Type)};
        if (argument.Flags) {
            settings.push_back(ctx.ExprCtx.NewAtom(TPositionHandle(), ""));
            settings.push_back(ctx.ExprCtx.NewAtom(
                TPositionHandle(), ToString(*argument.Flags)));
        }
        children.push_back(ctx.ExprCtx.NewList(
            TPositionHandle(), std::move(settings)));
    }
    return TypedCallable(
        ctx,
        "CallableType",
        std::move(children),
        ctx.ExprCtx.MakeType<TTypeExprType>(callableType));
}

TExprNode::TPtr UdfSettings(
    TExportTestContext& ctx,
    std::initializer_list<TStringBuf> settings)
{
    TExprNode::TListType children;
    children.reserve(settings.size());
    for (const TStringBuf setting : settings) {
        children.push_back(ctx.ExprCtx.NewList(
            TPositionHandle(),
            {ctx.ExprCtx.NewAtom(TPositionHandle(), setting)}));
    }
    return ctx.ExprCtx.NewList(TPositionHandle(), std::move(children));
}

const TCallableExprType* IntervalFromDaysCallableType(
    TExportTestContext& ctx,
    const TTypeAnnotationNode* resultType,
    const TTypeAnnotationNode* argumentType,
    ui64 flags)
{
    TCallableExprType::TArgumentInfo argument;
    argument.Type = argumentType;
    argument.Flags = flags;
    return ctx.ExprCtx.MakeType<TCallableExprType>(
        resultType,
        TVector<TCallableExprType::TArgumentInfo>{argument},
        0,
        TStringBuf());
}

TExprNode::TPtr TypedConstantDateInterval(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf sourceCallable,
    TStringBuf date,
    TStringBuf days,
    EDateIntervalShape shape = EDateIntervalShape::Exact)
{
    const auto sourceSlot = sourceCallable == "Utf8"
        ? NUdf::EDataSlot::Utf8
        : NUdf::EDataSlot::String;
    const auto* sourceType = ScalarType(ctx, sourceSlot);
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType = ScalarType(ctx, NUdf::EDataSlot::Date, true);
    const auto* intervalType = ScalarType(ctx, NUdf::EDataSlot::Interval);
    const auto* optionalIntervalType = ScalarType(
        ctx, NUdf::EDataSlot::Interval, true);
    const auto* int32Type = ScalarType(ctx, NUdf::EDataSlot::Int32);
    const auto* optionalInt32Type = ScalarType(
        ctx, NUdf::EDataSlot::Int32, true);
    const auto* int64Type = ScalarType(ctx, NUdf::EDataSlot::Int64);

    TExprNode::TPtr dateTarget;
    if (shape == EDateIntervalShape::NonOptionalDateTarget) {
        dateTarget = DataTypeDescriptor(ctx, "Date", dateType);
    } else {
        dateTarget = OptionalDataTypeDescriptor(
            ctx, "Date", dateType, optionalDateType);
        if (shape == EDateIntervalShape::MismatchedDateTargetAnnotation) {
            dateTarget->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(dateType));
        }
    }
    auto dateCast = TypedCallable(
        ctx,
        "SafeCast",
        {
            TypedLiteral(ctx, sourceCallable, date, sourceType),
            std::move(dateTarget),
        },
        shape == EDateIntervalShape::NonOptionalDateCastResult
            ? dateType
            : optionalDateType);

    const ui64 autoMap = NUdf::ICallablePayload::TArgumentFlags::AutoMap;
    const ui64 annotationFlags =
        shape == EDateIntervalShape::WrongCallableAnnotation ? 0 : autoMap;
    const auto* callableType = IntervalFromDaysCallableType(
        ctx, optionalIntervalType, int32Type, annotationFlags);
    const auto* cachedCallableType =
        shape == EDateIntervalShape::MismatchedCachedCallableAnnotation
            ? IntervalFromDaysCallableType(
                ctx, optionalIntervalType, int32Type, 0)
            : callableType;

    auto callableDescriptor = TypedCallable(
        ctx,
        "CallableType",
        {
            ctx.ExprCtx.NewList(TPositionHandle(), {}),
            ctx.ExprCtx.NewList(
                TPositionHandle(),
                {shape == EDateIntervalShape::WrongCachedReturnDescriptor
                    ? OptionalDataTypeDescriptor(
                        ctx,
                        "Date",
                        dateType,
                        optionalDateType)
                    : OptionalDataTypeDescriptor(
                        ctx,
                        "Interval",
                        intervalType,
                        optionalIntervalType)}),
            ctx.ExprCtx.NewList(
                TPositionHandle(),
                {
                    DataTypeDescriptor(ctx, "Int32", int32Type),
                    ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
                    ctx.ExprCtx.NewAtom(
                        TPositionHandle(),
                        shape == EDateIntervalShape::WrongCachedArgumentFlags
                            ? "0"
                            : "1"),
                }),
        },
        ctx.ExprCtx.MakeType<TTypeExprType>(cachedCallableType));

    auto settings = ctx.ExprCtx.NewList(
        TPositionHandle(),
        {
            ctx.ExprCtx.NewList(
                TPositionHandle(),
                {ctx.ExprCtx.NewAtom(
                    TPositionHandle(),
                    shape == EDateIntervalShape::ReversedUdfSettings
                        ? "strict"
                        : "blocks")}),
            ctx.ExprCtx.NewList(
                TPositionHandle(),
                {ctx.ExprCtx.NewAtom(
                    TPositionHandle(),
                    shape == EDateIntervalShape::ReversedUdfSettings
                        ? "blocks"
                        : "strict")}),
        });

    TExprNode::TListType udfChildren = {
        ctx.ExprCtx.NewAtom(
            TPositionHandle(),
            shape == EDateIntervalShape::WrongUdfName
                ? "DateTime2.IntervalFromHours"
                : "DateTime2.IntervalFromDays"),
        shape == EDateIntervalShape::NonVoidRunConfig
            ? TypedLiteral(ctx, "Int32", "0", int32Type)
            : VoidValue(ctx),
        shape == EDateIntervalShape::NonVoidUserType
            ? DataTypeDescriptor(ctx, "Int32", int32Type)
            : VoidTypeDescriptor(ctx),
        ctx.ExprCtx.NewAtom(
            TPositionHandle(),
            shape == EDateIntervalShape::NonEmptyTypeConfig ? "config" : ""),
        std::move(callableDescriptor),
        shape == EDateIntervalShape::NonVoidCachedRunConfigType
            ? DataTypeDescriptor(ctx, "Int32", int32Type)
            : VoidTypeDescriptor(ctx),
        ctx.ExprCtx.NewAtom(
            TPositionHandle(),
            shape == EDateIntervalShape::NonEmptyFileAlias ? "module" : ""),
        std::move(settings),
    };
    if (shape == EDateIntervalShape::MissingUdfSetting) {
        udfChildren[7] = ctx.ExprCtx.NewList(
            TPositionHandle(),
            {ctx.ExprCtx.NewList(
                TPositionHandle(),
                {ctx.ExprCtx.NewAtom(TPositionHandle(), "blocks")})});
    }
    auto udf = TypedCallable(
        ctx, "Udf", std::move(udfChildren), callableType);

    const bool wrongDaysType = shape == EDateIntervalShape::WrongDaysType;
    const auto* daysType = wrongDaysType
        ? int64Type
        : shape == EDateIntervalShape::NullableDays
            ? optionalInt32Type
            : int32Type;
    auto interval = TypedCallable(
        ctx,
        "Apply",
        {
            std::move(udf),
            TypedLiteral(
                ctx,
                wrongDaysType ? TStringBuf("Int64") : TStringBuf("Int32"),
                days,
                daysType),
        },
        shape == EDateIntervalShape::WrongApplyResult
            ? intervalType
            : optionalIntervalType);

    return TypedCallable(
        ctx,
        operation,
        {std::move(dateCast), std::move(interval)},
        shape == EDateIntervalShape::NonOptionalArithmeticResult
            ? dateType
            : optionalDateType);
}

TExprNode::TPtr TypedDirectDateInterval(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf date,
    TStringBuf interval,
    EDirectDateIntervalShape shape = EDirectDateIntervalShape::Exact)
{
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType = ScalarType(
        ctx, NUdf::EDataSlot::Date, true);
    const auto* intervalType = ScalarType(ctx, NUdf::EDataSlot::Interval);
    const auto* optionalIntervalType = ScalarType(
        ctx, NUdf::EDataSlot::Interval, true);
    const auto* int64Type = ScalarType(ctx, NUdf::EDataSlot::Int64);
    const auto* optionalDatetimeType = ScalarType(
        ctx, NUdf::EDataSlot::Datetime, true);

    TExprNode::TListType dateChildren;
    if (shape != EDirectDateIntervalShape::EmptyDate) {
        dateChildren.push_back(ctx.ExprCtx.NewAtom(TPositionHandle(), date));
    }
    if (shape == EDirectDateIntervalShape::BinaryDate) {
        dateChildren.push_back(
            ctx.ExprCtx.NewAtom(TPositionHandle(), "unexpected"));
    }
    auto dateLiteral = TypedCallable(
        ctx,
        shape == EDirectDateIntervalShape::WrongDateCallable
            ? TStringBuf("Uint16")
            : TStringBuf("Date"),
        std::move(dateChildren),
        shape == EDirectDateIntervalShape::WrongDateType
            ? int64Type
            : shape == EDirectDateIntervalShape::NullableDate
                ? optionalDateType
                : dateType);

    TExprNode::TListType intervalChildren;
    if (shape != EDirectDateIntervalShape::EmptyInterval) {
        intervalChildren.push_back(
            ctx.ExprCtx.NewAtom(TPositionHandle(), interval));
    }
    if (shape == EDirectDateIntervalShape::BinaryInterval) {
        intervalChildren.push_back(
            ctx.ExprCtx.NewAtom(TPositionHandle(), "unexpected"));
    }
    auto intervalLiteral = TypedCallable(
        ctx,
        shape == EDirectDateIntervalShape::WrongIntervalCallable
            ? TStringBuf("Int64")
            : TStringBuf("Interval"),
        std::move(intervalChildren),
        shape == EDirectDateIntervalShape::WrongIntervalType
            ? int64Type
            : shape == EDirectDateIntervalShape::NullableInterval
                ? optionalIntervalType
                : intervalType);

    TExprNode::TListType arguments = {
        std::move(dateLiteral),
        std::move(intervalLiteral),
    };
    if (shape == EDirectDateIntervalShape::UnaryArithmetic) {
        arguments.pop_back();
    } else if (shape == EDirectDateIntervalShape::TernaryArithmetic) {
        arguments.push_back(TypedLiteral(ctx, "Date", "0", dateType));
    }

    return TypedCallable(
        ctx,
        shape == EDirectDateIntervalShape::WrongArithmeticCallable
            ? TStringBuf("*")
            : operation,
        std::move(arguments),
        shape == EDirectDateIntervalShape::NonOptionalResult
            ? dateType
            : shape == EDirectDateIntervalShape::WrongResultType
                ? optionalDatetimeType
                : optionalDateType);
}

TExprNode::TPtr TypedDateTime2Shift(
    TExportTestContext& ctx,
    TStringBuf shiftCallable,
    TStringBuf date,
    TStringBuf shift,
    EDateTime2ShiftShape shape = EDateTime2ShiftShape::Exact)
{
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType = ScalarType(
        ctx, NUdf::EDataSlot::Date, true);
    const auto* int32Type = ScalarType(ctx, NUdf::EDataSlot::Int32);
    const auto* resourceType =
        ctx.ExprCtx.MakeType<TResourceExprType>("DateTime2.TM");
    const auto* optionalResourceType =
        ctx.ExprCtx.MakeType<TOptionalExprType>(resourceType);
    const ui64 autoMap = NUdf::ICallablePayload::TArgumentFlags::AutoMap;

    const auto makeUserType = [&ctx](
        std::initializer_list<
            std::pair<TStringBuf, const TTypeAnnotationNode*>> arguments)
    {
        TTypeAnnotationNode::TListType argumentTypes;
        TExprNode::TListType argumentDescriptors;
        for (const auto& [name, type] : arguments) {
            argumentTypes.push_back(type);
            argumentDescriptors.push_back(DataTypeDescriptor(ctx, name, type));
        }

        const auto* argumentsType = ctx.ExprCtx.MakeType<TTupleExprType>(
            std::move(argumentTypes));
        const auto* emptyStructType = ctx.ExprCtx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{});
        const auto* emptyTupleType = ctx.ExprCtx.MakeType<TTupleExprType>(
            TTypeAnnotationNode::TListType{});
        const auto* userType = ctx.ExprCtx.MakeType<TTupleExprType>(
            TTypeAnnotationNode::TListType{
                argumentsType,
                emptyStructType,
                emptyTupleType,
            });

        return TupleTypeDescriptor(
            ctx,
            {
                TupleTypeDescriptor(
                    ctx,
                    std::move(argumentDescriptors),
                    argumentsType),
                EmptyStructTypeDescriptor(ctx, emptyStructType),
                TupleTypeDescriptor(ctx, {}, emptyTupleType),
            },
            userType);
    };

    const auto* splitCallableType = UdfCallableType(
        ctx, resourceType, {{dateType, autoMap}});
    const auto* splitCachedResultType =
        shape == EDateTime2ShiftShape::WrongSplitReturnDescriptor
            ? dateType
            : resourceType;
    const ui64 splitCachedFlags =
        shape == EDateTime2ShiftShape::WrongSplitFlags ? 0 : autoMap;
    const auto* splitCachedCallableType = UdfCallableType(
        ctx, splitCachedResultType, {{dateType, splitCachedFlags}});
    auto splitCallableDescriptor = CallableTypeDescriptor(
        ctx,
        shape == EDateTime2ShiftShape::WrongSplitReturnDescriptor
            ? DataTypeDescriptor(ctx, "Date", dateType)
            : ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
        {{
            DataTypeDescriptor(ctx, "Date", dateType),
            splitCachedFlags,
        }},
        splitCachedCallableType);
    auto split = TypedCallable(
        ctx,
        "Udf",
        {
            ctx.ExprCtx.NewAtom(TPositionHandle(), "DateTime2.Split"),
            VoidValue(ctx),
            shape == EDateTime2ShiftShape::WrongSplitUserType
                ? makeUserType({{"Int32", int32Type}})
                : makeUserType({{"Date", dateType}}),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            std::move(splitCallableDescriptor),
            VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            shape == EDateTime2ShiftShape::WrongSplitSettings
                ? UdfSettings(ctx, {"strict", "blocks"})
                : UdfSettings(ctx, {"blocks", "strict"}),
        },
        splitCallableType);
    auto splitApply = TypedCallable(
        ctx,
        "Apply",
        {
            std::move(split),
            TypedLiteral(ctx, "Date", date, dateType),
        },
        resourceType);

    const auto* shiftCallableType = UdfCallableType(
        ctx,
        optionalResourceType,
        {{resourceType, autoMap}, {int32Type, 0}});
    const TTypeAnnotationNode* shiftCachedResultType =
        shape == EDateTime2ShiftShape::WrongShiftReturnDescriptor
            ? static_cast<const TTypeAnnotationNode*>(resourceType)
            : static_cast<const TTypeAnnotationNode*>(optionalResourceType);
    const ui64 shiftCachedResourceFlags =
        shape == EDateTime2ShiftShape::WrongShiftFlags ? 0 : autoMap;
    const auto* shiftCachedCallableType = UdfCallableType(
        ctx,
        shiftCachedResultType,
        {{resourceType, shiftCachedResourceFlags}, {int32Type, 0}});
    auto shiftCallableDescriptor = CallableTypeDescriptor(
        ctx,
        shape == EDateTime2ShiftShape::WrongShiftReturnDescriptor
            ? ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType)
            : OptionalTypeDescriptor(
                ctx,
                ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
                optionalResourceType),
        {
            {
                ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
                shiftCachedResourceFlags,
            },
            {DataTypeDescriptor(ctx, "Int32", int32Type), std::nullopt},
        },
        shiftCachedCallableType);
    auto shiftUdf = TypedCallable(
        ctx,
        "Udf",
        {
            ctx.ExprCtx.NewAtom(
                TPositionHandle(),
                TStringBuilder() << "DateTime2." << shiftCallable),
            VoidValue(ctx),
            shape == EDateTime2ShiftShape::WrongShiftUserType
                ? makeUserType({{"Date", dateType}})
                : makeUserType({
                    {"Date", dateType},
                    {"Int32", int32Type},
                }),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            std::move(shiftCallableDescriptor),
            VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            shape == EDateTime2ShiftShape::WrongShiftSettings
                ? UdfSettings(ctx, {"blocks"})
                : UdfSettings(ctx, {"strict"}),
        },
        shiftCallableType);
    auto shifted = TypedCallable(
        ctx,
        "Apply",
        {
            std::move(shiftUdf),
            std::move(splitApply),
            TypedLiteral(ctx, "Int32", shift, int32Type),
        },
        optionalResourceType);

    const auto* makeDateCallableType = UdfCallableType(
        ctx, dateType, {{resourceType, autoMap}});
    const auto* makeDateCachedArgumentType =
        shape == EDateTime2ShiftShape::WrongMakeDateArgumentDescriptor
            ? dateType
            : resourceType;
    const ui64 makeDateCachedFlags =
        shape == EDateTime2ShiftShape::WrongMakeDateFlags ? 0 : autoMap;
    const auto* makeDateCachedCallableType = UdfCallableType(
        ctx,
        dateType,
        {{makeDateCachedArgumentType, makeDateCachedFlags}});
    auto makeDateCallableDescriptor = CallableTypeDescriptor(
        ctx,
        DataTypeDescriptor(ctx, "Date", dateType),
        {{
            shape == EDateTime2ShiftShape::WrongMakeDateArgumentDescriptor
                ? DataTypeDescriptor(ctx, "Date", dateType)
                : ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
            makeDateCachedFlags,
        }},
        makeDateCachedCallableType);
    auto makeDate = TypedCallable(
        ctx,
        "Udf",
        {
            ctx.ExprCtx.NewAtom(TPositionHandle(), "DateTime2.MakeDate"),
            VoidValue(ctx),
            shape == EDateTime2ShiftShape::WrongMakeDateUserType
                ? DataTypeDescriptor(ctx, "Date", dateType)
                : VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            std::move(makeDateCallableDescriptor),
            VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            shape == EDateTime2ShiftShape::WrongMakeDateSettings
                ? UdfSettings(ctx, {"strict"})
                : UdfSettings(ctx, {"blocks", "strict"}),
        },
        makeDateCallableType);

    auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "tm");
    argument->SetTypeAnn(resourceType);
    TExprNode::TPtr makeDateArgument = argument;
    if (shape == EDateTime2ShiftShape::WrongLambdaBinder) {
        makeDateArgument = ctx.ExprCtx.NewArgument(TPositionHandle(), "tm");
        makeDateArgument->SetTypeAnn(resourceType);
    }
    auto makeDateApply = TypedCallable(
        ctx,
        "Apply",
        {std::move(makeDate), std::move(makeDateArgument)},
        dateType);
    return TypedCallable(
        ctx,
        "Map",
        {
            std::move(shifted),
            TypedUnaryLambda(ctx, argument, std::move(makeDateApply)),
        },
        optionalDateType);
}

TExprNode::TPtr TypedNullableDateYear(
    TExportTestContext& ctx,
    EDateTime2YearShape shape = EDateTime2YearShape::Exact)
{
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType =
        ScalarType(ctx, NUdf::EDataSlot::Date, true);
    const auto* datetimeType = ScalarType(ctx, NUdf::EDataSlot::Datetime);
    const auto* optionalDatetimeType =
        ScalarType(ctx, NUdf::EDataSlot::Datetime, true);
    const auto* timestampType = ScalarType(ctx, NUdf::EDataSlot::Timestamp);
    const auto* optionalTimestampType =
        ScalarType(ctx, NUdf::EDataSlot::Timestamp, true);
    const auto* uint16Type = ScalarType(ctx, NUdf::EDataSlot::Uint16);
    const auto* optionalUint16Type =
        ScalarType(ctx, NUdf::EDataSlot::Uint16, true);
    const auto* uint32Type = ScalarType(ctx, NUdf::EDataSlot::Uint32);
    const auto* optionalUint32Type =
        ScalarType(ctx, NUdf::EDataSlot::Uint32, true);
    const auto* resourceType =
        ctx.ExprCtx.MakeType<TResourceExprType>("DateTime2.TM");
    const ui64 autoMap = NUdf::ICallablePayload::TArgumentFlags::AutoMap;

    const auto makeUserType = [&ctx](
        TTypeAnnotationNode::TListType argumentTypes,
        TExprNode::TListType argumentDescriptors)
    {
        const auto* argumentsType = ctx.ExprCtx.MakeType<TTupleExprType>(
            std::move(argumentTypes));
        const auto* emptyStructType = ctx.ExprCtx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{});
        const auto* emptyTupleType = ctx.ExprCtx.MakeType<TTupleExprType>(
            TTypeAnnotationNode::TListType{});
        const auto* userType = ctx.ExprCtx.MakeType<TTupleExprType>(
            TTypeAnnotationNode::TListType{
                argumentsType,
                emptyStructType,
                emptyTupleType,
            });
        return TupleTypeDescriptor(
            ctx,
            {
                TupleTypeDescriptor(
                    ctx,
                    std::move(argumentDescriptors),
                    argumentsType),
                EmptyStructTypeDescriptor(ctx, emptyStructType),
                TupleTypeDescriptor(ctx, {}, emptyTupleType),
            },
            userType);
    };

    TExprNode::TPtr source;
    if (shape == EDateTime2YearShape::NonMemberSource) {
        source = TypedNothing(
            ctx, "Date", dateType, optionalDateType);
    } else {
        const bool nonOptional =
            shape == EDateTime2YearShape::NonOptionalSource;
        const bool wrongType =
            shape == EDateTime2YearShape::WrongSourceType;
        source = TypedMember(
            ctx,
            shape == EDateTime2YearShape::InvisibleSource
                ? TStringBuf("a.hidden")
                : TStringBuf("a.d"),
            wrongType
                ? optionalDatetimeType
                : nonOptional
                    ? dateType
                    : optionalDateType);
    }

    const bool wrongCastTarget =
        shape == EDateTime2YearShape::WrongCastTarget;
    const bool nonOptionalCast =
        shape == EDateTime2YearShape::NonOptionalCastResult;
    auto cast = TypedCallable(
        ctx,
        shape == EDateTime2YearShape::WrongCastCallable
            ? TStringBuf("Convert")
            : TStringBuf("SafeCast"),
        {
            std::move(source),
            nonOptionalCast
                ? DataTypeDescriptor(ctx, "Timestamp", timestampType)
                : wrongCastTarget
                    ? OptionalDataTypeDescriptor(
                        ctx,
                        "Datetime",
                        datetimeType,
                        optionalDatetimeType)
                    : OptionalDataTypeDescriptor(
                        ctx,
                        "Timestamp",
                        timestampType,
                        optionalTimestampType),
        },
        nonOptionalCast ? timestampType : optionalTimestampType);

    auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "timestamp");
    argument->SetTypeAnn(
        shape == EDateTime2YearShape::WrongLambdaType
            ? dateType
            : timestampType);
    TExprNode::TPtr splitArgument = argument;
    if (shape == EDateTime2YearShape::WrongLambdaBinder) {
        splitArgument =
            ctx.ExprCtx.NewArgument(TPositionHandle(), "other_timestamp");
        splitArgument->SetTypeAnn(timestampType);
    }

    const ui64 splitFlags =
        shape == EDateTime2YearShape::WrongSplitFlags ? 0 : autoMap;
    const auto* splitCallableType = UdfCallableType(
        ctx, resourceType, {{timestampType, splitFlags}});
    const auto* splitCachedResultType =
        shape == EDateTime2YearShape::WrongSplitReturnDescriptor
            ? dateType
            : resourceType;
    const auto* splitCachedCallableType = UdfCallableType(
        ctx, splitCachedResultType, {{timestampType, splitFlags}});
    auto splitCachedType = CallableTypeDescriptor(
        ctx,
        shape == EDateTime2YearShape::WrongSplitReturnDescriptor
            ? DataTypeDescriptor(ctx, "Date", dateType)
            : ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
        {{
            DataTypeDescriptor(ctx, "Timestamp", timestampType),
            splitFlags,
        }},
        splitCachedCallableType);
    auto splitUdf = TypedCallable(
        ctx,
        "Udf",
        {
            ctx.ExprCtx.NewAtom(
                TPositionHandle(),
                shape == EDateTime2YearShape::WrongSplitName
                    ? TStringBuf("DateTime2.MakeTimestamp")
                    : TStringBuf("DateTime2.Split")),
            VoidValue(ctx),
            shape == EDateTime2YearShape::WrongSplitUserType
                ? makeUserType(
                    {dateType},
                    {DataTypeDescriptor(ctx, "Date", dateType)})
                : makeUserType(
                    {timestampType},
                    {DataTypeDescriptor(
                        ctx, "Timestamp", timestampType)}),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            std::move(splitCachedType),
            VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            shape == EDateTime2YearShape::WrongSplitSettings
                ? UdfSettings(ctx, {"strict", "blocks"})
                : UdfSettings(ctx, {"blocks", "strict"}),
        },
        splitCallableType);
    auto split = TypedCallable(
        ctx,
        "Apply",
        {std::move(splitUdf), std::move(splitArgument)},
        resourceType);

    const ui64 getYearFlags =
        shape == EDateTime2YearShape::WrongGetYearFlags ? 0 : autoMap;
    const auto* getYearCallableType = UdfCallableType(
        ctx, uint16Type, {{resourceType, getYearFlags}});
    const auto* getYearCachedResultType =
        shape == EDateTime2YearShape::WrongGetYearReturnDescriptor
            ? uint32Type
            : uint16Type;
    const auto* getYearCachedCallableType = UdfCallableType(
        ctx, getYearCachedResultType, {{resourceType, getYearFlags}});
    auto getYearCachedType = CallableTypeDescriptor(
        ctx,
        DataTypeDescriptor(
            ctx,
            shape == EDateTime2YearShape::WrongGetYearReturnDescriptor
                ? TStringBuf("Uint32")
                : TStringBuf("Uint16"),
            getYearCachedResultType),
        {{
            ResourceTypeDescriptor(ctx, "DateTime2.TM", resourceType),
            getYearFlags,
        }},
        getYearCachedCallableType);
    auto getYearUdf = TypedCallable(
        ctx,
        "Udf",
        {
            ctx.ExprCtx.NewAtom(
                TPositionHandle(),
                shape == EDateTime2YearShape::WrongGetYearName
                    ? TStringBuf("DateTime2.GetMonth")
                    : TStringBuf("DateTime2.GetYear")),
            VoidValue(ctx),
            shape == EDateTime2YearShape::WrongGetYearUserType
                ? makeUserType(
                    {timestampType},
                    {DataTypeDescriptor(
                        ctx, "Timestamp", timestampType)})
                : makeUserType(
                    {optionalTimestampType},
                    {OptionalDataTypeDescriptor(
                        ctx,
                        "Timestamp",
                        timestampType,
                        optionalTimestampType)}),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            std::move(getYearCachedType),
            VoidTypeDescriptor(ctx),
            ctx.ExprCtx.NewAtom(TPositionHandle(), ""),
            shape == EDateTime2YearShape::WrongGetYearSettings
                ? UdfSettings(ctx, {"blocks", "strict"})
                : UdfSettings(ctx, {"strict"}),
        },
        getYearCallableType);
    auto getYear = TypedCallable(
        ctx,
        "Apply",
        {std::move(getYearUdf), std::move(split)},
        uint16Type);

    TExprNode::TPtr lambda;
    if (shape == EDateTime2YearShape::NonUnaryLambda) {
        auto second =
            ctx.ExprCtx.NewArgument(TPositionHandle(), "second_timestamp");
        second->SetTypeAnn(timestampType);
        lambda = ctx.ExprCtx.NewLambda(
            TPositionHandle(),
            ctx.ExprCtx.NewArguments(
                TPositionHandle(), {argument, std::move(second)}),
            std::move(getYear));
    } else {
        lambda = TypedUnaryLambda(ctx, argument, std::move(getYear));
    }

    return TypedCallable(
        ctx,
        "Map",
        {std::move(cast), std::move(lambda)},
        shape == EDateTime2YearShape::NonOptionalResult
            ? uint16Type
            : shape == EDateTime2YearShape::WrongResultType
                ? optionalUint32Type
                : optionalUint16Type);
}

TSemanticSnapshotExportResult ExportNullableDateYear(
    TExportTestContext& ctx,
    EDateTime2YearShape shape = EDateTime2YearShape::Exact)
{
    const auto& table = AddTable(ctx, "/Root/DateYear", {
        {"d", "Date", false},
        {"hidden", "Date", false},
    });
    auto read = MakeRead(ctx, table, "a", {"d"});
    auto map = MakeIntrusive<TOpMap>(
        read,
        TPositionHandle(),
        TVector<TMapElement>{TMapElement(
            TInfoUnit("result"),
            TExpression(
                TypedNullableDateYear(ctx, shape),
                &ctx.ExprCtx,
                &ctx.ExpressionProps))});
    TOpRoot root(map, TPositionHandle(), {"result"});
    return ExportSemanticSnapshotV1(root, ctx.RboCtx);
}

TExprNode::TPtr MakeOlapFilterProcess(
    TExportTestContext& ctx,
    TExprNode::TPtr condition)
{
    const auto pos = TPositionHandle();
    const auto argument = ctx.ExprCtx.NewArgument(pos, "row");
    const auto filter = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
        .Input(TExprBase(argument))
        .Condition(TExprBase(std::move(condition)))
        .Done();
    return ctx.ExprCtx.NewLambda(
        pos,
        ctx.ExprCtx.NewArguments(pos, {argument}),
        filter.Ptr());
}

TExprNode::TPtr MakeOlapFilterChain(
    TExportTestContext& ctx,
    TExprNode::TListType conditions)
{
    const auto pos = TPositionHandle();
    const auto argument = ctx.ExprCtx.NewArgument(pos, "row");
    TExprNode::TPtr input = argument;
    for (auto& condition : conditions) {
        input = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
            .Input(TExprBase(std::move(input)))
            .Condition(TExprBase(std::move(condition)))
            .Done().Ptr();
    }
    return ctx.ExprCtx.NewLambda(
        pos,
        ctx.ExprCtx.NewArguments(pos, {argument}),
        std::move(input));
}

TExprNode::TPtr MakeOlapComparisonCondition(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column,
    TStringBuf literal)
{
    const auto pos = TPositionHandle();
    return Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value(operation).Build()
        .Left<TCoAtom>().Value(column).Build()
        .Right(TypedLiteral(
            ctx,
            "Int32",
            literal,
            ScalarType(ctx, NUdf::EDataSlot::Int32)))
        .Done().Ptr();
}

TExprNode::TPtr MakeOlapCoalesceFalse(
    TExportTestContext& ctx,
    TExprNode::TPtr condition)
{
    const auto pos = TPositionHandle();
    return Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value("??").Build()
        .Left(TExprBase(std::move(condition)))
        .Right(TypedLiteral(
            ctx,
            "Bool",
            "false",
            ScalarType(ctx, NUdf::EDataSlot::Bool)))
        .Done().Ptr();
}

TExprNode::TPtr MakeOlapStringPredicate(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column,
    TStringBuf literal,
    bool coalesceFalse = true)
{
    const auto pos = TPositionHandle();
    const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
    const auto* optionalBool = ScalarType(
        ctx,
        NUdf::EDataSlot::Bool,
        true);
    auto condition = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value(operation).Build()
        .Left<TCoAtom>().Value(column).Build()
        .Right(TypedLiteral(
            ctx,
            "String",
            literal,
            ScalarType(ctx, NUdf::EDataSlot::String)))
        .OpType(TExprBase(OptionalDataTypeDescriptor(
            ctx,
            "Bool",
            boolType,
            optionalBool)))
        .Done().Ptr();
    if (!coalesceFalse) {
        return condition;
    }
    return Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value("??").Build()
        .Left(TExprBase(std::move(condition)))
        .Right(TypedLiteral(ctx, "Bool", "false", boolType))
        .OpType(TExprBase(DataTypeDescriptor(ctx, "Bool", boolType)))
        .Done().Ptr();
}

TExprNode::TPtr MakeOlapComparisonProcess(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column,
    TStringBuf literal,
    bool coalesceFalse = false)
{
    auto condition = MakeOlapComparisonCondition(
        ctx,
        operation,
        column,
        literal);
    if (coalesceFalse) {
        condition = MakeOlapCoalesceFalse(ctx, std::move(condition));
    }

    return MakeOlapFilterProcess(ctx, std::move(condition));
}

TExprNode::TPtr MakeOlapUnaryProcess(
    TExportTestContext& ctx,
    TStringBuf operation,
    TExprNode::TPtr argument)
{
    const auto pos = TPositionHandle();
    const auto condition = Build<TKqpOlapFilterUnaryOp>(ctx.ExprCtx, pos)
        .Operator().Value(operation).Build()
        .Arg(TExprBase(std::move(argument)))
        .Done();
    return MakeOlapFilterProcess(ctx, condition.Ptr());
}

TExprNode::TPtr MakeOlapUnaryProcess(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column)
{
    return MakeOlapUnaryProcess(
        ctx,
        operation,
        ctx.ExprCtx.NewAtom(TPositionHandle(), column));
}

TExprNode::TPtr MakeOlapDecimalComparisonProcess(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column,
    TStringBuf literal,
    TStringBuf precision,
    TStringBuf scale)
{
    const auto pos = TPositionHandle();
    const auto* decimalType = DecimalType(ctx, precision, scale);
    auto comparison = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value(operation).Build()
        .Left<TCoAtom>().Value(column).Build()
        .Right(TypedDecimalLiteral(
            ctx,
            literal,
            precision,
            scale,
            decimalType))
        .Done();

    return MakeOlapFilterProcess(ctx, comparison.Ptr());
}

struct TDecimalOracleType {
    ui8 Precision;
    ui8 Scale;
};

NYql::NDecimal::TInt128 DecimalOracleCheckBounds(
    NYql::NDecimal::TInt128 value,
    ui8 precision)
{
    using namespace NYql::NDecimal;
    if (IsNormal(value, precision)) {
        return value;
    }
    if (IsNan(value)) {
        return Nan();
    }
    return value > 0 ? Inf() : -Inf();
}

NYql::NDecimal::TInt128 DecimalOracleAlignScale(
    NYql::NDecimal::TInt128 value,
    TDecimalOracleType source,
    ui8 targetScale)
{
    using namespace NYql::NDecimal;
    UNIT_ASSERT_C(source.Scale <= targetScale, "comparison must only increase scale");
    if (source.Scale == targetScale) {
        return value;
    }

    const ui8 delta = targetScale - source.Scale;
    const ui8 targetPrecision = std::min<ui8>(
        MaxPrecision,
        source.Precision + delta);
    const ui8 targetIntegralDigits = targetPrecision - targetScale;
    const ui8 sourceIntegralDigits = source.Precision - source.Scale;
    if (targetIntegralDigits < sourceIntegralDigits) {
        const ui8 intermediatePrecision = targetIntegralDigits + source.Scale;
        UNIT_ASSERT_C(
            intermediatePrecision > 0,
            "Decimal(0,0) alignment is deliberately unsupported");
        value = DecimalOracleCheckBounds(value, intermediatePrecision);
    }
    return Mul(value, static_cast<TInt128>(GetDivider(delta)));
}

std::pair<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>
DecimalOracleAlign(
    NYql::NDecimal::TInt128 left,
    TDecimalOracleType leftType,
    NYql::NDecimal::TInt128 right,
    TDecimalOracleType rightType)
{
    if (leftType.Scale < rightType.Scale) {
        left = DecimalOracleAlignScale(left, leftType, rightType.Scale);
    } else if (rightType.Scale < leftType.Scale) {
        right = DecimalOracleAlignScale(right, rightType, leftType.Scale);
    }
    return {left, right};
}

TSemanticSnapshotExportResult ExportMapExpressionResult(
    TExportTestContext& ctx,
    const TString& alias,
    TExpression expression,
    bool nullableInput = false)
{
    const auto& table = AddTable(ctx, "/Root/Opaque", {
        {"x", "Int32", !nullableInput},
        {"y", "Int32", !nullableInput},
    });
    auto read = MakeRead(ctx, table, alias, {"x", "y"});
    auto map = MakeIntrusive<TOpMap>(
        read,
        TPositionHandle(),
        TVector<TMapElement>{TMapElement(
            TInfoUnit("result"),
            std::move(expression))});
    TOpRoot root(map, TPositionHandle(), {"result"});

    return ExportSemanticSnapshotV1(root, ctx.RboCtx);
}

TSemanticSnapshotExportResult ExportMapExpressionResult(
    TExportTestContext& ctx,
    const TString& alias,
    TExprNode::TPtr expression,
    bool nullableInput = false)
{
    return ExportMapExpressionResult(
        ctx,
        alias,
        TExpression(std::move(expression), &ctx.ExprCtx, &ctx.ExpressionProps),
        nullableInput);
}

TSemanticSnapshotExportResult ExportTypedMapExpressionResult(
    TExportTestContext& ctx,
    const TString& alias,
    TStringBuf sourceType,
    bool sourceNullable,
    TExprNode::TPtr expression)
{
    const auto& table = AddTable(ctx, "/Root/TypedExpression", {
        {"x", TString(sourceType), !sourceNullable},
    });
    auto read = MakeRead(ctx, table, alias, {"x"});
    auto map = MakeIntrusive<TOpMap>(
        read,
        TPositionHandle(),
        TVector<TMapElement>{TMapElement(
            TInfoUnit("result"),
            TExpression(
                std::move(expression),
                &ctx.ExprCtx,
                &ctx.ExpressionProps))});
    TOpRoot root(map, TPositionHandle(), {"result"});

    return ExportSemanticSnapshotV1(root, ctx.RboCtx);
}

NJson::TJsonValue ExportTypedMapExpression(
    TExportTestContext& ctx,
    const TString& alias,
    TStringBuf sourceType,
    bool sourceNullable,
    TExprNode::TPtr expression)
{
    const auto snapshot = ParseSupported(ExportTypedMapExpressionResult(
        ctx,
        alias,
        sourceType,
        sourceNullable,
        std::move(expression)));

    const auto& columns = FindNode(snapshot, "project")["columns"].GetArraySafe();
    UNIT_ASSERT_VALUES_EQUAL(columns.back()["output"].GetStringSafe(), "result");
    return columns.back()["expression"];
}

NJson::TJsonValue ExportMapExpression(
    TExportTestContext& ctx,
    const TString& alias,
    TExprNode::TPtr expression,
    bool nullableInput = false)
{
    const auto snapshot = ParseSupported(ExportMapExpressionResult(
        ctx,
        alias,
        std::move(expression),
        nullableInput));

    const auto& columns = FindNode(snapshot, "project")["columns"].GetArraySafe();
    UNIT_ASSERT_VALUES_EQUAL(columns.back()["output"].GetStringSafe(), "result");
    return columns.back()["expression"];
}

enum class EDateUnwrapShape {
    ExactSafeCast,
    ExactJust,
    SafeCastNonzero,
    JustNonzero,
    ConvertFallback,
    WrongRootType,
    WrongCoalesceType,
    WrongMemberType,
    WrongSafeCastSourceType,
    WrongSafeCastTargetType,
    ReversedCoalesce,
    InvisibleMember,
    StringUnwrap,
    UnsafeRoot,
    UnsafeSubtree,
};

TExprNode::TPtr TypedDateUnwrapCoalesceZero(
    TExportTestContext& ctx,
    EDateUnwrapShape shape)
{
    const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
    const auto* optionalDateType = ScalarType(
        ctx,
        NUdf::EDataSlot::Date,
        true);
    const auto* int32Type = ScalarType(ctx, NUdf::EDataSlot::Int32);

    if (shape == EDateUnwrapShape::StringUnwrap) {
        const auto* stringType = ScalarType(ctx, NUdf::EDataSlot::String);
        const auto* optionalStringType = ScalarType(
            ctx,
            NUdf::EDataSlot::String,
            true);
        return TypedCallable(
            ctx,
            "Unwrap",
            {
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedMember(ctx, "a.s", optionalStringType),
                        TypedCallable(
                            ctx,
                            "Just",
                            {TypedLiteral(ctx, "String", "", stringType)},
                            optionalStringType),
                    },
                    optionalStringType),
            },
            stringType);
    }

    const bool justFallback =
        shape == EDateUnwrapShape::ExactJust ||
        shape == EDateUnwrapShape::JustNonzero;
    const TStringBuf fallbackValue =
        shape == EDateUnwrapShape::SafeCastNonzero ||
            shape == EDateUnwrapShape::JustNonzero
        ? TStringBuf("1")
        : TStringBuf("0");

    TExprNode::TPtr fallback;
    if (justFallback) {
        fallback = TypedCallable(
            ctx,
            "Just",
            {TypedLiteral(ctx, "Date", fallbackValue, dateType)},
            optionalDateType);
    } else {
        const auto* sourceType =
            shape == EDateUnwrapShape::WrongSafeCastSourceType
            ? ScalarType(ctx, NUdf::EDataSlot::Int64)
            : int32Type;
        TExprNode::TPtr target = OptionalDataTypeDescriptor(
            ctx,
            "Date",
            dateType,
            optionalDateType);
        if (shape == EDateUnwrapShape::WrongSafeCastTargetType) {
            target = OptionalDataTypeDescriptor(
                ctx,
                "Int32",
                int32Type,
                ScalarType(ctx, NUdf::EDataSlot::Int32, true));
        }
        fallback = TypedCallable(
            ctx,
            shape == EDateUnwrapShape::ConvertFallback
                ? TStringBuf("Convert")
                : TStringBuf("SafeCast"),
            {
                TypedLiteral(
                    ctx,
                    sourceType == int32Type ? "Int32" : "Int64",
                    fallbackValue,
                    sourceType),
                std::move(target),
            },
            optionalDateType);
    }

    const auto* memberType =
        shape == EDateUnwrapShape::WrongMemberType
        ? dateType
        : optionalDateType;
    auto member = TypedMember(
        ctx,
        shape == EDateUnwrapShape::InvisibleMember
            ? TStringBuf("a.missing")
            : TStringBuf("a.x"),
        memberType);

    TExprNode::TListType coalesceArguments;
    if (shape == EDateUnwrapShape::ReversedCoalesce) {
        coalesceArguments = {std::move(fallback), std::move(member)};
    } else {
        coalesceArguments = {std::move(member), std::move(fallback)};
    }
    auto coalesce = TypedCallable(
        ctx,
        "Coalesce",
        std::move(coalesceArguments),
        shape == EDateUnwrapShape::WrongCoalesceType
            ? dateType
            : optionalDateType);
    auto unwrap = TypedCallable(
        ctx,
        "Unwrap",
        {std::move(coalesce)},
        shape == EDateUnwrapShape::WrongRootType
            ? optionalDateType
            : dateType);

    return unwrap;
}

TIntrusivePtr<TOpMap> MakeComputedMap(
    TExportTestContext& ctx,
    TIntrusivePtr<IOperator> input,
    TStringBuf output,
    TExprNode::TPtr expression)
{
    return MakeIntrusive<TOpMap>(
        std::move(input),
        TPositionHandle(),
        TVector<TMapElement>{TMapElement(
            TInfoUnit(TString(output)),
            TExpression(
                std::move(expression),
                &ctx.ExprCtx,
                &ctx.ExpressionProps))});
}

TSemanticSnapshotExportResult ExportDateUnwrapExpression(
    TExportTestContext& ctx,
    EDateUnwrapShape shape)
{
    const auto& table = AddTable(ctx, "/Root/DateUnwrap", {
        {"x", "Date", false},
        {"s", "String", false},
    });
    auto read = MakeRead(ctx, table, "a", {"x", "s"});
    TExpression expression(
        TypedDateUnwrapCoalesceZero(ctx, shape),
        &ctx.ExprCtx,
        &ctx.ExpressionProps);
    if (shape == EDateUnwrapShape::UnsafeRoot) {
        expression.GetExpressionBody()->SetSideEffects(
            ESideEffects::General);
    } else if (shape == EDateUnwrapShape::UnsafeSubtree) {
        expression.GetExpressionBody()
            ->Child(0)->Child(1)->Child(0)->SetUnorderedChildren();
    }
    auto map = MakeIntrusive<TOpMap>(
        read,
        TPositionHandle(),
        TVector<TMapElement>{TMapElement(
            TInfoUnit("result"),
            std::move(expression))});
    TOpRoot root(map, TPositionHandle(), {"result"});
    return ExportSemanticSnapshotV1(root, ctx.RboCtx);
}

TExprNode::TPtr StringLiteral(TExportTestContext& ctx, TStringBuf value) {
    return TypedLiteral(
        ctx,
        "String",
        value,
        ScalarType(ctx, NUdf::EDataSlot::String));
}

TExprNode::TPtr StringConcat(
    TExportTestContext& ctx,
    TExprNode::TPtr left,
    TExprNode::TPtr right)
{
    return TypedCallable(
        ctx,
        "Concat",
        {std::move(left), std::move(right)},
        ScalarType(ctx, NUdf::EDataSlot::String));
}

TExprNode::TPtr NonNullStoredString(
    TExportTestContext& ctx,
    TStringBuf column)
{
    return TypedMember(
        ctx,
        column,
        ScalarType(ctx, NUdf::EDataSlot::String));
}

TExprNode::TPtr CoalescedStoredString(
    TExportTestContext& ctx,
    TStringBuf column)
{
    return TypedCallable(
        ctx,
        "Coalesce",
        {
            TypedMember(
                ctx,
                column,
                ScalarType(ctx, NUdf::EDataSlot::String, true)),
            StringLiteral(ctx, ""),
        },
        ScalarType(ctx, NUdf::EDataSlot::String));
}

TExprNode::TPtr WideBooleanAnd(TExportTestContext& ctx, size_t nodes) {
    UNIT_ASSERT(nodes >= 2);
    const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
    TExprNode::TListType arguments;
    arguments.reserve(nodes - 1);
    for (size_t index = 1; index < nodes; ++index) {
        arguments.push_back(TypedLiteral(ctx, "Bool", "true", boolType));
    }
    return TypedCallable(ctx, "And", std::move(arguments), boolType);
}

TExprNode::TPtr ExponentialSharedAnd(TExportTestContext& ctx, size_t levels) {
    const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
    auto result = TypedLiteral(ctx, "Bool", "true", boolType);
    for (size_t level = 0; level < levels; ++level) {
        result = TypedCallable(ctx, "And", {result, result}, boolType);
    }
    return result;
}

TExprNode::TPtr DeepBooleanNot(TExportTestContext& ctx, size_t depth) {
    UNIT_ASSERT(depth >= 1);
    const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
    auto result = TypedLiteral(ctx, "Bool", "true", boolType);
    for (size_t level = 1; level < depth; ++level) {
        result = TypedCallable(ctx, "Not", {std::move(result)}, boolType);
    }
    return result;
}

TExprNode::TPtr WideOlapAnd(
    TExportTestContext& ctx,
    size_t comparisons,
    size_t booleanLeaves)
{
    TVector<TExprBase> arguments;
    arguments.reserve(comparisons + booleanLeaves);
    for (size_t index = 0; index < comparisons; ++index) {
        arguments.emplace_back(MakeOlapComparisonCondition(ctx, "eq", "k", "0"));
    }
    for (size_t index = 0; index < booleanLeaves; ++index) {
        arguments.emplace_back(TypedLiteral(
            ctx,
            "Bool",
            "true",
            ScalarType(ctx, NUdf::EDataSlot::Bool)));
    }
    return Build<TKqpOlapAnd>(ctx.ExprCtx, TPositionHandle())
        .Add(arguments)
        .Done().Ptr();
}

TExprNode::TPtr DeepOlapNot(TExportTestContext& ctx, size_t depth) {
    UNIT_ASSERT(depth >= 2);
    auto result = MakeOlapComparisonCondition(ctx, "eq", "k", "0");
    for (size_t level = 2; level < depth; ++level) {
        result = Build<TKqpOlapNot>(ctx.ExprCtx, TPositionHandle())
            .Value(TExprBase(std::move(result)))
            .Done().Ptr();
    }
    return result;
}

TString ExportDeterministicPlan() {
    TExportTestContext ctx;
    const auto& table = AddTable(ctx, "/Root/A", {
        {"payload", "Utf8", false},
        {"k", "Int32", true},
        {"flag", "Bool", false},
    }, {"k"});
    auto read = MakeRead(ctx, table, "a", {"k", "flag", "payload"});
    TOpRoot root(read, TPositionHandle(), {"a.payload", "a.k"});
    const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    return result.Json;
}

TString ExportDeterministicStageGraph() {
    TExportTestContext ctx;
    const auto& table = AddTable(ctx, "/Root/A", {
        {"k", "Int32", true},
        {"payload", "Utf8", false},
    });
    auto read = MakeRead(ctx, table, "a", {"k", "payload"});
    auto project = MakeCopyMap(ctx, read, "result", "a.k");
    TOpRoot root(project, TPositionHandle(), {"result"});

    auto& graph = root.PlanProps.StageGraph;
    const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
    const ui32 consumer = graph.AddStage();
    read->Props.StageId = producer;
    project->Props.StageId = consumer;
    graph.Connect(
        producer,
        consumer,
        MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));

    const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
    UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    return result.Json;
}

struct TCorrelatedScalarExportFixture {
    TCorrelatedScalarExportFixture()
        : Int32(ScalarType(Ctx, NUdf::EDataSlot::Int32))
        , OptionalInt32(ScalarType(
              Ctx,
              NUdf::EDataSlot::Int32,
              true))
        , Int64(ScalarType(Ctx, NUdf::EDataSlot::Int64))
        , OptionalInt64(ScalarType(
              Ctx,
              NUdf::EDataSlot::Int64,
              true))
        , Bool(ScalarType(Ctx, NUdf::EDataSlot::Bool))
        , OptionalBool(ScalarType(
              Ctx,
              NUdf::EDataSlot::Bool,
              true))
        , String(ScalarType(Ctx, NUdf::EDataSlot::String))
    {
        const auto& outerTable = AddTable(
            Ctx,
            "/Root/CorrelatedScalarOuter",
            {{"k", "Int32", false}});
        const auto& innerTable = AddTable(
            Ctx,
            "/Root/CorrelatedScalarInner",
            {
                {"k", "Int32", false},
                {"value", "Int64", true},
                {"text", "String", true},
                {"flag", "Bool", true},
            });
        OuterRead = MakeRead(
            Ctx,
            outerTable,
            "outer",
            {"k"});
        InnerRead = MakeRead(
            Ctx,
            innerTable,
            "inner",
            {"k", "value", "text", "flag"});
        SetExactOutputType(Ctx, *OuterRead, {
            {"outer.k", OptionalInt32},
        });
        SetExactOutputType(Ctx, *InnerRead, {
            {"inner.k", OptionalInt32},
            {"inner.value", Int64},
            {"inner.text", String},
            {"inner.flag", Bool},
        });

        Root = std::make_unique<TOpRoot>(
            OuterRead,
            Pos,
            TVector<TString>{"outer.k"});
        OuterBind = MakeIntrusive<TOpAddDependencies>(
            InnerRead,
            Pos,
            TVector<std::pair<
                TInfoUnit,
                const TTypeAnnotationNode*>>{{
                Dependency,
                OptionalInt32,
            }});
        SetExactOutputType(Ctx, *OuterBind, {
            {"inner.k", OptionalInt32},
            {"inner.value", Int64},
            {"inner.text", String},
            {"inner.flag", Bool},
            {"outer.k", OptionalInt32},
        });

        Equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                TInfoUnit("inner.k"),
                Pos,
                &Ctx.ExprCtx,
                &Root->PlanProps),
            MakeColumnAccess(
                Dependency,
                Pos,
                &Ctx.ExprCtx,
                &Root->PlanProps));
        AnnotateBinaryExpression(
            Equality,
            OptionalInt32,
            OptionalInt32,
            OptionalBool);
        Residual = MakeColumnAccess(
            TInfoUnit("inner.flag"),
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        AnnotateExpression(Residual, Bool);
        CorrelationPredicate =
            MakeConjunction({Residual, Equality});
        AnnotateExpression(CorrelationPredicate, OptionalBool);
        CorrelationFilter = MakeIntrusive<TOpFilter>(
            OuterBind,
            Pos,
            CorrelationPredicate);
        SetExactOutputType(Ctx, *CorrelationFilter, {
            {"inner.k", OptionalInt32},
            {"inner.value", Int64},
            {"inner.text", String},
            {"inner.flag", Bool},
            {"outer.k", OptionalInt32},
        });

        auto mappedValue = MakeColumnAccess(
            TInfoUnit("inner.value"),
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        AnnotateExpression(mappedValue, Int64);
        CorrelationMap = MakeIntrusive<TOpMap>(
            CorrelationFilter,
            Pos,
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("mapped.value"),
                    mappedValue),
                TMapElement(
                    TInfoUnit("mapped.text"),
                    TExpression(
                        StringConcat(
                            Ctx,
                            StringLiteral(Ctx, "prefix:"),
                            NonNullStoredString(Ctx, "inner.text")),
                        &Ctx.ExprCtx,
                        &Root->PlanProps)),
            });
        SetExactOutputType(Ctx, *CorrelationMap, {
            {"inner.k", OptionalInt32},
            {"inner.value", Int64},
            {"inner.text", String},
            {"inner.flag", Bool},
            {"outer.k", OptionalInt32},
            {"mapped.value", Int64},
            {"mapped.text", String},
        });

        ScalarAggregate = MakeIntrusive<TOpAggregate>(
            CorrelationMap,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("mapped.value"),
                "sum",
                TInfoUnit("scalar.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            Pos);
        SetExactOutputType(Ctx, *ScalarAggregate, {
            {"scalar.value", OptionalInt64},
        });
        Root->PlanProps.Subplans.Add(
            Binding,
            TSubplanEntry{
                ScalarAggregate,
                {},
                ESubplanType::EXPR,
                Binding,
                {Dependency}});

        auto bindingValue = MakeColumnAccess(
            Binding,
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        auto one = MakeConstant(
            "Int64",
            "1",
            Pos,
            &Ctx.ExprCtx);
        ConsumerPredicate = MakeBinaryPredicate(
            "==",
            bindingValue,
            one);
        AnnotateBinaryExpression(
            ConsumerPredicate,
            OptionalInt64,
            Int64,
            OptionalBool);
        Consumer = MakeIntrusive<TOpFilter>(
            OuterRead,
            Pos,
            ConsumerPredicate);
        SetExactOutputType(Ctx, *Consumer, {
            {"outer.k", OptionalInt32},
        });
        Root->SetInput(Consumer);
    }

    TSubplanEntry& Entry() {
        return Root->PlanProps.Subplans.PlanMap.at(Binding);
    }

    TExportTestContext Ctx;
    const TPositionHandle Pos;
    const TTypeAnnotationNode* const Int32;
    const TTypeAnnotationNode* const OptionalInt32;
    const TTypeAnnotationNode* const Int64;
    const TTypeAnnotationNode* const OptionalInt64;
    const TTypeAnnotationNode* const Bool;
    const TTypeAnnotationNode* const OptionalBool;
    const TTypeAnnotationNode* const String;
    const TInfoUnit Binding{"_rbo_arg_correlated_scalar", true};
    const TInfoUnit Dependency{"outer.k"};
    TIntrusivePtr<TOpRead> OuterRead;
    TIntrusivePtr<TOpRead> InnerRead;
    std::unique_ptr<TOpRoot> Root;
    TIntrusivePtr<TOpAddDependencies> OuterBind;
    TExpression Equality;
    TExpression Residual;
    TExpression CorrelationPredicate;
    TIntrusivePtr<TOpFilter> CorrelationFilter;
    TIntrusivePtr<TOpMap> CorrelationMap;
    TIntrusivePtr<TOpAggregate> ScalarAggregate;
    TExpression ConsumerPredicate;
    TIntrusivePtr<TOpFilter> Consumer;
};

struct TTwoDependencyExistsExportFixture {
    TTwoDependencyExistsExportFixture()
        : Int32(ScalarType(Ctx, NUdf::EDataSlot::Int32))
        , OptionalInt32(ScalarType(
              Ctx,
              NUdf::EDataSlot::Int32,
              true))
        , Bool(ScalarType(Ctx, NUdf::EDataSlot::Bool))
        , OptionalBool(ScalarType(
              Ctx,
              NUdf::EDataSlot::Bool,
              true))
    {
        const auto& outerTable = AddTable(
            Ctx,
            "/Root/TwoDependencyExistsOuter",
            {
                {"order_key", "Int32", false},
                {"warehouse_key", "Int32", false},
            });
        const auto& innerTable = AddTable(
            Ctx,
            "/Root/TwoDependencyExistsInner",
            {
                {"order_key", "Int32", false},
                {"warehouse_key", "Int32", false},
                {"flag", "Bool", true},
            });
        OuterRead = MakeRead(
            Ctx,
            outerTable,
            "outer",
            {"order_key", "warehouse_key"});
        InnerRead = MakeRead(
            Ctx,
            innerTable,
            "inner",
            {"order_key", "warehouse_key", "flag"});
        SetExactOutputType(Ctx, *OuterRead, {
            {"outer.order_key", OptionalInt32},
            {"outer.warehouse_key", OptionalInt32},
        });
        SetExactOutputType(Ctx, *InnerRead, {
            {"inner.order_key", OptionalInt32},
            {"inner.warehouse_key", OptionalInt32},
            {"inner.flag", Bool},
        });

        Root = std::make_unique<TOpRoot>(
            OuterRead,
            Pos,
            TVector<TString>{"outer.order_key"});
        AddDependencies = MakeIntrusive<TOpAddDependencies>(
            InnerRead,
            Pos,
            TVector<std::pair<
                TInfoUnit,
                const TTypeAnnotationNode*>>{
                {OrderDependency, OptionalInt32},
                {WarehouseDependency, OptionalInt32},
            });
        SetExactOutputType(Ctx, *AddDependencies, {
            {"inner.order_key", OptionalInt32},
            {"inner.warehouse_key", OptionalInt32},
            {"inner.flag", Bool},
            {"outer.order_key", OptionalInt32},
            {"outer.warehouse_key", OptionalInt32},
        });

        Equality = Comparison(
            "==",
            TInfoUnit("inner.order_key"),
            OrderDependency);
        Inequality = Comparison(
            "!=",
            WarehouseDependency,
            TInfoUnit("inner.warehouse_key"));
        Residual = MakeColumnAccess(
            TInfoUnit("inner.flag"),
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        AnnotateExpression(Residual, Bool);
        SetPredicate({Equality, Inequality, Residual});

        Filter = MakeIntrusive<TOpFilter>(
            AddDependencies,
            Pos,
            Predicate);
        SetExactOutputType(Ctx, *Filter, {
            {"inner.order_key", OptionalInt32},
            {"inner.warehouse_key", OptionalInt32},
            {"inner.flag", Bool},
            {"outer.order_key", OptionalInt32},
            {"outer.warehouse_key", OptionalInt32},
        });
        Root->PlanProps.Subplans.Add(
            Binding,
            TSubplanEntry{
                Filter,
                {},
                ESubplanType::EXISTS,
                Binding,
                {OrderDependency, WarehouseDependency}});

        BindingValue = MakeColumnAccess(
            Binding,
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        AnnotateExpression(BindingValue, Bool);
        Consumer = MakeIntrusive<TOpFilter>(
            OuterRead,
            Pos,
            BindingValue);
        SetExactOutputType(Ctx, *Consumer, {
            {"outer.order_key", OptionalInt32},
            {"outer.warehouse_key", OptionalInt32},
        });
        Root->SetInput(Consumer);
    }

    TExpression Comparison(
        TStringBuf callable,
        const TInfoUnit& left,
        const TInfoUnit& right)
    {
        auto result = MakeBinaryPredicate(
            TString(callable),
            MakeColumnAccess(
                left,
                Pos,
                &Ctx.ExprCtx,
                &Root->PlanProps),
            MakeColumnAccess(
                right,
                Pos,
                &Ctx.ExprCtx,
                &Root->PlanProps));
        AnnotateBinaryExpression(
            result,
            OptionalInt32,
            OptionalInt32,
            OptionalBool);
        return result;
    }

    void SetPredicate(const TVector<TExpression>& conjuncts) {
        Predicate = MakeConjunction(conjuncts);
        AnnotateExpression(Predicate, OptionalBool);
        if (Filter) {
            Filter->FilterExpr = Predicate;
        }
    }

    TSubplanEntry& Entry() {
        return Root->PlanProps.Subplans.PlanMap.at(Binding);
    }

    TExportTestContext Ctx;
    const TPositionHandle Pos;
    const TTypeAnnotationNode* const Int32;
    const TTypeAnnotationNode* const OptionalInt32;
    const TTypeAnnotationNode* const Bool;
    const TTypeAnnotationNode* const OptionalBool;
    const TInfoUnit Binding{"_rbo_exists_two_dependencies", true};
    const TInfoUnit OrderDependency{"outer.order_key"};
    const TInfoUnit WarehouseDependency{"outer.warehouse_key"};
    TIntrusivePtr<TOpRead> OuterRead;
    TIntrusivePtr<TOpRead> InnerRead;
    std::unique_ptr<TOpRoot> Root;
    TIntrusivePtr<TOpAddDependencies> AddDependencies;
    TExpression Equality;
    TExpression Inequality;
    TExpression Residual;
    TExpression Predicate;
    TIntrusivePtr<TOpFilter> Filter;
    TExpression BindingValue;
    TIntrusivePtr<TOpFilter> Consumer;
};

enum class EInSubplanColumnKind {
    Int32,
    String,
};

struct TInSubplanExportFixture {
    explicit TInSubplanExportFixture(
        EInSubplanColumnKind columnKind = EInSubplanColumnKind::Int32)
        : Int32(ScalarType(Ctx, NUdf::EDataSlot::Int32))
        , OptionalInt32(ScalarType(
              Ctx,
              NUdf::EDataSlot::Int32,
              true))
        , Int64(ScalarType(Ctx, NUdf::EDataSlot::Int64))
        , Bool(ScalarType(Ctx, NUdf::EDataSlot::Bool))
        , OptionalBool(ScalarType(
              Ctx,
              NUdf::EDataSlot::Bool,
              true))
        , String(ScalarType(Ctx, NUdf::EDataSlot::String))
        , OptionalString(ScalarType(
              Ctx,
              NUdf::EDataSlot::String,
              true))
        , Utf8(ScalarType(Ctx, NUdf::EDataSlot::Utf8))
        , Date(ScalarType(Ctx, NUdf::EDataSlot::Date))
        , ColumnType(
              columnKind == EInSubplanColumnKind::String
                  ? String
                  : Int32)
    {
        const TString columnType =
            columnKind == EInSubplanColumnKind::String
                ? TString("String")
                : TString("Int32");
        const auto& outerTable = AddTable(
            Ctx,
            "/Root/InOuter",
            {{"k", columnType, true}});
        const auto& innerTable = AddTable(
            Ctx,
            "/Root/InInner",
            {
                {"k", columnType, true},
                {"other", "Int32", true},
            });
        OuterRead = MakeRead(Ctx, outerTable, "outer", {"k"});
        InnerRead = MakeRead(Ctx, innerTable, "inner", {"k"});
        WideInnerRead = MakeRead(
            Ctx,
            innerTable,
            "wide",
            {"k", "other"});
        SetExactOutputType(Ctx, *OuterRead, {{"outer.k", ColumnType}});
        SetExactOutputType(Ctx, *InnerRead, {{"inner.k", ColumnType}});
        SetExactOutputType(Ctx, *WideInnerRead, {
            {"wide.k", ColumnType},
            {"wide.other", Int32},
        });

        Root = std::make_unique<TOpRoot>(
            OuterRead,
            Pos,
            TVector<TString>{"outer.k"});
        Root->PlanProps.Subplans.Add(
            Binding,
            TSubplanEntry{
                InnerRead,
                {Lookup},
                ESubplanType::IN_SUBPLAN,
                Binding,
                {}});

        BindingValue = MakeColumnAccess(
            Binding,
            Pos,
            &Ctx.ExprCtx,
            &Root->PlanProps);
        AnnotateExpression(BindingValue, Bool);
        Consumer = MakeIntrusive<TOpFilter>(
            OuterRead,
            Pos,
            BindingValue);
        SetExactOutputType(Ctx, *Consumer, {{"outer.k", ColumnType}});
        Root->SetInput(Consumer);
    }

    TSubplanEntry& Entry() {
        return Root->PlanProps.Subplans.PlanMap.at(Binding);
    }

    TExportTestContext Ctx;
    const TPositionHandle Pos;
    const TTypeAnnotationNode* const Int32;
    const TTypeAnnotationNode* const OptionalInt32;
    const TTypeAnnotationNode* const Int64;
    const TTypeAnnotationNode* const Bool;
    const TTypeAnnotationNode* const OptionalBool;
    const TTypeAnnotationNode* const String;
    const TTypeAnnotationNode* const OptionalString;
    const TTypeAnnotationNode* const Utf8;
    const TTypeAnnotationNode* const Date;
    const TTypeAnnotationNode* const ColumnType;
    const TInfoUnit Binding{"_rbo_in", true};
    const TInfoUnit Lookup{"outer.k"};
    TIntrusivePtr<TOpRead> OuterRead;
    TIntrusivePtr<TOpRead> InnerRead;
    TIntrusivePtr<TOpRead> WideInnerRead;
    std::unique_ptr<TOpRoot> Root;
    TExpression BindingValue;
    TIntrusivePtr<TOpFilter> Consumer;
};

Y_UNIT_TEST_SUITE(TSemanticSnapshotExporter) {
    Y_UNIT_TEST(OutputIsDeterministicAcrossEquivalentAllocations) {
        UNIT_ASSERT_VALUES_EQUAL(ExportDeterministicPlan(), ExportDeterministicPlan());
    }

    Y_UNIT_TEST(ExportsOnlyAuditedStoredStringConcatShapes) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/Concat", {
            {"id", "String", true},
            {"first", "String", false},
            {"last", "String", false},
        });
        auto read = MakeRead(ctx, table, "a", {"id", "first", "last"});

        auto q5 = StringConcat(
            ctx,
            StringLiteral(ctx, "store"),
            NonNullStoredString(ctx, "a.id"));
        auto twoMembers = StringConcat(
            ctx,
            StringConcat(
                ctx,
                CoalescedStoredString(ctx, "a.last"),
                StringLiteral(ctx, ", ")),
            CoalescedStoredString(ctx, "a.first"));
        auto map = MakeIntrusive<TOpMap>(
            read,
            TPositionHandle(),
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("q5"),
                    TExpression(q5, &ctx.ExprCtx, &ctx.ExpressionProps)),
                TMapElement(
                    TInfoUnit("two_members"),
                    TExpression(twoMembers, &ctx.ExprCtx, &ctx.ExpressionProps)),
            });
        TOpRoot root(map, TPositionHandle(), {"q5", "two_members"});

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& columns = FindNode(snapshot, "project")["columns"].GetArraySafe();
        THashMap<TString, const NJson::TJsonValue*> expressions;
        for (const auto& column : columns) {
            expressions.emplace(
                column["output"].GetStringSafe(),
                &column["expression"]);
        }

        for (const TString& output : {TString("q5"), TString("two_members")}) {
            const auto& expression = **expressions.FindPtr(output);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "String");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_STRING_CONTAINS(
                expression["fingerprint"].GetStringSafe(),
                "Concat");
        }
        UNIT_ASSERT_VALUES_EQUAL(
            (**expressions.FindPtr("q5"))["args"].GetArraySafe().size(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            (**expressions.FindPtr("two_members"))["args"].GetArraySafe().size(),
            2);
    }

    Y_UNIT_TEST(RestrictedStoredStringConcatFailsClosed) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/LiteralOnly", {
                {"id", "Int32", true},
            });
            auto read = MakeRead(ctx, table, "a", {"id"});
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "left"),
                    StringLiteral(ctx, "right")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "no storage-bounded String member");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Utf8", {
                {"text", "Utf8", true},
            });
            auto read = MakeRead(ctx, table, "a", {"text"});
            SetOutputType(ctx, *read, {{"a.text", NUdf::EDataSlot::Utf8}});
            auto expression = TypedCallable(
                ctx,
                "Concat",
                {
                    TypedLiteral(
                        ctx,
                        "Utf8",
                        "prefix",
                        ScalarType(ctx, NUdf::EDataSlot::Utf8)),
                    TypedMember(
                        ctx,
                        "a.text",
                        ScalarType(ctx, NUdf::EDataSlot::Utf8)),
                },
                ScalarType(ctx, NUdf::EDataSlot::Utf8));
            auto map = MakeComputedMap(ctx, read, "result", std::move(expression));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Restricted Concat");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Nullable", {
                {"text", "String", false},
            });
            auto read = MakeRead(ctx, table, "a", {"text"});
            SetOutputType(ctx, *read, {
                {"a.text", NUdf::EDataSlot::String, true},
            });
            auto badCoalesce = TypedCallable(
                ctx,
                "Coalesce",
                {
                    TypedMember(
                        ctx,
                        "a.text",
                        ScalarType(ctx, NUdf::EDataSlot::String, true)),
                    StringLiteral(ctx, "not empty"),
                },
                ScalarType(ctx, NUdf::EDataSlot::String));
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(ctx, std::move(badCoalesce), StringLiteral(ctx, "!")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "empty String");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Three", {
                {"x", "String", true},
            });
            auto read = MakeRead(ctx, table, "a", {"x"});
            SetOutputType(ctx, *read, {
                {"a.x", NUdf::EDataSlot::String},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    StringConcat(
                        ctx,
                        NonNullStoredString(ctx, "a.x"),
                        NonNullStoredString(ctx, "a.x")),
                    NonNullStoredString(ctx, "a.x")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "two stored-member");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/DirectOptional", {
                {"text", "String", false},
            });
            auto read = MakeRead(ctx, table, "a", {"text"});
            SetOutputType(ctx, *read, {
                {"a.text", NUdf::EDataSlot::String, true},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "prefix"),
                    TypedMember(
                        ctx,
                        "a.text",
                        ScalarType(ctx, NUdf::EDataSlot::String, true))));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "non-null String");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Computed", {
                {"stored", "String", true},
            });
            auto read = MakeRead(ctx, table, "a", {"stored"});
            SetOutputType(ctx, *read, {
                {"a.stored", NUdf::EDataSlot::String},
            });
            auto computed = MakeComputedMap(
                ctx,
                read,
                "computed",
                StringLiteral(ctx, "small"));
            auto concat = MakeComputedMap(
                ctx,
                computed,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "prefix"),
                    NonNullStoredString(ctx, "computed")));
            TOpRoot root(concat, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "no storage-bounded");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/OlapTwoCells", {
                {"text", "String", false},
            });
            table.Metadata->Kind = EKikimrTableKind::Olap;
            auto read = MakeRead(ctx, table, "a", {"text"});
            SetOutputType(ctx, *read, {
                {"a.text", NUdf::EDataSlot::String, true},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    CoalescedStoredString(ctx, "a.text"),
                    CoalescedStoredString(ctx, "a.text")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "safe Concat allocation bound");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Nested", {
                {"stored", "String", true},
            });
            auto read = MakeRead(ctx, table, "a", {"stored"});
            SetOutputType(ctx, *read, {
                {"a.stored", NUdf::EDataSlot::String},
            });
            auto concat = StringConcat(
                ctx,
                StringLiteral(ctx, "prefix"),
                NonNullStoredString(ctx, "a.stored"));
            auto comparison = TypedCallable(
                ctx,
                "==",
                {std::move(concat), StringLiteral(ctx, "value")},
                ScalarType(ctx, NUdf::EDataSlot::Bool));
            auto map = MakeComputedMap(ctx, read, "result", std::move(comparison));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Unsupported scalar callable Concat");
        }

        for (const bool olapSysView : {false, true}) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/SysView", {
                {"generated", "String", true},
            });
            table.Metadata->Kind = olapSysView
                ? EKikimrTableKind::Olap
                : EKikimrTableKind::SysView;
            table.Metadata->SysView = olapSysView ? "top_queries" : "";
            auto read = MakeRead(ctx, table, "a", {"generated"});
            SetOutputType(ctx, *read, {
                {"a.generated", NUdf::EDataSlot::String},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "prefix"),
                    NonNullStoredString(ctx, "a.generated")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "no storage-bounded");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/CatalogNullability", {
                {"text", "String", true},
            });
            auto read = MakeRead(ctx, table, "a", {"text"});
            SetOutputType(ctx, *read, {
                {"a.text", NUdf::EDataSlot::String, true},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "prefix"),
                    CoalescedStoredString(ctx, "a.text")));
            TOpRoot root(map, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "storage provenance is non-null but the expression is nullable");
        }
    }

    Y_UNIT_TEST(StoredStringConcatProvenanceUsesOnlyPreservingEdges) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/PassThrough", {
                {"text", "String", true},
                {"flag", "Bool", true},
            });
            auto read = MakeRead(ctx, table, "a", {"text", "flag"});
            const TVector<TOutputTypeSpec> types = {
                {"a.text", NUdf::EDataSlot::String},
                {"a.flag", NUdf::EDataSlot::Bool},
            };
            SetOutputType(ctx, *read, types);

            const auto pos = TPositionHandle();
            auto filter = MakeIntrusive<TOpFilter>(
                read,
                pos,
                TExpression(
                    TypedMember(
                        ctx,
                        "a.flag",
                        ScalarType(ctx, NUdf::EDataSlot::Bool)),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            SetOutputType(ctx, *filter, types);
            auto limit = MakeIntrusive<TOpLimit>(
                filter,
                pos,
                MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
                EOpPhase::Undefined);
            SetOutputType(ctx, *limit, types);
            auto sort = MakeIntrusive<TOpSort>(
                limit,
                pos,
                TVector<TSortElement>{
                    TSortElement(TInfoUnit("a.text"), true, false),
            });
            SetOutputType(ctx, *sort, types);
            auto rename = MakeCopyMap(ctx, sort, "renamed", "a.text");
            SetOutputType(ctx, *rename, {
                {"a.flag", NUdf::EDataSlot::Bool},
                {"renamed", NUdf::EDataSlot::String},
            });
            auto concat = MakeComputedMap(
                ctx,
                rename,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "prefix"),
                    NonNullStoredString(ctx, "renamed")));
            TOpRoot root(concat, pos, {"result"});
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Aggregate", {
                {"key", "String", true},
                {"value", "Int64", true},
            });
            auto read = MakeRead(ctx, table, "a", {"key", "value"});
            SetOutputType(ctx, *read, {
                {"a.key", NUdf::EDataSlot::String},
                {"a.value", NUdf::EDataSlot::Int64},
            });
            auto aggregate = MakeIntrusive<TOpAggregate>(
                read,
                TVector<TOpAggregationTraits>{TOpAggregationTraits(
                    TInfoUnit("a.value"),
                    "count",
                    TInfoUnit("count"))},
                TVector<TInfoUnit>{TInfoUnit("a.key")},
                EOpPhase::Undefined,
                false,
                TPositionHandle());
            SetOutputType(ctx, *aggregate, {
                {"a.key", NUdf::EDataSlot::String},
                {"count", NUdf::EDataSlot::Uint64},
            });
            auto concat = MakeComputedMap(
                ctx,
                aggregate,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "group"),
                    NonNullStoredString(ctx, "a.key")));
            TOpRoot root(concat, TPositionHandle(), {"result"});
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        }

        {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/JoinLeft", {
                {"k", "Int32", true},
                {"text", "String", true},
            });
            AddTable(ctx, "/Root/JoinRight", {
                {"k", "Int32", true},
                {"text", "String", true},
            });
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinLeft"),
                "a",
                {"k", "text"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinRight"),
                "b",
                {"k", "text"});
            SetOutputType(ctx, *left, {
                {"a.k", NUdf::EDataSlot::Int32},
                {"a.text", NUdf::EDataSlot::String},
            });
            SetOutputType(ctx, *right, {
                {"b.k", NUdf::EDataSlot::Int32},
                {"b.text", NUdf::EDataSlot::String},
            });
            auto join = MakeIntrusive<TOpJoin>(
                left,
                right,
                TPositionHandle(),
                "Left",
                TVector<std::pair<TInfoUnit, TInfoUnit>>{
                    {TInfoUnit("a.k"), TInfoUnit("b.k")},
                },
                TVector<TExpression>{});
            SetOutputType(ctx, *join, {
                {"a.k", NUdf::EDataSlot::Int32},
                {"a.text", NUdf::EDataSlot::String},
                {"b.k", NUdf::EDataSlot::Int32},
                {"b.text", NUdf::EDataSlot::String, true},
            });
            auto concat = MakeIntrusive<TOpMap>(
                join,
                TPositionHandle(),
                TVector<TMapElement>{
                    TMapElement(
                        TInfoUnit("left_result"),
                        TExpression(
                            StringConcat(
                                ctx,
                                StringLiteral(ctx, "left"),
                                NonNullStoredString(ctx, "a.text")),
                            &ctx.ExprCtx,
                            &ctx.ExpressionProps)),
                    TMapElement(
                        TInfoUnit("right_result"),
                        TExpression(
                            StringConcat(
                                ctx,
                                StringLiteral(ctx, "right"),
                                CoalescedStoredString(ctx, "b.text")),
                            &ctx.ExprCtx,
                            &ctx.ExpressionProps)),
                });
            TOpRoot root(
                concat,
                TPositionHandle(),
                {"left_result", "right_result"});
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        }

        {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/UnionLeft", {{"text", "String", true}});
            AddTable(ctx, "/Root/UnionRight", {{"text", "String", true}});
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/UnionLeft"),
                "u",
                {"text"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/UnionRight"),
                "u",
                {"text"});
            SetOutputType(ctx, *left, {{"u.text", NUdf::EDataSlot::String}});
            SetOutputType(ctx, *right, {{"u.text", NUdf::EDataSlot::String}});
            auto unionAll = MakeIntrusive<TOpUnionAll>(
                left,
                right,
                TPositionHandle(),
                TVector<TInfoUnit>{TInfoUnit("u.text")});
            SetOutputType(ctx, *unionAll, {
                {"u.text", NUdf::EDataSlot::String},
            });
            auto concat = MakeComputedMap(
                ctx,
                unionAll,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "union"),
                    NonNullStoredString(ctx, "u.text")));
            TOpRoot root(concat, TPositionHandle(), {"result"});
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        }

        for (const TStringBuf joinKind : {TStringBuf("LeftSemi"), TStringBuf("LeftOnly")}) {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/SemiLeft", {{"k", "Int32", true}});
            AddTable(ctx, "/Root/SemiRight", {
                {"k", "Int32", true},
                {"text", "String", true},
            });
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/SemiLeft"),
                "a",
                {"k"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/SemiRight"),
                "b",
                {"k", "text"});
            SetOutputType(ctx, *left, {{"a.k", NUdf::EDataSlot::Int32}});
            SetOutputType(ctx, *right, {
                {"b.k", NUdf::EDataSlot::Int32},
                {"b.text", NUdf::EDataSlot::String},
            });
            auto join = MakeIntrusive<TOpJoin>(
                left,
                right,
                TPositionHandle(),
                TString(joinKind),
                TVector<std::pair<TInfoUnit, TInfoUnit>>{
                    {TInfoUnit("a.k"), TInfoUnit("b.k")},
                },
                TVector<TExpression>{});
            SetOutputType(ctx, *join, {{"a.k", NUdf::EDataSlot::Int32}});
            auto concat = MakeComputedMap(
                ctx,
                join,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "dropped"),
                    NonNullStoredString(ctx, "b.text")));
            TOpRoot root(concat, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "not visible at the Map input");
        }

        {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/UnionNonNullLeft", {{"text", "String", true}});
            AddTable(ctx, "/Root/UnionNullableRight", {{"text", "String", false}});
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/UnionNonNullLeft"),
                "u",
                {"text"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/UnionNullableRight"),
                "u",
                {"text"});
            // Stale equal annotations must not wash out the catalog nullability.
            SetOutputType(ctx, *left, {{"u.text", NUdf::EDataSlot::String}});
            SetOutputType(ctx, *right, {{"u.text", NUdf::EDataSlot::String}});
            auto unionAll = MakeIntrusive<TOpUnionAll>(
                left,
                right,
                TPositionHandle(),
                TVector<TInfoUnit>{TInfoUnit("u.text")});
            SetOutputType(ctx, *unionAll, {
                {"u.text", NUdf::EDataSlot::String},
            });
            auto concat = MakeComputedMap(
                ctx,
                unionAll,
                "result",
                StringConcat(
                    ctx,
                    StringLiteral(ctx, "union"),
                    NonNullStoredString(ctx, "u.text")));
            TOpRoot root(concat, TPositionHandle(), {"result"});
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "storage provenance is nullable but the expression is non-null");
        }
    }

    Y_UNIT_TEST(StoredStringConcatJoinProvenanceMatchesEveryJoinKind) {
        struct TJoinCase {
            TStringBuf Kind;
            bool KeepLeft;
            bool KeepRight;
            bool NullableLeft;
            bool NullableRight;
        };
        const TJoinCase cases[] = {
            {"Cross",      true,  true,  false, false},
            {"Inner",      true,  true,  false, false},
            {"Left",       true,  true,  false, true},
            {"Right",      true,  true,  true,  false},
            {"Full",       true,  true,  true,  true},
            {"Exclusion",  true,  true,  true,  true},
            {"LeftSemi",   true,  false, false, false},
            {"LeftOnly",   true,  false, false, false},
            {"RightSemi",  false, true,  false, false},
            {"RightOnly",  false, true,  false, false},
        };

        for (const auto& test : cases) {
            for (const bool inspectLeft : {true, false}) {
                TExportTestContext ctx;
                AddTable(ctx, "/Root/JoinLeft", {
                    {"k", "Int32", true},
                    {"text", "String", true},
                });
                AddTable(ctx, "/Root/JoinRight", {
                    {"k", "Int32", true},
                    {"text", "String", true},
                });
                auto left = MakeRead(
                    ctx,
                    ctx.Tables->ExistingTable("ut", "/Root/JoinLeft"),
                    "a",
                    {"k", "text"});
                auto right = MakeRead(
                    ctx,
                    ctx.Tables->ExistingTable("ut", "/Root/JoinRight"),
                    "b",
                    {"k", "text"});
                SetOutputType(ctx, *left, {
                    {"a.k", NUdf::EDataSlot::Int32},
                    {"a.text", NUdf::EDataSlot::String},
                });
                SetOutputType(ctx, *right, {
                    {"b.k", NUdf::EDataSlot::Int32},
                    {"b.text", NUdf::EDataSlot::String},
                });

                TVector<std::pair<TInfoUnit, TInfoUnit>> keys;
                if (test.Kind != "Cross") {
                    keys.emplace_back(TInfoUnit("a.k"), TInfoUnit("b.k"));
                }
                auto join = MakeIntrusive<TOpJoin>(
                    left,
                    right,
                    TPositionHandle(),
                    TString(test.Kind),
                    std::move(keys),
                    TVector<TExpression>{});

                TVector<TOutputTypeSpec> outputs;
                const auto appendSide = [&](TStringBuf alias, bool keep, bool nullable) {
                    if (keep) {
                        outputs.push_back({
                            TStringBuilder() << alias << ".k",
                            NUdf::EDataSlot::Int32,
                            nullable});
                        outputs.push_back({
                            TStringBuilder() << alias << ".text",
                            NUdf::EDataSlot::String,
                            nullable});
                    }
                };
                appendSide("a", test.KeepLeft, test.NullableLeft);
                appendSide("b", test.KeepRight, test.NullableRight);
                SetOutputType(ctx, *join, outputs);

                const bool keep = inspectLeft ? test.KeepLeft : test.KeepRight;
                const bool nullable = inspectLeft
                    ? test.NullableLeft
                    : test.NullableRight;
                const TStringBuf column = inspectLeft ? "a.text" : "b.text";
                auto member = nullable
                    ? CoalescedStoredString(ctx, column)
                    : NonNullStoredString(ctx, column);
                auto concat = MakeComputedMap(
                    ctx,
                    join,
                    "result",
                    StringConcat(
                        ctx,
                        StringLiteral(ctx, "prefix"),
                        std::move(member)));
                TOpRoot root(concat, TPositionHandle(), {"result"});
                const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
                const TString label = TStringBuilder()
                    << test.Kind << '/' << (inspectLeft ? "left" : "right");
                if (keep) {
                    UNIT_ASSERT_C(result.IsSupported(),
                        label << ": " << result.UnsupportedReason);
                } else {
                    UNIT_ASSERT_C(!result.IsSupported(), label);
                    UNIT_ASSERT_STRING_CONTAINS(
                        result.UnsupportedReason,
                        "not visible at the Map input");
                }
            }
        }
    }

    Y_UNIT_TEST(StageGraphOutputIgnoresRandomRuntimeGuids) {
        UNIT_ASSERT_VALUES_EQUAL(
            ExportDeterministicStageGraph(),
            ExportDeterministicStageGraph());
    }

    Y_UNIT_TEST(ExactScalarAuditEnforcesExpandedNodeBoundary) {
        {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                WideBooleanAnd(ctx, 1024));
            UNIT_ASSERT_VALUES_EQUAL(expression["args"].GetArraySafe().size(), 1023);
        }
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                WideBooleanAnd(ctx, 1025));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Exact scalar expression exceeds the node audit limit");
        }
        {
            TExportTestContext ctx;
            auto shared = WideBooleanAnd(ctx, 512);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(ctx, "And", {shared, shared}, boolType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Exact scalar expression exceeds the node audit limit");
        }
        {
            TExportTestContext ctx;
            const auto pos = TPositionHandle();
            auto argument = ctx.ExprCtx.NewArgument(pos, "row");
            auto expression = ctx.ExprCtx.NewLambda(
                pos,
                ctx.ExprCtx.NewArguments(pos, {argument}),
                ExponentialSharedAnd(ctx, 32));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TExpression(
                    std::move(expression),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Exact scalar expression exceeds the node audit limit");
        }
    }

    Y_UNIT_TEST(MapSubplanDiscoveryDeduplicatesSharedExpressionDag) {
        TExportTestContext ctx;
        const auto pos = TPositionHandle();
        auto argument = ctx.ExprCtx.NewArgument(pos, "row");
        auto expression = ctx.ExprCtx.NewLambda(
            pos,
            ctx.ExprCtx.NewArguments(pos, {argument}),
            ExponentialSharedAnd(ctx, 32));
        auto map = MakeIntrusive<TOpMap>(
            MakeIntrusive<TOpEmptySource>(TPositionHandle()),
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                TExpression(
                    std::move(expression),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps))});
        TOpRoot root(map, pos, {"result"});

        UNIT_ASSERT(map->GetSubplanIUs(root.PlanProps).empty());
    }

    Y_UNIT_TEST(ExactScalarAuditEnforcesDepthBoundary) {
        {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                DeepBooleanNot(ctx, 128));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "not");
        }
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                DeepBooleanNot(ctx, 129));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Exact scalar expression exceeds the depth audit limit");
        }
    }

    Y_UNIT_TEST(ExactScalarAuditResetsForSeparateProjectionRoots) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/BudgetRoots", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("first"),
                    TExpression(
                        WideBooleanAnd(ctx, 1024),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
                TMapElement(
                    TInfoUnit("second"),
                    TExpression(
                        WideBooleanAnd(ctx, 1024),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
            });
        TOpRoot root(map, pos, {"first", "second"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& columns = FindNode(snapshot, "project")["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["expression"]["args"].GetArraySafe().size(), 1023);
        UNIT_ASSERT_VALUES_EQUAL(columns[2]["expression"]["args"].GetArraySafe().size(), 1023);
    }

    Y_UNIT_TEST(ExactScalarAuditCoversAssembledOlapPredicate) {
        const auto exportPredicate = [](bool exceedLimit) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/OlapBudget", {{"k", "Int32", true}});
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            // Each equality contributes three normalized nodes.  The two
            // filter roots contain 512 and 511/512 nodes; their assembled AND
            // therefore contains exactly 1024/1025 nodes.
            read->OlapFilterLambda = MakeOlapFilterChain(
                ctx,
                {
                    WideOlapAnd(ctx, 170, 1),
                    WideOlapAnd(ctx, 170, exceedLimit ? 1 : 0),
                });
            TOpRoot root(read, TPositionHandle(), {"a.k"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        const auto accepted = exportPredicate(false);
        UNIT_ASSERT_C(accepted.IsSupported(), accepted.UnsupportedReason);
        const auto rejected = exportPredicate(true);
        UNIT_ASSERT(!rejected.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            rejected.UnsupportedReason,
            "Exact scalar expression exceeds the node audit limit");
    }

    Y_UNIT_TEST(ExactScalarAuditEnforcesOlapDepthBoundary) {
        const auto exportPredicate = [](size_t depth) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/OlapDepth", {{"k", "Int32", true}});
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            read->OlapFilterLambda = MakeOlapFilterProcess(
                ctx,
                DeepOlapNot(ctx, depth));
            TOpRoot root(read, TPositionHandle(), {"a.k"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        const auto accepted = exportPredicate(128);
        UNIT_ASSERT_C(accepted.IsSupported(), accepted.UnsupportedReason);
        const auto rejected = exportPredicate(129);
        UNIT_ASSERT(!rejected.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            rejected.UnsupportedReason,
            "Exact scalar expression exceeds the depth audit limit");
    }

    Y_UNIT_TEST(ExactScalarAuditCoversSynthesizedJoinPredicate) {
        const auto exportJoin = [](size_t keyCount) {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/JoinBudgetLeft", {{"k", "Int32", true}});
            AddTable(ctx, "/Root/JoinBudgetRight", {{"k", "Int32", true}});
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinBudgetLeft"),
                "a",
                {"k"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinBudgetRight"),
                "b",
                {"k"});
            TVector<std::pair<TInfoUnit, TInfoUnit>> keys;
            keys.reserve(keyCount);
            for (size_t index = 0; index < keyCount; ++index) {
                keys.emplace_back(TInfoUnit("a.k"), TInfoUnit("b.k"));
            }
            const auto pos = TPositionHandle();
            auto join = MakeIntrusive<TOpJoin>(
                left,
                right,
                pos,
                "Inner",
                std::move(keys));
            TOpRoot root(join, pos, {"a.k", "b.k"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        // 340 three-node keys, their effective AND, and literal-true residual
        // consume 1022 nodes. One more key crosses the 1024-node limit.
        const auto accepted = exportJoin(340);
        UNIT_ASSERT_C(accepted.IsSupported(), accepted.UnsupportedReason);
        const auto rejected = exportJoin(341);
        UNIT_ASSERT(!rejected.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            rejected.UnsupportedReason,
            "Exact scalar expression exceeds the node audit limit");
    }

    Y_UNIT_TEST(ExactScalarAuditCoversImplicitJoinConjunctionDepth) {
        const auto exportJoin = [](bool withKey) {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/JoinDepthLeft", {{"k", "Int32", true}});
            AddTable(ctx, "/Root/JoinDepthRight", {{"k", "Int32", true}});
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinDepthLeft"),
                "a",
                {"k"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/JoinDepthRight"),
                "b",
                {"k"});
            TVector<std::pair<TInfoUnit, TInfoUnit>> keys;
            if (withKey) {
                keys.emplace_back(TInfoUnit("a.k"), TInfoUnit("b.k"));
            }
            const auto pos = TPositionHandle();
            auto join = MakeIntrusive<TOpJoin>(
                left,
                right,
                pos,
                "Inner",
                std::move(keys),
                TVector<TExpression>{TExpression(
                    DeepBooleanNot(ctx, 128),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps)});
            TOpRoot root(join, pos, {"a.k", "b.k"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        const auto standalone = exportJoin(false);
        UNIT_ASSERT_C(standalone.IsSupported(), standalone.UnsupportedReason);

        // The residual fits exactly at the standalone depth boundary. A join
        // key adds an implicit effective conjunction above it.
        const auto combined = exportJoin(true);
        UNIT_ASSERT(!combined.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            combined.UnsupportedReason,
            "Exact scalar expression exceeds the depth audit limit");
    }

    Y_UNIT_TEST(ExportsSchemaScanAndExactMapProjection) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"flag", "Bool", false},
            {"payload", "Utf8", false},
        }, {"k"});
        auto read = MakeRead(ctx, table, "a", {"k", "flag", "payload"});

        const auto pos = TPositionHandle();
        auto map = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{
            TMapElement(TInfoUnit("out.k"), TInfoUnit("a.k"), pos, &ctx.ExprCtx, &ctx.ExpressionProps),
            TMapElement(
                TInfoUnit("out.flag_copy"),
                MakeColumnAccess(TInfoUnit("a.flag"), pos, &ctx.ExprCtx, &ctx.ExpressionProps)),
        });
        TOpRoot root(map, pos, {"out.flag_copy", "out.k", "a.payload"});

        const auto exported = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        const auto snapshot = ParseSupported(exported);
        UNIT_ASSERT_VALUES_EQUAL(snapshot["format"].GetStringSafe(), "ydb-rbo-semantic-snapshot");
        UNIT_ASSERT_VALUES_EQUAL(snapshot["version"].GetIntegerSafe(), 1);
        UNIT_ASSERT(snapshot["stage_graph"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(snapshot["plan"].GetMapSafe().size(), 4);
        UNIT_ASSERT(snapshot["plan"]["subplans"].GetArraySafe().empty());

        const auto& tables = snapshot["schema"]["tables"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(tables[0]["name"].GetStringSafe(), "/Root/A");
        const auto& schemaColumns = tables[0]["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[0]["name"].GetStringSafe(), "k");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[0]["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[0]["nullable"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[1]["name"].GetStringSafe(), "flag");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[1]["type"].GetStringSafe(), "Bool");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[1]["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[2]["name"].GetStringSafe(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[2]["type"].GetStringSafe(), "Utf8");
        UNIT_ASSERT_VALUES_EQUAL(schemaColumns[2]["nullable"].GetBooleanSafe(), true);

        const auto& keys = tables[0]["unique_keys"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Strings(keys[0]["columns"]), TVector<TString>{"k"});
        UNIT_ASSERT_VALUES_EQUAL(keys[0]["nulls_distinct"].GetBooleanSafe(), false);

        const auto& scan = FindNode(snapshot, "scan");
        UNIT_ASSERT_VALUES_EQUAL(
            scan["table"].GetStringSafe(),
            tables[0]["name"].GetStringSafe());
        const auto& scanColumns = scan["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(scanColumns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[0]["source"].GetStringSafe(), "k");
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[0]["output"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[1]["source"].GetStringSafe(), "flag");
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[1]["output"].GetStringSafe(), "a.flag");
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[2]["source"].GetStringSafe(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(scanColumns[2]["output"].GetStringSafe(), "a.payload");
        UNIT_ASSERT(scan["pushed_limit"].IsNull());

        const auto& project = FindNode(snapshot, "project");
        UNIT_ASSERT_VALUES_EQUAL(project["ordered"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(
            ProjectionOutputs(project),
            (TVector<TString>{"a.flag", "a.payload", "out.k", "out.flag_copy"}));
        const auto& projections = project["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(projections[0]["expression"]["column"].GetStringSafe(), "a.flag");
        UNIT_ASSERT_VALUES_EQUAL(projections[1]["expression"]["column"].GetStringSafe(), "a.payload");
        UNIT_ASSERT_VALUES_EQUAL(projections[2]["expression"]["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(projections[3]["expression"]["column"].GetStringSafe(), "a.flag");
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(snapshot["plan"]["output"]),
            (TVector<TString>{"out.flag_copy", "out.k", "a.payload"}));
    }

    Y_UNIT_TEST(ExportsOrderedMapProjection) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                TInfoUnit("a.k"),
                pos,
                &ctx.ExprCtx,
                &ctx.ExpressionProps)},
            true);
        TOpRoot root(map, pos, {"result"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& project = FindNode(snapshot, "project");
        UNIT_ASSERT_VALUES_EQUAL(project["ordered"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(ProjectionOutputs(project), TVector<TString>{"result"});
    }

    Y_UNIT_TEST(ExportsUnionAllOrdering) {
        for (const bool ordered : {false, true}) {
            TExportTestContext ctx;
            const auto& table = AddTable(
                ctx,
                "/Root/A",
                {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            auto unionAll = MakeIntrusive<TOpUnionAll>(
                read,
                read,
                TPositionHandle(),
                TVector<TInfoUnit>{TInfoUnit("a.k")},
                ordered);
            TOpRoot root(unionAll, TPositionHandle(), {"a.k"});

            const auto snapshot =
                ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& node = FindNode(snapshot, "union_all");
            UNIT_ASSERT_VALUES_EQUAL(node.GetMapSafe().size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(
                node["ordered"].GetBooleanSafe(),
                ordered);
        }
    }

    Y_UNIT_TEST(ExportsPassiveDateAndExactDecimalCatalogTypes) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/Passive", {
            {"d", "Date", false},
            {"n", "Decimal(5,2)", false},
        });
        auto read = MakeRead(ctx, table, "p", {"d", "n"});
        TOpRoot root(read, TPositionHandle(), {"p.d", "p.n"});

        const auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog));
        const auto& columns = snapshot["schema"]["tables"].GetArraySafe()[0]
            ["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["type"].GetStringSafe(), "Date");
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["type"].GetStringSafe(), "Decimal(5,2)");

        for (const TString type : {
            "Decimal",
            "Decimal(0,0)",
            "Decimal(05,2)",
            "Decimal(5,02)",
            "Decimal(5,6)",
            "Decimal(36,2)",
            "Decimal(5, 2)",
        }) {
            auto malformed = catalog.Catalog;
            malformed.Tables[0].Columns[1].Type = type;
            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx, malformed);
            UNIT_ASSERT_C(!result.IsSupported(), type);
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Invalid catalog column");
        }
    }

    Y_UNIT_TEST(ExportsExactNumericDateLiteralAndRejectsInvalidForms) {
        for (const ui16 day : {ui16{0}, ui16{NUdf::MAX_DATE - 1}}) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedLiteral(
                    ctx,
                    "Date",
                    ToString(day),
                    ScalarType(ctx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(expression["value"].GetUIntegerSafe(), day);
        }

        for (const TString value : {"-1", "49673", "65535", "not-a-day"}) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedLiteral(
                    ctx,
                    "Date",
                    value,
                    ScalarType(ctx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT_C(!result.IsSupported(), value);
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Date literal");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Date",
                    {},
                    ScalarType(ctx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported literal callable Date");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedLiteral(
                    ctx,
                    "Date",
                    "0",
                    ScalarType(ctx, NUdf::EDataSlot::Uint16)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Date literal type annotation does not match");
        }
    }

    Y_UNIT_TEST(FoldsDirectTextLiteralDateSafeCastsExactly) {
        struct TCase {
            TString Source;
            TString Date;
        };
        const TVector<TCase> cases = {
            {"String", "1970-01-01"},
            // TPC-DS q5/q77 lower-bound spelling.
            {"String", "1998-08-04"},
            {"Utf8", "1998-08-04"},
            {"Utf8", "2105-12-31"},
        };

        for (const auto& test : cases) {
            const auto parsed = NKikimr::NMiniKQL::ValueFromString(
                NUdf::EDataSlot::Date,
                NUdf::TStringRef(test.Date.data(), test.Date.size()));
            UNIT_ASSERT_C(parsed.HasValue(), test.Date);
            const ui16 expected = parsed.Get<ui16>();

            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedTextLiteralDateCast(
                    ctx,
                    "SafeCast",
                    test.Source,
                    test.Date));
            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["value"].GetUIntegerSafe(),
                expected);

            TExportTestContext directCtx;
            const auto direct = ExportMapExpression(
                directCtx,
                "a",
                TypedLiteral(
                    directCtx,
                    "Date",
                    ToString(expected),
                    ScalarType(directCtx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT_VALUES_EQUAL(
                NJson::WriteJson(expression, false, true),
                NJson::WriteJson(direct, false, true));
        }

        const TString q5LowerBound = "1998-08-04";
        const auto parsed = NKikimr::NMiniKQL::ValueFromString(
            NUdf::EDataSlot::Date,
            NUdf::TStringRef(q5LowerBound.data(), q5LowerBound.size()));
        UNIT_ASSERT(parsed.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(parsed.Get<ui16>(), 10'442);
    }

    Y_UNIT_TEST(InvalidDirectTextLiteralDateSafeCastsBecomeTypedNull) {
        const TVector<TString> invalid = {
            "",
            "1969-12-31",
            "1998-02-30",
            "1998-13-04",
            "1998-08-04 ",
            "2106-01-01",
            "not-a-date",
        };
        for (const TString source : {"String", "Utf8"}) {
            for (const auto& date : invalid) {
                const auto parsed = NKikimr::NMiniKQL::ValueFromString(
                    NUdf::EDataSlot::Date,
                    NUdf::TStringRef(date.data(), date.size()));
                UNIT_ASSERT_C(!parsed.HasValue(), date);

                TExportTestContext ctx;
                const auto expression = ExportMapExpression(
                    ctx,
                    "a",
                    TypedTextLiteralDateCast(
                        ctx,
                        "SafeCast",
                        source,
                        date));
                UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["kind"].GetStringSafe(),
                    "null");
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["type"].GetStringSafe(),
                    "Date");
            }
        }
    }

    Y_UNIT_TEST(DirectTextLiteralDateSafeCastGateFailsClosed) {
        auto checkUnsupported = [](
            TStringBuf label,
            auto&& makeExpression)
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                makeExpression(ctx));
            UNIT_ASSERT_C(
                !result.IsSupported(),
                TStringBuilder() << label << " unexpectedly exported " << result.Json);
        };

        checkUnsupported("dynamic source", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(
                ctx,
                NUdf::EDataSlot::String);
            const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* optionalDateType = ScalarType(
                ctx,
                NUdf::EDataSlot::Date,
                true);
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedMember(ctx, "a.x", sourceType),
                    OptionalDataTypeDescriptor(
                        ctx,
                        "Date",
                        dateType,
                        optionalDateType),
                },
                optionalDateType);
        });
        checkUnsupported("nullable source", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDateCast(
                ctx,
                "SafeCast",
                "Utf8",
                "1998-08-04");
            expression->Child(0)->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Utf8, true));
            return expression;
        });
        checkUnsupported("source annotation mismatch", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDateCast(
                ctx,
                "SafeCast",
                "String",
                "1998-08-04");
            expression->Child(0)->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Utf8));
            return expression;
        });
        checkUnsupported("StrictCast", [](TExportTestContext& ctx) {
            return TypedTextLiteralDateCast(
                ctx,
                "StrictCast",
                "Utf8",
                "1998-08-04");
        });
        checkUnsupported("non-optional result", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDateCast(
                ctx,
                "SafeCast",
                "String",
                "1998-08-04");
            expression->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Date));
            return expression;
        });
        checkUnsupported("non-optional target", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(
                ctx,
                NUdf::EDataSlot::String);
            const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* optionalDateType = ScalarType(
                ctx,
                NUdf::EDataSlot::Date,
                true);
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedLiteral(
                        ctx,
                        "String",
                        "1998-08-04",
                        sourceType),
                    DataTypeDescriptor(ctx, "Date", dateType),
                },
                optionalDateType);
        });
        checkUnsupported("outer target annotation mismatch", [](
            TExportTestContext& ctx)
        {
            auto expression = TypedTextLiteralDateCast(
                ctx,
                "SafeCast",
                "String",
                "1998-08-04");
            expression->Child(1)->SetTypeAnn(
                ctx.ExprCtx.MakeType<TTypeExprType>(
                    ScalarType(ctx, NUdf::EDataSlot::Utf8, true)));
            return expression;
        });
        checkUnsupported("nested target annotation mismatch", [](
            TExportTestContext& ctx)
        {
            auto expression = TypedTextLiteralDateCast(
                ctx,
                "SafeCast",
                "Utf8",
                "1998-08-04");
            expression->Child(1)->Child(0)->SetTypeAnn(
                ctx.ExprCtx.MakeType<TTypeExprType>(
                    ScalarType(ctx, NUdf::EDataSlot::Utf8)));
            return expression;
        });
        checkUnsupported("malformed source literal", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(
                ctx,
                NUdf::EDataSlot::String);
            const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* optionalDateType = ScalarType(
                ctx,
                NUdf::EDataSlot::Date,
                true);
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedCallable(ctx, "String", {}, sourceType),
                    OptionalDataTypeDescriptor(
                        ctx,
                        "Date",
                        dateType,
                        optionalDateType),
                },
                optionalDateType);
        });
    }

    Y_UNIT_TEST(DateUnwrapSafeCastAndJustNormalizeByteIdentically) {
        TString normalized;
        for (const auto shape : {
            EDateUnwrapShape::ExactSafeCast,
            EDateUnwrapShape::ExactJust,
        }) {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(
                ExportDateUnwrapExpression(ctx, shape));
            const auto& expression = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];

            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "if_present");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["optional"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["optional"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["present"]["kind"].GetStringSafe(),
                "bound");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["present"]["depth"].GetUIntegerSafe(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["missing"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["missing"]["type"].GetStringSafe(),
                "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["missing"]["value"].GetUIntegerSafe(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                "Date");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());

            const TString encoded = NJson::WriteJson(
                expression,
                false,
                true);
            if (normalized.empty()) {
                normalized = encoded;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(encoded, normalized);
            }
        }
    }

    Y_UNIT_TEST(DateUnwrapExactGateFailsClosed) {
        struct TCase {
            EDateUnwrapShape Shape;
            TStringBuf Label;
            TStringBuf Reason;
        };
        const TVector<TCase> cases = {
            {
                EDateUnwrapShape::SafeCastNonzero,
                "SafeCast nonzero",
                "source must be Int32 zero",
            },
            {
                EDateUnwrapShape::JustNonzero,
                "Just nonzero",
                "Just fallback must contain Date zero",
            },
            {
                EDateUnwrapShape::ConvertFallback,
                "Convert fallback",
                "fallback must be Just(Date(0)) or SafeCast",
            },
            {
                EDateUnwrapShape::WrongRootType,
                "wrong Unwrap type",
                "non-null Date result",
            },
            {
                EDateUnwrapShape::WrongCoalesceType,
                "wrong Coalesce type",
                "binary Optional<Date> Coalesce",
            },
            {
                EDateUnwrapShape::WrongMemberType,
                "wrong member type",
                "direct visible Optional<Date>",
            },
            {
                EDateUnwrapShape::WrongSafeCastSourceType,
                "wrong SafeCast source type",
                "not an exact Int32 literal",
            },
            {
                EDateUnwrapShape::WrongSafeCastTargetType,
                "wrong SafeCast target type",
                "target annotation disagrees",
            },
            {
                EDateUnwrapShape::ReversedCoalesce,
                "reversed Coalesce",
                "direct visible Optional<Date>",
            },
            {
                EDateUnwrapShape::InvisibleMember,
                "invisible member",
                "direct visible Optional<Date>",
            },
            {
                EDateUnwrapShape::StringUnwrap,
                "generic String Unwrap",
                "non-null Date result",
            },
            {
                EDateUnwrapShape::UnsafeRoot,
                "unsafe root",
                "side-effecting or CSE-unsafe",
            },
            {
                EDateUnwrapShape::UnsafeSubtree,
                "unsafe subtree",
                "unordered children",
            },
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto result = ExportDateUnwrapExpression(
                ctx,
                test.Shape);
            UNIT_ASSERT_C(
                !result.IsSupported(),
                TStringBuilder()
                    << test.Label << " unexpectedly exported "
                    << result.Json);
            UNIT_ASSERT_STRING_CONTAINS_C(
                result.UnsupportedReason,
                test.Reason,
                test.Label);
        }
    }

    Y_UNIT_TEST(FoldsDirectNumericDateAndIntervalLiteralsExactly) {
        constexpr i64 MicrosPerDay = 86'400'000'000LL;
        static_assert(
            static_cast<ui64>(MicrosPerDay) * NUdf::MAX_DATE ==
            NUdf::MAX_TIMESTAMP);

        struct TCase {
            TString Operation;
            TString Date;
            TString Interval;
            ui16 Expected;
        };
        const TVector<TCase> cases = {
            // Exact TPC-H q1 initial-plan constant: 1998-12-01 - 90 days.
            {"-", "10561", "7776000000000", 10471},
            {"+", "10471", "7776000000000", 10561},

            // MiniKQL truncates only after signed microsecond arithmetic.
            {"+", "10561", "1", 10561},
            {"-", "10561", "1", 10560},
            {"+", "10561", "-1", 10560},
            {"-", "10561", "-1", 10561},

            // Interval's open range accepts both immediately adjacent values.
            {"+", "0", ToString(
                static_cast<i64>(NUdf::MAX_TIMESTAMP) - 1), 49672},
            {"-", "0", ToString(
                -static_cast<i64>(NUdf::MAX_TIMESTAMP) + 1), 49672},
        };

        for (const auto& testCase : cases) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedDirectDateInterval(
                    ctx,
                    testCase.Operation,
                    testCase.Date,
                    testCase.Interval));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["value"].GetUIntegerSafe(), testCase.Expected);
        }
    }

    Y_UNIT_TEST(DirectNumericDateAndIntervalOverflowBecomesTypedNull) {
        constexpr i64 MicrosPerDay = 86'400'000'000LL;
        struct TCase {
            TString Operation;
            TString Date;
            TString Interval;
        };
        const TVector<TCase> cases = {
            {"+", "0", "-1"},
            {"-", "0", "1"},
            {"+", "49672", ToString(MicrosPerDay)},
            {"-", "49672", ToString(-MicrosPerDay)},
        };

        for (const auto& testCase : cases) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedDirectDateInterval(
                    ctx,
                    testCase.Operation,
                    testCase.Date,
                    testCase.Interval));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(), "null");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(), "Date");
        }
    }

    Y_UNIT_TEST(DirectNumericDateAndIntervalGateFailsClosed) {
        const TVector<EDirectDateIntervalShape> malformedShapes = {
            EDirectDateIntervalShape::NonOptionalResult,
            EDirectDateIntervalShape::WrongResultType,
            EDirectDateIntervalShape::WrongArithmeticCallable,
            EDirectDateIntervalShape::UnaryArithmetic,
            EDirectDateIntervalShape::TernaryArithmetic,
            EDirectDateIntervalShape::WrongDateCallable,
            EDirectDateIntervalShape::EmptyDate,
            EDirectDateIntervalShape::BinaryDate,
            EDirectDateIntervalShape::WrongDateType,
            EDirectDateIntervalShape::NullableDate,
            EDirectDateIntervalShape::WrongIntervalCallable,
            EDirectDateIntervalShape::EmptyInterval,
            EDirectDateIntervalShape::BinaryInterval,
            EDirectDateIntervalShape::WrongIntervalType,
            EDirectDateIntervalShape::NullableInterval,
        };
        for (const auto shape : malformedShapes) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDirectDateInterval(
                    ctx, "-", "10561", "7776000000000", shape));
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<ui32>(shape));
        }

        for (const TString date : {"-1", "49673", "not-a-date"}) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDirectDateInterval(
                    ctx, "-", date, "7776000000000"));
            UNIT_ASSERT_C(!result.IsSupported(), date);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason, "Date literal");
        }

        struct TInvalidInterval {
            TString Value;
            TString Reason;
        };
        const TVector<TInvalidInterval> invalidIntervals = {
            {"not-an-interval", "Invalid Interval literal"},
            {"1.0", "Invalid Interval literal"},
            {"9223372036854775808", "Invalid Interval literal"},
            {ToString(static_cast<i64>(NUdf::MAX_TIMESTAMP)),
                "Interval literal is outside"},
            {ToString(-static_cast<i64>(NUdf::MAX_TIMESTAMP)),
                "Interval literal is outside"},
        };
        for (const auto& testCase : invalidIntervals) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDirectDateInterval(
                    ctx, "-", "10561", testCase.Value));
            UNIT_ASSERT_C(!result.IsSupported(), testCase.Value);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason, testCase.Reason);
        }
    }

    Y_UNIT_TEST(ExportsExactNullableDateYearMap) {
        TExportTestContext ctx;
        const auto snapshot = ParseSupported(ExportNullableDateYear(ctx));
        const auto& expression = FindNode(snapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];

        UNIT_ASSERT_VALUES_EQUAL(
            expression["kind"].GetStringSafe(), "if_present");
        UNIT_ASSERT_VALUES_EQUAL(
            expression["type"].GetStringSafe(), "Uint16");
        UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            expression["optional"]["kind"].GetStringSafe(), "column");
        UNIT_ASSERT_VALUES_EQUAL(
            expression["optional"]["column"].GetStringSafe(), "a.d");

        const auto& present = expression["present"];
        UNIT_ASSERT_VALUES_EQUAL(present["kind"].GetStringSafe(), "if");
        UNIT_ASSERT_VALUES_EQUAL(present["type"].GetStringSafe(), "Uint16");
        UNIT_ASSERT(present["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            present["condition"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(
            present["condition"]["type"].GetStringSafe(), "Bool");
        UNIT_ASSERT(present["condition"]["value"].GetBooleanSafe());

        const auto& year = present["then"];
        UNIT_ASSERT_VALUES_EQUAL(year["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(
            year["fingerprint"].GetStringSafe(),
            "yql-datetime-year-v1");
        UNIT_ASSERT_VALUES_EQUAL(year["type"].GetStringSafe(), "Uint16");
        UNIT_ASSERT(!year["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(year["args"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            year["args"][0]["kind"].GetStringSafe(), "bound");
        UNIT_ASSERT_VALUES_EQUAL(
            year["args"][0]["depth"].GetUIntegerSafe(), 0);

        for (const auto* missing : {
            &present["else"],
            &expression["missing"],
        }) {
            UNIT_ASSERT_VALUES_EQUAL(
                (*missing)["kind"].GetStringSafe(), "null");
            UNIT_ASSERT_VALUES_EQUAL(
                (*missing)["type"].GetStringSafe(), "Uint16");
        }
    }

    Y_UNIT_TEST(NullableDateYearMapGateFailsClosed) {
        const TVector<EDateTime2YearShape> malformedShapes = {
            EDateTime2YearShape::NonOptionalResult,
            EDateTime2YearShape::WrongResultType,
            EDateTime2YearShape::NonMemberSource,
            EDateTime2YearShape::InvisibleSource,
            EDateTime2YearShape::NonOptionalSource,
            EDateTime2YearShape::WrongSourceType,
            EDateTime2YearShape::WrongCastCallable,
            EDateTime2YearShape::WrongCastTarget,
            EDateTime2YearShape::NonOptionalCastResult,
            EDateTime2YearShape::NonUnaryLambda,
            EDateTime2YearShape::WrongLambdaType,
            EDateTime2YearShape::WrongLambdaBinder,
            EDateTime2YearShape::WrongSplitName,
            EDateTime2YearShape::WrongSplitUserType,
            EDateTime2YearShape::WrongSplitReturnDescriptor,
            EDateTime2YearShape::WrongSplitSettings,
            EDateTime2YearShape::WrongSplitFlags,
            EDateTime2YearShape::WrongGetYearName,
            EDateTime2YearShape::WrongGetYearUserType,
            EDateTime2YearShape::WrongGetYearReturnDescriptor,
            EDateTime2YearShape::WrongGetYearSettings,
            EDateTime2YearShape::WrongGetYearFlags,
        };
        for (const auto shape : malformedShapes) {
            TExportTestContext ctx;
            const auto result = ExportNullableDateYear(ctx, shape);
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<ui32>(shape));
        }
    }

    Y_UNIT_TEST(FoldsExactDateTime2CalendarShiftShapes) {
        struct TCase {
            TString Callable;
            TString Date;
            TString Shift;
            ui16 Expected;
        };
        const TVector<TCase> cases = {
            // TPC-H q5/q6/q12 and q14 ShiftYears constants.
            {"ShiftYears", "8766", "1", 9131},
            {"ShiftYears", "9374", "1", 9740},

            // TPC-H q10 ShiftMonths constant.
            {"ShiftMonths", "8674", "3", 8766},

            // Runtime calendar shifts clamp the day to the target month.
            {"ShiftYears", "11016", "1", 11381},
            {"ShiftMonths", "8796", "1", 8824},

            // Exercise C++'s negative remainder and year-decrement branch.
            {"ShiftMonths", "8796", "-1", 8765},
        };

        for (const auto& testCase : cases) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedDateTime2Shift(
                    ctx,
                    testCase.Callable,
                    testCase.Date,
                    testCase.Shift));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["value"].GetUIntegerSafe(), testCase.Expected);
        }
    }

    Y_UNIT_TEST(DateTime2CalendarShiftOutsideDateDomainBecomesTypedNull) {
        for (const auto& [date, shift] : {
            std::pair<TStringBuf, TStringBuf>{"49672", "1"},
            std::pair<TStringBuf, TStringBuf>{"0", "-1"},
        }) {
            for (const TString callable : {"ShiftYears", "ShiftMonths"}) {
                TExportTestContext ctx;
                const auto expression = ExportMapExpression(
                    ctx,
                    "a",
                    TypedDateTime2Shift(ctx, callable, date, shift));
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["kind"].GetStringSafe(), "null");
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["type"].GetStringSafe(), "Date");
            }
        }
    }

    Y_UNIT_TEST(DateTime2CalendarShiftGateFailsClosed) {
        const TVector<EDateTime2ShiftShape> malformedShapes = {
            EDateTime2ShiftShape::WrongSplitUserType,
            EDateTime2ShiftShape::WrongShiftUserType,
            EDateTime2ShiftShape::WrongMakeDateUserType,
            EDateTime2ShiftShape::WrongSplitReturnDescriptor,
            EDateTime2ShiftShape::WrongShiftReturnDescriptor,
            EDateTime2ShiftShape::WrongMakeDateArgumentDescriptor,
            EDateTime2ShiftShape::WrongSplitSettings,
            EDateTime2ShiftShape::WrongShiftSettings,
            EDateTime2ShiftShape::WrongMakeDateSettings,
            EDateTime2ShiftShape::WrongSplitFlags,
            EDateTime2ShiftShape::WrongShiftFlags,
            EDateTime2ShiftShape::WrongMakeDateFlags,
            EDateTime2ShiftShape::WrongLambdaBinder,
        };
        for (const auto shape : malformedShapes) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDateTime2Shift(
                    ctx, "ShiftYears", "8766", "1", shape));
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<ui32>(shape));
        }

        for (const auto& [callable, shift] : {
            std::pair<TStringBuf, TStringBuf>{"ShiftYears", "4096"},
            std::pair<TStringBuf, TStringBuf>{"ShiftYears", "-4096"},
            std::pair<TStringBuf, TStringBuf>{"ShiftMonths", "49152"},
            std::pair<TStringBuf, TStringBuf>{"ShiftMonths", "-49152"},
        }) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDateTime2Shift(ctx, callable, "8766", shift));
            UNIT_ASSERT_C(!result.IsSupported(), callable);
        }
    }

    Y_UNIT_TEST(FoldsExactDateAndIntervalLiteralsAgainstRuntimeParser) {
        struct TCase {
            TString Operation;
            TString SourceCallable;
            TString Date;
            TString Days;
            i32 SignedDays;
        };
        const TVector<TCase> cases = {
            {"-", "String", "1998-04-08", "30", -30},
            {"+", "String", "1998-04-08", "30", 30},
            {"+", "Utf8", "1970-01-01", "0", 0},
            {"+", "Utf8", "1998-04-08", "-30", -30},
            {"-", "Utf8", "1998-04-08", "-30", 30},
            {"+", "String", "1970-01-01", "49672", 49'672},
            {"+", "String", "2105-12-31", "-49672", -49'672},
            {"-", "String", "1970-01-01", "-49672", 49'672},
            {"-", "String", "2105-12-31", "49672", -49'672},
        };

        for (const auto& testCase : cases) {
            const auto parsed = NKikimr::NMiniKQL::ValueFromString(
                NUdf::EDataSlot::Date,
                NUdf::TStringRef(testCase.Date.data(), testCase.Date.size()));
            UNIT_ASSERT_C(parsed.HasValue(), testCase.Date);
            const i64 expected = static_cast<i64>(parsed.Get<ui16>()) +
                testCase.SignedDays;
            UNIT_ASSERT(expected >= 0 && expected < NUdf::MAX_DATE);

            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedConstantDateInterval(
                    ctx,
                    testCase.Operation,
                    testCase.SourceCallable,
                    testCase.Date,
                    testCase.Days));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["value"].GetUIntegerSafe(),
                static_cast<ui64>(expected));
        }
    }

    Y_UNIT_TEST(DateAndIntervalLiteralRuntimeFailuresBecomeTypedNull) {
        struct TCase {
            TString Operation;
            TString Date;
            TString Days;
        };
        const TVector<TCase> cases = {
            {"+", "1998-02-30", "30"},
            {"-", "1970-01-01", "1"},
            {"+", "2105-12-31", "1"},
            {"+", "1970-01-01", "-1"},
            {"-", "2105-12-31", "-1"},
        };

        for (const auto& testCase : cases) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedConstantDateInterval(
                    ctx,
                    testCase.Operation,
                    "String",
                    testCase.Date,
                    testCase.Days));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "null");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Date");
        }
    }

    Y_UNIT_TEST(DateAndIntervalLiteralGateFailsClosed) {
        const TVector<EDateIntervalShape> malformedShapes = {
            EDateIntervalShape::NonOptionalDateTarget,
            EDateIntervalShape::MismatchedDateTargetAnnotation,
            EDateIntervalShape::NonOptionalDateCastResult,
            EDateIntervalShape::NonOptionalArithmeticResult,
            EDateIntervalShape::WrongUdfName,
            EDateIntervalShape::NonVoidRunConfig,
            EDateIntervalShape::NonVoidUserType,
            EDateIntervalShape::NonEmptyTypeConfig,
            EDateIntervalShape::WrongCachedArgumentFlags,
            EDateIntervalShape::MismatchedCachedCallableAnnotation,
            EDateIntervalShape::WrongCachedReturnDescriptor,
            EDateIntervalShape::NonVoidCachedRunConfigType,
            EDateIntervalShape::NonEmptyFileAlias,
            EDateIntervalShape::WrongCallableAnnotation,
            EDateIntervalShape::ReversedUdfSettings,
            EDateIntervalShape::MissingUdfSetting,
            EDateIntervalShape::WrongApplyResult,
            EDateIntervalShape::WrongDaysType,
            EDateIntervalShape::NullableDays,
        };
        for (const auto shape : malformedShapes) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedConstantDateInterval(
                    ctx,
                    "+",
                    "String",
                    "1998-04-08",
                    "30",
                    shape));
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<ui32>(shape));
        }

        for (const TString days : {"49673", "-49673", "2147483648"}) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedConstantDateInterval(
                    ctx, "+", "String", "1998-04-08", days));
            UNIT_ASSERT_C(!result.IsSupported(), days);
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedConstantDateInterval(
                    ctx, "*", "String", "1998-04-08", "30"));
            UNIT_ASSERT(!result.IsSupported());
        }
    }

    Y_UNIT_TEST(ExportsAllIntegralDataComparisonPairs) {
        struct TIntegralCase {
            NUdf::EDataSlot Slot;
            TString Type;
        };
        const TVector<TIntegralCase> integralCases = {
            {NUdf::EDataSlot::Int8, "Int8"},
            {NUdf::EDataSlot::Int16, "Int16"},
            {NUdf::EDataSlot::Int32, "Int32"},
            {NUdf::EDataSlot::Int64, "Int64"},
            {NUdf::EDataSlot::Uint8, "Uint8"},
            {NUdf::EDataSlot::Uint16, "Uint16"},
            {NUdf::EDataSlot::Uint32, "Uint32"},
            {NUdf::EDataSlot::Uint64, "Uint64"},
        };
        struct TComparisonCase {
            TString Callable;
            TString Kind;
            bool Negated = false;
            bool NullSafe = false;
        };
        const TVector<TComparisonCase> comparisonCases = {
            {"==", "eq"},
            {"!=", "eq", true},
            {"IsNotDistinctFrom", "eq", false, true},
            {"<", "lt"},
            {"<=", "lte"},
            {">", "gt"},
            {">=", "gte"},
        };

        for (const auto& left : integralCases) {
            for (const auto& right : integralCases) {
                for (ui32 nullMask = 0; nullMask < 4; ++nullMask) {
                    TExportTestContext ctx;
                    const bool leftNullable = nullMask & 1;
                    const bool rightNullable = nullMask & 2;
                    const auto operand = [&](const TIntegralCase& type, bool nullable) {
                        auto value = TypedLiteral(
                            ctx,
                            type.Type,
                            "0",
                            ScalarType(ctx, type.Slot));
                        return nullable
                            ? TypedCallable(
                                ctx,
                                "Just",
                                {std::move(value)},
                                ScalarType(ctx, type.Slot, true))
                            : std::move(value);
                    };

                    TExprNode::TListType comparisons;
                    for (const auto& comparison : comparisonCases) {
                        comparisons.push_back(TypedCallable(
                            ctx,
                            comparison.Callable,
                            {
                                operand(left, leftNullable),
                                operand(right, rightNullable),
                            },
                            ScalarType(
                                ctx,
                                NUdf::EDataSlot::Bool,
                                !comparison.NullSafe &&
                                    (leftNullable || rightNullable))));
                    }
                    const auto expression = ExportMapExpression(
                        ctx,
                        "a",
                        TypedCallable(
                            ctx,
                            "And",
                            std::move(comparisons),
                            ScalarType(
                                ctx,
                                NUdf::EDataSlot::Bool,
                                leftNullable || rightNullable)));

                    const auto& args = expression["args"].GetArraySafe();
                    UNIT_ASSERT_VALUES_EQUAL(args.size(), comparisonCases.size());
                    for (ui32 index = 0; index < comparisonCases.size(); ++index) {
                        const auto& comparison = comparisonCases[index];
                        const auto& actual = args[index];
                        if (comparison.Negated) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                actual["kind"].GetStringSafe(),
                                "not");
                            UNIT_ASSERT_VALUES_EQUAL(
                                actual["arg"]["kind"].GetStringSafe(),
                                comparison.Kind);
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(
                                actual["kind"].GetStringSafe(),
                                comparison.Kind);
                        }
                        if (comparison.NullSafe) {
                            UNIT_ASSERT(actual["null_safe"].GetBooleanSafe());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ExportsQ8ShapedUint64GreaterThanInt32) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/Q8IntegralComparison", {
            {"x", "Uint64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x"});
        const auto* optionalUint64 = ScalarType(
            ctx,
            NUdf::EDataSlot::Uint64,
            true);
        const auto* int32 = ScalarType(ctx, NUdf::EDataSlot::Int32);
        SetExactOutputType(ctx, *read, {{"a.x", optionalUint64}});
        auto map = MakeIntrusive<TOpMap>(
            read,
            TPositionHandle(),
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                TExpression(
                    TypedCallable(
                        ctx,
                        ">",
                        {
                            TypedMember(ctx, "a.x", optionalUint64),
                            TypedLiteral(ctx, "Int32", "6", int32),
                        },
                        ScalarType(ctx, NUdf::EDataSlot::Bool, true)),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps))});
        TOpRoot root(map, TPositionHandle(), {"result"});

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& expression = FindNode(snapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];
        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "gt");
        UNIT_ASSERT_VALUES_EQUAL(expression["left"]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Int32");
    }

    Y_UNIT_TEST(IntegralDataComparisonExpansionRejectsNonIntegralMismatches) {
        for (const TStringBuf callable : {
            TStringBuf("=="), TStringBuf("!="), TStringBuf("IsNotDistinctFrom"),
            TStringBuf("<"), TStringBuf("<="), TStringBuf(">"), TStringBuf(">=")})
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedLiteral(
                            ctx,
                            "Bool",
                            "false",
                            ScalarType(ctx, NUdf::EDataSlot::Bool)),
                        TypedLiteral(
                            ctx,
                            "Int8",
                            "0",
                            ScalarType(ctx, NUdf::EDataSlot::Int8)),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_C(!result.IsSupported(), callable);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "comparison operand types differ");
        }
    }

    Y_UNIT_TEST(ExportsSameTypeDateOrderingAndRejectsNumericCarrierSubstitution) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/DateComparison", {
                {"d", "Date", false},
            });
            auto read = MakeRead(ctx, table, "a", {"d"});
            const auto* optionalDate = ScalarType(ctx, NUdf::EDataSlot::Date, true);
            const auto* date = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* optionalBool = ScalarType(ctx, NUdf::EDataSlot::Bool, true);
            SetExactOutputType(ctx, *read, {{"a.d", optionalDate}});
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            ">=",
                            {
                                TypedMember(ctx, "a.d", optionalDate),
                                TypedLiteral(ctx, "Date", "49672", date),
                            },
                            optionalBool),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps))});
            TOpRoot root(map, TPositionHandle(), {"result"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& expression = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "gte");
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["column"].GetStringSafe(), "a.d");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"].GetUIntegerSafe(), 49672);
        }

        {
            TExportTestContext ctx;
            const auto* date = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* uint16 = ScalarType(ctx, NUdf::EDataSlot::Uint16);
            const auto* boolean = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "<",
                    {
                        TypedLiteral(ctx, "Date", "0", date),
                        TypedLiteral(ctx, "Uint16", "1", uint16),
                    },
                    boolean));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "comparison operand types differ");
        }
    }

    Y_UNIT_TEST(ExportsStringUtf8ScalarComparisonsWithoutWideningStaticSqlIn) {
        for (const auto& [callable, kind] : TVector<std::pair<TString, TString>>{
            {"==", "eq"},
            {"<", "lt"},
            {"<=", "lte"},
            {">", "gt"},
            {">=", "gte"},
        }) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedLiteral(
                            ctx,
                            "String",
                            "e\xCC\x81",
                            ScalarType(ctx, NUdf::EDataSlot::String)),
                        TypedLiteral(
                            ctx,
                            "Utf8",
                            "\xC3\xA9",
                            ScalarType(ctx, NUdf::EDataSlot::Utf8)),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), kind);
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["type"].GetStringSafe(), "String");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Utf8");
        }

        {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "!=",
                    {
                        TypedLiteral(
                            ctx,
                            "Utf8",
                            "a",
                            ScalarType(ctx, NUdf::EDataSlot::Utf8)),
                        TypedLiteral(
                            ctx,
                            "String",
                            "ab",
                            ScalarType(ctx, NUdf::EDataSlot::String)),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "not");
            UNIT_ASSERT_VALUES_EQUAL(expression["arg"]["kind"].GetStringSafe(), "eq");
        }

        {
            TExportTestContext ctx;
            const auto* optionalString = ScalarType(
                ctx,
                NUdf::EDataSlot::String,
                true);
            const auto* optionalUtf8 = ScalarType(
                ctx,
                NUdf::EDataSlot::Utf8,
                true);
            const auto& table = AddTable(ctx, "/Root/TextComparison", {
                {"bytes", "String", false},
                {"text", "Utf8", false},
            });
            auto read = MakeRead(ctx, table, "a", {"bytes", "text"});
            SetExactOutputType(ctx, *read, {
                {"a.bytes", optionalString},
                {"a.text", optionalUtf8},
            });
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "IsNotDistinctFrom",
                            {
                                TypedMember(ctx, "a.bytes", optionalString),
                                TypedMember(ctx, "a.text", optionalUtf8),
                            },
                            ScalarType(ctx, NUdf::EDataSlot::Bool)),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps))});
            TOpRoot root(map, TPositionHandle(), {"result"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& expression = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "eq");
            UNIT_ASSERT(expression["null_safe"].GetBooleanSafe());
        }

        {
            TExportTestContext ctx;
            const auto* stringType = ScalarType(ctx, NUdf::EDataSlot::String);
            const auto* utf8Type = ScalarType(ctx, NUdf::EDataSlot::Utf8);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    TypedStaticTuple(
                        ctx,
                        {TypedLiteral(ctx, "String", "a", stringType)},
                        stringType),
                    TypedLiteral(ctx, "Utf8", "a", utf8Type),
                    SqlInOptions(ctx, {}),
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "not equality-compatible");
        }
    }

    Y_UNIT_TEST(LowersExactBoolCoalesceFalseThroughIfPresent) {
        const auto makeCoalesce = [](TExportTestContext& ctx) {
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            return TypedCallable(
                ctx,
                "Coalesce",
                {
                    TypedCallable(
                        ctx,
                        ">=",
                        {
                            TypedMember(ctx, "a.x", optionalInt),
                            TypedLiteral(ctx, "Int32", "7", intType),
                        },
                        optionalBool),
                    TypedLiteral(ctx, "Bool", "false", boolType),
                },
                boolType);
        };

        {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                makeCoalesce(ctx),
                true);

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 6);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "if_present");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["optional"]["kind"].GetStringSafe(),
                "gte");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["optional"]["left"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["present"]["kind"].GetStringSafe(),
                "bound");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["present"]["depth"].GetUIntegerSafe(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["missing"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT(!expression["missing"]["value"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Bool");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
        }

        {
            TExportTestContext ctx;
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Not",
                    {makeCoalesce(ctx)},
                    boolType),
                true);

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "not");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["kind"].GetStringSafe(),
                "if_present");
        }

        {
            TExportTestContext ctx;
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedLiteral(ctx, "Bool", "false", boolType),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto comparison = [&]() {
                return TypedCallable(
                    ctx,
                    ">=",
                    {
                        TypedMember(ctx, "a.x", optionalInt),
                        TypedLiteral(ctx, "Int32", "7", intType),
                    },
                    optionalBool);
            };
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            "And",
                            {comparison(), comparison()},
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType),
                true);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            ">=",
                            {
                                TypedMember(ctx, "a.x", optionalInt),
                                TypedLiteral(ctx, "Int32", "7", intType),
                            },
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "true", boolType),
                    },
                    boolType),
                true);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            ">=",
                            {
                                TypedLiteral(ctx, "Int32", "7", intType),
                                TypedLiteral(ctx, "Int32", "1", intType),
                            },
                            boolType),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            ">=",
                            {
                                TypedMember(ctx, "a.x", optionalInt),
                                TypedLiteral(ctx, "Int32", "7", intType),
                            },
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    optionalBool),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "inconsistent nullability");
        }
    }

    Y_UNIT_TEST(LowersOnlyExactOptionalCountRepair) {
        const auto exportExpression = [](
            TExportTestContext& ctx,
            TExprNode::TPtr expression)
        {
            const auto& table = AddTable(ctx, "/Root/CountRepair", {
                {"x", "Uint64", false},
                {"y", "Uint64", false},
                {"i", "Int64", false},
            });
            auto read = MakeRead(ctx, table, "a", {"x", "y", "i"});
            SetExactOutputType(ctx, *read, {
                {"a.x", ScalarType(ctx, NUdf::EDataSlot::Uint64, true)},
                {"a.y", ScalarType(ctx, NUdf::EDataSlot::Uint64)},
                {"a.i", ScalarType(ctx, NUdf::EDataSlot::Int64, true)},
            });
            auto map = MakeComputedMap(
                ctx,
                read,
                "result",
                std::move(expression));
            TOpRoot root(map, TPositionHandle(), {"result"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            return FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
        };
        const auto makeRepair = [](
            TExportTestContext& ctx,
            NUdf::EDataSlot slot,
            TStringBuf callable,
            TStringBuf member,
            TStringBuf fallback,
            bool directMember = true,
            bool wrapJust = true)
        {
            const auto* valueType = ScalarType(ctx, slot);
            const auto* optionalType = ScalarType(ctx, slot, true);
            TExprNode::TPtr optional = directMember
                ? TypedMember(ctx, member, optionalType)
                : TypedCallable(
                    ctx,
                    "Just",
                    {TypedMember(ctx, "a.y", valueType)},
                    optionalType);
            auto coalesce = TypedCallable(
                ctx,
                "Coalesce",
                {
                    std::move(optional),
                    TypedLiteral(ctx, callable, fallback, valueType),
                },
                valueType);
            if (!wrapJust) {
                return coalesce;
            }
            return TypedCallable(
                ctx,
                "Just",
                {std::move(coalesce)},
                optionalType);
        };

        {
            TExportTestContext ctx;
            const auto expression = exportExpression(
                ctx,
                makeRepair(
                    ctx,
                    NUdf::EDataSlot::Uint64,
                    "Uint64",
                    "a.x",
                    "0"));

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Uint64");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["condition"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["condition"]["type"].GetStringSafe(),
                "Bool");
            UNIT_ASSERT(expression["condition"]["value"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["kind"].GetStringSafe(),
                "null");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["type"].GetStringSafe(),
                "Uint64");

            const auto& repaired = expression["then"];
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["kind"].GetStringSafe(),
                "if_present");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["optional"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["optional"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["present"]["kind"].GetStringSafe(),
                "bound");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["present"]["depth"].GetUIntegerSafe(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["missing"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["missing"]["type"].GetStringSafe(),
                "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["missing"]["value"].GetUIntegerSafe(),
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                repaired["type"].GetStringSafe(),
                "Uint64");
            UNIT_ASSERT(!repaired["nullable"].GetBooleanSafe());
        }

        struct TNearMiss {
            TString Name;
            NUdf::EDataSlot Slot;
            TString Callable;
            TString Member;
            TString Fallback;
            bool DirectMember = true;
            bool WrapJust = true;
        };
        const TVector<TNearMiss> nearMisses = {
            {"nonzero fallback", NUdf::EDataSlot::Uint64, "Uint64", "a.x", "1"},
            {"wrong scalar type", NUdf::EDataSlot::Int64, "Int64", "a.i", "0"},
            {"non-direct optional", NUdf::EDataSlot::Uint64, "Uint64", "a.x", "0", false},
            {"missing Just", NUdf::EDataSlot::Uint64, "Uint64", "a.x", "0", true, false},
        };
        for (const auto& test : nearMisses) {
            TExportTestContext ctx;
            const auto encoded = exportExpression(
                ctx,
                makeRepair(
                    ctx,
                    test.Slot,
                    test.Callable,
                    test.Member,
                    test.Fallback,
                    test.DirectMember,
                    test.WrapJust));
            UNIT_ASSERT_VALUES_EQUAL_C(
                encoded["kind"].GetStringSafe(),
                "opaque",
                test.Name);
        }
    }

    Y_UNIT_TEST(LowersOnlyExactDirectUint64MemberJust) {
        const auto exportExpression = [](
            TExportTestContext& ctx,
            TExpression expression)
        {
            const auto& table = AddTable(ctx, "/Root/DirectJust", {
                {"x", "Uint64", true},
                {"n", "Uint64", false},
                {"i", "Int64", true},
            });
            auto read = MakeRead(ctx, table, "a", {"x", "n", "i"});
            SetExactOutputType(ctx, *read, {
                {"a.x", ScalarType(ctx, NUdf::EDataSlot::Uint64)},
                {"a.n", ScalarType(ctx, NUdf::EDataSlot::Uint64, true)},
                {"a.i", ScalarType(ctx, NUdf::EDataSlot::Int64)},
            });
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    std::move(expression))});
            TOpRoot root(map, TPositionHandle(), {"result"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };
        const auto directJust = [](
            TExportTestContext& ctx,
            TStringBuf member,
            NUdf::EDataSlot slot,
            bool memberNullable = false)
        {
            return TypedCallable(
                ctx,
                "Just",
                {TypedMember(
                    ctx,
                    member,
                    ScalarType(ctx, slot, memberNullable))},
                ScalarType(ctx, slot, true));
        };

        {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                TExpression(
                    directJust(
                        ctx,
                        "a.x",
                        NUdf::EDataSlot::Uint64),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps)));
            const auto& expression = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "if");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["condition"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT(expression["condition"]["value"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["then"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["then"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["kind"].GetStringSafe(),
                "null");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["type"].GetStringSafe(),
                "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                "Uint64");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
        }

        {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                TExpression(
                    directJust(
                        ctx,
                        "a.i",
                        NUdf::EDataSlot::Int64),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps)));
            UNIT_ASSERT_VALUES_EQUAL(
                FindNode(snapshot, "project")
                    ["columns"].GetArraySafe().back()["expression"]
                    ["kind"].GetStringSafe(),
                "opaque");
        }

        {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                TExpression(
                    directJust(
                        ctx,
                        "a.n",
                        NUdf::EDataSlot::Uint64,
                        true),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps)));
            UNIT_ASSERT_VALUES_EQUAL(
                FindNode(snapshot, "project")
                    ["columns"].GetArraySafe().back()["expression"]
                    ["kind"].GetStringSafe(),
                "opaque");
        }

        {
            TExportTestContext ctx;
            auto expression = directJust(
                ctx,
                "a.x",
                NUdf::EDataSlot::Uint64);
            expression->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Uint64));
            const auto result = exportExpression(
                ctx,
                TExpression(
                    std::move(expression),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "requires an Optional<Uint64> result");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                TExpression(
                    directJust(
                        ctx,
                        "a.missing",
                        NUdf::EDataSlot::Uint64),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "requires a direct visible input member");
        }

        {
            TExportTestContext ctx;
            const auto pos = TPositionHandle();
            auto row = ctx.ExprCtx.NewArgument(pos, "row");
            auto foreign = ctx.ExprCtx.NewArgument(pos, "foreign");
            auto member = ctx.ExprCtx.NewCallable(
                pos,
                "Member",
                {foreign, ctx.ExprCtx.NewAtom(pos, "a.x")});
            member->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Uint64));
            auto body = TypedCallable(
                ctx,
                "Just",
                {std::move(member)},
                ScalarType(ctx, NUdf::EDataSlot::Uint64, true));
            const auto result = exportExpression(
                ctx,
                TExpression(
                    ctx.ExprCtx.NewLambda(
                        pos,
                        ctx.ExprCtx.NewArguments(pos, {row}),
                        std::move(body)),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "requires a direct visible input member");
        }

        {
            TExportTestContext ctx;
            const auto pos = TPositionHandle();
            auto row = ctx.ExprCtx.NewArgument(pos, "row");
            row->SetSideEffects(ESideEffects::General);
            auto member = ctx.ExprCtx.NewCallable(
                pos,
                "Member",
                {row, ctx.ExprCtx.NewAtom(pos, "a.x")});
            member->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Uint64));
            auto body = TypedCallable(
                ctx,
                "Just",
                {std::move(member)},
                ScalarType(ctx, NUdf::EDataSlot::Uint64, true));
            const auto result = exportExpression(
                ctx,
                TExpression(
                    ctx.ExprCtx.NewLambda(
                        pos,
                        ctx.ExprCtx.NewArguments(pos, {row}),
                        std::move(body)),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "side-effecting or CSE-unsafe");
        }

        for (const bool unsafeMember : {false, true}) {
            TExportTestContext ctx;
            TExpression expression(
                directJust(
                    ctx,
                    "a.x",
                    NUdf::EDataSlot::Uint64),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            TExprNode* unsafe = unsafeMember
                ? expression.GetExpressionBody()->Child(0)
                : expression.GetExpressionBody().Get();
            unsafe->SetSideEffects(ESideEffects::General);
            const auto result = exportExpression(
                ctx,
                std::move(expression));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "side-effecting or CSE-unsafe");
        }
    }

    Y_UNIT_TEST(FoldsExactConstantDecimalJust) {
        struct TCase {
            TString Value;
            TString Precision;
            TString Scale;
            TString ExpectedScaled;
            TString CastCallable;
        };
        const TVector<TCase> cases = {
            {"0", "12", "2", "0", "SafeCast"},
            {"0", "12", "2", "0", "Convert"},
            {"0", "12", "2", "0", ""},
            {"100", "35", "2", "10000", ""},
            {"0", "35", "2", "0", ""},
        };

        TString safeCastZero;
        TString convertZero;
        TString literalZero;
        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(
                ctx,
                test.Precision,
                test.Scale);
            const auto* optionalDecimalType = DecimalType(
                ctx,
                test.Precision,
                test.Scale,
                true);
            TExprNode::TPtr argument;
            if (!test.CastCallable.empty()) {
                const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
                argument = TypedCallable(
                    ctx,
                    test.CastCallable,
                    {
                        TypedLiteral(ctx, "Int32", test.Value, intType),
                        DecimalDataTypeDescriptor(
                            ctx,
                            test.Precision,
                            test.Scale,
                            decimalType),
                    },
                    decimalType);
            } else {
                argument = TypedDecimalLiteral(
                    ctx,
                    test.Value,
                    test.Precision,
                    test.Scale,
                    decimalType);
            }

            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Just",
                    {std::move(argument)},
                    optionalDecimalType));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                TStringBuilder()
                    << "Decimal(" << test.Precision << "," << test.Scale << ")");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["condition"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT(expression["condition"]["value"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["kind"].GetStringSafe(),
                "null");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["else"]["type"].GetStringSafe(),
                expression["type"].GetStringSafe());
            UNIT_ASSERT_VALUES_EQUAL(
                expression["then"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["then"]["value"]["kind"].GetStringSafe(),
                "finite");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["then"]["value"]["scaled"].GetStringSafe(),
                test.ExpectedScaled);
            if (test.CastCallable == "SafeCast") {
                safeCastZero = NJson::WriteJson(expression, false, true);
            } else if (test.CastCallable == "Convert") {
                convertZero = NJson::WriteJson(expression, false, true);
            } else if (test.Precision == "12") {
                literalZero = NJson::WriteJson(expression, false, true);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(safeCastZero, literalZero);
        UNIT_ASSERT_VALUES_EQUAL(convertZero, literalZero);

        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto* optionalDecimalType = DecimalType(ctx, "12", "2", true);
            const auto just = [&](TStringBuf value) {
                return TypedCallable(
                    ctx,
                    "Just",
                    {
                        TypedDecimalLiteral(
                            ctx,
                            value,
                            "12",
                            "2",
                            decimalType),
                    },
                    optionalDecimalType);
            };
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "+",
                    {just("0"), just("100")},
                    optionalDecimalType));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "add");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["kind"].GetStringSafe(), "if");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["kind"].GetStringSafe(), "if");
            UNIT_ASSERT(expression["left"]["nullable"].GetBooleanSafe());
            UNIT_ASSERT(expression["right"]["nullable"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(ConstantDecimalJustGateFailsClosed) {
        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Just",
                    {
                        TypedDecimalLiteral(
                            ctx,
                            "0",
                            "12",
                            "2",
                            decimalType),
                    },
                    DecimalType(ctx, "13", "2", true)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "matching Optional<Decimal>");
        }

        {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto* optionalDecimalType = DecimalType(ctx, "12", "2", true);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Just",
                    {
                        TypedCallable(
                            ctx,
                            "SafeCast",
                            {
                                TypedMember(ctx, "a.x", intType),
                                DecimalDataTypeDescriptor(
                                    ctx,
                                    "12",
                                    "2",
                                    decimalType),
                            },
                            decimalType),
                    },
                    optionalDecimalType));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
            UNIT_ASSERT_STRING_CONTAINS(
                expression["fingerprint"].GetStringSafe(),
                "Just");
        }
    }

    Y_UNIT_TEST(ExactDirectDecimalCoalesceZeroIsNarrow) {
        const auto exportExpression = [](
            TExportTestContext& ctx,
            TExprNode::TPtr expression,
            std::function<void(TExprNode&)> mutate = {},
            TStringBuf decimalColumnType = "Decimal(35,2)")
        {
            const auto& table = AddTable(ctx, "/Root/DecimalCoalesce", {
                {"x", TString(decimalColumnType), false},
                {"y", "Int32", true},
            });
            auto read = MakeRead(ctx, table, "a", {"x", "y"});
            TExpression typedExpression(
                std::move(expression),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            if (mutate) {
                mutate(*typedExpression.GetExpressionBody());
            }
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    std::move(typedExpression))});
            TOpRoot root(map, TPositionHandle(), {"result"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };
        const auto makeFallback = [](
            TExportTestContext& ctx,
            TStringBuf callable,
            TStringBuf value = "0",
            bool dynamic = false)
        {
            const auto* decimalType = DecimalType(ctx, "35", "2");
            if (callable == "Decimal") {
                return TypedDecimalLiteral(
                    ctx,
                    value,
                    "35",
                    "2",
                    decimalType);
            }

            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            return TypedCallable(
                ctx,
                callable,
                {
                    dynamic
                        ? TypedMember(ctx, "a.y", intType)
                        : TypedLiteral(ctx, "Int32", value, intType),
                    DecimalDataTypeDescriptor(
                        ctx,
                        "35",
                        "2",
                        decimalType),
                },
                decimalType);
        };
        const auto makeCoalesce = [](
            TExportTestContext& ctx,
            TExprNode::TPtr fallback,
            bool wrapJust = false,
            TStringBuf memberName = "x")
        {
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "35",
                "2",
                true);
            auto coalesce = TypedCallable(
                ctx,
                "Coalesce",
                {
                    TypedMember(
                        ctx,
                        TStringBuilder() << "a." << memberName,
                        optionalDecimalType),
                    std::move(fallback),
                },
                decimalType);
            if (!wrapJust) {
                return coalesce;
            }
            return TypedCallable(
                ctx,
                "Just",
                {std::move(coalesce)},
                optionalDecimalType);
        };
        const auto normalized = [&](
            TExportTestContext& ctx,
            TExprNode::TPtr expression,
            std::function<void(TExprNode&)> mutate = {})
        {
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                std::move(expression),
                std::move(mutate)));
            return FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
        };

        TString bareEncoding;
        TString wrappedEncoding;
        for (const TStringBuf spelling : {
            TStringBuf("Decimal"),
            TStringBuf("SafeCast"),
        }) {
            for (const bool wrapJust : {false, true}) {
                TExportTestContext ctx;
                const auto expression = normalized(
                    ctx,
                    makeCoalesce(
                        ctx,
                        makeFallback(ctx, spelling),
                        wrapJust));
                const auto& inner = wrapJust
                    ? expression["then"]
                    : expression;
                if (wrapJust) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expression["kind"].GetStringSafe(),
                        "if");
                    UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
                    UNIT_ASSERT_VALUES_EQUAL(
                        expression["else"]["kind"].GetStringSafe(),
                        "null");
                }
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["kind"].GetStringSafe(),
                    "if_present");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["optional"]["kind"].GetStringSafe(),
                    "column");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["optional"]["column"].GetStringSafe(),
                    "a.x");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["present"]["kind"].GetStringSafe(),
                    "bound");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["present"]["depth"].GetUIntegerSafe(),
                    0);
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["missing"]["kind"].GetStringSafe(),
                    "literal");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["missing"]["value"]["scaled"].GetStringSafe(),
                    "0");
                UNIT_ASSERT_VALUES_EQUAL(
                    inner["type"].GetStringSafe(),
                    "Decimal(35,2)");
                UNIT_ASSERT(!inner["nullable"].GetBooleanSafe());

                const TString encoded = NJson::WriteJson(
                    expression,
                    false,
                    true);
                TString& expected = wrapJust
                    ? wrappedEncoding
                    : bareEncoding;
                if (expected.empty()) {
                    expected = encoded;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(encoded, expected);
                }
            }
        }

        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto expression = normalized(
                ctx,
                TypedCallable(
                    ctx,
                    "-",
                    {
                        makeCoalesce(
                            ctx,
                            makeFallback(ctx, "SafeCast")),
                        TypedDecimalLiteral(
                            ctx,
                            "1",
                            "35",
                            "2",
                            decimalType),
                    },
                    decimalType));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "sub");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["left"]["kind"].GetStringSafe(),
                "if_present");
        }

        for (const auto [callable, value, dynamic] : {
            std::tuple<TStringBuf, TStringBuf, bool>{"Decimal", "1", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"Decimal", "nan", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"Decimal", "inf", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"Decimal", "-inf", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"SafeCast", "1", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"Convert", "0", false},
            std::tuple<TStringBuf, TStringBuf, bool>{"SafeCast", "0", true},
        }) {
            TExportTestContext ctx;
            const auto expression = normalized(
                ctx,
                makeCoalesce(
                    ctx,
                    makeFallback(ctx, callable, value, dynamic)));
            UNIT_ASSERT_VALUES_EQUAL_C(
                expression["kind"].GetStringSafe(),
                "opaque",
                callable);
        }

        {
            TExportTestContext ctx;
            const auto expression = normalized(
                ctx,
                makeCoalesce(
                    ctx,
                    makeFallback(ctx, "Decimal", "1"),
                    true));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "opaque");
            UNIT_ASSERT_STRING_CONTAINS(
                expression["fingerprint"].GetStringSafe(),
                "Just");
        }

        for (const bool wrapJust : {false, true}) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* decimalType = DecimalType(ctx, "7", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "7",
                "2",
                true);
            auto coalesce = TypedCallable(
                ctx,
                "Coalesce",
                {
                    TypedMember(ctx, "a.x", optionalDecimalType),
                    TypedCallable(
                        ctx,
                        "SafeCast",
                        {
                            TypedLiteral(ctx, "Int32", "0", intType),
                            DecimalDataTypeDescriptor(
                                ctx,
                                "7",
                                "2",
                                decimalType),
                        },
                        decimalType),
                },
                decimalType);
            TExprNode::TPtr expression = std::move(coalesce);
            if (wrapJust) {
                expression = TypedCallable(
                    ctx,
                    "Just",
                    {std::move(expression)},
                    optionalDecimalType);
            }
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                std::move(expression),
                {},
                "Decimal(7,2)"));
            const auto& normalizedExpression = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL_C(
                normalizedExpression["kind"].GetStringSafe(),
                "opaque",
                wrapJust);
        }

        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "35",
                "2",
                true);
            const auto expression = normalized(
                ctx,
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            "Just",
                            {
                                TypedDecimalLiteral(
                                    ctx,
                                    "1",
                                    "35",
                                    "2",
                                    decimalType),
                            },
                            optionalDecimalType),
                        makeFallback(ctx, "Decimal"),
                    },
                    decimalType));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "35",
                "2",
                true);
            const auto expression = normalized(
                ctx,
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        makeFallback(ctx, "Decimal"),
                        TypedMember(ctx, "a.x", optionalDecimalType),
                    },
                    decimalType));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "opaque");
        }

        {
            TExportTestContext ctx;
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "35",
                "2",
                true);
            const auto expression = normalized(
                ctx,
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedMember(ctx, "a.x", optionalDecimalType),
                        makeFallback(ctx, "Decimal"),
                        makeFallback(ctx, "Decimal"),
                    },
                    decimalType));
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "opaque");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(
                    ctx,
                    TypedDecimalLiteral(
                        ctx,
                        "0",
                        "35",
                        "3",
                        DecimalType(ctx, "35", "3"))));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "inconsistent types");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(
                    ctx,
                    makeFallback(ctx, "Decimal"),
                    false,
                    "missing"));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "direct visible input member");
        }

        for (const bool unsafeMember : {false, true}) {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(
                    ctx,
                    makeFallback(ctx, "Decimal")),
                [unsafeMember](TExprNode& expression) {
                    TExprNode* unsafe = unsafeMember
                        ? expression.Child(0)
                        : &expression;
                    unsafe->SetSideEffects(ESideEffects::General);
                });
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "side-effecting or CSE-unsafe");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(
                    ctx,
                    makeFallback(ctx, "SafeCast")),
                [](TExprNode& expression) {
                    expression.Child(1)->SetUnorderedChildren();
                });
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }
        const auto nestedDecimalCoalesce = [&](
            TExportTestContext& ctx,
            size_t depth)
        {
            const auto* decimalType = DecimalType(ctx, "35", "2");
            const auto* optionalDecimalType = DecimalType(
                ctx,
                "35",
                "2",
                true);
            TVector<TExprNode::TPtr> arguments;
            arguments.reserve(depth);
            for (size_t index = 0; index < depth; ++index) {
                auto argument = ctx.ExprCtx.NewArgument(
                    TPositionHandle(),
                    TStringBuilder() << "decimal_" << index);
                argument->SetTypeAnn(decimalType);
                arguments.push_back(std::move(argument));
            }

            auto row = ctx.ExprCtx.NewArgument(TPositionHandle(), "row");
            const auto directMember = [&]() {
                return TypedCallable(
                    ctx,
                    "Member",
                    {
                        row,
                        ctx.ExprCtx.NewAtom(TPositionHandle(), "a.x"),
                    },
                    optionalDecimalType);
            };
            TExprNode::TPtr expression = TypedCallable(
                ctx,
                "Coalesce",
                {
                    directMember(),
                    makeFallback(ctx, "Decimal"),
                },
                decimalType);
            for (size_t index = depth; index > 0; --index) {
                const size_t level = index - 1;
                auto optional = level == 0
                    ? directMember()
                    : TypedCallable(
                        ctx,
                        "Just",
                        {arguments[level - 1]},
                        optionalDecimalType);
                expression = TypedCallable(
                    ctx,
                    "IfPresent",
                    {
                        std::move(optional),
                        TypedUnaryLambda(
                            ctx,
                            arguments[level],
                            std::move(expression)),
                        TypedDecimalLiteral(
                            ctx,
                            "0",
                            "35",
                            "2",
                            decimalType),
                    },
                    decimalType);
            }
            return TypedUnaryLambda(ctx, row, std::move(expression));
        };

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                nestedDecimalCoalesce(ctx, 63));
            UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
        }
        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                nestedDecimalCoalesce(ctx, 64));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Exact Decimal Coalesce zero binding depth exceeds");
        }
    }

    Y_UNIT_TEST(ExactWrapperSafetyMetadataFailsClosed) {
        const auto makeDecimalJust = [](
            TExportTestContext& ctx,
            TExprNode::TPtr argument)
        {
            return TypedCallable(
                ctx,
                "Just",
                {std::move(argument)},
                DecimalType(ctx, "12", "2", true));
        };
        const auto makeCoalesce = [](
            TExportTestContext& ctx,
            TExprNode::TPtr comparison)
        {
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            return TypedCallable(
                ctx,
                "Coalesce",
                {
                    std::move(comparison),
                    TypedLiteral(ctx, "Bool", "false", boolType),
                },
                boolType);
        };
        const auto makeComparison = [](
            TExportTestContext& ctx,
            TStringBuf callable = ">=")
        {
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            return TypedCallable(
                ctx,
                callable,
                {
                    TypedMember(ctx, "a.x", optionalInt),
                    TypedLiteral(ctx, "Int32", "1", intType),
                },
                ScalarType(ctx, NUdf::EDataSlot::Bool, true));
        };

        {
            TExportTestContext ctx;
            TExpression expression(
                makeDecimalJust(
                    ctx,
                    TypedDecimalLiteral(
                        ctx,
                        "0",
                        "12",
                        "2",
                        DecimalType(ctx, "12", "2"))),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()->SetResult(
                ctx.ExprCtx.NewAtom(TPositionHandle(), "executed"));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "executed Result node");
        }

        for (const TStringBuf callable : {TStringBuf("=="), TStringBuf("!=")}) {
            TExportTestContext ctx;
            TExpression expression(
                makeCoalesce(ctx, makeComparison(ctx, callable)),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()->Child(0)->SetUnorderedChildren();
            const auto snapshot = ParseSupported(ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression),
                true));
            const auto& lowered = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(
                lowered["kind"].GetStringSafe(),
                "if_present");
            if (callable == "==") {
                UNIT_ASSERT_VALUES_EQUAL(
                    lowered["optional"]["kind"].GetStringSafe(),
                    "eq");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    lowered["optional"]["kind"].GetStringSafe(),
                    "not");
                UNIT_ASSERT_VALUES_EQUAL(
                    lowered["optional"]["arg"]["kind"].GetStringSafe(),
                    "eq");
            }
        }

        {
            TExportTestContext ctx;
            TExpression expression(
                makeCoalesce(ctx, makeComparison(ctx, "==")),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()
                ->Child(0)
                ->Child(0)
                ->SetUnorderedChildren();
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }

        {
            TExportTestContext ctx;
            TExpression expression(
                makeDecimalJust(
                    ctx,
                    TypedDecimalLiteral(
                        ctx,
                        "0",
                        "12",
                        "2",
                        DecimalType(ctx, "12", "2"))),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()->Child(0)->SetSideEffects(
                ESideEffects::General);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "side-effecting or CSE-unsafe");
        }

        {
            TExportTestContext ctx;
            TExpression expression(
                makeCoalesce(ctx, makeComparison(ctx)),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()->SetResult(
                ctx.ExprCtx.NewAtom(TPositionHandle(), "executed"));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "executed Result node");
        }

        {
            TExportTestContext ctx;
            TExpression expression(
                makeCoalesce(ctx, makeComparison(ctx)),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            expression.GetExpressionBody()->Child(0)->SetUnorderedChildren();
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(expression),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }
    }

    Y_UNIT_TEST(ExactBinaryStringMembershipCoalesceFalseIsNarrow) {
        const auto exportExpression = [](
            TExportTestContext& ctx,
            TExprNode::TPtr expression,
            std::function<void(TExprNode&)> mutate = {})
        {
            const auto& table = AddTable(ctx, "/Root/StringMembership", {
                {"x", "String", false},
                {"y", "String", false},
            });
            auto read = MakeRead(ctx, table, "a", {"x", "y"});
            TExpression typedExpression(
                std::move(expression),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            if (mutate) {
                mutate(*typedExpression.GetExpressionBody());
            }
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    std::move(typedExpression))});
            TOpRoot root(map, TPositionHandle(), {"result"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };
        const auto makeComparison = [](
            TExportTestContext& ctx,
            TStringBuf callable,
            TStringBuf memberName,
            TStringBuf literal,
            NUdf::EDataSlot slot,
            bool reverse = false)
        {
            const auto* valueType = ScalarType(ctx, slot);
            const auto* optionalValueType = ScalarType(ctx, slot, true);
            TVector<TExprNode::TPtr> operands = {
                TypedMember(
                    ctx,
                    TStringBuilder() << "a." << memberName,
                    optionalValueType),
                TypedLiteral(
                    ctx,
                    NUdf::GetDataTypeInfo(slot).Name,
                    literal,
                    valueType),
            };
            if (reverse) {
                std::swap(operands[0], operands[1]);
            }
            auto result = TypedCallable(
                ctx,
                callable,
                std::move(operands),
                ScalarType(ctx, NUdf::EDataSlot::Bool, true));
            return result;
        };
        const auto makeCoalesce = [&](
            TExportTestContext& ctx,
            TStringBuf booleanCallable,
            TStringBuf comparisonCallable,
            TStringBuf secondMember = "x",
            NUdf::EDataSlot slot = NUdf::EDataSlot::String)
        {
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            return TypedCallable(
                ctx,
                "Coalesce",
                {
                    TypedCallable(
                        ctx,
                        booleanCallable,
                        {
                            makeComparison(
                                ctx,
                                comparisonCallable,
                                "x",
                                "1-URGENT",
                                slot),
                            makeComparison(
                                ctx,
                                comparisonCallable,
                                secondMember,
                                "2-HIGH",
                                slot,
                                true),
                        },
                        optionalBool),
                    TypedLiteral(ctx, "Bool", "false", boolType),
                },
                boolType);
        };
        const auto markComparisonsUnordered = [](TExprNode& expression) {
            for (const auto& comparison : expression.Child(0)->Children()) {
                comparison->SetUnorderedChildren();
            }
        };

        for (const auto [booleanCallable, comparisonCallable] : {
            std::pair<TStringBuf, TStringBuf>{"Or", "=="},
            std::pair<TStringBuf, TStringBuf>{"And", "!="},
        }) {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportExpression(
                ctx,
                makeCoalesce(ctx, booleanCallable, comparisonCallable),
                markComparisonsUnordered));
            const auto& lowered = FindNode(snapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(
                lowered["kind"].GetStringSafe(),
                "if_present");
            UNIT_ASSERT_VALUES_EQUAL(
                lowered["optional"]["kind"].GetStringSafe(),
                to_lower(TString(booleanCallable)));
            for (const auto& child : lowered["optional"]["args"].GetArraySafe()) {
                if (comparisonCallable == "==") {
                    UNIT_ASSERT_VALUES_EQUAL(
                        child["kind"].GetStringSafe(),
                        "eq");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(
                        child["kind"].GetStringSafe(),
                        "not");
                    UNIT_ASSERT_VALUES_EQUAL(
                        child["arg"]["kind"].GetStringSafe(),
                        "eq");
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(
                lowered["missing"]["value"].GetBooleanSafe(),
                false);
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(
                    ctx,
                    "Or",
                    "==",
                    "x",
                    NUdf::EDataSlot::Utf8),
                markComparisonsUnordered);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "must contain one Member and one literal");
        }

        {
            TExportTestContext ctx;
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto result = exportExpression(
                ctx,
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            "Or",
                            {
                                makeComparison(
                                    ctx,
                                    "==",
                                    "x",
                                    "1-URGENT",
                                    NUdf::EDataSlot::String),
                                makeComparison(
                                    ctx,
                                    "==",
                                    "x",
                                    "2-HIGH",
                                    NUdf::EDataSlot::String),
                                makeComparison(
                                    ctx,
                                    "==",
                                    "x",
                                    "3-MEDIUM",
                                    NUdf::EDataSlot::String),
                            },
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType),
                markComparisonsUnordered);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(ctx, "Or", "==", "y"),
                markComparisonsUnordered);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "same member");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(ctx, "Or", "==", "missing"),
                markComparisonsUnordered);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "not a direct input value");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(ctx, "And", "!="),
                [&](TExprNode& expression) {
                    markComparisonsUnordered(expression);
                    expression.Child(0)
                        ->Child(0)
                        ->Child(0)
                        ->SetSideEffects(ESideEffects::General);
                });
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "side-effecting or CSE-unsafe");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(ctx, "Or", "=="),
                [&](TExprNode& expression) {
                    markComparisonsUnordered(expression);
                    expression.Child(0)
                        ->Child(0)
                        ->Child(1)
                        ->SetUnorderedChildren();
                });
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }

        {
            TExportTestContext ctx;
            const auto result = exportExpression(
                ctx,
                makeCoalesce(ctx, "Or", "=="),
                [](TExprNode& expression) {
                    expression.Child(0)->SetUnorderedChildren();
                });
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unordered children");
        }

        {
            TExportTestContext ctx;
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto expression = exportExpression(
                ctx,
                TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            "Or",
                            {
                                makeComparison(
                                    ctx,
                                    "==",
                                    "x",
                                    "1-URGENT",
                                    NUdf::EDataSlot::String),
                                makeComparison(
                                    ctx,
                                    "!=",
                                    "x",
                                    "2-HIGH",
                                    NUdf::EDataSlot::String),
                            },
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType),
                markComparisonsUnordered);
            UNIT_ASSERT(!expression.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                expression.UnsupportedReason,
                "unordered children");
        }
    }

    Y_UNIT_TEST(ExportsExactUnaryIfPresentWithScopedBoundValue) {
        TExportTestContext ctx;
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "present_value");
        argument->SetTypeAnn(intType);
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "IfPresent",
                {
                    TypedMember(ctx, "a.x", optionalInt),
                    TypedUnaryLambda(
                        ctx,
                        argument,
                        TypedCallable(
                            ctx,
                            "+",
                            {
                                argument,
                                TypedLiteral(ctx, "Int32", "2", intType),
                            },
                            intType)),
                    TypedLiteral(ctx, "Int32", "0", intType),
                },
                intType),
            true);

        UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if_present");
        UNIT_ASSERT_VALUES_EQUAL(expression["optional"]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(expression["present"]["kind"].GetStringSafe(), "add");
        UNIT_ASSERT_VALUES_EQUAL(expression["present"]["left"]["kind"].GetStringSafe(), "bound");
        UNIT_ASSERT_VALUES_EQUAL(expression["present"]["left"]["depth"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(expression["present"]["right"]["value"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(expression["missing"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
    }

    Y_UNIT_TEST(ExportsExactIfAndExistsAroundIfPresent) {
        TExportTestContext ctx;
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
        auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "present_value");
        argument->SetTypeAnn(intType);
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "If",
                {
                    TypedCallable(
                        ctx,
                        "Exists",
                        {TypedMember(ctx, "a.x", optionalInt)},
                        boolType),
                    TypedCallable(
                        ctx,
                        "IfPresent",
                        {
                            TypedMember(ctx, "a.x", optionalInt),
                            TypedUnaryLambda(
                                ctx,
                                argument,
                                TypedCallable(
                                    ctx,
                                    "==",
                                    {
                                        argument,
                                        TypedLiteral(ctx, "Int32", "7", intType),
                                    },
                                    boolType)),
                            TypedLiteral(ctx, "Bool", "false", boolType),
                        },
                        boolType),
                    TypedLiteral(ctx, "Bool", "false", boolType),
                },
                boolType),
            true);

        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
        UNIT_ASSERT_VALUES_EQUAL(expression["condition"]["kind"].GetStringSafe(), "exists");
        UNIT_ASSERT_VALUES_EQUAL(expression["condition"]["arg"]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(expression["then"]["kind"].GetStringSafe(), "if_present");
        UNIT_ASSERT_VALUES_EQUAL(expression["then"]["present"]["kind"].GetStringSafe(), "eq");
        UNIT_ASSERT_VALUES_EQUAL(expression["then"]["present"]["left"]["kind"].GetStringSafe(), "bound");
        UNIT_ASSERT_VALUES_EQUAL(expression["else"]["value"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Bool");
        UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
    }

    Y_UNIT_TEST(IfAndExistsEnforceExactTypesAndNullability) {
        TExportTestContext ctx;
        const auto* optionalBool = ScalarType(
            ctx,
            NUdf::EDataSlot::Bool,
            true);
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(
            ctx,
            NUdf::EDataSlot::Int32,
            true);
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "If",
                {
                    TypedMember(ctx, "a.x", optionalBool),
                    TypedLiteral(ctx, "Int32", "1", intType),
                    TypedLiteral(ctx, "Int32", "2", intType),
                },
                optionalInt),
            true);
        UNIT_ASSERT(expression["nullable"].GetBooleanSafe());

        const auto badIf = ExportMapExpressionResult(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "If",
                {
                    TypedMember(ctx, "a.x", optionalBool),
                    TypedLiteral(ctx, "Int32", "1", intType),
                    TypedLiteral(ctx, "Int32", "2", intType),
                },
                intType),
            true);
        UNIT_ASSERT(!badIf.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            badIf.UnsupportedReason,
            "result nullability");

        const auto badExists = ExportMapExpressionResult(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "Exists",
                {TypedMember(ctx, "a.x", optionalBool)},
                optionalBool),
            true);
        UNIT_ASSERT(!badExists.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            badExists.UnsupportedReason,
            "non-null Bool");
    }

    Y_UNIT_TEST(IfPresentBindingsAreAlphaNormalizedAndLexicallyScoped) {
        const auto exportIdentity = [](TStringBuf argumentName) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), argumentName);
            argument->SetTypeAnn(intType);
            return ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "IfPresent",
                    {
                        TypedMember(ctx, "a.x", optionalInt),
                        TypedUnaryLambda(ctx, argument, argument),
                        TypedLiteral(ctx, "Int32", "0", intType),
                    },
                    intType),
                true);
        };

        UNIT_ASSERT_VALUES_EQUAL(
            NJson::WriteJson(exportIdentity("left_name"), false, true),
            NJson::WriteJson(exportIdentity("right_name"), false, true));

        TExportTestContext nested;
        const auto* intType = ScalarType(nested, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(nested, NUdf::EDataSlot::Int32, true);
        auto outer = nested.ExprCtx.NewArgument(TPositionHandle(), "outer");
        auto inner = nested.ExprCtx.NewArgument(TPositionHandle(), "inner");
        outer->SetTypeAnn(intType);
        inner->SetTypeAnn(intType);
        const auto expression = ExportMapExpression(
            nested,
            "a",
            TypedCallable(
                nested,
                "IfPresent",
                {
                    TypedMember(nested, "a.x", optionalInt),
                    TypedUnaryLambda(
                        nested,
                        outer,
                        TypedCallable(
                            nested,
                            "IfPresent",
                            {
                                TypedCallable(nested, "Just", {outer}, optionalInt),
                                TypedUnaryLambda(
                                    nested,
                                    inner,
                                    TypedCallable(
                                        nested,
                                        "+",
                                        {outer, inner},
                                        intType)),
                                outer,
                            },
                            intType)),
                    TypedLiteral(nested, "Int32", "-1", intType),
                },
                intType),
            true);

        const auto& innerNode = expression["present"];
        UNIT_ASSERT_VALUES_EQUAL(innerNode["kind"].GetStringSafe(), "if_present");
        UNIT_ASSERT_VALUES_EQUAL(innerNode["present"]["left"]["depth"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(innerNode["present"]["right"]["depth"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(innerNode["missing"]["depth"].GetUIntegerSafe(), 0);
    }

    Y_UNIT_TEST(IfPresentBoundValuesRemainExplicitOpaqueArguments) {
        TExportTestContext ctx;
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "value");
        argument->SetTypeAnn(intType);
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "IfPresent",
                {
                    TypedMember(ctx, "a.x", optionalInt),
                    TypedUnaryLambda(
                        ctx,
                        argument,
                        TypedCallable(ctx, "Just", {argument}, optionalInt)),
                    TypedNothing(ctx, "Int32", intType, optionalInt),
                },
                optionalInt),
            true);

        const auto& present = expression["present"];
        UNIT_ASSERT_VALUES_EQUAL(present["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(present["args"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(present["args"][0]["kind"].GetStringSafe(), "bound");
        UNIT_ASSERT_VALUES_EQUAL(present["args"][0]["depth"].GetUIntegerSafe(), 0);
    }

    Y_UNIT_TEST(NormalizesExactStaticSetContainsInsideIfPresent) {
        TExportTestContext ctx;
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedStaticSetIfPresent(ctx, EStaticSetIfPresentShape::Exact),
            true);

        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if_present");
        const auto& present = expression["present"];
        UNIT_ASSERT_VALUES_EQUAL(present["kind"].GetStringSafe(), "in");
        UNIT_ASSERT_VALUES_EQUAL(present["lookup"]["kind"].GetStringSafe(), "bound");
        UNIT_ASSERT_VALUES_EQUAL(present["lookup"]["depth"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(present["items"].GetArraySafe().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(present["items"][0]["value"].GetStringSafe(), "AIR");
        UNIT_ASSERT_VALUES_EQUAL(present["items"][1]["value"].GetStringSafe(), "AIR REG");
    }

    Y_UNIT_TEST(StaticSetContainsRecognizerFailsClosed) {
        const TVector<std::pair<EStaticSetIfPresentShape, TStringBuf>> cases = {
            {
                EStaticSetIfPresentShape::NonIdentityKey,
                "key selector must be identity",
            },
            {
                EStaticSetIfPresentShape::NonVoidPayload,
                "payload selector must return Void",
            },
            {
                EStaticSetIfPresentShape::ReversedSettings,
                "settings must be exactly (One, Auto)",
            },
            {
                EStaticSetIfPresentShape::DecimalItems,
                "Decimal membership is unsupported",
            },
        };

        for (const auto& [shape, expectedReason] : cases) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedStaticSetIfPresent(ctx, shape),
                true);
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<size_t>(shape));
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReason);
        }
    }

    Y_UNIT_TEST(IfPresentFailsClosedForMalformedOrUnsafeHandlers) {
        for (size_t testCase = 0; testCase < 5; ++testCase) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* int64Type = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            auto argument = ctx.ExprCtx.NewArgument(TPositionHandle(), "value");
            argument->SetTypeAnn(testCase == 1 ? int64Type : intType);
            auto body = argument;
            auto missing = TypedLiteral(ctx, "Int32", "0", intType);
            TExprNode::TPtr optional = TypedMember(ctx, "a.x", optionalInt);
            if (testCase == 0) {
                optional = TypedMember(ctx, "a.x", intType);
            } else if (testCase == 2) {
                missing = TypedNothing(ctx, "Int32", intType, optionalInt);
            } else if (testCase == 3) {
                auto free = ctx.ExprCtx.NewArgument(TPositionHandle(), "free");
                free->SetTypeAnn(intType);
                body = free;
            } else if (testCase == 4) {
                body = TypedCallable(ctx, "Unwrap", {argument}, intType);
            }
            const auto* resultType = testCase == 2 ? optionalInt : intType;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "IfPresent",
                    {
                        std::move(optional),
                        TypedUnaryLambda(ctx, argument, std::move(body)),
                        std::move(missing),
                    },
                    resultType),
                true);
            UNIT_ASSERT_C(!result.IsSupported(), testCase);
        }
    }

    Y_UNIT_TEST(IfPresentBindingDepthIsBounded) {
        const auto nestedIfPresent = [](
            TExportTestContext& ctx,
            size_t depth,
            bool coalesceTerminal)
        {
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(
                ctx,
                NUdf::EDataSlot::Int32,
                true);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            TVector<TExprNode::TPtr> arguments;
            arguments.reserve(depth);
            for (size_t index = 0; index < depth; ++index) {
                auto argument = ctx.ExprCtx.NewArgument(
                    TPositionHandle(),
                    TStringBuilder() << "value_" << index);
                argument->SetTypeAnn(intType);
                arguments.push_back(std::move(argument));
            }

            TExprNode::TPtr expression = coalesceTerminal
                ? TypedCallable(
                    ctx,
                    "Coalesce",
                    {
                        TypedCallable(
                            ctx,
                            ">=",
                            {
                                TypedNothing(
                                    ctx,
                                    "Int32",
                                    intType,
                                    optionalInt),
                                TypedLiteral(ctx, "Int32", "1", intType),
                            },
                            optionalBool),
                        TypedLiteral(ctx, "Bool", "false", boolType),
                    },
                    boolType)
                : TypedLiteral(ctx, "Int32", "1", intType);
            const auto* resultType = coalesceTerminal ? boolType : intType;
            for (size_t index = depth; index > 0; --index) {
                const size_t level = index - 1;
                auto optional = level == 0
                    ? TypedMember(ctx, "a.x", optionalInt)
                    : TypedCallable(
                        ctx,
                        "Just",
                        {arguments[level - 1]},
                        optionalInt);
                expression = TypedCallable(
                    ctx,
                    "IfPresent",
                    {
                        std::move(optional),
                        TypedUnaryLambda(
                            ctx,
                            arguments[level],
                            std::move(expression)),
                        coalesceTerminal
                            ? TypedLiteral(ctx, "Bool", "false", boolType)
                            : TypedLiteral(ctx, "Int32", "0", intType),
                    },
                    resultType);
            }
            return expression;
        };

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                nestedIfPresent(ctx, 64, false),
                true);
            UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
        }
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                nestedIfPresent(ctx, 65, false),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "binding depth exceeds");
        }
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                nestedIfPresent(ctx, 63, true),
                true);
            UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
        }
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                nestedIfPresent(ctx, 64, true),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Coalesce false binding depth exceeds");
        }
    }

    Y_UNIT_TEST(ExportsOnlyReviewedPartialIntegralSafeCastsExactly) {
        struct TCase {
            NUdf::EDataSlot Source;
            NUdf::EDataSlot Target;
            bool SourceNullable;
        };
        const TVector<TCase> cases = {
            {NUdf::EDataSlot::Int64, NUdf::EDataSlot::Int32, true},
            {NUdf::EDataSlot::Int64, NUdf::EDataSlot::Uint64, true},
            {NUdf::EDataSlot::Uint64, NUdf::EDataSlot::Int64, true},
            {NUdf::EDataSlot::Int16, NUdf::EDataSlot::Uint8, false},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(
                ctx,
                test.Source,
                test.SourceNullable);
            const auto* targetType = ScalarType(ctx, test.Target);
            const auto* optionalTarget = ScalarType(ctx, test.Target, true);
            const TString targetName(NUdf::GetDataTypeInfo(test.Target).Name);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        OptionalDataTypeDescriptor(
                            ctx,
                            targetName,
                            targetType,
                            optionalTarget),
                    },
                    optionalTarget),
                test.SourceNullable);

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "cast_integral");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                targetName);
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
        }

        const auto assertOpaque = [](
            TStringBuf callable,
            NUdf::EDataSlot sourceSlot,
            NUdf::EDataSlot targetSlot)
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, sourceSlot, true);
            const auto* targetType = ScalarType(ctx, targetSlot);
            const auto* optionalTarget = ScalarType(ctx, targetSlot, true);
            const TString targetName(NUdf::GetDataTypeInfo(targetSlot).Name);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        OptionalDataTypeDescriptor(
                            ctx,
                            targetName,
                            targetType,
                            optionalTarget),
                    },
                    optionalTarget),
                true);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "opaque");
        };
        assertOpaque("SafeCast", NUdf::EDataSlot::Int32, NUdf::EDataSlot::Int64);
        assertOpaque("Convert", NUdf::EDataSlot::Int64, NUdf::EDataSlot::Int32);
        assertOpaque("SafeCast", NUdf::EDataSlot::Bool, NUdf::EDataSlot::Int32);
        assertOpaque("SafeCast", NUdf::EDataSlot::Date, NUdf::EDataSlot::Int32);

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const auto* targetType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DataTypeDescriptor(ctx, "Int32", targetType),
                    },
                    targetType),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "must have an optional integer result");
        }
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const auto* targetType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalTarget = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DataTypeDescriptor(ctx, "Int32", targetType),
                    },
                    optionalTarget),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "target does not match its optional result");
        }
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const auto* targetType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalTarget = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto* wrongAnnotation = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            auto descriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DataTypeDescriptor(ctx, "Int32", targetType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(wrongAnnotation));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        std::move(descriptor),
                    },
                    optionalTarget),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "annotation disagrees with its descriptor");
        }
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const auto* optionalTarget = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto* wrongItemAnnotation = ScalarType(ctx, NUdf::EDataSlot::Int64);
            auto descriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DataTypeDescriptor(ctx, "Int32", wrongItemAnnotation)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalTarget));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        std::move(descriptor),
                    },
                    optionalTarget),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "item annotation disagrees with its descriptor");
        }
    }

    Y_UNIT_TEST(ExportsDecimalNothingAndRejectsGeneralDecimalCast) {
        TExportTestContext ctx;
        const auto* decimalType = ctx.ExprCtx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal,
            "5",
            "2");
        const auto* optionalDecimal = ctx.ExprCtx.MakeType<TOptionalExprType>(decimalType);
        auto optionalDescriptor = TypedCallable(
            ctx,
            "OptionalType",
            {DecimalDataTypeDescriptor(ctx, "5", "2", decimalType)},
            ctx.ExprCtx.MakeType<TTypeExprType>(optionalDecimal));
        const auto nullExpression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "Nothing",
                {std::move(optionalDescriptor)},
                optionalDecimal));
        UNIT_ASSERT_VALUES_EQUAL(nullExpression["kind"].GetStringSafe(), "null");
        UNIT_ASSERT_VALUES_EQUAL(nullExpression["type"].GetStringSafe(), "Decimal(5,2)");

        TExportTestContext castContext;
        const auto* intType = ScalarType(castContext, NUdf::EDataSlot::Int32);
        const auto* castDecimal = castContext.ExprCtx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal,
            "5",
            "2");
        const auto* optionalCastDecimal = castContext.ExprCtx.MakeType<TOptionalExprType>(castDecimal);
        const auto castResult = ExportMapExpressionResult(
            castContext,
            "a",
            TypedCallable(
                castContext,
                "SafeCast",
                {
                    TypedMember(castContext, "a.x", intType),
                    DecimalDataTypeDescriptor(castContext, "5", "2", castDecimal),
                },
                optionalCastDecimal));
        UNIT_ASSERT(!castResult.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            castResult.UnsupportedReason,
            "target does not match its result");
    }

    Y_UNIT_TEST(MalformedDecimalTypeDescriptorsFailClosed) {
        for (const auto& [precision, scale] : TVector<std::pair<TString, TString>>{
            {"0", "0"},
            {"05", "2"},
            {"5", "02"},
            {"5", "6"},
            {"36", "2"},
            {"5", " 2"},
        }) {
            TExportTestContext ctx;
            const auto* decimalType = ctx.ExprCtx.MakeType<TDataExprParamsType>(
                NUdf::EDataSlot::Decimal,
                "5",
                "2");
            const auto* optionalDecimal = ctx.ExprCtx.MakeType<TOptionalExprType>(decimalType);
            auto optionalDescriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DecimalDataTypeDescriptor(ctx, precision, scale, decimalType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalDecimal));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Nothing",
                    {std::move(optionalDescriptor)},
                    optionalDecimal));
            UNIT_ASSERT_C(!result.IsSupported(), TStringBuilder() << precision << "," << scale);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "unsupported DataType descriptor");
        }

        for (const TString descriptorName : {"Decimal", "Decimal(5,2)"}) {
            TExportTestContext ctx;
            const auto* decimalType = ctx.ExprCtx.MakeType<TDataExprParamsType>(
                NUdf::EDataSlot::Decimal,
                "5",
                "2");
            const auto* optionalDecimal = ctx.ExprCtx.MakeType<TOptionalExprType>(decimalType);
            auto optionalDescriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DataTypeDescriptor(ctx, descriptorName, decimalType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalDecimal));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Nothing",
                    {std::move(optionalDescriptor)},
                    optionalDecimal));
            UNIT_ASSERT_C(!result.IsSupported(), descriptorName);
        }
    }

    Y_UNIT_TEST(ExportsCanonicalDecimalLiteralsAgainstNDecimal) {
        struct TCase {
            TString Input;
            TString Precision;
            TString Scale;
            TString Kind;
            TString Scaled;
        };
        const TVector<TCase> cases = {
            {"12.340", "5", "2", "finite", "1234"},
            {"+000.005", "5", "2", "finite", "0"},
            {"0.015", "5", "2", "finite", "2"},
            {"-0.015", "5", "2", "finite", "-2"},
            {"0.995", "2", "2", "pos_inf", ""},
            {"-0.995", "2", "2", "neg_inf", ""},
            {"nan", "5", "2", "nan", ""},
            {"INF", "5", "2", "pos_inf", ""},
            {"-inf", "5", "2", "neg_inf", ""},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedDecimalLiteral(
                    ctx,
                    test.Input,
                    test.Precision,
                    test.Scale,
                    DecimalType(ctx, test.Precision, test.Scale)));

            const ui8 precision = FromString<ui8>(test.Precision);
            const ui8 scale = FromString<ui8>(test.Scale);
            const auto oracle = NYql::NDecimal::FromString(test.Input, precision, scale);
            UNIT_ASSERT(!NYql::NDecimal::IsError(oracle));

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                TStringBuilder() << "Decimal(" << test.Precision << "," << test.Scale << ")");
            const auto& value = expression["value"];
            UNIT_ASSERT_VALUES_EQUAL(value["kind"].GetStringSafe(), test.Kind);
            if (test.Kind == "finite") {
                UNIT_ASSERT(NYql::NDecimal::IsNormal(oracle, precision));
                const TString oracleScaled(
                    NYql::NDecimal::ToString(
                        oracle,
                        NYql::NDecimal::MaxPrecision,
                        0));
                UNIT_ASSERT_VALUES_EQUAL(oracleScaled, test.Scaled);
                UNIT_ASSERT_VALUES_EQUAL(value["scaled"].GetStringSafe(), oracleScaled);
                UNIT_ASSERT_VALUES_EQUAL(value.GetMapSafe().size(), 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(value.GetMapSafe().size(), 1);
                if (test.Kind == "nan") {
                    UNIT_ASSERT(NYql::NDecimal::IsNan(oracle));
                } else if (test.Kind == "pos_inf") {
                    UNIT_ASSERT(oracle == NYql::NDecimal::Inf());
                } else {
                    UNIT_ASSERT(oracle == -NYql::NDecimal::Inf());
                }
            }
        }
    }

    Y_UNIT_TEST(MalformedDecimalLiteralsFailClosed) {
        for (const TString value : {"", "12.2.3", "+-12", "not-a-decimal"}) {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDecimalLiteral(
                    ctx,
                    value,
                    "5",
                    "2",
                    DecimalType(ctx, "5", "2")));
            UNIT_ASSERT_C(!result.IsSupported(), value);
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Invalid Decimal(5,2) literal");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDecimalLiteral(
                    ctx,
                    "1",
                    "05",
                    "2",
                    DecimalType(ctx, "5", "2")));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "invalid precision or scale");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedDecimalLiteral(
                    ctx,
                    "1",
                    "5",
                    "2",
                    DecimalType(ctx, "6", "2")));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "type annotation does not match");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Decimal",
                    {ctx.ExprCtx.NewAtom(TPositionHandle(), "1")},
                    DecimalType(ctx, "5", "2")));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported Decimal literal callable");
        }
    }

    Y_UNIT_TEST(NormalizesCompleteIntegerConstantDecimalCasts) {
        struct TCase {
            TString Callable;
            NUdf::EDataSlot SourceSlot;
            TString SourceType;
            TString Input;
            TString Scaled;
        };
        const TVector<TCase> cases = {
            {"SafeCast", NUdf::EDataSlot::Int32, "Int32", "100", "10000"},
            {"SafeCast", NUdf::EDataSlot::Int32, "Int32", "-2147483648", "-214748364800"},
            {"Convert", NUdf::EDataSlot::Uint32, "Uint32", "4000000000", "400000000000"},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, test.SourceSlot);
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    test.Callable,
                    {
                        TypedLiteral(ctx, test.SourceType, test.Input, sourceType),
                        DecimalDataTypeDescriptor(ctx, "12", "2", decimalType),
                    },
                    decimalType));

            const auto oracle = NYql::NDecimal::FromString(test.Input, 12, 2);
            UNIT_ASSERT(NYql::NDecimal::IsNormal(oracle, 12));
            UNIT_ASSERT_VALUES_EQUAL(
                TString(NYql::NDecimal::ToString(
                    oracle,
                    NYql::NDecimal::MaxPrecision,
                    0)),
                test.Scaled);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(12,2)");
            UNIT_ASSERT_VALUES_EQUAL(expression["value"]["kind"].GetStringSafe(), "finite");
            UNIT_ASSERT_VALUES_EQUAL(expression["value"]["scaled"].GetStringSafe(), test.Scaled);
        }
    }

    Y_UNIT_TEST(FoldsDirectTextLiteralDecimalSafeCastsWithRuntimeOracle) {
        struct TCase {
            TString Source;
            TString Input;
            TString Precision;
            TString Scale;
            TString Kind;
            TString Scaled;
        };
        const TVector<TCase> cases = {
            // TPC-DS q65 shape and identical String/Utf8 paths.
            {"String", "0.1", "35", "2", "finite", "10"},
            {"Utf8", "0.1", "35", "2", "finite", "10"},
            // FromStringEx-only syntax and round-half-to-even boundaries.
            {"String", "0.1E3", "10", "1", "finite", "1000"},
            {"Utf8", "1.225", "5", "2", "finite", "122"},
            {"String", "1.235", "5", "2", "finite", "124"},
            {"String", "-1.225", "5", "2", "finite", "-122"},
            {"Utf8", "-1.235", "5", "2", "finite", "-124"},
            // Specials are successful Decimal values, not failed casts.
            {"String", "NaN", "5", "2", "nan", ""},
            {"Utf8", "+INF", "5", "2", "pos_inf", ""},
            {"String", "-inf", "5", "2", "neg_inf", ""},
            // Numeric overflow saturates, while underflow rounds to zero.
            {"Utf8", "9.995", "3", "2", "pos_inf", ""},
            {"String", "-9.995", "3", "2", "neg_inf", ""},
            {"Utf8", "1E30", "10", "0", "pos_inf", ""},
            {"String", "1e-30", "10", "0", "finite", "0"},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const ui8 precision = FromString<ui8>(test.Precision);
            const ui8 scale = FromString<ui8>(test.Scale);
            const auto oracle = NYql::NDecimal::FromStringEx(
                test.Input,
                precision,
                scale);
            UNIT_ASSERT_C(!NYql::NDecimal::IsError(oracle), test.Input);

            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedTextLiteralDecimalCast(
                    ctx,
                    "SafeCast",
                    test.Source,
                    test.Input,
                    test.Precision,
                    test.Scale));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                TStringBuilder()
                    << "Decimal(" << test.Precision << "," << test.Scale << ")");
            const auto& value = expression["value"];
            UNIT_ASSERT_VALUES_EQUAL(value["kind"].GetStringSafe(), test.Kind);
            if (test.Kind == "finite") {
                UNIT_ASSERT(NYql::NDecimal::IsNormal(oracle, precision));
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(NYql::NDecimal::ToString(
                        oracle,
                        NYql::NDecimal::MaxPrecision,
                        0)),
                    test.Scaled);
                UNIT_ASSERT_VALUES_EQUAL(value["scaled"].GetStringSafe(), test.Scaled);
                UNIT_ASSERT_VALUES_EQUAL(value.GetMapSafe().size(), 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(value.GetMapSafe().size(), 1);
                if (test.Kind == "nan") {
                    UNIT_ASSERT(NYql::NDecimal::IsNan(oracle));
                } else if (test.Kind == "pos_inf") {
                    UNIT_ASSERT(oracle == NYql::NDecimal::Inf());
                } else {
                    UNIT_ASSERT(oracle == -NYql::NDecimal::Inf());
                }
            }
        }
    }

    Y_UNIT_TEST(InvalidDirectTextLiteralDecimalSafeCastsBecomeTypedNull) {
        const TVector<TString> invalid = {
            "not-a-decimal",
            "12.2.3",
            "+-12",
            "NANE5",
            "1e+",
            " 1",
            "1 ",
        };
        for (const TString source : {"String", "Utf8"}) {
            for (const auto& input : invalid) {
                TExportTestContext ctx;
                UNIT_ASSERT(NYql::NDecimal::IsError(
                    NYql::NDecimal::FromStringEx(input, 12, 2)));
                const auto expression = ExportMapExpression(
                    ctx,
                    "a",
                    TypedTextLiteralDecimalCast(
                        ctx,
                        "SafeCast",
                        source,
                        input,
                        "12",
                        "2"));
                UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "null");
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["type"].GetStringSafe(),
                    "Decimal(12,2)");
            }
        }
    }

    Y_UNIT_TEST(DirectTextLiteralDecimalSafeCastRejectsNonnormalExponentPayload) {
        const auto oracle = NYql::NDecimal::FromStringEx("0.1E3", 2, 0);
        UNIT_ASSERT(!NYql::NDecimal::IsError(oracle));
        UNIT_ASSERT(NYql::NDecimal::IsNormal(oracle));
        UNIT_ASSERT(!NYql::NDecimal::IsNormal(oracle, 2));

        TExportTestContext ctx;
        const auto result = ExportMapExpressionResult(
            ctx,
            "a",
            TypedTextLiteralDecimalCast(
                ctx,
                "SafeCast",
                "String",
                "0.1E3",
                "2",
                "0"));
        UNIT_ASSERT(!result.IsSupported());
    }

    Y_UNIT_TEST(DirectTextLiteralDecimalSafeCastGateFailsClosed) {
        auto checkUnsupported = [](
            TStringBuf label,
            auto&& makeExpression)
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                makeExpression(ctx));
            UNIT_ASSERT_C(
                !result.IsSupported(),
                TStringBuilder() << label << " unexpectedly exported " << result.Json);
        };

        checkUnsupported("empty String literal", [](TExportTestContext& ctx) {
            return TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "", "5", "2");
        });
        checkUnsupported("non-ASCII Utf8 literal", [](TExportTestContext& ctx) {
            const TString nonAscii("\xC3\xA9", 2);
            return TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "Utf8", nonAscii, "5", "2");
        });
        checkUnsupported("dynamic source", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::String);
            const auto* decimalType = DecimalType(ctx, "5", "2");
            const auto* optionalType = DecimalType(ctx, "5", "2", true);
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedMember(ctx, "a.x", sourceType),
                    OptionalDecimalDataTypeDescriptor(
                        ctx, "5", "2", decimalType, optionalType),
                },
                optionalType);
        });
        checkUnsupported("nullable source", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "Utf8", "0.1", "5", "2");
            expression->Child(0)->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Utf8, true));
            return expression;
        });
        checkUnsupported("source annotation mismatch", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "0.1", "5", "2");
            expression->Child(0)->SetTypeAnn(
                ScalarType(ctx, NUdf::EDataSlot::Utf8));
            return expression;
        });
        checkUnsupported("Convert", [](TExportTestContext& ctx) {
            return TypedTextLiteralDecimalCast(
                ctx, "Convert", "String", "0.1", "5", "2");
        });
        checkUnsupported("StrictCast", [](TExportTestContext& ctx) {
            return TypedTextLiteralDecimalCast(
                ctx, "StrictCast", "Utf8", "0.1", "5", "2");
        });
        checkUnsupported("non-optional result", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "0.1", "5", "2");
            expression->SetTypeAnn(DecimalType(ctx, "5", "2"));
            return expression;
        });
        checkUnsupported("non-optional target", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::String);
            const auto* decimalType = DecimalType(ctx, "5", "2");
            const auto* optionalType = DecimalType(ctx, "5", "2", true);
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedLiteral(ctx, "String", "0.1", sourceType),
                    DecimalDataTypeDescriptor(ctx, "5", "2", decimalType),
                },
                optionalType);
        });
        checkUnsupported("outer target annotation mismatch", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "0.1", "5", "2");
            expression->Child(1)->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(
                DecimalType(ctx, "6", "2", true)));
            return expression;
        });
        checkUnsupported("nested target annotation mismatch", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "Utf8", "0.1", "5", "2");
            expression->Child(1)->Child(0)->SetTypeAnn(
                ctx.ExprCtx.MakeType<TTypeExprType>(DecimalType(ctx, "6", "2")));
            return expression;
        });
        checkUnsupported("missing outer target annotation", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "0.1", "5", "2");
            expression->Child(1)->SetTypeAnn(nullptr);
            return expression;
        });
        checkUnsupported("missing nested target annotation", [](TExportTestContext& ctx) {
            auto expression = TypedTextLiteralDecimalCast(
                ctx, "SafeCast", "String", "0.1", "5", "2");
            expression->Child(1)->Child(0)->SetTypeAnn(nullptr);
            return expression;
        });
        checkUnsupported("malformed Decimal descriptor", [](TExportTestContext& ctx) {
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::String);
            const auto* decimalType = DecimalType(ctx, "5", "2");
            const auto* optionalType = DecimalType(ctx, "5", "2", true);
            auto target = TypedCallable(
                ctx,
                "OptionalType",
                {DecimalDataTypeDescriptor(ctx, "05", "2", decimalType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalType));
            return TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedLiteral(ctx, "String", "0.1", sourceType),
                    std::move(target),
                },
                optionalType);
        });
    }

    Y_UNIT_TEST(ExportsExactIntegralSafeCastsToDecimal) {
        const TVector<NUdf::EDataSlot> slots = {
            NUdf::EDataSlot::Int8,
            NUdf::EDataSlot::Uint8,
            NUdf::EDataSlot::Int16,
            NUdf::EDataSlot::Uint16,
            NUdf::EDataSlot::Int32,
            NUdf::EDataSlot::Uint32,
            NUdf::EDataSlot::Int64,
            NUdf::EDataSlot::Uint64,
        };

        for (const auto slot : slots) {
            TExportTestContext ctx;
            const TString sourceName(NUdf::GetDataTypeInfo(slot).Name);
            const auto* sourceType = ScalarType(ctx, slot);
            const auto* decimalType = DecimalType(ctx, "35", "4");
            const auto expression = ExportTypedMapExpression(
                ctx,
                "a",
                sourceName,
                false,
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "35", "4", decimalType),
                    },
                    decimalType));

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "cast_decimal");
            UNIT_ASSERT_VALUES_EQUAL(expression["arg"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL(expression["arg"]["column"].GetStringSafe(), "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["source_type"].GetStringSafe(),
                sourceName);
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(35,4)");
            UNIT_ASSERT_VALUES_EQUAL(expression["nullable"].GetBooleanSafe(), false);
        }
    }

    Y_UNIT_TEST(ExportsNullableIntegralAndDecimalWideningSafeCastsExactly) {
        for (const bool decimalSource : {false, true}) {
            TExportTestContext ctx;
            const auto* sourceType = decimalSource
                ? DecimalType(ctx, "7", "2", true)
                : ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const TString sourceName = decimalSource
                ? "Decimal(7,2)"
                : "Int64";
            const auto* targetType = DecimalType(ctx, "12", "2");
            const auto* optionalTargetType = DecimalType(ctx, "12", "2", true);
            const auto expression = ExportTypedMapExpression(
                ctx,
                "a",
                sourceName,
                true,
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        OptionalDecimalDataTypeDescriptor(
                            ctx,
                            "12",
                            "2",
                            targetType,
                            optionalTargetType),
                    },
                    optionalTargetType));

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["kind"].GetStringSafe(),
                "cast_decimal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["kind"].GetStringSafe(),
                "column");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["column"].GetStringSafe(),
                "a.x");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["source_type"].GetStringSafe(),
                sourceName);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                "Decimal(12,2)");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(ExportsNonNullableDecimalWideningSafeCastExactly) {
        TExportTestContext ctx;
        const auto* sourceType = DecimalType(ctx, "7", "2");
        const auto* targetType = DecimalType(ctx, "12", "2");
        const auto expression = ExportTypedMapExpression(
            ctx,
            "a",
            "Decimal(7,2)",
            false,
            TypedCallable(
                ctx,
                "SafeCast",
                {
                    TypedMember(ctx, "a.x", sourceType),
                    DecimalDataTypeDescriptor(
                        ctx,
                        "12",
                        "2",
                        targetType),
                },
                targetType));

        UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(
            expression["kind"].GetStringSafe(),
            "cast_decimal");
        UNIT_ASSERT_VALUES_EQUAL(
            expression["source_type"].GetStringSafe(),
            "Decimal(7,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            expression["type"].GetStringSafe(),
            "Decimal(12,2)");
        UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
    }

    Y_UNIT_TEST(IncompleteIntegralSafeCastLiteralsRemainExplicit) {
        struct TCase {
            NUdf::EDataSlot SourceSlot;
            TString SourceType;
            TString Input;
            TString Precision;
            TString Scale;
        };
        const TVector<TCase> cases = {
            {NUdf::EDataSlot::Int8, "Int8", "9", "3", "2"},
            {NUdf::EDataSlot::Int8, "Int8", "100", "3", "2"},
            {NUdf::EDataSlot::Int8, "Int8", "-100", "3", "2"},
            {NUdf::EDataSlot::Uint64, "Uint64", "10000000000000000000", "19", "0"},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, test.SourceSlot);
            const auto* decimalType = DecimalType(ctx, test.Precision, test.Scale);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedLiteral(ctx, test.SourceType, test.Input, sourceType),
                        DecimalDataTypeDescriptor(
                            ctx,
                            test.Precision,
                            test.Scale,
                            decimalType),
                    },
                    decimalType));

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "cast_decimal");
            UNIT_ASSERT_VALUES_EQUAL(expression["arg"]["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["arg"]["type"].GetStringSafe(),
                test.SourceType);
            UNIT_ASSERT_VALUES_EQUAL(
                expression["source_type"].GetStringSafe(),
                test.SourceType);
            if (test.SourceSlot == NUdf::EDataSlot::Uint64) {
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["arg"]["value"].GetUIntegerSafe(),
                    FromString<ui64>(test.Input));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    expression["arg"]["value"].GetIntegerSafe(),
                    FromString<i64>(test.Input));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                expression["type"].GetStringSafe(),
                TStringBuilder() << "Decimal(" << test.Precision << "," << test.Scale << ")");
            UNIT_ASSERT_VALUES_EQUAL(expression["nullable"].GetBooleanSafe(), false);
        }
    }

    Y_UNIT_TEST(DecimalSafeCastsFailClosedOutsideExactGate) {
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "Convert",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "12", "2", decimalType),
                    },
                    decimalType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "constant Decimal cast source is not a non-nullable integer literal");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto* decimalType = DecimalType(ctx, "12", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "12", "2", decimalType),
                    },
                    decimalType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "result nullability must match its source");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* decimalType = DecimalType(ctx, "11", "2");
            const auto* optionalDecimalType = DecimalType(ctx, "11", "2", true);
            auto optionalDescriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DecimalDataTypeDescriptor(ctx, "11", "2", decimalType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalDecimalType));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedLiteral(ctx, "Int32", "2147483647", sourceType),
                        std::move(optionalDescriptor),
                    },
                    optionalDecimalType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "result nullability must match its source");
        }

        struct TDecimalShape {
            TStringBuf SourcePrecision;
            TStringBuf SourceScale;
            TStringBuf TargetPrecision;
            TStringBuf TargetScale;
        };
        const TVector<TDecimalShape> decimalNearMisses = {
            {"13", "2", "12", "2"},
            {"7", "2", "12", "3"},
        };
        for (const auto& test : decimalNearMisses) {
            TExportTestContext ctx;
            const auto* sourceType = DecimalType(
                ctx,
                test.SourcePrecision,
                test.SourceScale,
                true);
            const auto* targetType = DecimalType(
                ctx,
                test.TargetPrecision,
                test.TargetScale);
            const auto* optionalTargetType = DecimalType(
                ctx,
                test.TargetPrecision,
                test.TargetScale,
                true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        OptionalDecimalDataTypeDescriptor(
                            ctx,
                            test.TargetPrecision,
                            test.TargetScale,
                            targetType,
                            optionalTargetType),
                    },
                    optionalTargetType),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "same-scale widening");
        }

        for (const bool corruptItemAnnotation : {false, true}) {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(
                ctx,
                NUdf::EDataSlot::Int64,
                true);
            const auto* targetType = DecimalType(ctx, "12", "2");
            const auto* optionalTargetType = DecimalType(ctx, "12", "2", true);
            auto target = OptionalDecimalDataTypeDescriptor(
                ctx,
                "12",
                "2",
                targetType,
                optionalTargetType);
            if (corruptItemAnnotation) {
                target->Child(0)->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(
                    DecimalType(ctx, "13", "2")));
            } else {
                target->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(
                    DecimalType(ctx, "13", "2", true)));
            }
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        std::move(target),
                    },
                    optionalTargetType),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                corruptItemAnnotation
                    ? "item annotation disagrees with its descriptor"
                    : "target annotation disagrees with its descriptor");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Bool, true);
            const auto* targetType = DecimalType(ctx, "12", "2");
            const auto* optionalTargetType = DecimalType(ctx, "12", "2", true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        OptionalDecimalDataTypeDescriptor(
                            ctx,
                            "12",
                            "2",
                            targetType,
                            optionalTargetType),
                    },
                    optionalTargetType),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "source must be an integer or Decimal");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* targetType = DecimalType(ctx, "4", "4");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "4", "4", targetType),
                    },
                    targetType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "at least one integral digit");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* resultType = DecimalType(ctx, "15", "4");
            const auto* targetType = DecimalType(ctx, "14", "4");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "14", "4", targetType),
                    },
                    resultType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "target does not match its result");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* resultType = DecimalType(ctx, "15", "4");
            auto targetDescriptor = DecimalDataTypeDescriptor(
                ctx,
                "15",
                "4",
                resultType);
            targetDescriptor->SetTypeAnn(nullptr);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        std::move(targetDescriptor),
                    },
                    resultType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "target is missing its Type annotation");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* resultType = DecimalType(ctx, "15", "4");
            const auto* annotationType = DecimalType(ctx, "14", "4");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "15", "4", annotationType),
                    },
                    resultType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "target annotation disagrees with its descriptor");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* targetType = DecimalType(ctx, "15", "4");
            const auto* optionalTargetType = DecimalType(ctx, "15", "4", true);
            auto optionalDescriptor = TypedCallable(
                ctx,
                "OptionalType",
                {DecimalDataTypeDescriptor(ctx, "15", "4", targetType)},
                ctx.ExprCtx.MakeType<TTypeExprType>(optionalTargetType));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        std::move(optionalDescriptor),
                    },
                    targetType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "target does not match its result");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* targetType = DecimalType(ctx, "15", "4");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "StrictCast",
                    {
                        TypedMember(ctx, "a.x", sourceType),
                        DecimalDataTypeDescriptor(ctx, "15", "4", targetType),
                    },
                    targetType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Unsupported scalar callable StrictCast");
        }
    }

    Y_UNIT_TEST(ExportsQ48DecimalComparisonAndConstantCastShape) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DecimalComparison", {
            {"d", "Decimal(7,2)", false},
        });
        auto read = MakeRead(ctx, table, "a", {"d"});
        const auto* optionalColumnType = DecimalType(ctx, "7", "2", true);
        const auto* constantType = DecimalType(ctx, "12", "2");
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalBool = ScalarType(ctx, NUdf::EDataSlot::Bool, true);
        SetExactOutputType(ctx, *read, {{"a.d", optionalColumnType}});

        auto cast = TypedCallable(
            ctx,
            "SafeCast",
            {
                TypedLiteral(ctx, "Int32", "100", intType),
                DecimalDataTypeDescriptor(ctx, "12", "2", constantType),
            },
            constantType);
        auto map = MakeIntrusive<TOpMap>(
            read,
            TPositionHandle(),
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                TExpression(
                    TypedCallable(
                        ctx,
                        ">=",
                        {
                            TypedMember(ctx, "a.d", optionalColumnType),
                            std::move(cast),
                        },
                        optionalBool),
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps))});
        TOpRoot root(map, TPositionHandle(), {"result"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& expression = FindNode(snapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];
        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "gte");
        UNIT_ASSERT_VALUES_EQUAL(expression["left"]["column"].GetStringSafe(), "a.d");
        UNIT_ASSERT_VALUES_EQUAL(expression["right"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"]["kind"].GetStringSafe(), "finite");
        UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"]["scaled"].GetStringSafe(), "10000");
    }

    Y_UNIT_TEST(ExportsAuditedDecimalComparisons) {
        for (const auto& [callable, kind] : TVector<std::pair<TString, TString>>{
            {"==", "eq"},
            {"<", "lt"},
            {"<=", "lte"},
            {">", "gt"},
            {">=", "gte"},
        }) {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedDecimalLiteral(
                            ctx,
                            "1.20",
                            "5",
                            "2",
                            DecimalType(ctx, "5", "2")),
                        TypedDecimalLiteral(
                            ctx,
                            "1.201",
                            "7",
                            "3",
                            DecimalType(ctx, "7", "3")),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), kind);
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["value"]["scaled"].GetStringSafe(), "120");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"]["scaled"].GetStringSafe(), "1201");
        }

        {
            TExportTestContext ctx;
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "!=",
                    {
                        TypedDecimalLiteral(
                            ctx,
                            "1",
                            "5",
                            "2",
                            DecimalType(ctx, "5", "2")),
                        TypedDecimalLiteral(
                            ctx,
                            "2",
                            "5",
                            "2",
                            DecimalType(ctx, "5", "2")),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "not");
            UNIT_ASSERT_VALUES_EQUAL(expression["arg"]["kind"].GetStringSafe(), "eq");
        }

        for (const bool decimalOnLeft : {false, true}) {
            TExportTestContext ctx;
            auto decimal = TypedDecimalLiteral(
                ctx,
                "1.25",
                "5",
                "2",
                DecimalType(ctx, "5", "2"));
            auto integer = TypedLiteral(
                ctx,
                "Int32",
                "2",
                ScalarType(ctx, NUdf::EDataSlot::Int32));
            TExprNode::TListType operands;
            if (decimalOnLeft) {
                operands = {std::move(decimal), std::move(integer)};
            } else {
                operands = {std::move(integer), std::move(decimal)};
            }
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    ">",
                    std::move(operands),
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "gt");
        }

        {
            TExportTestContext ctx;
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "IsNotDistinctFrom",
                    {
                        TypedMember(ctx, "a.x", optionalDecimal),
                        TypedMember(ctx, "a.y", optionalDecimal),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)),
                true);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "eq");
            UNIT_ASSERT(expression["null_safe"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(DecimalComparisonTypeAndNullabilityMismatchesFailClosed) {
        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    ">=",
                    {
                        TypedDecimalLiteral(
                            ctx,
                            "1",
                            "35",
                            "0",
                            DecimalType(ctx, "35", "0")),
                        TypedDecimalLiteral(
                            ctx,
                            "0.1",
                            "35",
                            "35",
                            DecimalType(ctx, "35", "35")),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "comparison operand types differ");
        }


        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    ">=",
                    {
                        TypedLiteral(
                            ctx,
                            "Int32",
                            "1",
                            ScalarType(ctx, NUdf::EDataSlot::Int32)),
                        TypedDecimalLiteral(
                            ctx,
                            "0.1",
                            "35",
                            "35",
                            DecimalType(ctx, "35", "35")),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "comparison operand types differ");
        }

        {
            TExportTestContext ctx;
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "==",
                    {
                        TypedDecimalLiteral(
                            ctx,
                            "1",
                            "5",
                            "2",
                            DecimalType(ctx, "5", "2")),
                        TypedDecimalLiteral(
                            ctx,
                            "1",
                            "6",
                            "2",
                            DecimalType(ctx, "6", "2")),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool, true)));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "inconsistent nullability");
        }

        {
            TExportTestContext ctx;
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "IsNotDistinctFrom",
                    {
                        TypedMember(ctx, "a.x", optionalDecimal),
                        TypedMember(ctx, "a.y", DecimalType(ctx, "6", "2", true)),
                    },
                    ScalarType(ctx, NUdf::EDataSlot::Bool)),
                true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "exactly the same type");
        }
    }

    Y_UNIT_TEST(DecimalComparisonOracleCoversSpecialsAndScaleSaturation) {
        using namespace NYql::NDecimal;

        const auto [q48Left, q48Right] = DecimalOracleAlign(
            FromString("100.00", 7, 2),
            {7, 2},
            FromString("100", 12, 2),
            {12, 2});
        UNIT_ASSERT(IsEqual(q48Left, q48Right));
        UNIT_ASSERT(IsGreaterOrEqual(q48Left, q48Right));

        const auto finite = DecimalOracleAlignScale(TInt128(9), {35, 0}, 34);
        const auto saturated = DecimalOracleAlignScale(TInt128(10), {35, 0}, 34);
        UNIT_ASSERT(IsNormal(finite, 35));
        UNIT_ASSERT(finite == TInt128(9) * TInt128(GetDivider(34)));
        UNIT_ASSERT(saturated == Inf());
        UNIT_ASSERT(IsLess(finite, saturated));
        UNIT_ASSERT(IsEqual(saturated, Inf()));

        const auto maxInt64 = DecimalOracleAlignScale(
            TInt128(std::numeric_limits<i64>::max()),
            {NUdf::GetDataTypeInfo(NUdf::EDataSlot::Int64).DecimalDigits, 0},
            18);
        UNIT_ASSERT(maxInt64 == Inf());

        UNIT_ASSERT(!IsEqual(Nan(), Nan()));
        UNIT_ASSERT(IsNotEqual(Nan(), Nan()));
        UNIT_ASSERT(!IsLess(Nan(), Inf()));
        UNIT_ASSERT(!IsGreaterOrEqual(Inf(), Nan()));
        UNIT_ASSERT(IsEqual(Inf(), Inf()));
        UNIT_ASSERT(IsLess(-Inf(), Inf()));
    }

    Y_UNIT_TEST(ExportsCanonicalVoidCountStarExtractor) {
        TExportTestContext ctx;
        auto extractor = TypedCallable(
            ctx,
            "Void",
            {},
            ctx.ExprCtx.MakeType<TVoidExprType>());

        const auto expression = ExportMapExpression(
            ctx,
            "a",
            std::move(extractor));
        UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "void");
    }

    Y_UNIT_TEST(MalformedVoidCountStarExtractorsFailClosed) {
        {
            TExportTestContext ctx;
            auto extractor = ctx.ExprCtx.NewCallable(
                TPositionHandle(),
                "Void",
                {});
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(extractor));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Void expression is not typed Void");
        }

        {
            TExportTestContext ctx;
            auto extractor = TypedCallable(
                ctx,
                "Void",
                {ctx.ExprCtx.NewAtom(TPositionHandle(), "unexpected")},
                ctx.ExprCtx.MakeType<TVoidExprType>());
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(extractor));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Void must have no arguments");
        }

        {
            TExportTestContext ctx;
            auto extractor = TypedCallable(
                ctx,
                "Void",
                {},
                ScalarType(ctx, NUdf::EDataSlot::Int32));
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                std::move(extractor));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Void expression is not typed Void");
        }
    }

    Y_UNIT_TEST(ExportsExactStaticSqlInTupleAndAsList) {
        for (const bool useAsList : {false, true}) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto* optionalBool = ScalarType(ctx, NUdf::EDataSlot::Bool, true);

            TExprNode::TListType items;
            items.push_back(TypedLiteral(ctx, "Int32", "1", intType));
            items.push_back(TypedCallable(
                ctx,
                "+",
                {
                    TypedLiteral(ctx, "Int32", "1", intType),
                    TypedLiteral(ctx, "Int32", "2", intType),
                },
                intType));
            auto collection = useAsList
                ? TypedStaticAsList(ctx, std::move(items), intType)
                : TypedStaticTuple(ctx, std::move(items), intType);

            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    std::move(collection),
                    TypedMember(ctx, "a.x", optionalInt),
                    SqlInOptions(ctx, {
                        "ansi",
                        "warnNoAnsi",
                        "isCompact",
                        "nullsProcessed",
                    }),
                    optionalBool),
                true);

            UNIT_ASSERT_VALUES_EQUAL(expression.GetMapSafe().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "in");
            UNIT_ASSERT_VALUES_EQUAL(expression["lookup"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL(expression["lookup"]["column"].GetStringSafe(), "a.x");
            const auto& exportedItems = expression["items"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(exportedItems.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(exportedItems[0]["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(exportedItems[0]["value"].GetIntegerSafe(), 1);
            UNIT_ASSERT_VALUES_EQUAL(exportedItems[1]["kind"].GetStringSafe(), "add");
        }
    }

    Y_UNIT_TEST(ExportsLosslessMixedIntegerStaticSqlIn) {
        for (const bool useAsList : {false, true}) {
            TExportTestContext ctx;
            const auto* int32Type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt64 = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
            const auto* optionalBool = ScalarType(ctx, NUdf::EDataSlot::Bool, true);
            TExprNode::TListType items = {
                TypedLiteral(ctx, "Int32", "-1", int32Type),
                TypedLiteral(ctx, "Int32", "2", int32Type),
            };
            auto collection = useAsList
                ? TypedStaticAsList(ctx, std::move(items), int32Type)
                : TypedStaticTuple(ctx, std::move(items), int32Type);

            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    std::move(collection),
                    TypedMember(ctx, "a.x", optionalInt64),
                    SqlInOptions(ctx, {}),
                    optionalBool),
                true);

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "in");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["items"][0]["type"].GetStringSafe(),
                "Int32");
        }
    }

    Y_UNIT_TEST(DecimalStaticSqlInFailsClosed) {
        TExportTestContext ctx;
        const auto* decimalType = DecimalType(ctx, "7", "2");
        const auto* optionalDecimalType = DecimalType(ctx, "7", "2", true);
        auto collection = TypedStaticTuple(
            ctx,
            {TypedDecimalLiteral(ctx, "1.25", "7", "2", decimalType)},
            decimalType);

        const auto result = ExportMapExpressionResult(
            ctx,
            "a",
            TypedSqlIn(
                ctx,
                std::move(collection),
                TypedMember(ctx, "a.d", optionalDecimalType),
                SqlInOptions(ctx, {}),
                ScalarType(ctx, NUdf::EDataSlot::Bool, true)),
            true);

        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "not equality-compatible with its lookup");
    }

    Y_UNIT_TEST(HeterogeneousStaticSqlInTupleFailsClosed) {
        TExportTestContext ctx;
        const auto* int32Type = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* uint32Type = ScalarType(ctx, NUdf::EDataSlot::Uint32);
        const auto* optionalInt64 = ScalarType(ctx, NUdf::EDataSlot::Int64, true);
        const auto* optionalBool = ScalarType(ctx, NUdf::EDataSlot::Bool, true);
        TExprNode::TListType items;
        for (ui32 index = 0; index < 3; ++index) {
            items.push_back(TypedLiteral(ctx, "Int32", ToString(index), int32Type));
            items.push_back(TypedLiteral(ctx, "Uint32", ToString(index), uint32Type));
        }
        const auto* tupleType = ctx.ExprCtx.MakeType<TTupleExprType>(
            TTypeAnnotationNode::TListType{
                int32Type,
                uint32Type,
                int32Type,
                uint32Type,
                int32Type,
                uint32Type,
            });
        auto collection = ctx.ExprCtx.NewList(TPositionHandle(), std::move(items));
        collection->SetTypeAnn(tupleType);

        const auto result = ExportMapExpressionResult(
            ctx,
            "a",
            TypedSqlIn(
                ctx,
                std::move(collection),
                TypedMember(ctx, "a.x", optionalInt64),
                SqlInOptions(ctx, {}),
                optionalBool),
            true);

        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "must have one item type");
    }

    Y_UNIT_TEST(StaticSqlInAcceptsMaximumCollectionSize) {
        TExportTestContext ctx;
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
        TExprNode::TListType items;
        for (ui32 index = 0; index < 512; ++index) {
            items.push_back(TypedLiteral(ctx, "Int32", ToString(index), intType));
        }

        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedSqlIn(
                ctx,
                TypedStaticAsList(ctx, std::move(items), intType),
                TypedLiteral(ctx, "Int32", "1", intType),
                SqlInOptions(ctx, {}),
                boolType));

        UNIT_ASSERT_VALUES_EQUAL(expression["items"].GetArraySafe().size(), 512);
    }

    Y_UNIT_TEST(StaticSqlInCollectionBoundsAndShapesFailClosed) {
        const TVector<TString> expectedReasons = {
            "collection size",
            "collection size",
            "not a direct static tuple or AsList",
            "AsList collection is not typed as a list",
            "not a direct static tuple or AsList",
        };
        for (ui32 testCase = 0; testCase < expectedReasons.size(); ++testCase) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            TExprNode::TPtr collection;
            switch (testCase) {
                case 0:
                    collection = TypedStaticTuple(ctx, {}, intType);
                    break;
                case 1: {
                    TExprNode::TListType items;
                    for (ui32 index = 0; index < 513; ++index) {
                        items.push_back(TypedLiteral(ctx, "Int32", ToString(index), intType));
                    }
                    collection = TypedStaticAsList(ctx, std::move(items), intType);
                    break;
                }
                case 2:
                    collection = TypedCallable(
                        ctx,
                        "List",
                        {TypedLiteral(ctx, "Int32", "1", intType)},
                        ctx.ExprCtx.MakeType<TListExprType>(intType));
                    break;
                case 3:
                    collection = TypedCallable(
                        ctx,
                        "AsList",
                        {TypedLiteral(ctx, "Int32", "1", intType)},
                        ctx.ExprCtx.MakeType<TOptionalExprType>(
                            ctx.ExprCtx.MakeType<TListExprType>(intType)));
                    break;
                case 4:
                    collection = TypedCallable(
                        ctx,
                        "AsDict",
                        {TypedLiteral(ctx, "Int32", "1", intType)},
                        ctx.ExprCtx.MakeType<TDictExprType>(intType, intType));
                    break;
            }

            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    std::move(collection),
                    TypedLiteral(ctx, "Int32", "1", intType),
                    SqlInOptions(ctx, {}),
                    boolType));
            UNIT_ASSERT_C(!result.IsSupported(), testCase);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReasons[testCase]);
        }
    }

    Y_UNIT_TEST(StaticSqlInTypesAndResultNullabilityFailClosed) {
        const TVector<TString> expectedReasons = {
            "item is nullable",
            "not equality-compatible",
            "collection annotation",
            "result nullability does not match",
            "result is not Bool",
        };
        for (ui32 testCase = 0; testCase < expectedReasons.size(); ++testCase) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
            const auto* int64Type = ScalarType(ctx, NUdf::EDataSlot::Int64);
            const auto* uint64Type = ScalarType(ctx, NUdf::EDataSlot::Uint64);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);

            TExprNode::TPtr item;
            const TTypeAnnotationNode* annotatedItemType = intType;
            const TTypeAnnotationNode* lookupType = intType;
            const TTypeAnnotationNode* resultType = boolType;
            switch (testCase) {
                case 0:
                    item = TypedCallable(ctx, "Nothing", {}, optionalInt);
                    break;
                case 1:
                    item = TypedLiteral(ctx, "Uint64", "1", uint64Type);
                    annotatedItemType = uint64Type;
                    lookupType = int64Type;
                    break;
                case 2:
                    item = TypedLiteral(ctx, "Int32", "1", intType);
                    annotatedItemType = int64Type;
                    break;
                case 3:
                    item = TypedLiteral(ctx, "Int32", "1", intType);
                    lookupType = optionalInt;
                    break;
                case 4:
                    item = TypedLiteral(ctx, "Int32", "1", intType);
                    resultType = intType;
                    break;
            }

            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    TypedStaticTuple(ctx, {std::move(item)}, annotatedItemType),
                    TypedMember(ctx, "a.x", lookupType),
                    SqlInOptions(ctx, {}),
                    resultType),
                testCase == 3);
            UNIT_ASSERT_C(!result.IsSupported(), testCase);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReasons[testCase]);
        }
    }

    Y_UNIT_TEST(StaticSqlInOptionsFailClosed) {
        const TVector<TString> expectedReasons = {
            "tableSource collections are unsupported",
            "Duplicate SqlIn option",
            "Unsupported SqlIn option",
            "one-atom tuple",
            "options are not a tuple",
            "one-atom tuple",
        };
        for (ui32 testCase = 0; testCase < expectedReasons.size(); ++testCase) {
            TExportTestContext ctx;
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            TExprNode::TPtr options;
            switch (testCase) {
                case 0:
                    options = SqlInOptions(ctx, {"tableSource"});
                    break;
                case 1:
                    options = SqlInOptions(ctx, {"ansi", "ansi"});
                    break;
                case 2:
                    options = SqlInOptions(ctx, {"futureOption"});
                    break;
                case 3:
                    options = ctx.ExprCtx.NewList(
                        TPositionHandle(),
                        {ctx.ExprCtx.NewList(
                            TPositionHandle(),
                            {
                                ctx.ExprCtx.NewAtom(TPositionHandle(), "ansi"),
                                ctx.ExprCtx.NewAtom(TPositionHandle(), "value"),
                            })});
                    break;
                case 4:
                    options = TypedStaticAsList(
                        ctx,
                        {ctx.ExprCtx.NewAtom(TPositionHandle(), "ansi")},
                        intType);
                    break;
                case 5:
                    options = ctx.ExprCtx.NewList(
                        TPositionHandle(),
                        {ctx.ExprCtx.NewAtom(TPositionHandle(), "ansi")});
                    break;
            }

            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedSqlIn(
                    ctx,
                    TypedStaticAsList(
                        ctx,
                        {TypedLiteral(ctx, "Int32", "1", intType)},
                        intType),
                    TypedLiteral(ctx, "Int32", "1", intType),
                    std::move(options),
                    boolType));
            UNIT_ASSERT_C(!result.IsSupported(), testCase);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReasons[testCase]);
        }
    }

    Y_UNIT_TEST(ExportsExactSameTypeIntegerArithmetic) {
        for (const auto [callable, kind] : {
                 std::pair<TStringBuf, TStringBuf>{"+", "add"},
                 std::pair<TStringBuf, TStringBuf>{"-", "sub"},
                 std::pair<TStringBuf, TStringBuf>{"*", "mul"},
             })
        {
            TExportTestContext ctx;
            const auto* type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedMember(ctx, "a.x", type),
                        TypedLiteral(ctx, "Int32", "2", type),
                    },
                    type));

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), kind);
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Int32");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["column"].GetStringSafe(), "a.x");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Int32");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["value"].GetIntegerSafe(), 2);
        }

        TExportTestContext nullable;
        const auto* optionalInt = ScalarType(nullable, NUdf::EDataSlot::Int32, true);
        const auto* intType = ScalarType(nullable, NUdf::EDataSlot::Int32);
        const auto nullableExpression = ExportMapExpression(
            nullable,
            "a",
            TypedCallable(
                nullable,
                "+",
                {
                    TypedMember(nullable, "a.x", optionalInt),
                    TypedLiteral(nullable, "Int32", "2", intType),
                },
                optionalInt),
            true);
        UNIT_ASSERT_VALUES_EQUAL(nullableExpression["kind"].GetStringSafe(), "add");
        UNIT_ASSERT_VALUES_EQUAL(nullableExpression["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT(nullableExpression["nullable"].GetBooleanSafe());

        TExportTestContext mismatchedNullability;
        const auto* mismatchInt = ScalarType(mismatchedNullability, NUdf::EDataSlot::Int32);
        const auto* mismatchOptional = ScalarType(
            mismatchedNullability,
            NUdf::EDataSlot::Int32,
            true);
        const auto nullabilityFallback = ExportMapExpression(
            mismatchedNullability,
            "a",
            TypedCallable(
                mismatchedNullability,
                "+",
                {
                    TypedMember(mismatchedNullability, "a.x", mismatchInt),
                    TypedLiteral(mismatchedNullability, "Int32", "2", mismatchInt),
                },
                mismatchOptional));
        UNIT_ASSERT_VALUES_EQUAL(nullabilityFallback["kind"].GetStringSafe(), "opaque");

        TExportTestContext mixedTypes;
        const auto* int32Type = ScalarType(mixedTypes, NUdf::EDataSlot::Int32);
        const auto* int64Type = ScalarType(mixedTypes, NUdf::EDataSlot::Int64);
        const auto typeFallback = ExportMapExpression(
            mixedTypes,
            "a",
            TypedCallable(
                mixedTypes,
                "+",
                {
                    TypedMember(mixedTypes, "a.x", int32Type),
                    TypedLiteral(mixedTypes, "Int64", "2", int64Type),
                },
                int64Type));
        UNIT_ASSERT_VALUES_EQUAL(typeFallback["kind"].GetStringSafe(), "opaque");
    }

    Y_UNIT_TEST(ExportsExactDecimalArithmetic) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DecimalMul", {
            {"d", "Decimal(5,2)", false},
            {"i", "Int32", true},
        });
        auto read = MakeRead(ctx, table, "a", {"d", "i"});
        const auto* decimalType = DecimalType(ctx, "5", "2");
        const auto* optionalDecimalType = DecimalType(ctx, "5", "2", true);
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);

        auto map = MakeIntrusive<TOpMap>(
            read,
            TPositionHandle(),
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("add_result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "+",
                            {
                                TypedMember(ctx, "a.d", optionalDecimalType),
                                TypedDecimalLiteral(
                                    ctx,
                                    "1.25",
                                    "5",
                                    "2",
                                    decimalType),
                            },
                            optionalDecimalType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
                TMapElement(
                    TInfoUnit("sub_result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "-",
                            {
                                TypedMember(ctx, "a.d", optionalDecimalType),
                                TypedDecimalLiteral(
                                    ctx,
                                    "0.50",
                                    "5",
                                    "2",
                                    decimalType),
                            },
                            optionalDecimalType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
                TMapElement(
                    TInfoUnit("decimal_result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "DecimalMul",
                            {
                                TypedMember(ctx, "a.d", optionalDecimalType),
                                TypedDecimalLiteral(
                                    ctx,
                                    "2.00",
                                    "5",
                                    "2",
                                    decimalType),
                            },
                            optionalDecimalType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
                TMapElement(
                    TInfoUnit("integer_result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "DecimalMul",
                            {
                                TypedMember(ctx, "a.d", optionalDecimalType),
                                TypedLiteral(ctx, "Int32", "3", intType),
                            },
                            optionalDecimalType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps)),
            });
        TOpRoot root(
            map,
            TPositionHandle(),
            {"add_result", "sub_result", "decimal_result", "integer_result"});
        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& columns = FindNode(snapshot, "project")["columns"].GetArraySafe();
        const auto findExpression = [&](TStringBuf output) -> const NJson::TJsonValue& {
            for (const auto& column : columns) {
                if (column["output"].GetStringSafe() == output) {
                    return column["expression"];
                }
            }
            UNIT_FAIL(TStringBuilder() << "missing projection " << output);
            return columns.front();
        };

        const auto assertDecimalBinary = [&](TStringBuf output, TStringBuf kind, TStringBuf scaled) {
            const auto& expression = findExpression(output);
            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), kind);
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["column"].GetStringSafe(), "a.d");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["right"]["value"]["kind"].GetStringSafe(),
                "finite");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["right"]["value"]["scaled"].GetStringSafe(),
                scaled);
        };
        assertDecimalBinary("add_result", "add", "125");
        assertDecimalBinary("sub_result", "sub", "50");

        const auto& decimalExpression = findExpression("decimal_result");
        UNIT_ASSERT_VALUES_EQUAL(decimalExpression["kind"].GetStringSafe(), "mul");
        UNIT_ASSERT_VALUES_EQUAL(decimalExpression["type"].GetStringSafe(), "Decimal(5,2)");
        UNIT_ASSERT(decimalExpression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(decimalExpression["left"]["column"].GetStringSafe(), "a.d");
        UNIT_ASSERT_VALUES_EQUAL(decimalExpression["right"]["type"].GetStringSafe(), "Decimal(5,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            decimalExpression["right"]["value"]["kind"].GetStringSafe(),
            "finite");
        UNIT_ASSERT_VALUES_EQUAL(
            decimalExpression["right"]["value"]["scaled"].GetStringSafe(),
            "200");

        const auto& integerExpression = findExpression("integer_result");
        UNIT_ASSERT_VALUES_EQUAL(integerExpression["kind"].GetStringSafe(), "mul");
        UNIT_ASSERT_VALUES_EQUAL(integerExpression["type"].GetStringSafe(), "Decimal(5,2)");
        UNIT_ASSERT(integerExpression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(integerExpression["right"]["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT_VALUES_EQUAL(integerExpression["right"]["value"].GetIntegerSafe(), 3);
    }

    Y_UNIT_TEST(ExportsExactDecimalDivSignatureMatrix) {
        struct TIntegerCase {
            NUdf::EDataSlot Slot;
            TStringBuf Type;
        };
        const TVector<TIntegerCase> integerCases = {
            {NUdf::EDataSlot::Int8, "Int8"},
            {NUdf::EDataSlot::Int16, "Int16"},
            {NUdf::EDataSlot::Int32, "Int32"},
            {NUdf::EDataSlot::Int64, "Int64"},
            {NUdf::EDataSlot::Uint8, "Uint8"},
            {NUdf::EDataSlot::Uint16, "Uint16"},
            {NUdf::EDataSlot::Uint32, "Uint32"},
            {NUdf::EDataSlot::Uint64, "Uint64"},
        };

        for (const auto& test : integerCases) {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalDiv",
                    {
                        TypedDecimalLiteral(ctx, "12.50", "5", "2", decimal),
                        TypedLiteral(
                            ctx,
                            test.Type,
                            "2",
                            ScalarType(ctx, test.Slot)),
                    },
                    decimal));

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "div");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["left"]["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), test.Type);
        }

        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalDiv",
                    {
                        TypedDecimalLiteral(ctx, "12.50", "5", "2", decimal),
                        TypedDecimalLiteral(ctx, "2.00", "5", "2", decimal),
                    },
                    decimal));

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "div");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(expression["right"]["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT_VALUES_EQUAL(
                expression["right"]["value"]["scaled"].GetStringSafe(),
                "200");
        }

        for (const auto [leftNullable, rightNullable] : {
                 std::pair{true, false},
                 std::pair{false, true},
                 std::pair{true, true},
             })
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            auto left = leftNullable
                ? TypedMember(ctx, "a.x", optionalDecimal)
                : TypedMember(ctx, "a.x", decimal);
            auto right = rightNullable
                ? TypedMember(ctx, "a.y", optionalDecimal)
                : TypedMember(ctx, "a.y", decimal);
            const auto expression = ExportMapExpression(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalDiv",
                    {std::move(left), std::move(right)},
                    optionalDecimal),
                true);

            UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "div");
            UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Decimal(5,2)");
            UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(DecimalDivSignatureMatrixFailsClosed) {
        enum class ECase {
            GenericDivision,
            NonDecimalResult,
            NonCanonicalResult,
            LeftTypeMismatch,
            RightDecimalMismatch,
            RightBool,
            RightDate,
            RightInterval,
            ExcessResultNullability,
            MissingResultNullability,
            Unary,
            Ternary,
        };
        struct TCase {
            ECase Case;
            TStringBuf Reason;
        };
        const TVector<TCase> cases = {
            {ECase::GenericDivision, "Unsupported scalar callable /"},
            {ECase::NonDecimalResult, "result is not Decimal"},
            {ECase::NonCanonicalResult, "Unsupported scalar type Decimal(05,2)"},
            {ECase::LeftTypeMismatch, "left operand must exactly match"},
            {ECase::RightDecimalMismatch, "same Decimal type or an integer"},
            {ECase::RightBool, "same Decimal type or an integer"},
            {ECase::RightDate, "same Decimal type or an integer"},
            {ECase::RightInterval, "Unsupported scalar type Interval"},
            {ECase::ExcessResultNullability, "OR of operand nullability"},
            {ECase::MissingResultNullability, "OR of operand nullability"},
            {ECase::Unary, "unsupported arity 1"},
            {ECase::Ternary, "unsupported arity 3"},
        };

        for (const auto& test : cases) {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            const auto* otherDecimal = DecimalType(ctx, "6", "2");
            TStringBuf callable = "DecimalDiv";
            const TTypeAnnotationNode* resultType = decimal;
            TExprNode::TListType children = {
                TypedMember(ctx, "a.x", decimal),
                TypedDecimalLiteral(ctx, "2.00", "5", "2", decimal),
            };

            switch (test.Case) {
                case ECase::GenericDivision:
                    callable = "/";
                    break;
                case ECase::NonDecimalResult:
                    resultType = ScalarType(ctx, NUdf::EDataSlot::Int32);
                    break;
                case ECase::NonCanonicalResult:
                    resultType = DecimalType(ctx, "05", "2");
                    break;
                case ECase::LeftTypeMismatch:
                    children[0] = TypedMember(ctx, "a.x", otherDecimal);
                    break;
                case ECase::RightDecimalMismatch:
                    children[1] = TypedDecimalLiteral(
                        ctx,
                        "2.00",
                        "6",
                        "2",
                        otherDecimal);
                    break;
                case ECase::RightBool:
                    children[1] = TypedLiteral(
                        ctx,
                        "Bool",
                        "true",
                        ScalarType(ctx, NUdf::EDataSlot::Bool));
                    break;
                case ECase::RightDate:
                    children[1] = TypedLiteral(
                        ctx,
                        "Date",
                        "1",
                        ScalarType(ctx, NUdf::EDataSlot::Date));
                    break;
                case ECase::RightInterval:
                    children[1] = TypedLiteral(
                        ctx,
                        "Interval",
                        "1",
                        ScalarType(ctx, NUdf::EDataSlot::Interval));
                    break;
                case ECase::ExcessResultNullability:
                    resultType = optionalDecimal;
                    break;
                case ECase::MissingResultNullability:
                    children[0] = TypedMember(ctx, "a.x", optionalDecimal);
                    break;
                case ECase::Unary:
                    children.pop_back();
                    break;
                case ECase::Ternary:
                    children.push_back(
                        TypedDecimalLiteral(ctx, "3.00", "5", "2", decimal));
                    break;
            }

            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    callable,
                    std::move(children),
                    resultType),
                true);
            UNIT_ASSERT_C(!result.IsSupported(), static_cast<ui32>(test.Case));
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, test.Reason);
        }
    }

    Y_UNIT_TEST(DecimalDivIsAllowedInsideExactIf) {
        TExportTestContext ctx;
        const auto* decimal = DecimalType(ctx, "5", "2");
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "If",
                {
                    TypedLiteral(
                        ctx,
                        "Bool",
                        "true",
                        ScalarType(ctx, NUdf::EDataSlot::Bool)),
                    TypedCallable(
                        ctx,
                        "DecimalDiv",
                        {
                            TypedMember(ctx, "a.x", decimal),
                            TypedDecimalLiteral(ctx, "0", "5", "2", decimal),
                        },
                        decimal),
                    TypedDecimalLiteral(ctx, "1", "5", "2", decimal),
                },
                decimal));

        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
        UNIT_ASSERT_VALUES_EQUAL(expression["then"]["kind"].GetStringSafe(), "div");
        UNIT_ASSERT_VALUES_EQUAL(expression["then"]["left"]["column"].GetStringSafe(), "a.x");
    }

    Y_UNIT_TEST(DecimalDivNDecimalOracleCoversRoundingAndSpecials) {
        using namespace NYql::NDecimal;

        const TDecimalDivisor<TInt128> decimalDivisor(5, 2);
        UNIT_ASSERT(decimalDivisor.Do(TInt128(100), TInt128(200)) == TInt128(50));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(100), TInt128(800)) == TInt128(12));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(300), TInt128(800)) == TInt128(38));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(-100), TInt128(800)) == TInt128(-12));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(-300), TInt128(800)) == TInt128(-38));
        // Runtime division with a negative divisor truncates these non-ties
        // toward zero: ideal nearest rounding would produce +67 and -67.
        UNIT_ASSERT(decimalDivisor.Do(TInt128(-200), TInt128(-300)) == TInt128(66));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(200), TInt128(-300)) == TInt128(-66));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(99999), TInt128(1)) == Inf());
        UNIT_ASSERT(decimalDivisor.Do(TInt128(100), TInt128(0)) == Inf());
        UNIT_ASSERT(decimalDivisor.Do(TInt128(-100), TInt128(0)) == -Inf());
        UNIT_ASSERT(IsNan(decimalDivisor.Do(TInt128(0), TInt128(0))));
        UNIT_ASSERT(IsNan(decimalDivisor.Do(Nan(), TInt128(100))));
        UNIT_ASSERT(IsNan(decimalDivisor.Do(Inf(), Inf())));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(100), Inf()) == TInt128(0));
        UNIT_ASSERT(decimalDivisor.Do(TInt128(100), -Inf()) == TInt128(0));
        UNIT_ASSERT(decimalDivisor.Do(Inf(), TInt128(-100)) == -Inf());

        // The exact widened quotient is the reserved TInt128 NaN code, but it
        // was produced by finite operands and must saturate to positive Inf.
        const TInt128 collisionLeft = (Inf() + TInt128(1)) / TInt128(11);
        UNIT_ASSERT(IsNormal(collisionLeft));
        UNIT_ASSERT(
            MulAndDivNormalMultiplier(
                collisionLeft,
                TInt128(11),
                TInt128(1)) == Inf());

        const TDecimalDivisor<i8> integerDivisor;
        UNIT_ASSERT(integerDivisor.Do(TInt128(-238973), i8(-128)) == TInt128(1866));
        UNIT_ASSERT(integerDivisor.Do(TInt128(-238973), i8(-19)) == TInt128(12577));
        UNIT_ASSERT(integerDivisor.Do(TInt128(-238973), i8(3)) == TInt128(-79658));
        UNIT_ASSERT(integerDivisor.Do(TInt128(-238973), i8(0)) == -Inf());
    }

    Y_UNIT_TEST(DecimalArithmeticTypeAndNullabilityMismatchesFailClosed) {
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "+",
                    {
                        TypedMember(ctx, "a.x", decimal),
                        TypedLiteral(ctx, "Int32", "1", intType),
                    },
                    decimal));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "right operand must be the same Decimal type");
        }
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* otherDecimal = DecimalType(ctx, "6", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalMul",
                    {
                        TypedMember(ctx, "a.x", decimal),
                        TypedDecimalLiteral(ctx, "1", "6", "2", otherDecimal),
                    },
                    decimal));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "same Decimal type or an integer");
        }
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalMul",
                    {
                        TypedMember(ctx, "a.x", decimal),
                        TypedLiteral(ctx, "Int32", "1", intType),
                    },
                    intType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "result is not Decimal");
        }
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalMul",
                    {
                        TypedMember(ctx, "a.x", decimal),
                        TypedLiteral(ctx, "Bool", "true", boolType),
                    },
                    decimal));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "same Decimal type or an integer");
        }
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalMul",
                    {
                        TypedMember(ctx, "a.x", decimal),
                        TypedDecimalLiteral(ctx, "1", "5", "2", decimal),
                    },
                    optionalDecimal));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "OR of operand nullability");
        }
        {
            TExportTestContext ctx;
            const auto* decimal = DecimalType(ctx, "5", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "DecimalMul",
                    {TypedMember(ctx, "a.x", decimal)},
                    decimal));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "unsupported arity 1");
        }
    }

    Y_UNIT_TEST(CanonicalizesReviewedStringPredicatesAcrossGenericAndOlapDialects) {
        struct TStringPredicateCase {
            TStringBuf Generic;
            TStringBuf Olap;
            TStringBuf Literal;
            TStringBuf Fingerprint;
        };
        const TVector<TStringPredicateCase> cases = {
            {
                "EndsWith",
                "ends_with",
                "BRASS",
                "yql-string-predicate-v1:ends_with",
            },
            {
                "StringContains",
                "string_contains",
                "green",
                "yql-string-predicate-v1:string_contains",
            },
        };

        for (const auto& test : cases) {
            TExportTestContext generic;
            const auto& genericTable = AddTable(
                generic,
                "/Root/GenericStringPredicate",
                {{"s", "String", false}});
            auto genericRead = MakeRead(
                generic,
                genericTable,
                "a",
                {"s"});
            const auto* stringType = ScalarType(
                generic,
                NUdf::EDataSlot::String);
            const auto* optionalString = ScalarType(
                generic,
                NUdf::EDataSlot::String,
                true);
            const auto* boolType = ScalarType(
                generic,
                NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                generic,
                NUdf::EDataSlot::Bool,
                true);
            SetExactOutputType(
                generic,
                *genericRead,
                {{"a.s", optionalString}});
            auto genericPredicate = TypedCallable(
                generic,
                test.Generic,
                {
                    TypedMember(generic, "a.s", optionalString),
                    TypedLiteral(
                        generic,
                        "String",
                        test.Literal,
                        stringType),
                },
                optionalBool);
            auto genericMap = MakeIntrusive<TOpMap>(
                genericRead,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    TExpression(
                        TypedCallable(
                            generic,
                            "Coalesce",
                            {
                                std::move(genericPredicate),
                                TypedLiteral(
                                    generic,
                                    "Bool",
                                    "false",
                                    boolType),
                            },
                            boolType),
                        &generic.ExprCtx,
                        &generic.ExpressionProps))});
            SetExactOutputType(
                generic,
                *genericMap,
                {{"result", boolType}});
            TOpRoot genericRoot(
                genericMap,
                TPositionHandle(),
                {"result"});
            const auto genericSnapshot = ParseSupported(
                ExportSemanticSnapshotV1(genericRoot, generic.RboCtx));
            const auto& coalesced = FindNode(genericSnapshot, "project")
                ["columns"].GetArraySafe().back()["expression"];
            UNIT_ASSERT_VALUES_EQUAL(
                coalesced["kind"].GetStringSafe(),
                "if_present");
            const auto& genericOpaque = coalesced["optional"];

            TExportTestContext olap;
            const auto& olapTable = AddTable(
                olap,
                "/Root/OlapStringPredicate",
                {{"s", "String", false}});
            auto olapRead = MakeRead(
                olap,
                olapTable,
                "a",
                {"s"},
                NYql::EStorageType::ColumnStorage);
            SetExactOutputType(
                olap,
                *olapRead,
                {{
                    "a.s",
                    ScalarType(
                        olap,
                        NUdf::EDataSlot::String,
                        true),
                }});
            olapRead->OlapFilterLambda = MakeOlapFilterProcess(
                olap,
                MakeOlapStringPredicate(
                    olap,
                    test.Olap,
                    "s",
                    test.Literal));
            TOpRoot olapRoot(
                olapRead,
                TPositionHandle(),
                {"a.s"});
            olapRead->Props.StageId =
                olapRoot.PlanProps.StageGraph.AddSourceStage(
                    NYql::EStorageType::ColumnStorage);
            const auto olapSnapshot = ParseSupported(
                ExportSemanticSnapshotV1(olapRoot, olap.RboCtx));
            const auto& olapOpaque =
                FindNode(olapSnapshot, "scan")["predicate"];

            for (const auto* expression : {&genericOpaque, &olapOpaque}) {
                UNIT_ASSERT_VALUES_EQUAL(
                    (*expression)["kind"].GetStringSafe(),
                    "opaque");
                UNIT_ASSERT_VALUES_EQUAL(
                    (*expression)["fingerprint"].GetStringSafe(),
                    test.Fingerprint);
                UNIT_ASSERT_VALUES_EQUAL(
                    (*expression)["type"].GetStringSafe(),
                    "Bool");
                UNIT_ASSERT((*expression)["nullable"].GetBooleanSafe());
                const auto& args = (*expression)["args"].GetArraySafe();
                UNIT_ASSERT_VALUES_EQUAL(args.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(
                    args[0]["column"].GetStringSafe(),
                    "a.s");
                UNIT_ASSERT_VALUES_EQUAL(
                    args[1]["kind"].GetStringSafe(),
                    "literal");
                UNIT_ASSERT_VALUES_EQUAL(
                    args[1]["type"].GetStringSafe(),
                    "String");
                UNIT_ASSERT_VALUES_EQUAL(
                    args[1]["value"].GetStringSafe(),
                    test.Literal);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                genericOpaque["fingerprint"].GetStringSafe(),
                olapOpaque["fingerprint"].GetStringSafe());
        }
    }

    Y_UNIT_TEST(CanonicalStringPredicateBridgeFailsClosed) {
        enum class EGenericShape {
            DynamicRight,
            Utf8Left,
            NonNullableLeft,
            NonNullableResult,
        };
        const auto exportGeneric = [](EGenericShape shape) {
            TExportTestContext ctx;
            const auto& table = AddTable(
                ctx,
                "/Root/GenericStringPredicateFailure",
                {
                    {"s", "String", false},
                    {"rhs", "String", true},
                });
            auto read = MakeRead(ctx, table, "a", {"s", "rhs"});
            const auto* stringType = ScalarType(
                ctx,
                NUdf::EDataSlot::String);
            const auto* optionalString = ScalarType(
                ctx,
                NUdf::EDataSlot::String,
                true);
            const auto* optionalUtf8 = ScalarType(
                ctx,
                NUdf::EDataSlot::Utf8,
                true);
            const auto* boolType = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            SetExactOutputType(
                ctx,
                *read,
                {
                    {"a.s", optionalString},
                    {"a.rhs", stringType},
                });

            const auto* leftType =
                shape == EGenericShape::Utf8Left
                ? optionalUtf8
                : shape == EGenericShape::NonNullableLeft
                    ? stringType
                    : optionalString;
            const auto* resultType =
                shape == EGenericShape::NonNullableResult
                ? boolType
                : optionalBool;
            auto right =
                shape == EGenericShape::DynamicRight
                ? TypedMember(ctx, "a.rhs", stringType)
                : TypedLiteral(ctx, "String", "tail", stringType);
            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "EndsWith",
                            {
                                TypedMember(ctx, "a.s", leftType),
                                std::move(right),
                            },
                            resultType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps))});
            SetExactOutputType(
                ctx,
                *map,
                {{"result", resultType}});
            TOpRoot root(map, TPositionHandle(), {"result"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        for (const auto [shape, reason] : {
                 std::pair{
                     EGenericShape::DynamicRight,
                     TStringBuf("requires one direct String member and one String literal"),
                 },
                 std::pair{
                     EGenericShape::Utf8Left,
                     TStringBuf("requires Optional<String> and non-null String operands"),
                 },
                 std::pair{
                     EGenericShape::NonNullableLeft,
                     TStringBuf("requires Optional<String> and non-null String operands"),
                 },
                 std::pair{
                     EGenericShape::NonNullableResult,
                     TStringBuf("result must be Optional<Bool>"),
                 },
             })
        {
            const auto result = exportGeneric(shape);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, reason);
        }

        enum class EOlapShape {
            MissingResultType,
            NonNullableResult,
            DynamicRight,
            Utf8Column,
            NonNullableColumn,
        };
        const auto exportOlap = [](EOlapShape shape) {
            TExportTestContext ctx;
            const bool utf8 = shape == EOlapShape::Utf8Column;
            const bool notNull = shape == EOlapShape::NonNullableColumn;
            const TString type = utf8 ? TString("Utf8") : TString("String");
            const auto slot = utf8
                ? NUdf::EDataSlot::Utf8
                : NUdf::EDataSlot::String;
            const auto& table = AddTable(
                ctx,
                "/Root/OlapStringPredicateFailure",
                {
                    {"s", type, notNull},
                    {"rhs", "String", true},
                });
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"s", "rhs"},
                NYql::EStorageType::ColumnStorage);
            SetExactOutputType(
                ctx,
                *read,
                {
                    {"a.s", ScalarType(ctx, slot, !notNull)},
                    {"a.rhs", ScalarType(
                        ctx,
                        NUdf::EDataSlot::String)},
                });

            const auto pos = TPositionHandle();
            const auto* stringType = ScalarType(
                ctx,
                NUdf::EDataSlot::String);
            const auto* boolType = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool);
            const auto* optionalBool = ScalarType(
                ctx,
                NUdf::EDataSlot::Bool,
                true);
            const auto right = [&]() {
                if (shape == EOlapShape::DynamicRight) {
                    return ctx.ExprCtx.NewAtom(pos, "rhs");
                }
                return TypedLiteral(ctx, "String", "tail", stringType);
            }();

            TExprNode::TPtr condition;
            if (shape == EOlapShape::MissingResultType) {
                condition = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
                    .Operator().Value("ends_with").Build()
                    .Left<TCoAtom>().Value("s").Build()
                    .Right(TExprBase(right))
                    .Done().Ptr();
            } else {
                auto descriptor =
                    shape == EOlapShape::NonNullableResult
                    ? DataTypeDescriptor(ctx, "Bool", boolType)
                    : OptionalDataTypeDescriptor(
                        ctx,
                        "Bool",
                        boolType,
                        optionalBool);
                condition = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
                    .Operator().Value("ends_with").Build()
                    .Left<TCoAtom>().Value("s").Build()
                    .Right(TExprBase(right))
                    .OpType(TExprBase(std::move(descriptor)))
                    .Done().Ptr();
            }
            read->OlapFilterLambda = MakeOlapFilterProcess(
                ctx,
                std::move(condition));
            TOpRoot root(read, pos, {"a.s"});
            read->Props.StageId =
                root.PlanProps.StageGraph.AddSourceStage(
                    NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        for (const auto [shape, reason] : {
                 std::pair{
                     EOlapShape::MissingResultType,
                     TStringBuf("requires an explicit Bool result type"),
                 },
                 std::pair{
                     EOlapShape::NonNullableResult,
                     TStringBuf("result nullability disagrees"),
                 },
                 std::pair{
                     EOlapShape::DynamicRight,
                     TStringBuf("right operand must be a non-null String literal"),
                 },
                 std::pair{
                     EOlapShape::Utf8Column,
                     TStringBuf("requires an Optional<String> column"),
                 },
                 std::pair{
                     EOlapShape::NonNullableColumn,
                     TStringBuf("requires an Optional<String> column"),
                 },
             })
        {
            const auto result = exportOlap(shape);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, reason);
        }
    }

    Y_UNIT_TEST(ExportsOnlyTheRestrictedTotalSubstringAsOpaque) {
        struct TSubstringCase {
            bool InputNullable = true;
            bool ResultNullable = true;
            bool DynamicStart = false;
            bool StartNullable = false;
            bool DynamicCount = false;
            bool CountNullable = false;
            bool ConvertBounds = false;
            bool WideConvertedBounds = false;
            bool Utf8 = false;
            bool OmitCount = false;
            NUdf::EDataSlot BoundSlot = NUdf::EDataSlot::Uint32;
            TString Count = "5";
        };

        const auto exportSubstring = [](const TSubstringCase& test) {
            TExportTestContext ctx;
            const TString stringName = test.Utf8 ? "Utf8" : "String";
            const TString boundName = test.BoundSlot == NUdf::EDataSlot::Uint32
                ? "Uint32"
                : "Int32";
            const auto& table = AddTable(ctx, "/Root/Substring", {
                {"s", stringName, !test.InputNullable},
                {"start", boundName, !test.StartNullable},
                {"count", boundName, !test.CountNullable},
            });
            auto read = MakeRead(ctx, table, "a", {"s", "start", "count"});
            const auto stringSlot = test.Utf8
                ? NUdf::EDataSlot::Utf8
                : NUdf::EDataSlot::String;
            const auto* inputType = ScalarType(
                ctx, stringSlot, test.InputNullable);
            const auto* resultType = ScalarType(
                ctx, stringSlot, test.ResultNullable);
            const auto* startType = ScalarType(
                ctx, test.BoundSlot, test.StartNullable);
            const auto* countType = ScalarType(
                ctx, test.BoundSlot, test.CountNullable);
            const auto boundLiteral = [&](TStringBuf value, const TTypeAnnotationNode* type) {
                if (!test.ConvertBounds) {
                    return TypedLiteral(ctx, boundName, value, type);
                }
                const auto sourceSlot = test.WideConvertedBounds
                    ? NUdf::EDataSlot::Uint64
                    : NUdf::EDataSlot::Int32;
                const TStringBuf sourceName = test.WideConvertedBounds
                    ? TStringBuf("Uint64")
                    : TStringBuf("Int32");
                const auto* sourceType = ScalarType(ctx, sourceSlot);
                const auto* targetType = ScalarType(ctx, NUdf::EDataSlot::Uint32);
                return TypedCallable(
                    ctx,
                    "Convert",
                    {
                        TypedLiteral(ctx, sourceName, value, sourceType),
                        DataTypeDescriptor(ctx, "Uint32", targetType),
                    },
                    targetType);
            };

            TExprNode::TListType arguments;
            arguments.push_back(TypedMember(ctx, "a.s", inputType));
            arguments.push_back(test.DynamicStart
                ? TypedMember(ctx, "a.start", startType)
                : boundLiteral("1", startType));
            if (!test.OmitCount) {
                arguments.push_back(test.DynamicCount
                    ? TypedMember(ctx, "a.count", countType)
                    : boundLiteral(test.Count, countType));
            }

            auto map = MakeIntrusive<TOpMap>(
                read,
                TPositionHandle(),
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("result"),
                    TExpression(
                        TypedCallable(
                            ctx,
                            "Substring",
                            std::move(arguments),
                            resultType),
                        &ctx.ExprCtx,
                        &ctx.ExpressionProps))});
            TOpRoot root(map, TPositionHandle(), {"result"});
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        const auto exactSnapshot = ParseSupported(exportSubstring({}));
        const auto& exact = FindNode(exactSnapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];
        UNIT_ASSERT_VALUES_EQUAL(exact["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(exact["type"].GetStringSafe(), "String");
        UNIT_ASSERT(exact["nullable"].GetBooleanSafe());
        UNIT_ASSERT_STRING_CONTAINS(
            exact["fingerprint"].GetStringSafe(),
            "Substring");
        const auto& arguments = exact["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(arguments.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            arguments[0]["column"].GetStringSafe(),
            "a.s");

        TSubstringCase changedBound;
        changedBound.Count = "6";
        const auto changedSnapshot = ParseSupported(exportSubstring(changedBound));
        const auto& changed = FindNode(changedSnapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];
        UNIT_ASSERT_VALUES_UNEQUAL(
            exact["fingerprint"].GetStringSafe(),
            changed["fingerprint"].GetStringSafe());

        TSubstringCase convertedBounds;
        convertedBounds.ConvertBounds = true;
        const auto convertedSnapshot = ParseSupported(exportSubstring(convertedBounds));
        const auto& converted = FindNode(convertedSnapshot, "project")
            ["columns"].GetArraySafe().back()["expression"];
        UNIT_ASSERT_STRING_CONTAINS(
            converted["fingerprint"].GetStringSafe(),
            "Convert");
        UNIT_ASSERT_VALUES_EQUAL(converted["args"].GetArraySafe().size(), 1);

        TVector<TSubstringCase> rejected;
        auto wrongArity = TSubstringCase{};
        wrongArity.OmitCount = true;
        rejected.push_back(wrongArity);
        auto nonOptional = TSubstringCase{};
        nonOptional.InputNullable = false;
        nonOptional.ResultNullable = false;
        rejected.push_back(nonOptional);
        auto mismatchedResult = TSubstringCase{};
        mismatchedResult.ResultNullable = false;
        rejected.push_back(mismatchedResult);
        auto utf8 = TSubstringCase{};
        utf8.Utf8 = true;
        rejected.push_back(utf8);
        auto dynamicStart = TSubstringCase{};
        dynamicStart.DynamicStart = true;
        rejected.push_back(dynamicStart);
        auto optionalStart = TSubstringCase{};
        optionalStart.StartNullable = true;
        rejected.push_back(optionalStart);
        auto dynamicCount = TSubstringCase{};
        dynamicCount.DynamicCount = true;
        rejected.push_back(dynamicCount);
        auto optionalCount = TSubstringCase{};
        optionalCount.CountNullable = true;
        rejected.push_back(optionalCount);
        auto signedBound = TSubstringCase{};
        signedBound.BoundSlot = NUdf::EDataSlot::Int32;
        rejected.push_back(signedBound);
        auto negativeConvertedBound = TSubstringCase{};
        negativeConvertedBound.ConvertBounds = true;
        negativeConvertedBound.Count = "-1";
        rejected.push_back(negativeConvertedBound);
        auto overflowingConvertedBound = TSubstringCase{};
        overflowingConvertedBound.ConvertBounds = true;
        overflowingConvertedBound.WideConvertedBounds = true;
        overflowingConvertedBound.Count = "4294967296";
        rejected.push_back(overflowingConvertedBound);

        for (const auto& test : rejected) {
            const auto result = exportSubstring(test);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Restricted Substring");
        }

        TExportTestContext standalone;
        const auto* standaloneSource = ScalarType(
            standalone, NUdf::EDataSlot::Int32);
        const auto* standaloneTarget = ScalarType(
            standalone, NUdf::EDataSlot::Uint32);
        const auto standaloneConvert = ExportMapExpressionResult(
            standalone,
            "a",
            TypedCallable(
                standalone,
                "Convert",
                {
                    TypedLiteral(
                        standalone, "Int32", "1", standaloneSource),
                    DataTypeDescriptor(
                        standalone, "Uint32", standaloneTarget),
                },
                standaloneTarget));
        UNIT_ASSERT(!standaloneConvert.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            standaloneConvert.UnsupportedReason,
            "Opaque Convert may fail");
    }

    Y_UNIT_TEST(OpaqueExpressionFingerprintIsAlphaStableAndKeepsOrderedUses) {
        TExportTestContext first;
        const auto* firstInt = ScalarType(first, NUdf::EDataSlot::Int32);
        const auto* firstResult = ScalarType(first, NUdf::EDataSlot::Int64);
        const auto firstExpression = ExportMapExpression(
            first,
            "a",
            TypedCallable(
                first,
                "+",
                {
                    TypedMember(first, "a.x", firstInt),
                    TypedMember(first, "a.y", firstInt),
                },
                firstResult));

        TExportTestContext renamed;
        const auto* renamedInt = ScalarType(renamed, NUdf::EDataSlot::Int32);
        const auto* renamedResult = ScalarType(renamed, NUdf::EDataSlot::Int64);
        const auto renamedExpression = ExportMapExpression(
            renamed,
            "renamed",
            TypedCallable(
                renamed,
                "+",
                {
                    TypedMember(renamed, "renamed.x", renamedInt),
                    TypedMember(renamed, "renamed.y", renamedInt),
                },
                renamedResult));

        UNIT_ASSERT_VALUES_EQUAL(firstExpression["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(firstExpression["type"].GetStringSafe(), "Int64");
        UNIT_ASSERT(!firstExpression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            firstExpression["fingerprint"].GetStringSafe(),
            renamedExpression["fingerprint"].GetStringSafe());
        UNIT_ASSERT_STRING_CONTAINS(firstExpression["fingerprint"].GetStringSafe(), "yql-opaque-v1");
        UNIT_ASSERT_STRING_CONTAINS(firstExpression["fingerprint"].GetStringSafe(), "+");
        UNIT_ASSERT(!firstExpression["fingerprint"].GetStringSafe().Contains("a.x"));
        UNIT_ASSERT(!firstExpression["fingerprint"].GetStringSafe().Contains("a.y"));

        const auto& firstArgs = firstExpression["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(firstArgs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(firstArgs[0]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(firstArgs[1]["column"].GetStringSafe(), "a.y");
        const auto& renamedArgs = renamedExpression["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(renamedArgs[0]["column"].GetStringSafe(), "renamed.x");
        UNIT_ASSERT_VALUES_EQUAL(renamedArgs[1]["column"].GetStringSafe(), "renamed.y");
    }

    Y_UNIT_TEST(OpaqueExpressionFingerprintPreservesStructureAndRepetition) {
        const auto exportBinary = [](
            TStringBuf callable,
            TStringBuf rightColumn,
            std::optional<TStringBuf> literal = std::nullopt,
            bool literalFirst = false)
        {
            TExportTestContext ctx;
            const auto* type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* resultType = ScalarType(ctx, NUdf::EDataSlot::Int64);
            auto left = TypedMember(ctx, "a.x", type);
            auto right = literal
                ? TypedLiteral(ctx, "Int32", *literal, type)
                : TypedMember(ctx, rightColumn, type);
            if (literalFirst) {
                std::swap(left, right);
            }
            return ExportMapExpression(
                ctx,
                "a",
                TypedCallable(ctx, callable, {left, right}, resultType));
        };

        const auto distinct = exportBinary("+", "a.y");
        const auto repeated = exportBinary("+", "a.x");
        const auto subtracted = exportBinary("-", "a.y");
        const auto multiplied = exportBinary("*", "a.y");
        const auto literalOne = exportBinary("+", "", TStringBuf("1"));
        const auto literalTwo = exportBinary("+", "", TStringBuf("2"));
        const auto literalFirst = exportBinary("+", "", TStringBuf("1"), true);

        UNIT_ASSERT_VALUES_UNEQUAL(
            distinct["fingerprint"].GetStringSafe(),
            repeated["fingerprint"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(distinct["args"].GetArraySafe().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(repeated["args"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_UNEQUAL(
            distinct["fingerprint"].GetStringSafe(),
            subtracted["fingerprint"].GetStringSafe());
        UNIT_ASSERT_VALUES_UNEQUAL(
            distinct["fingerprint"].GetStringSafe(),
            multiplied["fingerprint"].GetStringSafe());
        UNIT_ASSERT_VALUES_UNEQUAL(
            literalOne["fingerprint"].GetStringSafe(),
            literalTwo["fingerprint"].GetStringSafe());
        UNIT_ASSERT_VALUES_UNEQUAL(
            literalOne["fingerprint"].GetStringSafe(),
            literalFirst["fingerprint"].GetStringSafe());

        TExportTestContext shared;
        const auto* sharedType = ScalarType(shared, NUdf::EDataSlot::Int32);
        const auto* sharedResult = ScalarType(shared, NUdf::EDataSlot::Int64);
        auto sharedChild = TypedCallable(
            shared,
            "*",
            {
                TypedMember(shared, "a.x", sharedType),
                TypedLiteral(shared, "Int32", "2", sharedType),
            },
            sharedType);
        const auto sharedExpression = ExportMapExpression(
            shared,
            "a",
            TypedCallable(shared, "+", {sharedChild, sharedChild}, sharedResult));

        TExportTestContext duplicated;
        const auto* duplicatedType = ScalarType(duplicated, NUdf::EDataSlot::Int32);
        const auto* duplicatedResult = ScalarType(duplicated, NUdf::EDataSlot::Int64);
        const auto makeChild = [&]() {
            return TypedCallable(
                duplicated,
                "*",
                {
                    TypedMember(duplicated, "a.x", duplicatedType),
                    TypedLiteral(duplicated, "Int32", "2", duplicatedType),
                },
                duplicatedType);
        };
        const auto duplicatedExpression = ExportMapExpression(
            duplicated,
            "a",
            TypedCallable(duplicated, "+", {makeChild(), makeChild()}, duplicatedResult));
        UNIT_ASSERT_VALUES_EQUAL(
            sharedExpression["fingerprint"].GetStringSafe(),
            duplicatedExpression["fingerprint"].GetStringSafe());
    }

    Y_UNIT_TEST(ExportsReviewedTotalOpaqueStructuralFormsAndNullability) {
        TExportTestContext ctx;
        const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt = ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);

        auto safeCast = TypedCallable(
            ctx,
            "SafeCast",
            {
                TypedMember(ctx, "a.x", intType),
                DataTypeDescriptor(ctx, "Int32", intType),
            },
            optionalInt);
        auto exists = TypedCallable(ctx, "Exists", {safeCast}, boolType);
        auto just = TypedCallable(
            ctx,
            "Just",
            {TypedMember(ctx, "a.y", intType)},
            optionalInt);
        auto coalesce = TypedCallable(
            ctx,
            "Coalesce",
            {just, TypedMember(ctx, "a.x", intType)},
            intType);
        auto convert = TypedCallable(
            ctx,
            "Convert",
            {
                TypedMember(ctx, "a.x", intType),
                DataTypeDescriptor(ctx, "Int32", intType),
            },
            intType);
        const auto expression = ExportMapExpression(
            ctx,
            "a",
            TypedCallable(ctx, "If", {exists, coalesce, convert}, intType));

        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
        UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(expression["condition"]["kind"].GetStringSafe(), "exists");
        UNIT_ASSERT_STRING_CONTAINS(
            expression["condition"]["arg"]["fingerprint"].GetStringSafe(),
            "SafeCast");
        UNIT_ASSERT_STRING_CONTAINS(
            expression["then"]["fingerprint"].GetStringSafe(),
            "Coalesce");
        UNIT_ASSERT_STRING_CONTAINS(
            expression["then"]["fingerprint"].GetStringSafe(),
            "Just");
        UNIT_ASSERT_STRING_CONTAINS(
            expression["else"]["fingerprint"].GetStringSafe(),
            "Convert");

        TExportTestContext nullable;
        const auto* nullableType = ScalarType(nullable, NUdf::EDataSlot::Int32, true);
        const auto nullableExpression = ExportMapExpression(
            nullable,
            "a",
            TypedCallable(
                nullable,
                "+",
                {
                    TypedMember(nullable, "a.x", nullableType),
                    TypedMember(nullable, "a.y", nullableType),
                },
                nullableType),
            true);
        UNIT_ASSERT(nullableExpression["nullable"].GetBooleanSafe());

        TExportTestContext comparison;
        const auto* comparisonInt = ScalarType(comparison, NUdf::EDataSlot::Int32, true);
        const auto* comparisonBool = ScalarType(comparison, NUdf::EDataSlot::Bool, true);
        const auto comparisonExpression = ExportMapExpression(
            comparison,
            "a",
            TypedCallable(
                comparison,
                ">=",
                {
                    TypedMember(comparison, "a.x", comparisonInt),
                    TypedMember(comparison, "a.y", comparisonInt),
                },
                comparisonBool),
            true);
        UNIT_ASSERT_VALUES_EQUAL(comparisonExpression["kind"].GetStringSafe(), "gte");
        UNIT_ASSERT_VALUES_EQUAL(
            comparisonExpression["left"]["column"].GetStringSafe(),
            "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            comparisonExpression["right"]["column"].GetStringSafe(),
            "a.y");

        TExportTestContext mixedWidth;
        const auto& mixedTable = AddTable(mixedWidth, "/Root/Mixed", {
            {"x", "Int64", false},
        });
        auto mixedRead = MakeRead(mixedWidth, mixedTable, "a", {"x"});
        const auto* int64Type = ScalarType(mixedWidth, NUdf::EDataSlot::Int64, true);
        const auto* int32Type = ScalarType(mixedWidth, NUdf::EDataSlot::Int32);
        const auto* mixedBoolType = ScalarType(mixedWidth, NUdf::EDataSlot::Bool, true);
        auto mixedMap = MakeIntrusive<TOpMap>(
            mixedRead,
            TPositionHandle(),
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                TExpression(
                    TypedCallable(
                        mixedWidth,
                        ">=",
                        {
                            TypedMember(mixedWidth, "a.x", int64Type),
                            TypedLiteral(mixedWidth, "Int32", "30", int32Type),
                        },
                        mixedBoolType),
                    &mixedWidth.ExprCtx,
                    &mixedWidth.ExpressionProps))});
        TOpRoot mixedRoot(mixedMap, TPositionHandle(), {"result"});
        const auto mixedSnapshot = ParseSupported(
            ExportSemanticSnapshotV1(mixedRoot, mixedWidth.RboCtx));
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(mixedSnapshot, "project")["columns"].GetArraySafe().back()
                ["expression"]["kind"].GetStringSafe(),
            "gte");

    }

    Y_UNIT_TEST(UnsafeOrUnauditedOpaqueExpressionsFailClosed) {
        const auto exportCallable = [](
            TStringBuf callable,
            const std::function<void(TExprNode&)>& mutate = {})
        {
            TExportTestContext ctx;
            const auto* type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            TExpression expression(
                TypedCallable(
                    ctx,
                    callable,
                    {
                        TypedMember(ctx, "a.x", type),
                        TypedLiteral(ctx, "Int32", "1", type),
                    },
                    type),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);
            if (mutate) {
                mutate(*expression.GetExpressionBody());
            }
            return ExportMapExpressionResult(ctx, "a", std::move(expression));
        };

        for (const auto callable : {"/", "StrictCast", "Udf", "Apply", "Now", "CurrentActorId"}) {
            const auto result = exportCallable(callable);
            UNIT_ASSERT_C(!result.IsSupported(), callable);
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported scalar callable");
        }

        auto result = exportCallable("Unwrap");
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "Exact Date Unwrap requires");

        result = exportCallable("+", [](TExprNode& node) {
            node.SetSideEffects(ESideEffects::General);
        });
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "side-effecting or CSE-unsafe");

        result = exportCallable("+", [](TExprNode& node) {
            node.SetPosAware();
        });
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "position-aware");

        result = exportCallable("+", [](TExprNode& node) {
            node.SetUnorderedChildren();
        });
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "unordered children");

        {
            TExportTestContext ctx;
            const auto pos = TPositionHandle();
            const auto* type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            auto row = ctx.ExprCtx.NewArgument(pos, "row");
            auto foreign = ctx.ExprCtx.NewArgument(pos, "foreign");
            auto member = ctx.ExprCtx.NewCallable(
                pos,
                "Member",
                {foreign, ctx.ExprCtx.NewAtom(pos, "a.x")});
            member->SetTypeAnn(type);
            auto body = TypedCallable(
                ctx,
                "+",
                {member, TypedLiteral(ctx, "Int32", "1", type)},
                type);
            auto arguments = ctx.ExprCtx.NewArguments(pos, {row});
            TExpression expression(
                ctx.ExprCtx.NewLambda(pos, std::move(arguments), std::move(body)),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);

            result = ExportMapExpressionResult(ctx, "a", std::move(expression));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "input row column a.x");
        }

        {
            TExportTestContext ctx;
            const auto pos = TPositionHandle();
            const auto* type = ScalarType(ctx, NUdf::EDataSlot::Int32);
            auto row = ctx.ExprCtx.NewArgument(pos, "row");
            auto free = ctx.ExprCtx.NewArgument(pos, "free");
            free->SetTypeAnn(type);
            auto body = TypedCallable(
                ctx,
                "+",
                {free, TypedLiteral(ctx, "Int32", "1", type)},
                type);
            auto arguments = ctx.ExprCtx.NewArguments(pos, {row});
            TExpression expression(
                ctx.ExprCtx.NewLambda(pos, std::move(arguments), std::move(body)),
                &ctx.ExprCtx,
                &ctx.ExpressionProps);

            result = ExportMapExpressionResult(ctx, "a", std::move(expression));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "free Argument");
        }
    }

    Y_UNIT_TEST(ExportsJoinKeysAndResidualPredicate) {
        TExportTestContext ctx;
        AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"flag", "Bool", false},
        });
        AddTable(ctx, "/Root/B", {
            {"k", "Int32", true},
            {"flag", "Bool", false},
        });
        const auto& leftTable = ctx.Tables->ExistingTable("ut", "/Root/A");
        const auto& rightTable = ctx.Tables->ExistingTable("ut", "/Root/B");
        auto left = MakeRead(ctx, leftTable, "a", {"k", "flag"});
        auto right = MakeRead(ctx, rightTable, "b", {"k", "flag"});

        const auto pos = TPositionHandle();
        const auto residual = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(TInfoUnit("a.flag"), pos, &ctx.ExprCtx, &ctx.ExpressionProps),
            MakeColumnAccess(TInfoUnit("b.flag"), pos, &ctx.ExprCtx, &ctx.ExpressionProps));
        auto join = MakeIntrusive<TOpJoin>(
            left,
            right,
            pos,
            "Inner",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a.k"), TInfoUnit("b.k")}},
            TVector<TExpression>{residual});
        TOpRoot root(join, pos, {"a.k", "a.flag", "b.k", "b.flag"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& joinJson = FindNode(snapshot, "join");
        UNIT_ASSERT_VALUES_EQUAL(joinJson["kind"].GetStringSafe(), "inner");
        const auto& keys = joinJson["keys"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys[0].GetMapSafe().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(keys[0]["left"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(keys[0]["right"].GetStringSafe(), "b.k");
        const auto& predicate = joinJson["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "eq");
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(predicate),
            (std::pair<TString, TString>{"a.flag", "b.flag"}));
    }

    Y_UNIT_TEST(ExportsSharedJoinInputIUsForSingleOutputKinds) {
        struct TCase {
            TStringBuf SourceKind;
            TStringBuf SnapshotKind;
        };
        const TCase cases[] = {
            {"LeftSemi", "left_semi"},
            {"LeftOnly", "left_anti"},
            {"RightSemi", "right_semi"},
            {"RightOnly", "right_anti"},
        };

        for (const auto& test : cases) {
            const auto snapshot =
                ParseSupported(ExportSharedInputJoin(test.SourceKind));
            const auto& joinJson = FindNode(snapshot, "join");
            UNIT_ASSERT_VALUES_EQUAL(
                joinJson["kind"].GetStringSafe(),
                test.SnapshotKind);
            const auto& keys = joinJson["keys"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(keys[0]["left"].GetStringSafe(), "shared.k");
            UNIT_ASSERT_VALUES_EQUAL(keys[0]["right"].GetStringSafe(), "shared.k");
            UNIT_ASSERT_VALUES_EQUAL(
                joinJson["predicate"]["kind"].GetStringSafe(),
                "literal");
            UNIT_ASSERT_VALUES_EQUAL(
                joinJson["predicate"]["type"].GetStringSafe(),
                "Bool");
            UNIT_ASSERT(joinJson["predicate"]["value"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(SharedJoinInputIUsFailClosedForTwoOutputsAndFilters) {
        {
            const auto result = ExportSharedInputJoin("Inner");
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "only when exactly one side is present in the output");
        }

        {
            const auto result = ExportSharedInputJoin("LeftSemi", true);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Join filters cannot disambiguate shared input IUs");
        }
    }

    Y_UNIT_TEST(ExportsAggregateTraitsTypesAndSplitPhases) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int64", true},
            {"x", "Int64", true},
        });
        auto read = MakeRead(
            ctx,
            table,
            "a",
            {"k", "x"},
            NYql::EStorageType::ColumnStorage);
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"a.x", NUdf::EDataSlot::Int64},
        });
        const auto pos = TPositionHandle();
        auto partial = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"), "count", TInfoUnit("_state"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Intermediate,
            false,
            pos);
        SetOutputType(ctx, *partial, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"_state", NUdf::EDataSlot::Uint64},
        });
        auto final = MakeIntrusive<TOpAggregate>(
            partial,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("_state"), "sum", TInfoUnit("result"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Final,
            false,
            pos);
        SetOutputType(ctx, *final, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"result", NUdf::EDataSlot::Uint64},
        });
        TOpRoot root(final, pos, {"a.k", "result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 source = graph.AddSourceStage(NYql::EStorageType::ColumnStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = source;
        partial->Props.StageId = source;
        final->Props.StageId = consumer;
        auto shuffle = MakeIntrusive<TShuffleConnection>(
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            graph.GetOutputIndex(source));
        shuffle->HashFuncType = NYql::NDq::EHashShuffleFuncType::HashV1;
        graph.Connect(
            source,
            consumer,
            shuffle);

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        TVector<const NJson::TJsonValue*> aggregates;
        for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() == "aggregate") {
                aggregates.push_back(&node);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(aggregates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((*aggregates[0])["phase"].GetStringSafe(), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL((*aggregates[1])["phase"].GetStringSafe(), "final");
        UNIT_ASSERT_VALUES_EQUAL(Strings((*aggregates[0])["keys"]), TVector<TString>{"a.k"});

        const auto& partialTrait = (*aggregates[0])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["input"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["function"].GetStringSafe(), "count");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["output"].GetStringSafe(), "_state");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["nullable"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["distinct"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["unwrap"].GetBooleanSafe(), false);

        const auto& finalTrait = (*aggregates[1])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["input"].GetStringSafe(), "_state");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["function"].GetStringSafe(), "sum");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["output"].GetStringSafe(), "result");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL((*aggregates[1])["distinct_all"].GetBooleanSafe(), false);
    }

    Y_UNIT_TEST(ExportsDecimalSumSplitPhaseTypeContract) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"x", "Decimal(7,2)", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x"});
        const auto* inputType = DecimalType(ctx, "7", "2", true);
        const auto* sumType = DecimalType(ctx, "35", "2", true);
        SetExactOutputType(ctx, *read, {{"a.x", inputType}});

        const auto pos = TPositionHandle();
        auto partial = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"), "sum", TInfoUnit("_state"))},
            TVector<TInfoUnit>{},
            EOpPhase::Intermediate,
            false,
            pos);
        SetExactOutputType(ctx, *partial, {{"_state", sumType}});

        auto final = MakeIntrusive<TOpAggregate>(
            partial,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("_state"), "sum", TInfoUnit("result"))},
            TVector<TInfoUnit>{},
            EOpPhase::Final,
            false,
            pos);
        SetExactOutputType(ctx, *final, {{"result", sumType}});
        TOpRoot root(final, pos, {"result"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& inputColumn = snapshot["schema"]["tables"][0]["columns"][0];
        UNIT_ASSERT_VALUES_EQUAL(inputColumn["type"].GetStringSafe(), "Decimal(7,2)");
        UNIT_ASSERT_VALUES_EQUAL(inputColumn["nullable"].GetBooleanSafe(), true);

        TVector<const NJson::TJsonValue*> aggregates;
        for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() == "aggregate") {
                aggregates.push_back(&node);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(aggregates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((*aggregates[0])["phase"].GetStringSafe(), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL((*aggregates[1])["phase"].GetStringSafe(), "final");

        const auto& partialTrait = (*aggregates[0])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["input"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["function"].GetStringSafe(), "sum");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["output"].GetStringSafe(), "_state");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["type"].GetStringSafe(), "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(partialTrait["nullable"].GetBooleanSafe(), true);

        const auto& finalTrait = (*aggregates[1])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["input"].GetStringSafe(), "_state");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["function"].GetStringSafe(), "sum");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["output"].GetStringSafe(), "result");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["type"].GetStringSafe(), "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(finalTrait["nullable"].GetBooleanSafe(), true);
    }

    Y_UNIT_TEST(ExportsDecimalAvgUndefinedStateContract) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int64", true},
            {"x", "Decimal(12,2)", true},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        const auto* keyType = ScalarType(ctx, NUdf::EDataSlot::Int64);
        const auto* valueType = DecimalType(ctx, "12", "2");
        const auto* sumType = DecimalType(ctx, "35", "2");
        SetExactOutputType(ctx, *read, {
            {"a.k", keyType},
            {"a.x", valueType},
        });

        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "avg",
                    TInfoUnit("average")),
                TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "sum",
                    TInfoUnit("total")),
            },
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Undefined,
            false,
            pos);
        SetExactOutputType(ctx, *aggregate, {
            {"a.k", keyType},
            {"average", valueType},
            {"total", sumType},
        });
        TOpRoot root(aggregate, pos, {"a.k", "average", "total"});

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& node = FindNode(snapshot, "aggregate");
        UNIT_ASSERT_VALUES_EQUAL(node["phase"].GetStringSafe(), "undefined");
        const auto& traits = node["aggregates"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(traits.size(), 2);

        const auto& average = traits[0];
        UNIT_ASSERT_VALUES_EQUAL(average.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(average["input"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(average["function"].GetStringSafe(), "avg");
        UNIT_ASSERT_VALUES_EQUAL(average["output"].GetStringSafe(), "average");
        UNIT_ASSERT_VALUES_EQUAL(average["type"].GetStringSafe(), "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(average["nullable"].GetBooleanSafe(), false);
        const auto& state = average["state"];
        UNIT_ASSERT_VALUES_EQUAL(state.GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            state["sum_type"].GetStringSafe(),
            "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            state["count_type"].GetStringSafe(),
            "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(state["nullable"].GetBooleanSafe(), false);

        const auto& sum = traits[1];
        UNIT_ASSERT_VALUES_EQUAL(sum["function"].GetStringSafe(), "sum");
        UNIT_ASSERT(!sum.Has("state"));
    }

    Y_UNIT_TEST(ExportsDecimalAvgSplitStateContract) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int64", true},
            {"x", "Decimal(12,2)", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        const auto* keyType = ScalarType(ctx, NUdf::EDataSlot::Int64);
        const auto* valueType = DecimalType(ctx, "12", "2", true);
        SetExactOutputType(ctx, *read, {
            {"a.k", keyType},
            {"a.x", valueType},
        });

        const auto pos = TPositionHandle();
        auto partial = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"),
                "avg",
                TInfoUnit("_intermediate_average"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Intermediate,
            false,
            pos);
        SetExactOutputType(ctx, *partial, {
            {"a.k", keyType},
            {"_intermediate_average", valueType},
        });

        auto final = MakeIntrusive<TOpAggregate>(
            partial,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("_intermediate_average"),
                "avg",
                TInfoUnit("average"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Final,
            false,
            pos);
        SetExactOutputType(ctx, *final, {
            {"a.k", keyType},
            {"average", valueType},
        });
        TOpRoot root(final, pos, {"a.k", "average"});

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        TVector<const NJson::TJsonValue*> aggregates;
        for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() == "aggregate") {
                aggregates.push_back(&node);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(aggregates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            (*aggregates[0])["phase"].GetStringSafe(),
            "intermediate");
        UNIT_ASSERT_VALUES_EQUAL(
            (*aggregates[1])["phase"].GetStringSafe(),
            "final");
        UNIT_ASSERT_VALUES_EQUAL(
            (*aggregates[1])["input"].GetStringSafe(),
            (*aggregates[0])["id"].GetStringSafe());

        const auto& partialTrait = (*aggregates[0])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["input"].GetStringSafe(),
            "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["output"].GetStringSafe(),
            "_intermediate_average");
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["type"].GetStringSafe(),
            "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["state"]["sum_type"].GetStringSafe(),
            "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["state"]["count_type"].GetStringSafe(),
            "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(
            partialTrait["state"]["nullable"].GetBooleanSafe(),
            true);

        const auto& finalTrait = (*aggregates[1])["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["input"].GetStringSafe(),
            "_intermediate_average");
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["output"].GetStringSafe(),
            "average");
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["type"].GetStringSafe(),
            "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["state"]["sum_type"].GetStringSafe(),
            "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["state"]["count_type"].GetStringSafe(),
            "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(
            finalTrait["state"]["nullable"].GetBooleanSafe(),
            true);
    }

    Y_UNIT_TEST(DecimalAvgStateContractFailsClosed) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Integral", {
                {"x", "Int64", false},
            });
            auto read = MakeRead(ctx, table, "a", {"x"});
            SetOutputType(ctx, *read, {
                {"a.x", NUdf::EDataSlot::Int64, true},
            });
            const auto pos = TPositionHandle();
            auto aggregate = MakeIntrusive<TOpAggregate>(
                read,
                TVector<TOpAggregationTraits>{TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "avg",
                    TInfoUnit("average"))},
                TVector<TInfoUnit>{},
                EOpPhase::Undefined,
                false,
                pos);
            SetOutputType(ctx, *aggregate, {
                {"average", NUdf::EDataSlot::Int64, true},
            });
            TOpRoot root(aggregate, pos, {"average"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Aggregate avg requires identical canonical Decimal input and output types");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/Mismatch", {
                {"x", "Decimal(12,2)", false},
            });
            auto read = MakeRead(ctx, table, "a", {"x"});
            SetExactOutputType(ctx, *read, {
                {"a.x", DecimalType(ctx, "12", "2", true)},
            });
            const auto pos = TPositionHandle();
            auto aggregate = MakeIntrusive<TOpAggregate>(
                read,
                TVector<TOpAggregationTraits>{TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "avg",
                    TInfoUnit("average"))},
                TVector<TInfoUnit>{},
                EOpPhase::Undefined,
                false,
                pos);
            SetExactOutputType(ctx, *aggregate, {
                {"average", DecimalType(ctx, "13", "2", true)},
            });
            TOpRoot root(aggregate, pos, {"average"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Aggregate avg requires identical canonical Decimal input and output types");
        }
    }

    Y_UNIT_TEST(ExportsExactDirectCountDistinctContract) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DirectCountDistinct", {
            {"x", "Int64", true},
            {"y", "Int64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x", "y"});
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64},
            {"a.y", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "count",
                    TInfoUnit("distinct_count"),
                    true,
                    false),
                TOpAggregationTraits(
                    TInfoUnit("a.y"),
                    "sum",
                    TInfoUnit("ordinary_sum")),
            },
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"distinct_count", NUdf::EDataSlot::Uint64},
            {"ordinary_sum", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot root(
            aggregate,
            pos,
            {"distinct_count", "ordinary_sum"});

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& node = FindNode(snapshot, "aggregate");
        UNIT_ASSERT_VALUES_EQUAL(
            node["phase"].GetStringSafe(),
            "undefined");
        UNIT_ASSERT_VALUES_EQUAL(
            node["distinct_all"].GetBooleanSafe(),
            false);
        UNIT_ASSERT(node["keys"].GetArraySafe().empty());
        const auto& traits = node["aggregates"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(traits.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["input"].GetStringSafe(),
            "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["function"].GetStringSafe(),
            "count");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["output"].GetStringSafe(),
            "distinct_count");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["type"].GetStringSafe(),
            "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["nullable"].GetBooleanSafe(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["distinct"].GetBooleanSafe(),
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["unwrap"].GetBooleanSafe(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            traits[1]["function"].GetStringSafe(),
            "sum");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[1]["distinct"].GetBooleanSafe(),
            false);
    }

    Y_UNIT_TEST(DirectCountDistinctContractFailsClosedForEveryLocalMutation) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DirectCountDistinct", {
            {"x", "Int64", true},
        });
        auto read = MakeRead(ctx, table, "a", {"x"});
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"),
                "count",
                TInfoUnit("result"),
                true,
                false)},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64},
        });
        TOpRoot root(aggregate, pos, {"result"});
        ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));

        const auto reject = [&](TStringBuf expectedReason) {
            const auto result =
                ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReason);
        };

        aggregate->AggregationTraitsList.push_back(
            TOpAggregationTraits(
                TInfoUnit("a.x"),
                "count",
                TInfoUnit("second_result"),
                true,
                false));
        reject("at most one ordinary distinct trait");
        aggregate->AggregationTraitsList.pop_back();

        aggregate->KeyColumns.push_back(TInfoUnit("a.x"));
        SetOutputType(ctx, *aggregate, {
            {"a.x", NUdf::EDataSlot::Int64},
            {"result", NUdf::EDataSlot::Uint64},
        });
        reject("requires a keyless Aggregate");
        aggregate->KeyColumns.clear();
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64},
        });

        aggregate->AggregationPhase = EOpPhase::Intermediate;
        reject("requires undefined phase");
        aggregate->AggregationPhase = EOpPhase::Final;
        reject("requires undefined phase");
        aggregate->AggregationPhase = EOpPhase::Undefined;

        aggregate->DistinctAll = true;
        aggregate->KeyColumns.push_back(TInfoUnit("a.x"));
        reject("plain distinct aliases");
        aggregate->KeyColumns.clear();
        aggregate->DistinctAll = false;

        aggregate->AggregationTraitsList.front().AggFunction = "sum";
        reject("Ordinary distinct requires the count function");
        aggregate->AggregationTraitsList.front().AggFunction = "count";

        aggregate->AggregationTraitsList.front().Unwrap = true;
        reject("Aggregate unwrap requires distinct=false");
        aggregate->AggregationTraitsList.front().Unwrap = false;

        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64, true},
        });
        reject("requires an exact non-null Int64 input");
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Uint64},
        });
        reject("requires an exact non-null Int64 input");
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64},
        });

        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64, true},
        });
        reject("requires an exact non-null Uint64 output");
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Int64},
        });
        reject("requires an exact non-null Uint64 output");
    }

    Y_UNIT_TEST(ExportsExactFinalScalarUint64UnwrapContract) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"x", "Int64", false},
            {"y", "Uint64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x", "y"});
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64, true},
            {"a.y", NUdf::EDataSlot::Uint64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "sum",
                    TInfoUnit("ordinary_sum_result"),
                    false,
                    false),
                TOpAggregationTraits(
                    TInfoUnit("a.y"),
                    "sum",
                    TInfoUnit("unwrap_result"),
                    false,
                    true),
            },
            TVector<TInfoUnit>{},
            EOpPhase::Final,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"ordinary_sum_result", NUdf::EDataSlot::Int64, true},
            {"unwrap_result", NUdf::EDataSlot::Uint64, true},
        });
        TOpRoot root(aggregate, pos, {"ordinary_sum_result", "unwrap_result"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& node = FindNode(snapshot, "aggregate");
        const auto& traits = node["aggregates"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(traits.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["input"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            traits[0]["output"].GetStringSafe(),
            "ordinary_sum_result");
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["distinct"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["unwrap"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["input"].GetStringSafe(), "a.y");
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["output"].GetStringSafe(), "unwrap_result");
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["distinct"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["unwrap"].GetBooleanSafe(), true);
    }

    Y_UNIT_TEST(AggregateUnwrapContractFailsClosedForEveryLocalMutation) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/Unwrap", {
            {"x", "Uint64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x"});
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Uint64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"),
                "sum",
                TInfoUnit("result"),
                false,
                true)},
            TVector<TInfoUnit>{},
            EOpPhase::Final,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64, true},
        });
        TOpRoot root(aggregate, pos, {"result"});

        const auto reject = [&](TStringBuf expectedReason) {
            const auto result =
                ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReason);
        };

        aggregate->AggregationPhase = EOpPhase::Intermediate;
        reject("Aggregate unwrap requires final phase");
        aggregate->AggregationPhase = EOpPhase::Final;

        aggregate->AggregationTraitsList.front().AggFunction = "count";
        reject("Aggregate unwrap requires the sum function");
        aggregate->AggregationTraitsList.front().AggFunction = "sum";

        aggregate->AggregationTraitsList.front().Distinct = true;
        reject("Aggregate unwrap requires distinct=false");
        aggregate->AggregationTraitsList.front().Distinct = false;

        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Uint64},
        });
        reject("requires an exact Optional<Uint64> input");
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64, true},
        });
        reject("requires an exact Optional<Uint64> input");
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Uint64, true},
        });

        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64},
        });
        reject("requires an exact Optional<Uint64> raw output");
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        reject("requires an exact Optional<Uint64> raw output");

        {
            TExportTestContext groupedCtx;
            const auto& groupedTable = AddTable(
                groupedCtx,
                "/Root/GroupedUnwrap",
                {{"x", "Uint64", false}});
            auto groupedRead = MakeRead(
                groupedCtx,
                groupedTable,
                "a",
                {"x"});
            SetOutputType(groupedCtx, *groupedRead, {
                {"a.x", NUdf::EDataSlot::Uint64, true},
            });
            auto grouped = MakeIntrusive<TOpAggregate>(
                groupedRead,
                TVector<TOpAggregationTraits>{TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "sum",
                    TInfoUnit("result"),
                    false,
                    true)},
                TVector<TInfoUnit>{TInfoUnit("a.x")},
                EOpPhase::Final,
                false,
                pos);
            SetOutputType(groupedCtx, *grouped, {
                {"a.x", NUdf::EDataSlot::Uint64, true},
                {"result", NUdf::EDataSlot::Uint64, true},
            });
            TOpRoot groupedRoot(
                grouped,
                pos,
                {"a.x", "result"});
            const auto result = ExportSemanticSnapshotV1(
                groupedRoot,
                groupedCtx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Aggregate unwrap requires a keyless Aggregate");
        }
    }

    Y_UNIT_TEST(DistinctAllContractIsExactAndPositional) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"x", "Int64", false},
            {"y", "Int64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"x", "y"});
        SetOutputType(ctx, *read, {
            {"a.x", NUdf::EDataSlot::Int64, true},
            {"a.y", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto distinctAll = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(
                    TInfoUnit("a.x"), "distinct", TInfoUnit("distinct_x")),
                TOpAggregationTraits(
                    TInfoUnit("a.y"), "distinct", TInfoUnit("distinct_y")),
            },
            TVector<TInfoUnit>{TInfoUnit("a.x"), TInfoUnit("a.y")},
            EOpPhase::Undefined,
            true,
            pos);
        SetOutputType(ctx, *distinctAll, {
            {"distinct_x", NUdf::EDataSlot::Int64, true},
            {"distinct_y", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot distinctRoot(
            distinctAll,
            pos,
            {"distinct_x", "distinct_y"});
        const auto distinctSnapshot = ParseSupported(
            ExportSemanticSnapshotV1(distinctRoot, ctx.RboCtx));
        const auto& distinctNode = FindNode(distinctSnapshot, "aggregate");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctNode["distinct_all"].GetBooleanSafe(),
            true);
        const auto& distinctKeys = distinctNode["keys"].GetArraySafe();
        const auto& distinctTraits =
            distinctNode["aggregates"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(distinctKeys.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(distinctTraits.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            distinctKeys[0].GetStringSafe(),
            "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctTraits[0]["input"].GetStringSafe(),
            "a.x");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctTraits[0]["output"].GetStringSafe(),
            "distinct_x");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctKeys[1].GetStringSafe(),
            "a.y");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctTraits[1]["input"].GetStringSafe(),
            "a.y");
        UNIT_ASSERT_VALUES_EQUAL(
            distinctTraits[1]["output"].GetStringSafe(),
            "distinct_y");

        const auto assertDistinctUnsupported = [&](
            TStringBuf expectedReason)
        {
            const auto result =
                ExportSemanticSnapshotV1(distinctRoot, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                expectedReason);
        };

        distinctAll->KeyColumns.clear();
        assertDistinctUnsupported("one distinct trait for each ordered key");
        distinctAll->KeyColumns = {
            TInfoUnit("a.x"),
            TInfoUnit("a.y"),
        };

        distinctAll->KeyColumns = {
            TInfoUnit("a.y"),
            TInfoUnit("a.x"),
        };
        assertDistinctUnsupported("plain distinct aliases");
        distinctAll->KeyColumns = {
            TInfoUnit("a.x"),
            TInfoUnit("a.y"),
        };

        distinctAll->AggregationTraitsList.front().OriginalColName =
            TInfoUnit("a.y");
        assertDistinctUnsupported("plain distinct aliases");
        distinctAll->AggregationTraitsList.front().OriginalColName =
            TInfoUnit("a.x");

        distinctAll->AggregationTraitsList.front().AggFunction = "count";
        assertDistinctUnsupported("plain distinct aliases");
        distinctAll->AggregationTraitsList.front().AggFunction = "distinct";

        distinctAll->AggregationTraitsList.front().Distinct = true;
        assertDistinctUnsupported("plain distinct aliases");
        distinctAll->AggregationTraitsList.front().Distinct = false;

        distinctAll->AggregationTraitsList.front().Unwrap = true;
        assertDistinctUnsupported(
            "Aggregate unwrap is not supported for DistinctAll");
        distinctAll->AggregationTraitsList.front().Unwrap = false;

        SetOutputType(ctx, *distinctAll, {
            {"distinct_x", NUdf::EDataSlot::Uint64, true},
            {"distinct_y", NUdf::EDataSlot::Int64, true},
        });
        assertDistinctUnsupported("type and nullability");
    }

    Y_UNIT_TEST(AggregateTypeAnnotationMismatchFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int64", true},
            {"x", "Int64", true},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"a.x", NUdf::EDataSlot::Int64},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"), "count", TInfoUnit("result"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Undefined,
            false,
            pos);
        TOpRoot root(aggregate, pos, {"a.k", "result"});

        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Uint64},
        });
        auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "omits IU a.k");

        SetOutputType(ctx, *aggregate, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"result", NUdf::EDataSlot::Uint64},
            {"ghost", NUdf::EDataSlot::Int64},
        });
        result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "field count");

        SetOutputType(ctx, *aggregate, {
            {"a.k", NUdf::EDataSlot::Uint64},
            {"result", NUdf::EDataSlot::Uint64},
        });
        result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "key output type");

        SetOutputType(ctx, *aggregate, {
            {"a.k", NUdf::EDataSlot::Int64},
            {"result", NUdf::EDataSlot::Uint64},
        });
        aggregate->Props.OutputIUs = TVector<TInfoUnit>{
            TInfoUnit("result"),
            TInfoUnit("a.k"),
        };
        result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "output IU order");
    }

    Y_UNIT_TEST(ExportsLimitCountOffsetAndPhase) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto limit = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "3", pos, &ctx.ExprCtx),
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Final);
        TOpRoot root(limit, pos, {"a.k"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& node = FindNode(snapshot, "limit");
        UNIT_ASSERT_VALUES_EQUAL(node["input"].GetStringSafe(), FindNode(snapshot, "scan")["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(node["count"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(node["count"]["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(node["count"]["value"].GetUIntegerSafe(), 3);
        UNIT_ASSERT_VALUES_EQUAL(node["offset"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(node["offset"]["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(node["offset"]["value"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(node["phase"].GetStringSafe(), "final");
        UNIT_ASSERT_VALUES_EQUAL(
            node["ensure_at_most_one"].GetBooleanSafe(),
            false);

        limit->Props.EnsureAtMostOne = true;
        const auto checked =
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(checked, "limit")["ensure_at_most_one"].GetBooleanSafe(),
            true);
    }

    Y_UNIT_TEST(ExportsEveryLimitPhaseNullOffsetAndUint64Max) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        const TVector<std::pair<EOpPhase, TString>> phases = {
            {EOpPhase::Undefined, "undefined"},
            {EOpPhase::Intermediate, "intermediate"},
            {EOpPhase::Final, "final"},
        };
        for (const auto& [phase, expected] : phases) {
            auto limit = MakeIntrusive<TOpLimit>(
                read,
                pos,
                MakeConstant(
                    "Uint64",
                    "18446744073709551615",
                    pos,
                    &ctx.ExprCtx),
                phase);
            TOpRoot root(limit, pos, {"a.k"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& node = FindNode(snapshot, "limit");
            UNIT_ASSERT_VALUES_EQUAL(
                node["count"]["value"].GetUIntegerSafe(),
                std::numeric_limits<ui64>::max());
            UNIT_ASSERT(node["offset"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(node["phase"].GetStringSafe(), expected);
        }
    }

    Y_UNIT_TEST(ExportsPlainSortTopSortOrderLimitAndEveryPhase) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"payload", "Int32", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "payload"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.payload", NUdf::EDataSlot::Int32, true},
        });
        const auto pos = TPositionHandle();
        const TVector<TSortElement> order = {
            TSortElement(TInfoUnit("a.k"), true, false),
            TSortElement(TInfoUnit("a.payload"), false, true),
        };

        auto plainSort = MakeIntrusive<TOpSort>(read, pos, order);
        SetOutputType(ctx, *plainSort, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.payload", NUdf::EDataSlot::Int32, true},
        });
        TOpRoot plainRoot(plainSort, pos, {"a.k", "a.payload"});
        const auto plainSnapshot = ParseSupported(
            ExportSemanticSnapshotV1(plainRoot, ctx.RboCtx));
        const auto& plain = FindNode(plainSnapshot, "sort");
        UNIT_ASSERT_VALUES_EQUAL(plain.GetMapSafe().size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(
            plain["input"].GetStringSafe(),
            FindNode(plainSnapshot, "scan")["id"].GetStringSafe());
        UNIT_ASSERT(plain["limit"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(plain["phase"].GetStringSafe(), "undefined");
        const auto& plainOrder = plain["order"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(plainOrder.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[0].GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[0]["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[0]["ascending"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[0]["nulls_first"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[1]["column"].GetStringSafe(), "a.payload");
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[1]["ascending"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(plainOrder[1]["nulls_first"].GetBooleanSafe(), true);

        const TVector<std::pair<EOpPhase, TString>> phases = {
            {EOpPhase::Undefined, "undefined"},
            {EOpPhase::Intermediate, "intermediate"},
            {EOpPhase::Final, "final"},
        };
        for (const auto& [phase, expected] : phases) {
            auto topSort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TPhysicalOpProps{},
                order,
                std::optional<TExpression>{MakeConstant(
                    "Uint64",
                    "18446744073709551615",
                    pos,
                    &ctx.ExprCtx)},
                phase);
            SetOutputType(ctx, *topSort, {
                {"a.k", NUdf::EDataSlot::Int32},
                {"a.payload", NUdf::EDataSlot::Int32, true},
            });
            TOpRoot topRoot(topSort, pos, {"a.k", "a.payload"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(topRoot, ctx.RboCtx));
            const auto& top = FindNode(snapshot, "sort");
            UNIT_ASSERT_VALUES_EQUAL(top["limit"]["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(top["limit"]["type"].GetStringSafe(), "Uint64");
            UNIT_ASSERT_VALUES_EQUAL(
                top["limit"]["value"].GetUIntegerSafe(),
                std::numeric_limits<ui64>::max());
            UNIT_ASSERT_VALUES_EQUAL(top["phase"].GetStringSafe(), expected);
            UNIT_ASSERT_VALUES_EQUAL(top["order"].GetArraySafe().size(), 2);
        }
    }

    Y_UNIT_TEST(DecimalOrderingContractMatchesRuntimeComparator) {
        using NYql::NDecimal::Inf;
        using NYql::NDecimal::Nan;
        using NYql::NDecimal::TInt128;

        const TVector<TInt128> values = {
            -Inf(),
            TInt128(-1),
            TInt128(0),
            TInt128(1),
            Inf(),
            Nan(),
        };
        for (size_t left = 0; left < values.size(); ++left) {
            for (size_t right = 0; right < values.size(); ++right) {
                const auto comparison =
                    NUdf::CompareValues<NUdf::EDataSlot::Decimal>(
                        NUdf::TUnboxedValuePod(values[left]),
                        NUdf::TUnboxedValuePod(values[right]));
                UNIT_ASSERT_VALUES_EQUAL(
                    comparison,
                    left == right ? 0 : (left < right ? -1 : 1));
            }
        }
    }

    Y_UNIT_TEST(StringOrderingContractMatchesRuntimeComparator) {
        const auto value = [](const TString& bytes) {
            return NUdf::TUnboxedValuePod::Embedded(
                NUdf::TStringRef(bytes.data(), bytes.size()));
        };
        const auto expectedSign = [](size_t left, size_t right) {
            return left == right ? 0 : (left < right ? -1 : 1);
        };
        const auto assertComparison = [&](int comparison, int expected) {
            UNIT_ASSERT_VALUES_EQUAL(
                comparison == 0 ? 0 : (comparison < 0 ? -1 : 1),
                expected);
        };

        const TVector<TString> validUtf8 = {
            TString(),
            TString("\0", 1),
            TString("\0\0", 2),
            TString("a"),
            TString("a\0", 2),
            TString("aa"),
            TString("e\xCC\x81", 3), // NFD: e + combining acute accent.
            TString("z"),
            TString("\x7F", 1),
            TString("\xC3\xA9", 2), // NFC: precomposed e-acute.
            TString("\xE4\xB8\xAD", 3),
        };
        for (size_t left = 0; left < validUtf8.size(); ++left) {
            for (size_t right = 0; right < validUtf8.size(); ++right) {
                const int expected = expectedSign(left, right);
                assertComparison(
                    NUdf::CompareValues<NUdf::EDataSlot::String>(
                        value(validUtf8[left]),
                        value(validUtf8[right])),
                    expected);
                assertComparison(
                    NUdf::CompareValues<NUdf::EDataSlot::Utf8>(
                        value(validUtf8[left]),
                        value(validUtf8[right])),
                    expected);
            }
        }

        const TVector<TString> arbitraryBytes = {
            TString("\x7F", 1),
            TString(1, static_cast<char>(0x80)),
            TString("\xC3\xA9", 2),
            TString(1, static_cast<char>(0xFF)),
        };
        for (size_t left = 0; left < arbitraryBytes.size(); ++left) {
            for (size_t right = 0; right < arbitraryBytes.size(); ++right) {
                assertComparison(
                    NUdf::CompareValues<NUdf::EDataSlot::String>(
                        value(arbitraryBytes[left]),
                        value(arbitraryBytes[right])),
                    expectedSign(left, right));
            }
        }

        UNIT_ASSERT(NUdf::IsComparable(
            NUdf::EDataSlot::String,
            NUdf::EDataSlot::Utf8));
        UNIT_ASSERT(NUdf::IsComparable(
            NUdf::EDataSlot::Utf8,
            NUdf::EDataSlot::String));
    }

    Y_UNIT_TEST(StringHashContractIsTypeIndependent) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NKikimr::NMiniKQL::TTypeEnvironment environment(alloc);
        auto* stringType = NKikimr::NMiniKQL::TDataType::Create(
            NUdf::GetDataTypeInfo(NUdf::EDataSlot::String).TypeId,
            environment);
        auto* utf8Type = NKikimr::NMiniKQL::TDataType::Create(
            NUdf::GetDataTypeInfo(NUdf::EDataSlot::Utf8).TypeId,
            environment);
        auto* optionalStringType = NKikimr::NMiniKQL::TOptionalType::Create(
            stringType,
            environment);
        auto* optionalUtf8Type = NKikimr::NMiniKQL::TOptionalType::Create(
            utf8Type,
            environment);

        const auto stringHasher = NKikimr::NMiniKQL::MakeHashImpl(stringType);
        const auto utf8Hasher = NKikimr::NMiniKQL::MakeHashImpl(utf8Type);
        const auto optionalStringHasher = NKikimr::NMiniKQL::MakeHashImpl(
            optionalStringType);
        const auto optionalUtf8Hasher = NKikimr::NMiniKQL::MakeHashImpl(
            optionalUtf8Type);

        NKikimr::NMiniKQL::TBlockTypeHelper blockTypeHelper;
        const auto stringBlockHasher = blockTypeHelper.MakeHasher(stringType);
        const auto utf8BlockHasher = blockTypeHelper.MakeHasher(utf8Type);
        const auto optionalStringBlockHasher = blockTypeHelper.MakeHasher(
            optionalStringType);
        const auto optionalUtf8BlockHasher = blockTypeHelper.MakeHasher(
            optionalUtf8Type);

        const TVector<TString> bytes = {
            TString(),
            TString("\0", 1),
            TString("a\0b", 3),
            TString(1, static_cast<char>(0x80)),
            TString("\xC3\xA9", 2),
            TString(1, static_cast<char>(0xFF)),
        };
        for (const auto& item : bytes) {
            const NUdf::TStringRef reference(item.data(), item.size());
            const auto value = NUdf::TUnboxedValuePod::Embedded(reference);
            const NUdf::TBlockItem blockValue(reference);

            const ui64 directHash =
                NUdf::GetValueHash<NUdf::EDataSlot::String>(value);
            UNIT_ASSERT_VALUES_EQUAL(
                directHash,
                NUdf::GetValueHash<NUdf::EDataSlot::Utf8>(value));
            UNIT_ASSERT_VALUES_EQUAL(
                directHash,
                NUdf::GetValueHash(NUdf::EDataSlot::String, value));
            UNIT_ASSERT_VALUES_EQUAL(
                directHash,
                NUdf::GetValueHash(NUdf::EDataSlot::Utf8, value));

            UNIT_ASSERT_VALUES_EQUAL(
                stringHasher->Hash(value),
                utf8Hasher->Hash(value));
            UNIT_ASSERT_VALUES_EQUAL(stringHasher->Hash(value), directHash);
            UNIT_ASSERT_VALUES_EQUAL(
                optionalStringHasher->Hash(value),
                optionalUtf8Hasher->Hash(value));

            UNIT_ASSERT_VALUES_EQUAL(
                stringBlockHasher->Hash(blockValue),
                utf8BlockHasher->Hash(blockValue));
            UNIT_ASSERT_VALUES_EQUAL(
                stringBlockHasher->Hash(blockValue),
                directHash);
            UNIT_ASSERT_VALUES_EQUAL(
                optionalStringBlockHasher->Hash(blockValue),
                optionalUtf8BlockHasher->Hash(blockValue));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            optionalStringHasher->Hash(NUdf::TUnboxedValuePod()),
            optionalUtf8Hasher->Hash(NUdf::TUnboxedValuePod()));
        UNIT_ASSERT_VALUES_EQUAL(
            optionalStringBlockHasher->Hash(NUdf::TBlockItem()),
            optionalUtf8BlockHasher->Hash(NUdf::TBlockItem()));
    }

    Y_UNIT_TEST(DecimalSumAccumulatorOverflowRequiresHeadroom) {
        using NYql::NDecimal::Add;
        using NYql::NDecimal::Inf;

        const auto maximum = Inf() - 1;
        const auto leftAssociated = Add(Add(maximum, maximum, 35), -maximum, 35);
        const auto rightAssociated = Add(
            maximum,
            Add(maximum, -maximum, 35),
            35);
        UNIT_ASSERT(leftAssociated == Inf());
        UNIT_ASSERT(rightAssociated == maximum);
    }

    Y_UNIT_TEST(ExportsDateDecimalAndTextSortOrdering) {
        const auto pos = TPositionHandle();
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/DateSort", {
                {"d", "Date", false},
            });
            auto read = MakeRead(ctx, table, "a", {"d"});
            const auto* optionalDate = ScalarType(ctx, NUdf::EDataSlot::Date, true);
            SetExactOutputType(ctx, *read, {{"a.d", optionalDate}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.d"), true, false)});
            SetExactOutputType(ctx, *sort, {{"a.d", optionalDate}});
            TOpRoot root(sort, pos, {"a.d"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot["schema"]["tables"][0]["columns"][0]["type"].GetStringSafe(),
                "Date");
            const auto& order = FindNode(snapshot, "sort")["order"][0];
            UNIT_ASSERT_VALUES_EQUAL(order["column"].GetStringSafe(), "a.d");
            UNIT_ASSERT(order["ascending"].GetBooleanSafe());
            UNIT_ASSERT(!order["nulls_first"].GetBooleanSafe());
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/DecimalSort", {
                {"value", "Decimal(5,2)", false},
            });
            const auto* optionalDecimal = DecimalType(ctx, "5", "2", true);
            auto read = MakeRead(ctx, table, "a", {"value"});
            SetExactOutputType(ctx, *read, {{"a.value", optionalDecimal}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{
                    TSortElement(TInfoUnit("a.value"), false, true)});
            SetExactOutputType(ctx, *sort, {{"a.value", optionalDecimal}});
            TOpRoot root(sort, pos, {"a.value"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot["schema"]["tables"][0]["columns"][0]["type"].GetStringSafe(),
                "Decimal(5,2)");
            const auto& order = FindNode(snapshot, "sort")["order"][0];
            UNIT_ASSERT_VALUES_EQUAL(order["column"].GetStringSafe(), "a.value");
            UNIT_ASSERT(!order["ascending"].GetBooleanSafe());
            UNIT_ASSERT(order["nulls_first"].GetBooleanSafe());
        }

        for (const TString typeName : {"String", "Utf8"}) {
            TExportTestContext ctx;
            const TTypeAnnotationNode* dataType = nullptr;
            if (typeName == "String") {
                dataType = ScalarType(ctx, NUdf::EDataSlot::String);
            } else {
                dataType = ScalarType(ctx, NUdf::EDataSlot::Utf8);
            }
            const auto* optionalType = ctx.ExprCtx.MakeType<TOptionalExprType>(dataType);
            const auto& table = AddTable(ctx, "/Root/NonOrdered", {
                {"value", typeName, false},
            });
            auto read = MakeRead(ctx, table, "a", {"value"});
            SetExactOutputType(ctx, *read, {{"a.value", optionalType}});
            const bool ascending = typeName == "String";
            const bool nullsFirst = !ascending;
            TIntrusivePtr<TOpSort> sort;
            if (typeName == "String") {
                sort = MakeIntrusive<TOpSort>(
                    read,
                    pos,
                    TVector<TSortElement>{TSortElement(
                        TInfoUnit("a.value"),
                        ascending,
                        nullsFirst)});
            } else {
                sort = MakeIntrusive<TOpSort>(
                    read,
                    pos,
                    TPhysicalOpProps{},
                    TVector<TSortElement>{TSortElement(
                        TInfoUnit("a.value"),
                        ascending,
                        nullsFirst)},
                    std::optional<TExpression>{MakeConstant(
                        "Uint64",
                        "7",
                        pos,
                        &ctx.ExprCtx)},
                    EOpPhase::Final);
            }
            SetExactOutputType(ctx, *sort, {{"a.value", optionalType}});
            TOpRoot root(sort, pos, {"a.value"});

            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(root, ctx.RboCtx));
            UNIT_ASSERT_VALUES_EQUAL(
                snapshot["schema"]["tables"][0]["columns"][0]["type"].GetStringSafe(),
                typeName);
            const auto& node = FindNode(snapshot, "sort");
            const auto& order = node["order"][0];
            UNIT_ASSERT_VALUES_EQUAL(order["column"].GetStringSafe(), "a.value");
            UNIT_ASSERT_VALUES_EQUAL(order["ascending"].GetBooleanSafe(), ascending);
            UNIT_ASSERT_VALUES_EQUAL(order["nulls_first"].GetBooleanSafe(), nullsFirst);
            if (typeName == "String") {
                UNIT_ASSERT(node["limit"].IsNull());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(node["limit"]["value"].GetUIntegerSafe(), 7);
                UNIT_ASSERT_VALUES_EQUAL(node["phase"].GetStringSafe(), "final");
            }
        }
    }

    Y_UNIT_TEST(InvalidSortSemanticsFailClosed) {
        const auto pos = TPositionHandle();

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            auto sort = MakeIntrusive<TOpSort>(read, pos, TVector<TSortElement>{});
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Int32}});
            TOpRoot root(sort, pos, {"a.k"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort order must not be empty");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("missing"), true, true)});
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Int32}});
            TOpRoot root(sort, pos, {"a.k"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Invalid Sort key missing");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"flag", "Bool", true}});
            auto read = MakeRead(ctx, table, "a", {"flag"});
            SetOutputType(ctx, *read, {{"a.flag", NUdf::EDataSlot::Bool}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.flag"), true, true)});
            SetOutputType(ctx, *sort, {{"a.flag", NUdf::EDataSlot::Bool}});
            TOpRoot root(sort, pos, {"a.flag"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Sort ordering column a.flag has unsupported type Bool");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            const TVector<TSortElement> order = {
                TSortElement(TInfoUnit("a.k"), true, true),
            };
            auto exportLimit = [&](TExpression expression) {
                auto sort = MakeIntrusive<TOpSort>(
                    read,
                    pos,
                    TPhysicalOpProps{},
                    order,
                    std::optional<TExpression>{std::move(expression)},
                    EOpPhase::Undefined);
                SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Int32}});
                TOpRoot root(sort, pos, {"a.k"});
                return ExportSemanticSnapshotV1(root, ctx.RboCtx);
            };

            auto malformed = MakeConstant("Uint64", "1", pos, &ctx.ExprCtx);
            malformed.Node = nullptr;
            auto result = exportLimit(std::move(malformed));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort limit is not a one-body lambda");

            auto nonLiteralBody = ctx.ExprCtx.NewCallable(
                pos,
                "Uint64",
                {ctx.ExprCtx.NewCallable(pos, "Void", {})});
            result = exportLimit(TExpression(nonLiteralBody, &ctx.ExprCtx));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort limit must be a Uint64 literal");

            result = exportLimit(MakeConstant("Int64", "1", pos, &ctx.ExprCtx));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort limit must be a Uint64 literal");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {
                {"k", "Int32", true},
                {"x", "Int32", true},
            });
            auto read = MakeRead(ctx, table, "a", {"k", "x"});
            SetOutputType(ctx, *read, {
                {"a.k", NUdf::EDataSlot::Int32},
                {"a.x", NUdf::EDataSlot::Int32},
            });
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.k"), true, true)});
            SetOutputType(ctx, *sort, {
                {"a.k", NUdf::EDataSlot::Int32},
                {"a.x", NUdf::EDataSlot::Int32},
            });
            sort->Props.OutputIUs = TVector<TInfoUnit>{
                TInfoUnit("a.x"),
                TInfoUnit("a.k"),
            };
            TOpRoot root(sort, pos, {"a.k", "a.x"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort output IUs");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.k"), true, true)});
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Uint32}});
            TOpRoot root(sort, pos, {"a.k"});

            auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort output type");

        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TPhysicalOpProps{},
                TVector<TSortElement>{TSortElement(TInfoUnit("a.k"), true, true)},
                std::nullopt,
                static_cast<EOpPhase>(99));
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Int32}});
            TOpRoot root(sort, pos, {"a.k"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unknown operator phase");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.k"), true, true)});
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Int32}});
            sort->Children.clear();
            TOpRoot root(sort, pos, {"a.k"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Sort must have one input");
        }
    }

    Y_UNIT_TEST(ExportsActualOlapFilterDialectAtAStageBoundary) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(
            ctx,
            table,
            "a",
            {"k"},
            NYql::EStorageType::ColumnStorage);
        SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
        read->OlapFilterLambda = MakeOlapComparisonProcess(
            ctx,
            "gte",
            "k",
            "30",
            true);
        TOpRoot root(read, TPositionHandle(), {"a.k"});
        read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& predicate = FindNode(snapshot, "scan")["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "gte");
        UNIT_ASSERT_VALUES_EQUAL(predicate["left"]["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["value"].GetIntegerSafe(), 30);
    }

    Y_UNIT_TEST(OlapFilterResolvesExactReadOutputNames) {
        const auto check = [](TString output, TString reference) {
            TExportTestContext ctx;
            const auto& table = AddTable(
                ctx,
                "/Root/A",
                {{"k", "Int32", true}});
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            read->OutputIUs = {TInfoUnit(output)};
            SetOutputType(
                ctx,
                *read,
                {{output, NUdf::EDataSlot::Int32}});
            read->OlapFilterLambda = MakeOlapComparisonProcess(
                ctx,
                "eq",
                reference,
                "1");
            TOpRoot root(read, TPositionHandle(), {output});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);

            const auto snapshot =
                ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& predicate = FindNode(snapshot, "scan")["predicate"];
            UNIT_ASSERT_VALUES_EQUAL(
                predicate["left"]["column"].GetStringSafe(),
                output);
        };

        check("projected.renamed", "projected.renamed");
        check("projected.renamed", "renamed");
        check("__kqp_rbo_ignore_arg_149", "__kqp_rbo_ignore_arg_149");
    }

    Y_UNIT_TEST(AmbiguousOlapReadColumnNamesFailClosed) {
        const auto check = [](
            const TVector<TString>& outputs,
            TString reference)
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {
                {"left", "Int32", true},
                {"right", "Int32", true},
            });
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"left", "right"},
                NYql::EStorageType::ColumnStorage);
            read->OutputIUs = {
                TInfoUnit(outputs[0]),
                TInfoUnit(outputs[1]),
            };
            SetOutputType(ctx, *read, {
                {outputs[0], NUdf::EDataSlot::Int32},
                {outputs[1], NUdf::EDataSlot::Int32},
            });
            read->OlapFilterLambda = MakeOlapComparisonProcess(
                ctx,
                "eq",
                reference,
                "1");
            TOpRoot root(
                read,
                TPositionHandle(),
                {outputs[0], outputs[1]});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);

            const auto result =
                ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                TStringBuilder()
                    << "Ambiguous OLAP read column reference "
                    << reference);
        };

        check({"right", "other"}, "right");
        check({"first.shared", "second.shared"}, "shared");
    }

    Y_UNIT_TEST(UnusedAmbiguousOlapReadColumnNamesAreAccepted) {
        const auto check = [](bool withPredicate) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {
                {"left", "Int32", true},
                {"right", "Int32", true},
            });
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"left", "right"},
                NYql::EStorageType::ColumnStorage);
            read->OutputIUs = {
                TInfoUnit("first.shared"),
                TInfoUnit("second.shared"),
            };
            SetOutputType(ctx, *read, {
                {"first.shared", NUdf::EDataSlot::Int32},
                {"second.shared", NUdf::EDataSlot::Int32},
            });
            if (withPredicate) {
                read->OlapFilterLambda = MakeOlapComparisonProcess(
                    ctx,
                    "eq",
                    "left",
                    "1");
            }
            TOpRoot root(
                read,
                TPositionHandle(),
                {"first.shared", "second.shared"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);

            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        };

        check(false);
        check(true);
    }

    Y_UNIT_TEST(OlapCoalesceTracksPositiveFilterContext) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", false}});
        const auto pos = TPositionHandle();

        auto exportCondition = [&](TExprNode::TPtr condition) {
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32, true}});
            read->OlapFilterLambda = MakeOlapFilterProcess(
                ctx,
                std::move(condition));
            TOpRoot root(read, pos, {"a.k"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        auto coalescedComparison = [&]() {
            return MakeOlapCoalesceFalse(
                ctx,
                MakeOlapComparisonCondition(ctx, "eq", "k", "0"));
        };

        for (const bool useAnd : {false, true}) {
            TVector<TExprBase> arguments = {
                TExprBase(coalescedComparison()),
                TExprBase(MakeOlapComparisonCondition(ctx, "gte", "k", "1")),
            };
            TExprNode::TPtr condition;
            if (useAnd) {
                condition = Build<TKqpOlapAnd>(ctx.ExprCtx, pos)
                    .Add(arguments)
                    .Done().Ptr();
            } else {
                condition = Build<TKqpOlapOr>(ctx.ExprCtx, pos)
                    .Add(arguments)
                    .Done().Ptr();
            }

            const auto snapshot = ParseSupported(exportCondition(std::move(condition)));
            const auto& predicate = FindNode(snapshot, "scan")["predicate"];
            UNIT_ASSERT_VALUES_EQUAL(
                predicate["kind"].GetStringSafe(),
                useAnd ? "and" : "or");
            UNIT_ASSERT_VALUES_EQUAL(predicate["args"].GetArraySafe().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(
                predicate["args"][0]["kind"].GetStringSafe(),
                "eq");
            UNIT_ASSERT_VALUES_EQUAL(
                predicate["args"][1]["kind"].GetStringSafe(),
                "gte");
        }

        auto beneathNot = Build<TKqpOlapNot>(ctx.ExprCtx, pos)
            .Value(TExprBase(coalescedComparison()))
            .Done();
        auto result = exportCondition(beneathNot.Ptr());
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "requires a positive filter context");

        auto beneathComparison = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
            .Operator().Value("eq").Build()
            .Left(TExprBase(coalescedComparison()))
            .Right(TypedLiteral(
                ctx,
                "Bool",
                "true",
                ScalarType(ctx, NUdf::EDataSlot::Bool)))
            .Done();
        result = exportCondition(beneathComparison.Ptr());
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "requires a positive filter context");
    }

    Y_UNIT_TEST(ExportsExactOlapPresencePredicatesAtAStageBoundary) {
        auto check = [](TStringBuf operation, bool negated) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", false}});
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32, true}});
            read->OlapFilterLambda = MakeOlapUnaryProcess(ctx, operation, "k");
            TOpRoot root(read, TPositionHandle(), {"a.k"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);

            const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
            const auto& predicate = FindNode(snapshot, "scan")["predicate"];
            const auto& exists = negated ? predicate["arg"] : predicate;
            UNIT_ASSERT_VALUES_EQUAL(
                predicate["kind"].GetStringSafe(),
                negated ? "not" : "exists");
            UNIT_ASSERT_VALUES_EQUAL(exists["kind"].GetStringSafe(), "exists");
            UNIT_ASSERT_VALUES_EQUAL(exists["arg"]["kind"].GetStringSafe(), "column");
            UNIT_ASSERT_VALUES_EQUAL(exists["arg"]["column"].GetStringSafe(), "a.k");
        };

        check("exists", false);
        check("empty", true);
    }

    Y_UNIT_TEST(OlapJustErasesOnlyDirectNonNullDateLiterals) {
        const auto exportArgument = [](TExportTestContext& ctx, TExprNode::TPtr argument) {
            const auto& table = AddTable(ctx, "/Root/OlapDate", {
                {"d", "Date", false},
            });
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"d"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.d", NUdf::EDataSlot::Date, true}});
            const auto just = Build<TKqpOlapFilterUnaryOp>(
                ctx.ExprCtx,
                TPositionHandle())
                .Operator().Value("just").Build()
                .Arg(TExprBase(std::move(argument)))
                .Done();
            const auto comparison = Build<TKqpOlapFilterBinaryOp>(
                ctx.ExprCtx,
                TPositionHandle())
                .Operator().Value("gte").Build()
                .Left<TCoAtom>().Value("d").Build()
                .Right(just)
                .Done();
            read->OlapFilterLambda = MakeOlapFilterProcess(
                ctx, comparison.Ptr());
            TOpRoot root(read, TPositionHandle(), {"a.d"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        for (const ui16 day : {ui16{0}, ui16{10'354}}) {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportArgument(
                ctx,
                TypedLiteral(
                    ctx,
                    "Date",
                    ToString(day),
                    ScalarType(ctx, NUdf::EDataSlot::Date))));
            const auto& right = FindNode(snapshot, "scan")["predicate"]["right"];
            UNIT_ASSERT_VALUES_EQUAL(right["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(right["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(right["value"].GetUIntegerSafe(), day);
        }

        {
            TExportTestContext ctx;
            const auto result = exportArgument(
                ctx,
                TypedLiteral(
                    ctx,
                    "Date",
                    "49673",
                    ScalarType(ctx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT(!result.IsSupported());
        }
        {
            TExportTestContext ctx;
            const auto result = exportArgument(
                ctx,
                TypedLiteral(
                    ctx,
                    "Date",
                    "not-a-day",
                    ScalarType(ctx, NUdf::EDataSlot::Date)));
            UNIT_ASSERT(!result.IsSupported());
        }
        {
            TExportTestContext ctx;
            const auto result = exportArgument(
                ctx,
                TypedLiteral(
                    ctx,
                    "Date",
                    "0",
                    ScalarType(ctx, NUdf::EDataSlot::Date, true)));
            UNIT_ASSERT(!result.IsSupported());
        }
        {
            TExportTestContext ctx;
            const auto result = exportArgument(
                ctx,
                TypedLiteral(
                    ctx,
                    "Int32",
                    "0",
                    ScalarType(ctx, NUdf::EDataSlot::Int32)));
            UNIT_ASSERT(!result.IsSupported());
        }
        {
            TExportTestContext ctx;
            const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto result = exportArgument(
                ctx,
                TypedCallable(
                    ctx,
                    "Just",
                    {TypedLiteral(ctx, "Date", "0", dateType)},
                    ScalarType(ctx, NUdf::EDataSlot::Date, true)));
            UNIT_ASSERT(!result.IsSupported());
        }
        {
            TExportTestContext ctx;
            const auto result = exportArgument(
                ctx,
                ctx.ExprCtx.NewAtom(TPositionHandle(), "d"));
            UNIT_ASSERT(!result.IsSupported());
        }
    }

    Y_UNIT_TEST(FoldsTextLiteralDateSafeCastInActualOlapFilterDialect) {
        const auto exportRight = [](
            TExportTestContext& ctx,
            TExprNode::TPtr right)
        {
            const auto& table = AddTable(ctx, "/Root/OlapDateCast", {
                {"d", "Date", false},
                {"s", "String", true},
            });
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"d", "s"},
                NYql::EStorageType::ColumnStorage);
            SetExactOutputType(ctx, *read, {
                {"a.d", ScalarType(ctx, NUdf::EDataSlot::Date, true)},
                {"a.s", ScalarType(ctx, NUdf::EDataSlot::String)},
            });
            auto comparison = Build<TKqpOlapFilterBinaryOp>(
                ctx.ExprCtx,
                TPositionHandle())
                .Operator().Value("gte").Build()
                .Left<TCoAtom>().Value("d").Build()
                .Right(std::move(right))
                .Done();
            read->OlapFilterLambda = MakeOlapFilterProcess(
                ctx,
                comparison.Ptr());
            TOpRoot root(read, TPositionHandle(), {"a.d"});
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
                NYql::EStorageType::ColumnStorage);
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        for (const TString source : {"String", "Utf8"}) {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportRight(
                ctx,
                TypedTextLiteralDateCast(
                    ctx,
                    "SafeCast",
                    source,
                    "1998-08-04")));
            const auto& right = FindNode(snapshot, "scan")["predicate"]["right"];
            UNIT_ASSERT_VALUES_EQUAL(right["kind"].GetStringSafe(), "literal");
            UNIT_ASSERT_VALUES_EQUAL(right["type"].GetStringSafe(), "Date");
            UNIT_ASSERT_VALUES_EQUAL(
                right["value"].GetUIntegerSafe(),
                10'442);
        }

        {
            TExportTestContext ctx;
            const auto snapshot = ParseSupported(exportRight(
                ctx,
                TypedTextLiteralDateCast(
                    ctx,
                    "SafeCast",
                    "String",
                    "1998-02-30")));
            const auto& right = FindNode(snapshot, "scan")["predicate"]["right"];
            UNIT_ASSERT_VALUES_EQUAL(right["kind"].GetStringSafe(), "null");
            UNIT_ASSERT_VALUES_EQUAL(right["type"].GetStringSafe(), "Date");
        }

        {
            TExportTestContext ctx;
            const auto* dateType = ScalarType(ctx, NUdf::EDataSlot::Date);
            const auto* optionalDateType = ScalarType(
                ctx,
                NUdf::EDataSlot::Date,
                true);
            const auto result = exportRight(
                ctx,
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedMember(
                            ctx,
                            "a.s",
                            ScalarType(ctx, NUdf::EDataSlot::String)),
                        OptionalDataTypeDescriptor(
                            ctx,
                            "Date",
                            dateType,
                            optionalDateType),
                    },
                    optionalDateType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "not a direct String or Utf8 literal");
        }
    }

    Y_UNIT_TEST(ExportsDecimalLiteralInActualOlapFilterDialect) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/Decimal", {
            {"d", "Decimal(7,2)", false},
        });
        auto read = MakeRead(
            ctx,
            table,
            "a",
            {"d"},
            NYql::EStorageType::ColumnStorage);
        SetExactOutputType(ctx, *read, {
            {"a.d", DecimalType(ctx, "7", "2", true)},
        });
        read->OlapFilterLambda = MakeOlapDecimalComparisonProcess(
            ctx,
            "gte",
            "d",
            "100",
            "12",
            "2");
        TOpRoot root(read, TPositionHandle(), {"a.d"});
        read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& predicate = FindNode(snapshot, "scan")["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "gte");
        UNIT_ASSERT_VALUES_EQUAL(predicate["left"]["column"].GetStringSafe(), "a.d");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["type"].GetStringSafe(), "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["value"]["kind"].GetStringSafe(), "finite");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["value"]["scaled"].GetStringSafe(), "10000");
    }

    Y_UNIT_TEST(FoldsTextLiteralDecimalSafeCastInActualOlapFilterDialect) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DecimalTextCast", {
            {"d", "Decimal(7,2)", false},
        });
        auto read = MakeRead(
            ctx,
            table,
            "a",
            {"d"},
            NYql::EStorageType::ColumnStorage);
        SetExactOutputType(ctx, *read, {
            {"a.d", DecimalType(ctx, "7", "2", true)},
        });
        auto comparison = Build<TKqpOlapFilterBinaryOp>(
            ctx.ExprCtx,
            TPositionHandle())
            .Operator().Value("gte").Build()
            .Left<TCoAtom>().Value("d").Build()
            .Right(TypedTextLiteralDecimalCast(
                ctx,
                "SafeCast",
                "String",
                "0.1",
                "35",
                "2"))
            .Done();
        read->OlapFilterLambda = MakeOlapFilterProcess(ctx, comparison.Ptr());
        TOpRoot root(read, TPositionHandle(), {"a.d"});
        read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& predicate = FindNode(snapshot, "scan")["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "gte");
        UNIT_ASSERT_VALUES_EQUAL(predicate["left"]["column"].GetStringSafe(), "a.d");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["type"].GetStringSafe(), "Decimal(35,2)");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["value"]["kind"].GetStringSafe(), "finite");
        UNIT_ASSERT_VALUES_EQUAL(predicate["right"]["value"]["scaled"].GetStringSafe(), "10");
    }

    Y_UNIT_TEST(UnsupportedOlapFilterFormsFailClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", false}});
        auto makeRead = [&]() {
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32, true}});
            return read;
        };

        auto unsupported = makeRead();
        unsupported->OlapFilterLambda = MakeOlapComparisonProcess(ctx, "/", "k", "1");
        TOpRoot unsupportedRoot(unsupported, TPositionHandle(), {"a.k"});
        unsupported->Props.StageId = unsupportedRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        auto result = ExportSemanticSnapshotV1(unsupportedRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported OLAP binary operation /");

        auto missingColumn = makeRead();
        missingColumn->OlapFilterLambda = MakeOlapComparisonProcess(
            ctx,
            "eq",
            "missing",
            "1");
        TOpRoot missingColumnRoot(missingColumn, TPositionHandle(), {"a.k"});
        missingColumn->Props.StageId = missingColumnRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(missingColumnRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "unavailable read column missing");

        auto identity = makeRead();
        const auto pos = TPositionHandle();
        const auto argument = ctx.ExprCtx.NewArgument(pos, "row");
        auto body = argument;
        identity->OlapFilterLambda = ctx.ExprCtx.NewLambda(
            pos,
            ctx.ExprCtx.NewArguments(pos, {argument}),
            std::move(body));
        TOpRoot identityRoot(identity, pos, {"a.k"});
        identity->Props.StageId = identityRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(identityRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "contains no filter operation");

        auto noStage = makeRead();
        noStage->OlapFilterLambda = MakeOlapComparisonProcess(ctx, "eq", "k", "1");
        TOpRoot noStageRoot(noStage, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(noStageRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "StageGraph source boundary");

        auto rowRead = MakeRead(ctx, table, "a", {"k"});
        SetOutputType(ctx, *rowRead, {{"a.k", NUdf::EDataSlot::Int32}});
        rowRead->OlapFilterLambda = MakeOlapComparisonProcess(ctx, "eq", "k", "1");
        TOpRoot rowRoot(rowRead, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(rowRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "only for column storage");

        auto unsupportedUnary = makeRead();
        unsupportedUnary->OlapFilterLambda = MakeOlapUnaryProcess(ctx, "just", "k");
        TOpRoot unsupportedUnaryRoot(unsupportedUnary, pos, {"a.k"});
        unsupportedUnary->Props.StageId = unsupportedUnaryRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(unsupportedUnaryRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "OLAP just may erase only a direct non-null Date literal");

        auto missingUnaryColumn = makeRead();
        missingUnaryColumn->OlapFilterLambda = MakeOlapUnaryProcess(ctx, "empty", "missing");
        TOpRoot missingUnaryColumnRoot(missingUnaryColumn, pos, {"a.k"});
        missingUnaryColumn->Props.StageId = missingUnaryColumnRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(missingUnaryColumnRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "unavailable read column missing");

        auto nonAtomUnaryOperator = makeRead();
        nonAtomUnaryOperator->OlapFilterLambda = MakeOlapFilterProcess(
            ctx,
            ctx.ExprCtx.NewList(
                pos,
                {
                    TypedLiteral(ctx, "Int32", "0", ScalarType(ctx, NUdf::EDataSlot::Int32)),
                    ctx.ExprCtx.NewAtom(pos, "k"),
                }));
        TOpRoot nonAtomUnaryOperatorRoot(nonAtomUnaryOperator, pos, {"a.k"});
        nonAtomUnaryOperator->Props.StageId = nonAtomUnaryOperatorRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(nonAtomUnaryOperatorRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "non-Atom operator");

        auto nestedJust = makeRead();
        const auto just = Build<TKqpOlapFilterUnaryOp>(ctx.ExprCtx, pos)
            .Operator().Value("just").Build()
            .Arg<TCoAtom>().Value("k").Build()
            .Done();
        nestedJust->OlapFilterLambda = MakeOlapUnaryProcess(
            ctx,
            "exists",
            just.Ptr());
        TOpRoot nestedJustRoot(nestedJust, pos, {"a.k"});
        nestedJust->Props.StageId = nestedJustRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(nestedJustRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "OLAP just may erase only a direct non-null Date literal");

        auto nestedCoalesce = makeRead();
        const auto comparison = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
            .Operator().Value("eq").Build()
            .Left<TCoAtom>().Value("k").Build()
            .Right(TypedLiteral(
                ctx,
                "Int32",
                "0",
                ScalarType(ctx, NUdf::EDataSlot::Int32)))
            .Done();
        const auto coalesce = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
            .Operator().Value("??").Build()
            .Left(comparison)
            .Right(TypedLiteral(
                ctx,
                "Bool",
                "false",
                ScalarType(ctx, NUdf::EDataSlot::Bool)))
            .Done();
        nestedCoalesce->OlapFilterLambda = MakeOlapUnaryProcess(
            ctx,
            "empty",
            coalesce.Ptr());
        TOpRoot nestedCoalesceRoot(nestedCoalesce, pos, {"a.k"});
        nestedCoalesce->Props.StageId = nestedCoalesceRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(nestedCoalesceRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "requires a positive filter context");
    }

    Y_UNIT_TEST(ExportsColumnReadPushdownAtAStageBoundary) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(
            ctx,
            table,
            "a",
            {"k"},
            NYql::EStorageType::ColumnStorage);
        const auto pos = TPositionHandle();
        read->Limit = ctx.ExprCtx.NewCallable(
            pos,
            "Uint64",
            {ctx.ExprCtx.NewAtom(pos, "7")});
        TOpRoot root(read, pos, {"a.k"});
        read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& pushedLimit = FindNode(snapshot, "scan")["pushed_limit"];
        UNIT_ASSERT_VALUES_EQUAL(pushedLimit["kind"].GetStringSafe(), "literal");
        UNIT_ASSERT_VALUES_EQUAL(pushedLimit["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(pushedLimit["value"].GetUIntegerSafe(), 7);
    }

    Y_UNIT_TEST(InvalidLimitSemanticsFailClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        const auto pos = TPositionHandle();

        auto read = MakeRead(ctx, table, "a", {"k"});
        auto invalidCount = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Int64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot invalidCountRoot(invalidCount, pos, {"a.k"});
        auto result = ExportSemanticSnapshotV1(invalidCountRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Limit count must be a Uint64 literal");

        auto invalidOffset = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            MakeConstant("Int64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot invalidOffsetRoot(invalidOffset, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(invalidOffsetRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Limit offset must be a Uint64 literal");

        auto nonLiteralBody = ctx.ExprCtx.NewCallable(
            pos,
            "Uint64",
            {ctx.ExprCtx.NewCallable(pos, "Void", {})});
        auto nonLiteralCount = MakeIntrusive<TOpLimit>(
            read,
            pos,
            TExpression(nonLiteralBody, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot nonLiteralCountRoot(nonLiteralCount, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(nonLiteralCountRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Limit count must be a Uint64 literal");

        auto invalidOutput = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        invalidOutput->Props.OutputIUs = {TInfoUnit("wrong")};
        TOpRoot invalidOutputRoot(invalidOutput, pos, {"wrong"});
        result = ExportSemanticSnapshotV1(invalidOutputRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Limit output IUs");

        auto invalidPhase = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            static_cast<EOpPhase>(99));
        TOpRoot invalidPhaseRoot(invalidPhase, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(invalidPhaseRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unknown operator phase");
    }

    Y_UNIT_TEST(InvalidReadPushdownLimitFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        const auto pos = TPositionHandle();

        auto rowRead = MakeRead(ctx, table, "a", {"k"});
        rowRead->Limit = ctx.ExprCtx.NewCallable(
            pos,
            "Uint64",
            {ctx.ExprCtx.NewAtom(pos, "1")});
        TOpRoot rowRoot(rowRead, pos, {"a.k"});
        auto result = ExportSemanticSnapshotV1(rowRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "only for column storage");

        auto columnRead = MakeRead(
            ctx,
            table,
            "a",
            {"k"},
            NYql::EStorageType::ColumnStorage);
        columnRead->Limit = ctx.ExprCtx.NewCallable(
            pos,
            "Int64",
            {ctx.ExprCtx.NewAtom(pos, "1")});
        TOpRoot columnRoot(columnRead, pos, {"a.k"});
        result = ExportSemanticSnapshotV1(columnRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "StageGraph source boundary");

        columnRead->Props.StageId = columnRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::ColumnStorage);
        result = ExportSemanticSnapshotV1(columnRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "Read pushed limit must be a Uint64 literal");

        columnRead->Limit = ctx.ExprCtx.NewCallable(
            pos,
            "Uint64",
            {ctx.ExprCtx.NewAtom(pos, "1")});
        columnRead->SortDir = ESortDir::Asc;
        result = ExportSemanticSnapshotV1(columnRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "range or ordering semantics");
    }

    Y_UNIT_TEST(InitialCatalogSurvivesAPlanThatRemovesItsTable) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"flag", "Bool", false},
        }, {"k"});
        auto initialRead = MakeRead(ctx, table, "a", {"k", "flag"});
        TOpRoot initialRoot(initialRead, TPositionHandle(), {"a.k"});

        const auto catalog = CaptureSemanticSnapshotCatalogV1(initialRoot, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        const auto pos = TPositionHandle();
        auto replacement = MakeIntrusive<TOpMap>(
            MakeIntrusive<TOpEmptySource>(pos),
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("a.k"),
                MakeConstant("Int32", "0", pos, &ctx.ExprCtx))});
        TOpRoot finalRoot(replacement, pos, {"a.k"});
        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(finalRoot, ctx.RboCtx, catalog.Catalog));

        const auto& tables = snapshot["schema"]["tables"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(tables[0]["name"].GetStringSafe(), "/Root/A");
        UNIT_ASSERT_VALUES_EQUAL(tables[0]["columns"].GetArraySafe().size(), 2);
    }

    Y_UNIT_TEST(InitialCatalogIncludesSubplanTables) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {{"value", "Int64", false}});
        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto innerRead = MakeRead(ctx, innerTable, "inner", {"value"});
        SetOutputType(ctx, *outerRead, {
            {"outer.k", NUdf::EDataSlot::Int32, true},
        });
        SetOutputType(ctx, *innerRead, {
            {"inner.value", NUdf::EDataSlot::Int64, false},
        });
        TOpRoot root(outerRead, TPositionHandle(), {"outer.k"});

        const TInfoUnit binding("scalar");
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                innerRead,
                {},
                ESubplanType::EXPR,
                binding,
                {}});

        const auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        UNIT_ASSERT_VALUES_EQUAL(catalog.Catalog.Tables.size(), 2);
        UNIT_ASSERT_STRING_CONTAINS(
            catalog.Catalog.Tables[0].Name,
            "/Root/Inner");
        UNIT_ASSERT_STRING_CONTAINS(
            catalog.Catalog.Tables[1].Name,
            "/Root/Outer");

        const auto snapshot =
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog);
        UNIT_ASSERT(!snapshot.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            snapshot.UnsupportedReason,
            "has no consumer");
    }

    Y_UNIT_TEST(ExportsExactUncorrelatedScalarSubplanDescriptors) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {{"value", "Int64", false}});
        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto innerRead = MakeRead(ctx, innerTable, "inner", {"value"});
        const auto pos = TPositionHandle();
        auto scalarAggregate = MakeIntrusive<TOpAggregate>(
            innerRead,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("inner.value"),
                "sum",
                TInfoUnit("scalar.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);

        TOpRoot root(outerRead, pos, {"outer.k"});
        const TInfoUnit binding("_rbo_arg_0", true);
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                scalarAggregate,
                {},
                ESubplanType::EXPR,
                binding,
                {}});

        auto predicate = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                binding,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps),
            MakeConstant("Int64", "1", pos, &ctx.ExprCtx));
        const auto* optionalInt64 =
            ScalarType(ctx, NUdf::EDataSlot::Int64, true);
        const auto* int64 = ScalarType(ctx, NUdf::EDataSlot::Int64);
        const auto* optionalBool =
            ScalarType(ctx, NUdf::EDataSlot::Bool, true);
        auto predicateBody = predicate.GetExpressionBody();
        predicateBody->Child(0)->SetTypeAnn(optionalInt64);
        predicateBody->Child(1)->SetTypeAnn(int64);
        predicateBody->SetTypeAnn(optionalBool);
        predicate.Node->SetTypeAnn(optionalBool);
        auto* predicateArguments = predicate.Node->Child(0);
        predicateArguments->Child(0)->SetTypeAnn(
            ctx.ExprCtx.MakeType<TStructExprType>(
                TVector<const TItemExprType*>{
                    ctx.ExprCtx.MakeType<TItemExprType>(
                        "outer.k",
                        ScalarType(ctx, NUdf::EDataSlot::Int32)),
                    ctx.ExprCtx.MakeType<TItemExprType>(
                        "_rbo_arg_0",
                        optionalInt64),
                }));
        predicateArguments->SetTypeAnn(
            ctx.ExprCtx.MakeType<TUnitExprType>());
        auto filter = MakeIntrusive<TOpFilter>(
            outerRead,
            pos,
            predicate);
        root.SetInput(filter);

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        const auto snapshot = ParseSupported(sink.Results[0]);

        const auto& plan = snapshot["plan"];
        UNIT_ASSERT_VALUES_EQUAL(plan.GetMapSafe().size(), 4);
        const auto& descriptors = plan["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(descriptors.size(), 1);
        const auto& descriptor = descriptors[0];
        UNIT_ASSERT_VALUES_EQUAL(descriptor.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["binding"].GetStringSafe(),
            "_rbo_arg_0");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["kind"].GetStringSafe(),
            "scalar");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["root"].GetStringSafe(),
            FindNode(snapshot, "aggregate")["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(descriptor["type"].GetStringSafe(), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(descriptor["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT(descriptor["dependencies"].GetArraySafe().empty());

        const auto& output = descriptor["output"];
        UNIT_ASSERT_VALUES_EQUAL(output.GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            output["column"].GetStringSafe(),
            "scalar.value");
        UNIT_ASSERT_VALUES_EQUAL(output["type"].GetStringSafe(), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(output["nullable"].GetBooleanSafe(), true);

        const auto& consumers = descriptor["consumers"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            consumers[0].GetStringSafe(),
            FindNode(snapshot, "filter")["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(plan["output"]),
            TVector<TString>{"outer.k"});

        auto& entry = root.PlanProps.Subplans.PlanMap.at(binding);
        entry.Plan = innerRead;
        const auto generalSnapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& generalDescriptor =
            generalSnapshot["plan"]["subplans"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            generalDescriptor["output"]["column"].GetStringSafe(),
            "inner.value");
        UNIT_ASSERT_VALUES_EQUAL(
            generalDescriptor["root"].GetStringSafe(),
            FindNode(generalSnapshot, "scan")["id"].GetStringSafe());
        entry.Plan = scalarAggregate;

        auto sharedPredicate = predicate.GetExpressionBody();
        for (size_t level = 0; level < 32; ++level) {
            sharedPredicate = TypedCallable(
                ctx,
                "And",
                {sharedPredicate, sharedPredicate},
                optionalBool);
        }
        auto sharedLambda = ctx.ExprCtx.NewLambda(
            pos,
            predicate.Node->ChildPtr(0),
            std::move(sharedPredicate));
        sharedLambda->SetTypeAnn(optionalBool);
        filter->FilterExpr = TExpression(
            std::move(sharedLambda),
            &ctx.ExprCtx,
            &root.PlanProps);

        const auto sharedResult =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!sharedResult.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            sharedResult.UnsupportedReason,
            "Exact scalar expression exceeds the node audit limit");
    }

    Y_UNIT_TEST(ExportsExactUncorrelatedNonNullIntegralInSubplan) {
        TInSubplanExportFixture fixture;
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog));
        const auto& descriptor =
            snapshot["plan"]["subplans"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(descriptor.GetMapSafe().size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["binding"].GetStringSafe(),
            "_rbo_in");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["kind"].GetStringSafe(),
            "in");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["type"].GetStringSafe(),
            "Bool");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["nullable"].GetBooleanSafe(),
            false);
        UNIT_ASSERT(descriptor["dependencies"].GetArraySafe().empty());
        UNIT_ASSERT(!descriptor.Has("predicate"));

        const auto& lookup = descriptor["lookup"];
        UNIT_ASSERT_VALUES_EQUAL(lookup.GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            lookup["column"].GetStringSafe(),
            "outer.k");
        UNIT_ASSERT_VALUES_EQUAL(
            lookup["type"].GetStringSafe(),
            "Int32");
        UNIT_ASSERT_VALUES_EQUAL(
            lookup["nullable"].GetBooleanSafe(),
            false);

        const auto& output = descriptor["output"];
        UNIT_ASSERT_VALUES_EQUAL(output.GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            output["column"].GetStringSafe(),
            "inner.k");
        UNIT_ASSERT_VALUES_EQUAL(
            output["type"].GetStringSafe(),
            "Int32");
        UNIT_ASSERT_VALUES_EQUAL(
            output["nullable"].GetBooleanSafe(),
            false);

        const auto& consumers = descriptor["consumers"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            consumers[0].GetStringSafe(),
            FindNode(snapshot, "filter")["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(snapshot, "filter")["predicate"]["column"]
                .GetStringSafe(),
            "_rbo_in");

        auto one = MakeConstant(
            "Uint64",
            "1",
            fixture.Pos,
            &fixture.Ctx.ExprCtx);
        auto limit = MakeIntrusive<TOpLimit>(
            fixture.InnerRead,
            fixture.Pos,
            one,
            EOpPhase::Undefined);
        SetExactOutputType(
            fixture.Ctx,
            *limit,
            {{"inner.k", fixture.Int32}});
        fixture.Entry().Plan = limit;
        auto wrapped = ParseSupported(ExportSemanticSnapshotV1(
            *fixture.Root,
            fixture.Ctx.RboCtx,
            catalog.Catalog));
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(wrapped, "limit")["id"].GetStringSafe(),
            wrapped["plan"]["subplans"][0]["root"].GetStringSafe());

        auto sort = MakeIntrusive<TOpSort>(
            fixture.InnerRead,
            fixture.Pos,
            TVector<TSortElement>{TSortElement(
                TInfoUnit("inner.k"),
                true,
                false)});
        SetExactOutputType(
            fixture.Ctx,
            *sort,
            {{"inner.k", fixture.Int32}});
        fixture.Entry().Plan = sort;
        wrapped = ParseSupported(ExportSemanticSnapshotV1(
            *fixture.Root,
            fixture.Ctx.RboCtx,
            catalog.Catalog));
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(wrapped, "sort")["id"].GetStringSafe(),
            wrapped["plan"]["subplans"][0]["root"].GetStringSafe());
        fixture.Entry().Plan = fixture.InnerRead;
    }

    Y_UNIT_TEST(ExportsNullableIntegralInOnlyAsPositiveFilterConjunct) {
        TInSubplanExportFixture fixture;
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        const auto assertNullability = [&](
            const TTypeAnnotationNode* lookupType,
            const TTypeAnnotationNode* outputType,
            bool lookupNullable,
            bool outputNullable)
        {
            SetExactOutputType(fixture.Ctx, *fixture.OuterRead, {
                {"outer.k", lookupType},
            });
            SetExactOutputType(fixture.Ctx, *fixture.InnerRead, {
                {"inner.k", outputType},
            });
            SetExactOutputType(fixture.Ctx, *fixture.Consumer, {
                {"outer.k", lookupType},
            });
            const auto snapshot = ParseSupported(
                ExportSemanticSnapshotV1(
                    *fixture.Root,
                    fixture.Ctx.RboCtx,
                    catalog.Catalog));
            const auto& descriptor =
                snapshot["plan"]["subplans"].GetArraySafe()[0];
            UNIT_ASSERT_VALUES_EQUAL(
                descriptor["lookup"]["type"].GetStringSafe(),
                "Int32");
            UNIT_ASSERT_VALUES_EQUAL(
                descriptor["lookup"]["nullable"].GetBooleanSafe(),
                lookupNullable);
            UNIT_ASSERT_VALUES_EQUAL(
                descriptor["output"]["type"].GetStringSafe(),
                "Int32");
            UNIT_ASSERT_VALUES_EQUAL(
                descriptor["output"]["nullable"].GetBooleanSafe(),
                outputNullable);
        };
        assertNullability(
            fixture.OptionalInt32,
            fixture.Int32,
            true,
            false);
        assertNullability(
            fixture.Int32,
            fixture.OptionalInt32,
            false,
            true);
        assertNullability(
            fixture.OptionalInt32,
            fixture.OptionalInt32,
            true,
            true);

        auto direct = fixture.BindingValue.GetExpressionBody();
        fixture.Consumer->FilterExpr = TExpression(
            TypedCallable(
                fixture.Ctx,
                "And",
                {
                    direct,
                    TypedLiteral(
                        fixture.Ctx,
                        "Bool",
                        "true",
                        fixture.Bool),
                },
                fixture.Bool),
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog).IsSupported(),
            "a direct positive nullable IN conjunct must remain supported");

        fixture.Consumer->FilterExpr = TExpression(
            TypedCallable(
                fixture.Ctx,
                "Not",
                {direct},
                fixture.Bool),
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        const auto negated = ExportSemanticSnapshotV1(
            *fixture.Root,
            fixture.Ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!negated.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            negated.UnsupportedReason,
            "must be a direct positive Filter conjunct");

        fixture.Consumer->FilterExpr = TExpression(
            TypedCallable(
                fixture.Ctx,
                "Or",
                {
                    direct,
                    TypedLiteral(
                        fixture.Ctx,
                        "Bool",
                        "false",
                        fixture.Bool),
                },
                fixture.Bool),
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        const auto disjoined = ExportSemanticSnapshotV1(
            *fixture.Root,
            fixture.Ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!disjoined.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            disjoined.UnsupportedReason,
            "must be a direct positive Filter conjunct");
    }

    Y_UNIT_TEST(ExportsExactUncorrelatedNonNullStringInSubplan) {
        TInSubplanExportFixture fixture(EInSubplanColumnKind::String);
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog));
        const auto& descriptor =
            snapshot["plan"]["subplans"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["binding"].GetStringSafe(),
            "_rbo_in");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["kind"].GetStringSafe(),
            "in");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["type"].GetStringSafe(),
            "Bool");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["nullable"].GetBooleanSafe(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["lookup"]["column"].GetStringSafe(),
            "outer.k");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["lookup"]["type"].GetStringSafe(),
            "String");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["lookup"]["nullable"].GetBooleanSafe(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["column"].GetStringSafe(),
            "inner.k");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["type"].GetStringSafe(),
            "String");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["nullable"].GetBooleanSafe(),
            false);
    }

    Y_UNIT_TEST(StringInSubplanDomainContractsFailClosed) {
        TInSubplanExportFixture fixture(EInSubplanColumnKind::String);
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        const auto reject = [&](TStringBuf fragment) {
            const auto result = ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                fragment);
        };
        const auto setResultType = [&](const TTypeAnnotationNode* type) {
            SetExactOutputType(
                fixture.Ctx,
                *fixture.InnerRead,
                {{"inner.k", type}});
        };
        const auto setLookupType = [&](const TTypeAnnotationNode* type) {
            SetExactOutputType(
                fixture.Ctx,
                *fixture.OuterRead,
                {{"outer.k", type}});
        };

        setResultType(fixture.OptionalString);
        reject("result must be a fixed-width integer or non-null String");
        setResultType(fixture.Utf8);
        reject("result must be a fixed-width integer or non-null String");
        setResultType(fixture.Bool);
        reject("result must be a fixed-width integer or non-null String");
        setResultType(fixture.Date);
        reject("result must be a fixed-width integer or non-null String");
        setResultType(DecimalType(fixture.Ctx, "12", "2"));
        reject("result must be a fixed-width integer or non-null String");
        setResultType(fixture.String);

        setLookupType(fixture.OptionalString);
        reject("nullable lookup must be a fixed-width integer");
        setLookupType(fixture.Utf8);
        reject("lookup and result must have the same supported type");
        setLookupType(fixture.Int32);
        reject("lookup and result must have the same supported type");
        setLookupType(fixture.String);

        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog).IsSupported(),
            "restored String IN subplan must remain supported");
    }

    Y_UNIT_TEST(InSubplanContractsFailClosed) {
        TInSubplanExportFixture fixture;
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        const auto reject = [&](TStringBuf fragment) {
            const auto result = ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                fragment);
        };
        auto& entry = fixture.Entry();

        entry.Tuple.clear();
        reject("exactly one tuple input");
        entry.Tuple = {
            fixture.Lookup,
            TInfoUnit("outer.other"),
        };
        reject("exactly one tuple input");
        entry.Tuple = {fixture.Lookup};

        entry.DependentIUs.push_back(fixture.Lookup);
        reject("must be uncorrelated");
        entry.DependentIUs.clear();

        entry.Tuple = {TInfoUnit("outer.missing")};
        reject("lookup column is absent");
        entry.Tuple = {fixture.Lookup};

        entry.Plan = fixture.WideInnerRead;
        reject("exactly one result column");
        entry.Plan = fixture.InnerRead;

        SetExactOutputType(fixture.Ctx, *fixture.InnerRead, {
            {"inner.k", fixture.Int64},
        });
        reject("lookup and result must have the same");
        SetExactOutputType(fixture.Ctx, *fixture.InnerRead, {
            {"inner.k", fixture.Int32},
        });

        SetExactOutputType(fixture.Ctx, *fixture.InnerRead, {
            {"inner.k", fixture.Bool},
        });
        reject("result must be a fixed-width integer or non-null String");
        SetExactOutputType(fixture.Ctx, *fixture.InnerRead, {
            {"inner.k", fixture.Int32},
        });

        auto residualOuterBind = MakeIntrusive<TOpAddDependencies>(
            fixture.InnerRead,
            fixture.Pos,
            TVector<std::pair<
                TInfoUnit,
                const TTypeAnnotationNode*>>{{
                fixture.Lookup,
                fixture.Int32,
            }});
        SetExactOutputType(fixture.Ctx, *residualOuterBind, {
            {"inner.k", fixture.Int32},
            {"outer.k", fixture.Int32},
        });
        entry.Plan = residualOuterBind;
        reject("residual AddDependencies");
        entry.Plan = fixture.InnerRead;

        auto checkedLimit = MakeIntrusive<TOpLimit>(
            fixture.InnerRead,
            fixture.Pos,
            MakeConstant(
                "Uint64",
                "1",
                fixture.Pos,
                &fixture.Ctx.ExprCtx),
            EOpPhase::Undefined);
        checkedLimit->Props.EnsureAtMostOne = true;
        SetExactOutputType(fixture.Ctx, *checkedLimit, {
            {"inner.k", fixture.Int32},
        });
        entry.Plan = checkedLimit;
        reject("error-bearing cardinality check");
        entry.Plan = fixture.InnerRead;

        AnnotateExpression(
            fixture.BindingValue,
            fixture.OptionalBool);
        reject("subplan Member _rbo_in must be Bool");
        AnnotateExpression(fixture.BindingValue, fixture.Bool);

        auto mapBindingValue = MakeColumnAccess(
            fixture.Binding,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        AnnotateExpression(mapBindingValue, fixture.Bool);
        auto mapConsumer = MakeIntrusive<TOpMap>(
            fixture.OuterRead,
            fixture.Pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("in.value"),
                mapBindingValue)});
        SetExactOutputType(fixture.Ctx, *mapConsumer, {
            {"outer.k", fixture.Int32},
            {"in.value", fixture.Bool},
        });
        fixture.Root->SetInput(mapConsumer);
        reject("cannot consume an IN subplan binding");
        fixture.Root->SetInput(fixture.Consumer);

        auto secondBindingValue = MakeColumnAccess(
            fixture.Binding,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        AnnotateExpression(secondBindingValue, fixture.Bool);
        auto secondConsumer = MakeIntrusive<TOpFilter>(
            fixture.Consumer,
            fixture.Pos,
            secondBindingValue);
        SetExactOutputType(
            fixture.Ctx,
            *secondConsumer,
            {{"outer.k", fixture.Int32}});
        fixture.Root->SetInput(secondConsumer);
        reject("exactly one Filter consumer");
        fixture.Root->SetInput(fixture.Consumer);

        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog).IsSupported(),
            "restored IN subplan must remain supported");
    }

    Y_UNIT_TEST(ExportsExactEqualityCorrelatedScalarSubplan) {
        TCorrelatedScalarExportFixture fixture;
        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx));
        const auto& plan = snapshot["plan"];
        const auto& descriptor =
            plan["subplans"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(descriptor.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["binding"].GetStringSafe(),
            "_rbo_arg_correlated_scalar");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["kind"].GetStringSafe(),
            "scalar");
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(descriptor["dependencies"]),
            TVector<TString>{"outer.k"});
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["type"].GetStringSafe(),
            "Int64");
        UNIT_ASSERT(descriptor["nullable"].GetBooleanSafe());
        UNIT_ASSERT(!descriptor.Has("predicate"));
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["column"].GetStringSafe(),
            "scalar.value");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["type"].GetStringSafe(),
            "Int64");
        UNIT_ASSERT(
            descriptor["output"]["nullable"].GetBooleanSafe());

        const auto& outerBind = FindNode(snapshot, "outer_bind");
        UNIT_ASSERT_VALUES_EQUAL(outerBind.GetMapSafe().size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(
            outerBind["dependency"].GetStringSafe(),
            "outer.k");
        UNIT_ASSERT_VALUES_EQUAL(
            outerBind["type"].GetStringSafe(),
            "Int32");
        UNIT_ASSERT(outerBind["nullable"].GetBooleanSafe());

        const NJson::TJsonValue* correlationFilter = nullptr;
        const NJson::TJsonValue* correlationProject = nullptr;
        const NJson::TJsonValue* scalarAggregate = nullptr;
        const NJson::TJsonValue* consumer = nullptr;
        for (const auto& node : plan["nodes"].GetArraySafe()) {
            const TString id = node["id"].GetStringSafe();
            if (node["op"].GetStringSafe() == "filter" &&
                node["input"].GetStringSafe() ==
                    outerBind["id"].GetStringSafe())
            {
                correlationFilter = &node;
            }
            if (correlationFilter &&
                node["op"].GetStringSafe() == "project" &&
                node["input"].GetStringSafe() ==
                    (*correlationFilter)["id"].GetStringSafe())
            {
                correlationProject = &node;
            }
            if (correlationProject &&
                node["op"].GetStringSafe() == "aggregate" &&
                node["input"].GetStringSafe() ==
                    (*correlationProject)["id"].GetStringSafe())
            {
                scalarAggregate = &node;
            }
            if (id == descriptor["consumers"][0].GetStringSafe()) {
                consumer = &node;
            }
        }
        UNIT_ASSERT(correlationFilter);
        UNIT_ASSERT_VALUES_EQUAL(
            (*correlationFilter)["predicate"]["kind"].GetStringSafe(),
            "and");
        UNIT_ASSERT_VALUES_EQUAL(
            (*correlationFilter)["predicate"]["args"].GetArraySafe().size(),
            2);
        UNIT_ASSERT(correlationProject);
        UNIT_ASSERT_VALUES_EQUAL(
            ProjectionOutputs(*correlationProject),
            TVector<TString>({
                "inner.k",
                "inner.value",
                "inner.text",
                "inner.flag",
                "outer.k",
                "mapped.value",
                "mapped.text",
            }));
        UNIT_ASSERT_VALUES_EQUAL(
            (*correlationProject)["columns"][6]["expression"]["kind"]
                .GetStringSafe(),
            "opaque");
        UNIT_ASSERT(scalarAggregate);
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["root"].GetStringSafe(),
            (*scalarAggregate)["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            (*scalarAggregate)["phase"].GetStringSafe(),
            "undefined");
        UNIT_ASSERT(
            (*scalarAggregate)["keys"].GetArraySafe().empty());
        UNIT_ASSERT(!(*scalarAggregate)["distinct_all"].GetBooleanSafe());
        UNIT_ASSERT(consumer);
        UNIT_ASSERT_VALUES_EQUAL(
            (*consumer)["op"].GetStringSafe(),
            "filter");
        UNIT_ASSERT_UNEQUAL(
            (*consumer)["id"].GetStringSafe(),
            (*correlationFilter)["id"].GetStringSafe());
    }

    Y_UNIT_TEST(CorrelatedScalarContractsFailClosed) {
        TCorrelatedScalarExportFixture fixture;
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog).IsSupported(),
            "baseline correlated scalar must be supported");

        const auto reject = [&](TStringBuf fragment) {
            const auto result = ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                fragment);
        };
        auto& entry = fixture.Entry();

        const auto savedTypes = fixture.OuterBind->Types;
        fixture.OuterBind->Types.clear();
        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(!sink.Results[0].IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            sink.Results[0].UnsupportedReason,
            "equally sized nonempty dependency and type vectors");
        fixture.OuterBind->Types = savedTypes;

        entry.DependentIUs.push_back(TInfoUnit("outer.second"));
        reject("exactly one outer dependency");
        entry.DependentIUs.pop_back();

        fixture.CorrelationFilter->FilterExpr = fixture.Residual;
        reject("has no equality for its outer dependency");
        fixture.CorrelationFilter->FilterExpr =
            fixture.CorrelationPredicate;

        auto notEqual = MakeBinaryPredicate(
            "!=",
            MakeColumnAccess(
                TInfoUnit("inner.k"),
                fixture.Pos,
                &fixture.Ctx.ExprCtx,
                &fixture.Root->PlanProps),
            MakeColumnAccess(
                fixture.Dependency,
                fixture.Pos,
                &fixture.Ctx.ExprCtx,
                &fixture.Root->PlanProps));
        AnnotateBinaryExpression(
            notEqual,
            fixture.OptionalInt32,
            fixture.OptionalInt32,
            fixture.OptionalBool);
        fixture.CorrelationFilter->FilterExpr = notEqual;
        reject("strict column equality");
        fixture.CorrelationFilter->FilterExpr =
            fixture.CorrelationPredicate;

        auto duplicateEquality =
            MakeConjunction({fixture.Equality, fixture.Equality});
        AnnotateExpression(
            duplicateEquality,
            fixture.OptionalBool);
        fixture.CorrelationFilter->FilterExpr =
            duplicateEquality;
        reject("multiple conjuncts");
        fixture.CorrelationFilter->FilterExpr =
            fixture.CorrelationPredicate;

        auto missingResidual = MakeColumnAccess(
            TInfoUnit("inner.missing"),
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        AnnotateExpression(missingResidual, fixture.Bool);
        auto unavailablePredicate =
            MakeConjunction({fixture.Equality, missingResidual});
        AnnotateExpression(
            unavailablePredicate,
            fixture.OptionalBool);
        fixture.CorrelationFilter->FilterExpr =
            unavailablePredicate;
        reject("residual predicate references unavailable column");
        fixture.CorrelationFilter->FilterExpr =
            fixture.CorrelationPredicate;

        auto leakedDependency = MakeColumnAccess(
            fixture.Dependency,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        AnnotateExpression(
            leakedDependency,
            fixture.OptionalInt32);
        auto leakingMap = MakeIntrusive<TOpMap>(
            fixture.ScalarAggregate,
            fixture.Pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("leaked.dependency"),
                leakedDependency)});
        SetExactOutputType(fixture.Ctx, *leakingMap, {
            {"scalar.value", fixture.OptionalInt64},
            {"leaked.dependency", fixture.OptionalInt32},
        });
        entry.Plan = leakingMap;
        reject("uses its outer dependency outside the correlation Filter");
        entry.Plan = fixture.ScalarAggregate;

        fixture.ScalarAggregate->AggregationPhase =
            EOpPhase::Final;
        reject("Aggregate must be ungrouped, undefined");
        fixture.ScalarAggregate->AggregationPhase =
            EOpPhase::Undefined;

        fixture.ScalarAggregate->KeyColumns.push_back(
            TInfoUnit("mapped.value"));
        reject("Aggregate must be ungrouped, undefined");
        fixture.ScalarAggregate->KeyColumns.clear();

        entry.Plan = fixture.CorrelationFilter;
        reject("exactly one Aggregate among Map wrappers");
        entry.Plan = fixture.ScalarAggregate;

        auto secondAggregate = MakeIntrusive<TOpAggregate>(
            fixture.ScalarAggregate,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("scalar.value"),
                "sum",
                TInfoUnit("scalar.second"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            fixture.Pos);
        SetExactOutputType(fixture.Ctx, *secondAggregate, {
            {"scalar.second", fixture.OptionalInt64},
        });
        entry.Plan = secondAggregate;
        reject("exactly one Aggregate among Map wrappers");
        entry.Plan = fixture.ScalarAggregate;

        fixture.InnerRead->Props.EnsureAtMostOne = true;
        reject("nondeterministic or error-bearing per-invocation semantics");
        fixture.InnerRead->Props.EnsureAtMostOne = false;

        auto one = MakeConstant(
            "Uint64",
            "1",
            fixture.Pos,
            &fixture.Ctx.ExprCtx);
        auto innerLimit = MakeIntrusive<TOpLimit>(
            fixture.InnerRead,
            fixture.Pos,
            one,
            EOpPhase::Undefined);
        SetExactOutputType(fixture.Ctx, *innerLimit, {
            {"inner.k", fixture.OptionalInt32},
            {"inner.value", fixture.Int64},
            {"inner.text", fixture.String},
            {"inner.flag", fixture.Bool},
        });
        fixture.OuterBind->SetInput(innerLimit);
        reject("nondeterministic or error-bearing per-invocation semantics");
        fixture.OuterBind->SetInput(fixture.InnerRead);

        SetExactOutputType(fixture.Ctx, *fixture.OuterRead, {
            {"outer.k", fixture.Int32},
        });
        reject("dependency type or nullability disagrees with its consumer input");
        SetExactOutputType(fixture.Ctx, *fixture.OuterRead, {
            {"outer.k", fixture.OptionalInt32},
        });

        SetExactOutputType(fixture.Ctx, *fixture.OuterBind, {
            {"inner.k", fixture.OptionalInt32},
            {"inner.value", fixture.Int64},
            {"inner.text", fixture.String},
            {"inner.flag", fixture.Bool},
            {"outer.k", fixture.Int32},
        });
        reject("outer_bind output type disagrees with AddDependencies");
        SetExactOutputType(fixture.Ctx, *fixture.OuterBind, {
            {"inner.k", fixture.OptionalInt32},
            {"inner.value", fixture.Int64},
            {"inner.text", fixture.String},
            {"inner.flag", fixture.Bool},
            {"outer.k", fixture.OptionalInt32},
        });

        auto secondBindingValue = MakeColumnAccess(
            fixture.Binding,
            fixture.Pos,
            &fixture.Ctx.ExprCtx,
            &fixture.Root->PlanProps);
        auto secondOne = MakeConstant(
            "Int64",
            "1",
            fixture.Pos,
            &fixture.Ctx.ExprCtx);
        auto secondPredicate = MakeBinaryPredicate(
            "==",
            secondBindingValue,
            secondOne);
        AnnotateBinaryExpression(
            secondPredicate,
            fixture.OptionalInt64,
            fixture.Int64,
            fixture.OptionalBool);
        auto secondConsumer = MakeIntrusive<TOpFilter>(
            fixture.Consumer,
            fixture.Pos,
            secondPredicate);
        SetExactOutputType(fixture.Ctx, *secondConsumer, {
            {"outer.k", fixture.OptionalInt32},
        });
        fixture.Root->SetInput(secondConsumer);
        reject("exactly one Project or Filter consumer");
        fixture.Root->SetInput(fixture.Consumer);

        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog).IsSupported(),
            "restored correlated scalar must remain supported");
    }

    Y_UNIT_TEST(ExportsExactUncorrelatedAndEqualityCorrelatedExists) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {
                {"k", "Int32", false},
                {"flag", "Bool", true},
                {"flag2", "Bool", true},
            });
        const auto pos = TPositionHandle();
        const auto* int32 = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt32 =
            ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
        const auto* optionalBool =
            ScalarType(ctx, NUdf::EDataSlot::Bool, true);

        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto uncorrelatedRead = MakeRead(
            ctx,
            innerTable,
            "uncorrelated",
            {"k", "flag", "flag2"});
        auto correlatedRead = MakeRead(
            ctx,
            innerTable,
            "inner",
            {"k", "flag", "flag2"});
        SetOutputType(ctx, *outerRead, {
            {"outer.k", NUdf::EDataSlot::Int32},
        });
        SetOutputType(ctx, *uncorrelatedRead, {
            {"uncorrelated.k", NUdf::EDataSlot::Int32, true},
            {"uncorrelated.flag", NUdf::EDataSlot::Bool},
            {"uncorrelated.flag2", NUdf::EDataSlot::Bool},
        });
        SetOutputType(ctx, *correlatedRead, {
            {"inner.k", NUdf::EDataSlot::Int32, true},
            {"inner.flag", NUdf::EDataSlot::Bool},
            {"inner.flag2", NUdf::EDataSlot::Bool},
        });

        TOpRoot root(outerRead, pos, {"outer.k"});
        auto one = MakeConstant(
            "Uint64",
            "1",
            pos,
            &ctx.ExprCtx);
        auto uncorrelatedLimit = MakeIntrusive<TOpLimit>(
            uncorrelatedRead,
            pos,
            one,
            EOpPhase::Undefined);
        SetExactOutputType(ctx, *uncorrelatedLimit, {
            {"uncorrelated.k", optionalInt32},
            {"uncorrelated.flag", boolType},
            {"uncorrelated.flag2", boolType},
        });
        auto uncorrelatedTopSort = MakeIntrusive<TOpSort>(
            uncorrelatedLimit,
            pos,
            TVector<TSortElement>{TSortElement(
                TInfoUnit("uncorrelated.k"),
                true,
                false)},
            one);
        SetExactOutputType(ctx, *uncorrelatedTopSort, {
            {"uncorrelated.k", optionalInt32},
            {"uncorrelated.flag", boolType},
            {"uncorrelated.flag2", boolType},
        });
        const TInfoUnit uncorrelatedBinding(
            "_rbo_exists_uncorrelated",
            true);
        const TInfoUnit correlatedBinding(
            "_rbo_exists_correlated",
            true);
        const TInfoUnit dependency("outer.k");

        auto addDependencies = MakeIntrusive<TOpAddDependencies>(
            correlatedRead,
            pos,
            TVector<std::pair<
                TInfoUnit,
                const TTypeAnnotationNode*>>{{dependency, int32}});
        SetExactOutputType(ctx, *addDependencies, {
            {"inner.k", optionalInt32},
            {"inner.flag", boolType},
            {"inner.flag2", boolType},
            {"outer.k", int32},
        });

        auto equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                TInfoUnit("inner.k"),
                pos,
                &ctx.ExprCtx,
                &root.PlanProps),
            MakeColumnAccess(
                dependency,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps));
        AnnotateBinaryExpression(
            equality,
            optionalInt32,
            int32,
            optionalBool);
        auto localPredicate = MakeColumnAccess(
            TInfoUnit("inner.flag"),
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(localPredicate, boolType);
        auto secondLocalPredicate = MakeColumnAccess(
            TInfoUnit("inner.flag2"),
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(secondLocalPredicate, boolType);
        auto correlatedPredicate =
            MakeConjunction({
                localPredicate,
                equality,
                secondLocalPredicate,
            });
        AnnotateExpression(correlatedPredicate, optionalBool);
        auto correlatedFilter = MakeIntrusive<TOpFilter>(
            addDependencies,
            pos,
            correlatedPredicate);
        SetExactOutputType(ctx, *correlatedFilter, {
            {"inner.k", optionalInt32},
            {"inner.flag", boolType},
            {"inner.flag2", boolType},
            {"outer.k", int32},
        });

        auto projectedColumn = MakeColumnAccess(
            TInfoUnit("inner.k"),
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(projectedColumn, optionalInt32);
        auto cardinalityPreservingMap = MakeIntrusive<TOpMap>(
            correlatedFilter,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("projected.k"),
                projectedColumn)});
        SetExactOutputType(ctx, *cardinalityPreservingMap, {
            {"inner.k", optionalInt32},
            {"inner.flag", boolType},
            {"inner.flag2", boolType},
            {"outer.k", int32},
            {"projected.k", optionalInt32},
        });

        root.PlanProps.Subplans.Add(
            uncorrelatedBinding,
            TSubplanEntry{
                uncorrelatedTopSort,
                {},
                ESubplanType::EXISTS,
                uncorrelatedBinding,
                {}});
        root.PlanProps.Subplans.Add(
            correlatedBinding,
            TSubplanEntry{
                cardinalityPreservingMap,
                {},
                ESubplanType::EXISTS,
                correlatedBinding,
                {dependency}});

        auto uncorrelatedValue = MakeColumnAccess(
            uncorrelatedBinding,
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        auto correlatedValue = MakeColumnAccess(
            correlatedBinding,
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(uncorrelatedValue, boolType);
        AnnotateExpression(correlatedValue, boolType);
        auto negatedCorrelatedValue = MakeNegation(correlatedValue);
        AnnotateExpression(negatedCorrelatedValue, boolType);
        auto consumerPredicate =
            MakeConjunction({
                uncorrelatedValue,
                negatedCorrelatedValue,
            });
        AnnotateExpression(consumerPredicate, boolType);
        auto consumer = MakeIntrusive<TOpFilter>(
            outerRead,
            pos,
            consumerPredicate);
        SetExactOutputType(ctx, *consumer, {{"outer.k", int32}});
        root.SetInput(consumer);

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& descriptors =
            snapshot["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(descriptors.size(), 2);

        const auto& uncorrelated = descriptors[0];
        UNIT_ASSERT_VALUES_EQUAL(uncorrelated.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(
            uncorrelated["binding"].GetStringSafe(),
            "_rbo_exists_uncorrelated");
        UNIT_ASSERT_VALUES_EQUAL(
            uncorrelated["kind"].GetStringSafe(),
            "exists");
        UNIT_ASSERT_VALUES_EQUAL(
            uncorrelated["type"].GetStringSafe(),
            "Bool");
        UNIT_ASSERT_VALUES_EQUAL(
            uncorrelated["nullable"].GetBooleanSafe(),
            false);
        UNIT_ASSERT(uncorrelated["predicate"].IsNull());
        UNIT_ASSERT(uncorrelated["dependencies"].GetArraySafe().empty());
        UNIT_ASSERT(!uncorrelated.Has("output"));

        const auto& correlated = descriptors[1];
        UNIT_ASSERT_VALUES_EQUAL(correlated.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(
            correlated["binding"].GetStringSafe(),
            "_rbo_exists_correlated");
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(correlated["dependencies"]),
            TVector<TString>{"outer.k"});
        UNIT_ASSERT_VALUES_EQUAL(
            correlated["predicate"]["kind"].GetStringSafe(),
            "and");
        const auto& correlatedConjuncts =
            correlated["predicate"]["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(correlatedConjuncts.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            correlatedConjuncts[0]["column"].GetStringSafe(),
            "inner.flag");
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(correlatedConjuncts[1]),
            std::make_pair(TString("inner.k"), TString("outer.k")));
        UNIT_ASSERT_VALUES_EQUAL(
            correlatedConjuncts[2]["kind"].GetStringSafe(),
            "column");
        UNIT_ASSERT_VALUES_EQUAL(
            correlatedConjuncts[2]["column"].GetStringSafe(),
            "inner.flag2");
        UNIT_ASSERT(!correlated.Has("output"));

        const auto& nodes = snapshot["plan"]["nodes"].GetArraySafe();
        const NJson::TJsonValue* normalizedRoot = nullptr;
        const NJson::TJsonValue* consumerNode = nullptr;
        for (const auto& node : nodes) {
            UNIT_ASSERT_UNEQUAL(
                node["op"].GetStringSafe(),
                "add_dependencies");
            if (node["id"].GetStringSafe() ==
                correlated["root"].GetStringSafe())
            {
                normalizedRoot = &node;
            }
            if (node["id"].GetStringSafe() ==
                correlated["consumers"][0].GetStringSafe())
            {
                consumerNode = &node;
            }
        }
        UNIT_ASSERT(normalizedRoot);
        UNIT_ASSERT_VALUES_EQUAL(
            (*normalizedRoot)["op"].GetStringSafe(),
            "scan");
        UNIT_ASSERT(consumerNode);
        UNIT_ASSERT_VALUES_EQUAL(
            (*consumerNode)["op"].GetStringSafe(),
            "filter");
        UNIT_ASSERT_VALUES_EQUAL(
            correlated["consumers"][0].GetStringSafe(),
            uncorrelated["consumers"][0].GetStringSafe());

        uncorrelatedLimit->Props.EnsureAtMostOne = true;
        const auto checkedExists =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!checkedExists.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            checkedExists.UnsupportedReason,
            "error-bearing cardinality check");
        uncorrelatedLimit->Props.EnsureAtMostOne = false;

        uncorrelatedTopSort->SetInput(correlatedRead);
        const auto normalizedNesting =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!normalizedNesting.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            normalizedNesting.UnsupportedReason,
            "exported subplan root");
        uncorrelatedTopSort->SetInput(uncorrelatedLimit);

        root.SetInput(correlatedRead);
        const auto normalizedMainRoot =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!normalizedMainRoot.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            normalizedMainRoot.UnsupportedReason,
            "reachable from the main plan");
        root.SetInput(consumer);
    }

    Y_UNIT_TEST(ExportsExactTwoDependencyEqualityInequalityCorrelatedExists) {
        TTwoDependencyExistsExportFixture fixture;
        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx));

        const auto& descriptors =
            snapshot["plan"]["subplans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(descriptors.size(), 1);
        const auto& descriptor = descriptors.front();
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["kind"].GetStringSafe(),
            "exists");
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(descriptor["dependencies"]),
            TVector<TString>({
                "outer.order_key",
                "outer.warehouse_key",
            }));

        const auto& predicate = descriptor["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(
            predicate["kind"].GetStringSafe(),
            "and");
        const auto& conjuncts = predicate["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(conjuncts[0]),
            std::make_pair(
                TString("inner.order_key"),
                TString("outer.order_key")));
        UNIT_ASSERT_VALUES_EQUAL(
            conjuncts[1]["kind"].GetStringSafe(),
            "not");
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(conjuncts[1]["arg"]),
            std::make_pair(
                TString("outer.warehouse_key"),
                TString("inner.warehouse_key")));
        UNIT_ASSERT_VALUES_EQUAL(
            conjuncts[2]["column"].GetStringSafe(),
            "inner.flag");

        const TString rootId = descriptor["root"].GetStringSafe();
        bool foundRoot = false;
        for (const auto& node :
            snapshot["plan"]["nodes"].GetArraySafe())
        {
            UNIT_ASSERT_UNEQUAL(
                node["op"].GetStringSafe(),
                "outer_bind");
            if (node["id"].GetStringSafe() == rootId) {
                foundRoot = true;
                UNIT_ASSERT_VALUES_EQUAL(
                    node["op"].GetStringSafe(),
                    "scan");
            }
        }
        UNIT_ASSERT(foundRoot);
    }

    Y_UNIT_TEST(TwoDependencyCorrelatedExistsContractsFailClosed) {
        TTwoDependencyExistsExportFixture fixture;
        const auto catalog = CaptureSemanticSnapshotCatalogV1(
            *fixture.Root,
            fixture.Ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        const auto reject = [&](TStringBuf fragment) {
            const auto result = ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                fragment);
        };
        const auto supported = [&]() {
            const auto result = ExportSemanticSnapshotV1(
                *fixture.Root,
                fixture.Ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT_C(
                result.IsSupported(),
                result.UnsupportedReason);
        };
        supported();

        auto& entry = fixture.Entry();
        entry.DependentIUs.push_back(TInfoUnit("outer.third"));
        reject("exactly two equality/inequality dependencies");
        entry.DependentIUs.pop_back();

        std::swap(
            fixture.AddDependencies->Dependencies[0],
            fixture.AddDependencies->Dependencies[1]);
        reject("registry disagrees with AddDependencies");
        std::swap(
            fixture.AddDependencies->Dependencies[0],
            fixture.AddDependencies->Dependencies[1]);

        fixture.AddDependencies->Types[1] = nullptr;
        reject("missing type");
        fixture.AddDependencies->Types[1] = fixture.OptionalInt32;

        auto warehouseEquality = fixture.Comparison(
            "==",
            fixture.WarehouseDependency,
            TInfoUnit("inner.warehouse_key"));
        fixture.SetPredicate({
            fixture.Equality,
            warehouseEquality,
            fixture.Residual,
        });
        reject("exactly one strict column equality");

        auto orderInequality = fixture.Comparison(
            "!=",
            TInfoUnit("inner.order_key"),
            fixture.OrderDependency);
        fixture.SetPredicate({
            orderInequality,
            fixture.Inequality,
            fixture.Residual,
        });
        reject("exactly one strict column equality");

        fixture.SetPredicate({
            fixture.Equality,
            fixture.Residual,
        });
        reject("exactly one strict column equality");

        auto repeatedOrderDependency = fixture.Comparison(
            "!=",
            fixture.OrderDependency,
            TInfoUnit("inner.warehouse_key"));
        fixture.SetPredicate({
            fixture.Equality,
            repeatedOrderDependency,
            fixture.Residual,
        });
        reject("references outer dependency outer.order_key in multiple conjuncts");

        auto crossDependency = fixture.Comparison(
            "==",
            fixture.OrderDependency,
            fixture.WarehouseDependency);
        fixture.SetPredicate({
            crossDependency,
            fixture.Inequality,
            fixture.Residual,
        });
        reject("must reference exactly one outer dependency");

        auto repeatedInnerColumn = fixture.Comparison(
            "!=",
            fixture.WarehouseDependency,
            TInfoUnit("inner.order_key"));
        fixture.SetPredicate({
            fixture.Equality,
            repeatedInnerColumn,
            fixture.Residual,
        });
        reject("must use distinct inner columns");

        auto orderedComparison = fixture.Comparison(
            "<",
            fixture.WarehouseDependency,
            TInfoUnit("inner.warehouse_key"));
        fixture.SetPredicate({
            fixture.Equality,
            orderedComparison,
            fixture.Residual,
        });
        reject("one strict column equality and one strict column inequality");

        fixture.SetPredicate({
            fixture.Equality,
            fixture.Inequality,
            fixture.Residual,
        });
        SetExactOutputType(
            fixture.Ctx,
            *fixture.AddDependencies,
            {
                {"inner.order_key", fixture.OptionalInt32},
                {"inner.warehouse_key", fixture.OptionalInt32},
                {"inner.flag", fixture.Bool},
                {"outer.order_key", fixture.OptionalInt32},
                {"outer.warehouse_key", fixture.Int32},
            });
        reject("dependency type or output order is inconsistent");
        SetExactOutputType(
            fixture.Ctx,
            *fixture.AddDependencies,
            {
                {"inner.order_key", fixture.OptionalInt32},
                {"inner.warehouse_key", fixture.OptionalInt32},
                {"inner.flag", fixture.Bool},
                {"outer.order_key", fixture.OptionalInt32},
                {"outer.warehouse_key", fixture.OptionalInt32},
            });

        AnnotateBinaryExpression(
            fixture.Inequality,
            fixture.OptionalInt32,
            fixture.Int32,
            fixture.OptionalBool);
        fixture.SetPredicate({
            fixture.Equality,
            fixture.Inequality,
            fixture.Residual,
        });
        reject("inner comparison Member type disagrees");
        AnnotateBinaryExpression(
            fixture.Inequality,
            fixture.OptionalInt32,
            fixture.OptionalInt32,
            fixture.OptionalBool);
        fixture.SetPredicate({
            fixture.Equality,
            fixture.Inequality,
            fixture.Residual,
        });
        supported();
    }

    Y_UNIT_TEST(CorrelatedExistsContractsFailClosed) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {{"k", "Int32", true}});
        const auto pos = TPositionHandle();
        const auto* int32 = ScalarType(ctx, NUdf::EDataSlot::Int32);
        const auto* optionalInt32 =
            ScalarType(ctx, NUdf::EDataSlot::Int32, true);
        const auto* boolType = ScalarType(ctx, NUdf::EDataSlot::Bool);
        const auto* optionalBool =
            ScalarType(ctx, NUdf::EDataSlot::Bool, true);

        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto innerRead = MakeRead(ctx, innerTable, "inner", {"k"});
        SetExactOutputType(ctx, *outerRead, {{"outer.k", int32}});
        SetExactOutputType(ctx, *innerRead, {{"inner.k", int32}});

        TOpRoot root(outerRead, pos, {"outer.k"});
        const TInfoUnit binding("_rbo_exists", true);
        const TInfoUnit dependency("outer.k");
        auto addDependencies = MakeIntrusive<TOpAddDependencies>(
            innerRead,
            pos,
            TVector<std::pair<
                TInfoUnit,
                const TTypeAnnotationNode*>>{{dependency, int32}});
        SetExactOutputType(ctx, *addDependencies, {
            {"inner.k", int32},
            {"outer.k", int32},
        });
        auto equality = MakeBinaryPredicate(
            "==",
            MakeColumnAccess(
                TInfoUnit("inner.k"),
                pos,
                &ctx.ExprCtx,
                &root.PlanProps),
            MakeColumnAccess(
                dependency,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps));
        AnnotateBinaryExpression(
            equality,
            int32,
            int32,
            boolType);
        auto subplanFilter = MakeIntrusive<TOpFilter>(
            addDependencies,
            pos,
            equality);
        SetExactOutputType(ctx, *subplanFilter, {
            {"inner.k", int32},
            {"outer.k", int32},
        });
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                subplanFilter,
                {},
                ESubplanType::EXISTS,
                binding,
                {dependency}});

        auto bindingValue = MakeColumnAccess(
            binding,
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(bindingValue, boolType);
        auto consumer = MakeIntrusive<TOpFilter>(
            outerRead,
            pos,
            bindingValue);
        SetExactOutputType(ctx, *consumer, {{"outer.k", int32}});
        root.SetInput(consumer);

        const auto catalog =
            CaptureSemanticSnapshotCatalogV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog)
                .IsSupported(),
            "baseline correlated EXISTS must be supported");

        const auto reject = [&](TStringBuf fragment) {
            const auto result = ExportSemanticSnapshotV1(
                root,
                ctx.RboCtx,
                catalog.Catalog);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                fragment);
        };
        auto& entry = root.PlanProps.Subplans.PlanMap.at(binding);

        entry.Type = ESubplanType::IN_SUBPLAN;
        reject("exactly one tuple input");
        entry.Type = static_cast<ESubplanType>(-1);
        reject("unknown subplan type");
        entry.Type = ESubplanType::EXISTS;

        entry.Tuple.push_back(dependency);
        reject("has tuple inputs");
        entry.Tuple.clear();

        entry.DependentIUs.push_back(TInfoUnit("outer.other"));
        entry.DependentIUs.push_back(TInfoUnit("outer.third"));
        reject("exactly two equality/inequality dependencies");
        entry.DependentIUs.pop_back();
        entry.DependentIUs.pop_back();

        addDependencies->Dependencies.front() =
            TInfoUnit("outer.other");
        reject("registry disagrees with AddDependencies");
        addDependencies->Dependencies.front() = dependency;

        auto notEqual = MakeBinaryPredicate(
            "!=",
            MakeColumnAccess(
                TInfoUnit("inner.k"),
                pos,
                &ctx.ExprCtx,
                &root.PlanProps),
            MakeColumnAccess(
                dependency,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps));
        AnnotateBinaryExpression(
            notEqual,
            int32,
            int32,
            boolType);
        subplanFilter->FilterExpr = notEqual;
        reject("strict column equality");
        subplanFilter->FilterExpr = equality;

        auto duplicateEquality =
            MakeConjunction({equality, equality});
        AnnotateExpression(duplicateEquality, boolType);
        subplanFilter->FilterExpr = duplicateEquality;
        reject("multiple conjuncts");
        subplanFilter->FilterExpr = equality;

        auto computedMap = MakeIntrusive<TOpMap>(
            subplanFilter,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("computed"),
                MakeConstant(
                    "Int32",
                    "1",
                    pos,
                    &ctx.ExprCtx))});
        SetExactOutputType(ctx, *computedMap, {
            {"inner.k", int32},
            {"outer.k", int32},
            {"computed", int32},
        });
        entry.Plan = computedMap;
        reject("not a plain column projection");
        entry.Plan = subplanFilter;

        auto residualAddDependencies =
            MakeIntrusive<TOpAddDependencies>(
                innerRead,
                pos,
                TVector<std::pair<
                    TInfoUnit,
                    const TTypeAnnotationNode*>>{{
                    TInfoUnit("outer.residual"),
                    int32,
                }});
        SetExactOutputType(ctx, *residualAddDependencies, {
            {"inner.k", int32},
            {"outer.residual", int32},
        });
        addDependencies->SetInput(residualAddDependencies);
        reject("residual AddDependencies");
        addDependencies->SetInput(innerRead);

        innerRead->Props.EnsureAtMostOne = true;
        reject("error-bearing cardinality check");
        innerRead->Props.EnsureAtMostOne = false;

        auto one = MakeConstant(
            "Uint64",
            "1",
            pos,
            &ctx.ExprCtx);
        innerRead->Limit = one.GetExpressionBody();
        reject("per-invocation row-selection semantics");
        innerRead->Limit.Reset();
        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog)
                .IsSupported(),
            "restored correlated scan without a pushed limit must remain supported");

        auto correlatedLimit = MakeIntrusive<TOpLimit>(
            innerRead,
            pos,
            one,
            EOpPhase::Undefined);
        SetExactOutputType(ctx, *correlatedLimit, {{"inner.k", int32}});
        addDependencies->SetInput(correlatedLimit);
        reject("per-invocation row-selection semantics");
        addDependencies->SetInput(innerRead);

        const TVector<TSortElement> innerOrder{
            TSortElement(TInfoUnit("inner.k"), true, false),
        };
        auto correlatedTopSort = MakeIntrusive<TOpSort>(
            innerRead,
            pos,
            innerOrder,
            one);
        SetExactOutputType(ctx, *correlatedTopSort, {{"inner.k", int32}});
        addDependencies->SetInput(correlatedTopSort);
        reject("per-invocation row-selection semantics");
        addDependencies->SetInput(innerRead);

        auto correlatedPlainSort = MakeIntrusive<TOpSort>(
            innerRead,
            pos,
            innerOrder);
        SetExactOutputType(ctx, *correlatedPlainSort, {{"inner.k", int32}});
        addDependencies->SetInput(correlatedPlainSort);
        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog)
                .IsSupported(),
            "plain Sort preserves every row and must remain supported");
        addDependencies->SetInput(innerRead);

        auto oversizedPredicate = equality.GetExpressionBody();
        for (size_t level = 0; level < 32; ++level) {
            oversizedPredicate = TypedCallable(
                ctx,
                "And",
                {oversizedPredicate, oversizedPredicate},
                boolType);
        }
        auto oversizedLambda = ctx.ExprCtx.NewLambda(
            pos,
            equality.Node->ChildPtr(0),
            std::move(oversizedPredicate));
        oversizedLambda->SetTypeAnn(boolType);
        subplanFilter->FilterExpr = TExpression(
            std::move(oversizedLambda),
            &ctx.ExprCtx,
            &root.PlanProps);
        reject("Exact scalar expression exceeds the node audit limit");
        subplanFilter->FilterExpr = equality;

        addDependencies->Types.front() = optionalInt32;
        SetExactOutputType(ctx, *addDependencies, {
            {"inner.k", int32},
            {"outer.k", optionalInt32},
        });
        reject("dependency Member type disagrees");
        addDependencies->Types.front() = int32;
        SetExactOutputType(ctx, *addDependencies, {
            {"inner.k", int32},
            {"outer.k", int32},
        });

        AnnotateBinaryExpression(
            equality,
            optionalInt32,
            int32,
            optionalBool);
        reject("inner comparison Member type disagrees");
        AnnotateBinaryExpression(
            equality,
            int32,
            int32,
            boolType);

        AnnotateExpression(bindingValue, optionalBool);
        reject("subplan Member _rbo_exists must be Bool");
        AnnotateExpression(bindingValue, boolType);

        auto mapBindingValue = MakeColumnAccess(
            binding,
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(mapBindingValue, boolType);
        auto mapConsumer = MakeIntrusive<TOpMap>(
            outerRead,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("exists.value"),
                mapBindingValue)});
        SetExactOutputType(ctx, *mapConsumer, {
            {"outer.k", int32},
            {"exists.value", boolType},
        });
        root.SetInput(mapConsumer);
        reject("cannot consume an EXISTS subplan binding");
        root.SetInput(consumer);

        auto secondBindingValue = MakeColumnAccess(
            binding,
            pos,
            &ctx.ExprCtx,
            &root.PlanProps);
        AnnotateExpression(secondBindingValue, boolType);
        auto secondConsumer = MakeIntrusive<TOpFilter>(
            consumer,
            pos,
            secondBindingValue);
        SetExactOutputType(ctx, *secondConsumer, {{"outer.k", int32}});
        root.SetInput(secondConsumer);
        reject("exactly one Filter consumer");
        root.SetInput(consumer);

        root.SetInput(addDependencies);
        reject("only admissible inside a validated correlated subplan");
        root.SetInput(consumer);

        UNIT_ASSERT_C(
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog)
                .IsSupported(),
            "restored correlated EXISTS must remain supported");
    }

    Y_UNIT_TEST(ExportsScalarSubplanMapRenameConsumer) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {{"value", "Int64", false}});
        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto innerRead = MakeRead(ctx, innerTable, "inner", {"value"});
        SetOutputType(ctx, *outerRead, {
            {"outer.k", NUdf::EDataSlot::Int32},
        });
        SetOutputType(ctx, *innerRead, {
            {"inner.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto scalarAggregate = MakeIntrusive<TOpAggregate>(
            innerRead,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("inner.value"),
                "sum",
                TInfoUnit("scalar.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *scalarAggregate, {
            {"scalar.value", NUdf::EDataSlot::Int64, true},
        });

        TOpRoot root(outerRead, pos, {"result"});
        const TInfoUnit binding("_rbo_arg_0", true);
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                scalarAggregate,
                {},
                ESubplanType::EXPR,
                binding,
                {}});
        auto renameProject = MakeIntrusive<TOpMap>(
            outerRead,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                binding,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps)});
        auto computedProject = MakeIntrusive<TOpMap>(
            renameProject,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("computed"),
                MakeColumnAccess(
                    binding,
                    pos,
                    &ctx.ExprCtx,
                    &root.PlanProps))});
        const auto* optionalInt64 =
            ScalarType(ctx, NUdf::EDataSlot::Int64, true);
        const auto annotate = [&](TMapElement& element) {
            auto& expression = element.GetExpressionRef();
            expression.GetExpressionBody()->SetTypeAnn(optionalInt64);
        };
        annotate(renameProject->MapElements.front());
        annotate(computedProject->MapElements.front());
        SetOutputType(ctx, *renameProject, {
            {"outer.k", NUdf::EDataSlot::Int32},
            {"result", NUdf::EDataSlot::Int64, true},
        });
        SetOutputType(ctx, *computedProject, {
            {"outer.k", NUdf::EDataSlot::Int32},
            {"result", NUdf::EDataSlot::Int64, true},
            {"computed", NUdf::EDataSlot::Int64, true},
        });
        root.SetInput(computedProject);
        UNIT_ASSERT_VALUES_EQUAL(
            renameProject->GetSubplanIUs(root.PlanProps).size(),
            1);
        UNIT_ASSERT_VALUES_EQUAL(
            computedProject->GetSubplanIUs(root.PlanProps).size(),
            1);

        const auto snapshot =
            ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto* projectType = computedProject->GetTypeAnn()
            ->Cast<TListExprType>()
            ->GetItemType()
            ->Cast<TStructExprType>();
        UNIT_ASSERT_VALUES_EQUAL(projectType->GetItems().size(), 3);
        UNIT_ASSERT(projectType->FindItemType("outer.k"));
        UNIT_ASSERT(projectType->FindItemType("result"));
        UNIT_ASSERT(projectType->FindItemType("computed"));
        UNIT_ASSERT(!projectType->FindItemType("_rbo_arg_0"));
        const auto& descriptor =
            snapshot["plan"]["subplans"].GetArraySafe()[0];
        const auto& consumers = descriptor["consumers"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 2);
        TVector<const NJson::TJsonValue*> projects;
        for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() == "project") {
                projects.push_back(&node);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(projects.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            consumers[0].GetStringSafe(),
            (*projects[0])["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            consumers[1].GetStringSafe(),
            (*projects[1])["id"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            (*projects[0])["columns"][1]["expression"]["column"].GetStringSafe(),
            "_rbo_arg_0");
        UNIT_ASSERT_VALUES_EQUAL(
            (*projects[1])["columns"][2]["expression"]["column"].GetStringSafe(),
            "_rbo_arg_0");
    }

    Y_UNIT_TEST(ScalarSubplanRegistryAndConsumerContractsFailClosed) {
        TExportTestContext ctx;
        const auto& outerTable = AddTable(
            ctx,
            "/Root/Outer",
            {{"k", "Int32", true}});
        const auto& innerTable = AddTable(
            ctx,
            "/Root/Inner",
            {{"value", "Int64", false}});
        auto outerRead = MakeRead(ctx, outerTable, "outer", {"k"});
        auto innerRead = MakeRead(ctx, innerTable, "inner", {"value"});
        SetOutputType(ctx, *outerRead, {
            {"outer.k", NUdf::EDataSlot::Int32},
        });
        SetOutputType(ctx, *innerRead, {
            {"inner.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto scalarAggregate = MakeIntrusive<TOpAggregate>(
            innerRead,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("inner.value"),
                "sum",
                TInfoUnit("scalar.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *scalarAggregate, {
            {"scalar.value", NUdf::EDataSlot::Int64, true},
        });

        TOpRoot root(outerRead, pos, {"outer.k"});
        const TInfoUnit binding("_rbo_arg_0", true);
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                scalarAggregate,
                {},
                ESubplanType::EXPR,
                binding,
                {}});
        const auto catalog =
            CaptureSemanticSnapshotCatalogV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        auto result =
            ExportSemanticSnapshotV1(root, ctx.RboCtx, catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "has no consumer");

        auto& entry = root.PlanProps.Subplans.PlanMap.at(binding);
        entry.DependentIUs.push_back(TInfoUnit("outer.k"));
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "must contain exactly one AddDependencies");
        entry.DependentIUs.clear();

        entry.Tuple.push_back(TInfoUnit("outer.k"));
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "tuple inputs");
        entry.Tuple.clear();

        entry.Type = ESubplanType::EXISTS;
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "EXISTS subplan binding _rbo_arg_0 has no consumer");
        entry.Type = ESubplanType::EXPR;

        entry.Plan = innerRead;
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "has no consumer");
        entry.Plan = scalarAggregate;

        auto collidingProject = MakeIntrusive<TOpMap>(
            outerRead,
            pos,
            TVector<TMapElement>{TMapElement(
                binding,
                binding,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps)});
        collidingProject->MapElements.front()
            .GetExpressionRef()
            .GetExpressionBody()
            ->SetTypeAnn(ScalarType(
                ctx,
                NUdf::EDataSlot::Int64,
                true));
        root.SetInput(collidingProject);
        root.ColumnOrder = {binding.GetFullName()};
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "output collides with scalar subplan binding");
        root.SetInput(outerRead);
        root.ColumnOrder = {"outer.k"};

        root.PlanProps.StageGraph.AddStage();
        result = ExportSemanticSnapshotV1(
            root,
            ctx.RboCtx,
            catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "cannot contain residual subplans");
    }

    Y_UNIT_TEST(NestedScalarSubplanReferenceFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetOutputType(ctx, *read, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto leaf = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("leaf.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *leaf, {
            {"leaf.value", NUdf::EDataSlot::Int64, true},
        });

        auto empty = MakeIntrusive<TOpEmptySource>(pos);
        SetExactOutputType(ctx, *empty, {});
        TOpRoot root(empty, pos, {"main.value"});
        const TInfoUnit nestedBinding("_rbo_arg_nested", true);
        const TInfoUnit leafBinding("_rbo_arg_leaf", true);
        auto nested = MakeIntrusive<TOpMap>(
            empty,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("nested.value"),
                leafBinding,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps)});
        SetOutputType(ctx, *nested, {
            {"nested.value", NUdf::EDataSlot::Int64, true},
        });
        root.PlanProps.Subplans.Add(
            nestedBinding,
            TSubplanEntry{
                nested,
                {},
                ESubplanType::EXPR,
                nestedBinding,
                {}});
        root.PlanProps.Subplans.Add(
            leafBinding,
            TSubplanEntry{
                leaf,
                {},
                ESubplanType::EXPR,
                leafBinding,
                {}});

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "contains a nested subplan reference");
    }

    Y_UNIT_TEST(ScalarSubplanRootTopologyFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetOutputType(ctx, *read, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto leaf = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("leaf.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *leaf, {
            {"leaf.value", NUdf::EDataSlot::Int64, true},
        });

        const TInfoUnit leafBinding("_rbo_arg_leaf", true);
        TOpRoot mainReachableRoot(leaf, pos, {"leaf.value"});
        mainReachableRoot.PlanProps.Subplans.Add(
            leafBinding,
            TSubplanEntry{
                leaf,
                {},
                ESubplanType::EXPR,
                leafBinding,
                {}});
        auto result =
            ExportSemanticSnapshotV1(mainReachableRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "is reachable from the main plan");

        auto empty = MakeIntrusive<TOpEmptySource>(pos);
        auto malformedLimit = MakeIntrusive<TOpLimit>(
            leaf,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        malformedLimit->Children.clear();
        TOpRoot malformedSubplanRoot(empty, pos, {"unused"});
        const TInfoUnit malformedBinding("_rbo_arg_malformed", true);
        malformedSubplanRoot.PlanProps.Subplans.Add(
            malformedBinding,
            TSubplanEntry{
                malformedLimit,
                {},
                ESubplanType::EXPR,
                malformedBinding,
                {}});
        result = ExportSemanticSnapshotV1(
            malformedSubplanRoot,
            ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "Limit must have one input");

        auto wrapper = MakeIntrusive<TOpMap>(
            leaf,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("wrapper.value"),
                MakeColumnAccess(
                    TInfoUnit("leaf.value"),
                    pos,
                    &ctx.ExprCtx,
                    &ctx.ExpressionProps))});
        wrapper->MapElements.front()
            .GetExpressionRef()
            .GetExpressionBody()
            ->SetTypeAnn(ScalarType(
                ctx,
                NUdf::EDataSlot::Int64,
                true));
        SetOutputType(ctx, *wrapper, {
            {"leaf.value", NUdf::EDataSlot::Int64, true},
            {"wrapper.value", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot nestedRoot(empty, pos, {"unused"});
        const TInfoUnit wrapperBinding("_rbo_arg_wrapper", true);
        nestedRoot.PlanProps.Subplans.Add(
            wrapperBinding,
            TSubplanEntry{
                wrapper,
                {},
                ESubplanType::EXPR,
                wrapperBinding,
                {}});
        nestedRoot.PlanProps.Subplans.Add(
            leafBinding,
            TSubplanEntry{
                leaf,
                {},
                ESubplanType::EXPR,
                leafBinding,
                {}});
        result = ExportSemanticSnapshotV1(nestedRoot, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "is reachable below distinct subplan binding");
    }

    Y_UNIT_TEST(AtMostOneMarkerIsSerializedOnEveryLimit) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetOutputType(ctx, *read, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("result"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        auto checkedLimit = MakeIntrusive<TOpLimit>(
            aggregate,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        SetOutputType(ctx, *checkedLimit, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        checkedLimit->Props.EnsureAtMostOne = true;
        auto propagatedLimit = MakeIntrusive<TOpLimit>(
            checkedLimit,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        SetOutputType(ctx, *propagatedLimit, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        propagatedLimit->Props.EnsureAtMostOne = true;
        TOpRoot root(propagatedLimit, pos, {"result"});

        const auto supported =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(supported.IsSupported(), supported.UnsupportedReason);
        const auto supportedSnapshot = ParseSupported(supported);
        size_t checkedCount = 0;
        for (const auto& node :
            supportedSnapshot["plan"]["nodes"].GetArraySafe())
        {
            if (node["op"].GetStringSafe() == "limit" &&
                node["ensure_at_most_one"].GetBooleanSafe())
            {
                ++checkedCount;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(checkedCount, 2);

        checkedLimit->SetInput(read);
        root.RecomputeOutputIUsSubtree();
        SetOutputType(ctx, *checkedLimit, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        SetOutputType(ctx, *propagatedLimit, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        root.ColumnOrder = {"a.value"};
        auto represented =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(represented.IsSupported(), represented.UnsupportedReason);

        checkedLimit->SetInput(aggregate);
        checkedLimit->Props.EnsureAtMostOne = false;
        auto map = MakeIntrusive<TOpMap>(
            aggregate,
            pos,
            TVector<TMapElement>{});
        map->Props.EnsureAtMostOne = true;
        TOpRoot mapRoot(map, pos, {"result"});
        auto rejected = ExportSemanticSnapshotV1(mapRoot, ctx.RboCtx);
        UNIT_ASSERT(!rejected.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            rejected.UnsupportedReason,
            "physical properties");
    }

    Y_UNIT_TEST(AtMostOneMarkerIsSerializedAcrossMultiTaskProducerStage) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetOutputType(ctx, *read, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("result"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        auto checkedLimit = MakeIntrusive<TOpLimit>(
            aggregate,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        SetOutputType(ctx, *checkedLimit, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot root(checkedLimit, pos, {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 source =
            graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 aggregateStage = graph.AddStage();
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = source;
        aggregate->Props.StageId = aggregateStage;
        checkedLimit->Props.StageId = consumer;
        graph.Connect(
            source,
            aggregateStage,
            MakeIntrusive<TMapConnection>(
                graph.GetOutputIndex(source)));
        graph.Connect(
            aggregateStage,
            consumer,
            MakeIntrusive<TUnionAllConnection>(
                graph.GetOutputIndex(aggregateStage),
                false));

        const auto baseline = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(baseline.IsSupported(), baseline.UnsupportedReason);

        checkedLimit->Props.EnsureAtMostOne = true;
        const auto result =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
        UNIT_ASSERT_UNEQUAL(result.Json, baseline.Json);
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(ParseSupported(result), "limit")
                ["ensure_at_most_one"].GetBooleanSafe(),
            true);
    }

    Y_UNIT_TEST(AtMostOneMarkerIsSerializedAcrossSingleTaskProducerStage) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetOutputType(ctx, *read, {
            {"a.value", NUdf::EDataSlot::Int64, true},
        });
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("result"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        auto checkedLimit = MakeIntrusive<TOpLimit>(
            aggregate,
            pos,
            MakeConstant("Uint64", "2", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        SetOutputType(ctx, *checkedLimit, {
            {"result", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot root(checkedLimit, pos, {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 source =
            graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 aggregateStage = graph.AddStage();
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = source;
        aggregate->Props.StageId = aggregateStage;
        checkedLimit->Props.StageId = consumer;
        graph.Connect(
            source,
            aggregateStage,
            MakeIntrusive<TUnionAllConnection>(
                graph.GetOutputIndex(source),
                false));
        graph.Connect(
            aggregateStage,
            consumer,
            MakeIntrusive<TUnionAllConnection>(
                graph.GetOutputIndex(aggregateStage),
                false));

        const auto baseline =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(baseline.IsSupported(), baseline.UnsupportedReason);

        checkedLimit->Props.EnsureAtMostOne = true;
        const auto result =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
        UNIT_ASSERT_UNEQUAL(result.Json, baseline.Json);
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(ParseSupported(result), "limit")
                ["ensure_at_most_one"].GetBooleanSafe(),
            true);

        auto groupedAggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.value"),
                "sum",
                TInfoUnit("result"))},
            TVector<TInfoUnit>{TInfoUnit("a.value")},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *groupedAggregate, {
            {"a.value", NUdf::EDataSlot::Int64, true},
            {"result", NUdf::EDataSlot::Int64, true},
        });
        groupedAggregate->Props.StageId = aggregateStage;
        checkedLimit->SetInput(groupedAggregate);
        root.RecomputeOutputIUsSubtree();
        SetOutputType(ctx, *checkedLimit, {
            {"a.value", NUdf::EDataSlot::Int64, true},
            {"result", NUdf::EDataSlot::Int64, true},
        });

        checkedLimit->Props.EnsureAtMostOne = false;
        const auto groupedBaseline =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(
            groupedBaseline.IsSupported(),
            groupedBaseline.UnsupportedReason);

        checkedLimit->Props.EnsureAtMostOne = true;
        const auto groupedResult =
            ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(
            groupedResult.IsSupported(),
            groupedResult.UnsupportedReason);
        UNIT_ASSERT_UNEQUAL(groupedResult.Json, groupedBaseline.Json);
    }

    Y_UNIT_TEST(MalformedSubplanRegistryFailsCatalogCaptureClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        const TInfoUnit binding("scalar");
        root.PlanProps.Subplans.PlanMap.emplace(
            binding,
            TSubplanEntry{
                read,
                {},
                ESubplanType::EXPR,
                binding,
                {}});

        const auto catalog = CaptureSemanticSnapshotCatalogV1(root, ctx.RboCtx);
        UNIT_ASSERT(!catalog.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            catalog.UnsupportedReason,
            "order and map have different sizes");
    }

    Y_UNIT_TEST(IncompleteStageGraphStateFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        // Every StageGraph container is semantic input.  Even inconsistent
        // partial state must not be serialized as stage_graph:null.
        root.PlanProps.StageGraph.StageInputs[7] = {};
        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "StageGraph");
    }

    Y_UNIT_TEST(ExportsGroupedDuplicateEdgesAndParallelUnion) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto unionAll = MakeIntrusive<TOpUnionAll>(
            read,
            read,
            pos,
            TVector<TInfoUnit>{TInfoUnit("a.k")});
        TOpRoot root(unionAll, pos, {"a.k"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        unionAll->Props.StageId = consumer;
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(producer), true));

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& stageGraph = snapshot["stage_graph"];
        UNIT_ASSERT_VALUES_EQUAL(stageGraph.GetMapSafe().size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(stageGraph["root_stage"].GetStringSafe(), "s1");
        UNIT_ASSERT(stageGraph["assumptions"].GetArraySafe().empty());

        const auto& stages = stageGraph["stages"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(stages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stages[0].GetMapSafe().size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(stages[1].GetMapSafe().size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(stages[0]["source_storage"].GetStringSafe(), "row");
        UNIT_ASSERT(stages[1]["source_storage"].IsNull());

        const auto& unionNode = FindNode(snapshot, "union_all");
        const auto& unionInputs = unionNode["inputs"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(unionInputs.size(), 2);
        const TString leftNode = unionInputs[0]["node"].GetStringSafe();
        const TString rightNode = unionInputs[1]["node"].GetStringSafe();
        UNIT_ASSERT_VALUES_EQUAL(leftNode, rightNode);
        UNIT_ASSERT_VALUES_EQUAL(
            Strings(stages[1]["inputs"]),
            (TVector<TString>{leftNode, rightNode}));

        const auto& producerOutputs = stages[0]["outputs"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(producerOutputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(producerOutputs[0]["index"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(producerOutputs[0]["node"].GetStringSafe(), leftNode);
        UNIT_ASSERT_VALUES_EQUAL(producerOutputs[1]["index"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(producerOutputs[1]["node"].GetStringSafe(), rightNode);

        const auto& rootOutputs = stages[1]["outputs"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(rootOutputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rootOutputs[0]["index"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            rootOutputs[0]["node"].GetStringSafe(),
            snapshot["plan"]["root"].GetStringSafe());

        const auto& edges = stageGraph["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(edges[0].GetMapSafe().size(), 7);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["id"].GetStringSafe(), "e0");
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["producer"].GetStringSafe(), "s0");
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["consumer"].GetStringSafe(), "s1");
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["occurrence"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["producer_output"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["consumer_input"].GetUIntegerSafe(), 0);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "map");

        UNIT_ASSERT_VALUES_EQUAL(edges[1].GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["id"].GetStringSafe(), "e1");
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["occurrence"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["producer_output"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["consumer_input"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["kind"].GetStringSafe(), "union_all");
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["parallel"].GetBooleanSafe(), true);
    }

    Y_UNIT_TEST(ExportsHashShuffleAndBroadcastConnectionSemantics) {
        TExportTestContext ctx;
        AddTable(ctx, "/Root/A", {{"k", "Int32", true}, {"x", "Int32", true}});
        AddTable(ctx, "/Root/B", {{"k", "Int32", true}, {"x", "Int32", true}});
        auto left = MakeRead(ctx, ctx.Tables->ExistingTable("ut", "/Root/A"), "a", {"k", "x"});
        auto right = MakeRead(ctx, ctx.Tables->ExistingTable("ut", "/Root/B"), "b", {"k", "x"});
        const auto pos = TPositionHandle();
        auto join = MakeIntrusive<TOpJoin>(
            left,
            right,
            pos,
            "Inner",
            TVector<std::pair<TInfoUnit, TInfoUnit>>{{TInfoUnit("a.k"), TInfoUnit("b.k")}});
        TOpRoot root(join, pos, {"a.k", "b.k"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 leftStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 rightStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 joinStage = graph.AddStage();
        left->Props.StageId = leftStage;
        right->Props.StageId = rightStage;
        join->Props.StageId = joinStage;
        auto shuffle = MakeIntrusive<TShuffleConnection>(
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            graph.GetOutputIndex(leftStage),
            true);
        shuffle->HashFuncType = NYql::NDq::EHashShuffleFuncType::HashV2;
        graph.Connect(leftStage, joinStage, shuffle);
        graph.Connect(
            rightStage,
            joinStage,
            MakeIntrusive<TBroadcastConnection>(graph.GetOutputIndex(rightStage)));

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& edges = snapshot["stage_graph"]["edges"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(edges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(edges[0].GetMapSafe().size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["kind"].GetStringSafe(), "hash_shuffle");
        UNIT_ASSERT_VALUES_EQUAL(Strings(edges[0]["keys"]), (TVector<TString>{"a.k"}));
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["hash_function"].GetStringSafe(), "HashV2");
        UNIT_ASSERT_VALUES_EQUAL(edges[0]["use_spilling"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(edges[1].GetMapSafe().size(), 7);
        UNIT_ASSERT_VALUES_EQUAL(edges[1]["kind"].GetStringSafe(), "broadcast");
    }

    Y_UNIT_TEST(ExportsEveryMergeOrderingField) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"x", "Date", false},
            {"bytes", "String", true},
            {"text", "Utf8", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x", "bytes", "text"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.x", NUdf::EDataSlot::Date, true},
            {"a.bytes", NUdf::EDataSlot::String},
            {"a.text", NUdf::EDataSlot::Utf8, true},
        });
        auto project = MakeCopyMap(ctx, read, "result", "a.k");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        project->Props.StageId = consumer;
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TMergeConnection>(
                TVector<TSortElement>{
                    TSortElement(TInfoUnit("a.k"), true, false),
                    TSortElement(TInfoUnit("a.x"), false, true),
                    TSortElement(TInfoUnit("a.bytes"), true, true),
                    TSortElement(TInfoUnit("a.text"), false, false),
                },
                graph.GetOutputIndex(producer)));

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& edge = snapshot["stage_graph"]["edges"][0];
        UNIT_ASSERT_VALUES_EQUAL(edge.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(edge["kind"].GetStringSafe(), "merge");
        const auto& order = edge["order"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(order.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(order[0].GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(order[0]["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(order[0]["ascending"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order[0]["nulls_first"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(order[1].GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(order[1]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(order[1]["ascending"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(order[1]["nulls_first"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order[2]["column"].GetStringSafe(), "a.bytes");
        UNIT_ASSERT_VALUES_EQUAL(order[2]["ascending"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order[2]["nulls_first"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order[3]["column"].GetStringSafe(), "a.text");
        UNIT_ASSERT_VALUES_EQUAL(order[3]["ascending"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(order[3]["nulls_first"].GetBooleanSafe(), false);
    }

    Y_UNIT_TEST(ExportsDecimalMergeOrdering) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/DecimalMerge", {
            {"value", "Decimal(7,2)", false},
        });
        const auto* optionalDecimal = DecimalType(ctx, "7", "2", true);
        auto read = MakeRead(ctx, table, "a", {"value"});
        SetExactOutputType(ctx, *read, {{"a.value", optionalDecimal}});
        auto project = MakeCopyMap(ctx, read, "result", "a.value");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        project->Props.StageId = consumer;
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TMergeConnection>(
                TVector<TSortElement>{
                    TSortElement(TInfoUnit("a.value"), false, true),
                },
                graph.GetOutputIndex(producer)));

        const auto snapshot = ParseSupported(
            ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& edge = snapshot["stage_graph"]["edges"][0];
        UNIT_ASSERT_VALUES_EQUAL(edge["kind"].GetStringSafe(), "merge");
        const auto& order = edge["order"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(order.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(order[0]["column"].GetStringSafe(), "a.value");
        UNIT_ASSERT(!order[0]["ascending"].GetBooleanSafe());
        UNIT_ASSERT(order[0]["nulls_first"].GetBooleanSafe());
    }

    Y_UNIT_TEST(UnsupportedSourceConnectionAndStorageFailClosed) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            auto project = MakeCopyMap(ctx, read, "result", "a.k");
            TOpRoot root(project, TPositionHandle(), {"result"});
            auto& graph = root.PlanProps.StageGraph;
            const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const ui32 consumer = graph.AddStage();
            read->Props.StageId = producer;
            project->Props.StageId = consumer;
            graph.Connect(producer, consumer, MakeIntrusive<TSourceConnection>());

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT(result.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "source connections");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            auto project = MakeCopyMap(ctx, read, "result", "a.k");
            TOpRoot root(project, TPositionHandle(), {"result"});
            auto& graph = root.PlanProps.StageGraph;
            const ui32 producer = graph.AddSourceStage(
                static_cast<NYql::EStorageType>(255));
            const ui32 consumer = graph.AddStage();
            read->Props.StageId = producer;
            project->Props.StageId = consumer;
            graph.Connect(
                producer,
                consumer,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT(result.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "source distribution storage");
        }
    }

    Y_UNIT_TEST(HashShuffleWithoutAHashFunctionFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto project = MakeCopyMap(ctx, read, "result", "a.k");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        project->Props.StageId = consumer;
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TShuffleConnection>(
                TVector<TInfoUnit>{TInfoUnit("a.k")},
                graph.GetOutputIndex(producer)));

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT(result.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "no hash function");
    }

    Y_UNIT_TEST(ColumnShardHashShuffleFailsClosedWithoutShardMapping) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto project = MakeCopyMap(ctx, read, "result", "a.k");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        project->Props.StageId = consumer;
        auto shuffle = MakeIntrusive<TShuffleConnection>(
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            graph.GetOutputIndex(producer));
        shuffle->HashFuncType = NYql::NDq::EHashShuffleFuncType::ColumnShardHashV1;
        graph.Connect(producer, consumer, shuffle);

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT(result.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "requires shard mapping");
    }

    Y_UNIT_TEST(RowStorageSourceStageRejectsLocalOperators) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto project = MakeCopyMap(ctx, read, "result", "a.k");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        read->Props.StageId = source;
        project->Props.StageId = source;

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT(result.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "must contain only its Read");
    }

    Y_UNIT_TEST(ExplicitShuffleEliminationAssumptionsFailClosed) {
        for (const bool eliminateLeft : {true, false}) {
            TExportTestContext ctx;
            AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            AddTable(ctx, "/Root/B", {{"k", "Int32", true}});
            auto left = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/A"),
                "a",
                {"k"});
            auto right = MakeRead(
                ctx,
                ctx.Tables->ExistingTable("ut", "/Root/B"),
                "b",
                {"k"});
            const auto pos = TPositionHandle();
            auto join = MakeIntrusive<TOpJoin>(
                left,
                right,
                pos,
                "Inner",
                TVector<std::pair<TInfoUnit, TInfoUnit>>{
                    {TInfoUnit("a.k"), TInfoUnit("b.k")},
                });
            if (eliminateLeft) {
                join->Props.LeftShuffleBy = TVector<TInfoUnit>{};
            } else {
                join->Props.RightShuffleBy = TVector<TInfoUnit>{};
            }
            TOpRoot root(join, pos, {"a.k", "b.k"});

            auto& graph = root.PlanProps.StageGraph;
            const ui32 leftStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const ui32 rightStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const ui32 joinStage = graph.AddStage();
            left->Props.StageId = leftStage;
            right->Props.StageId = rightStage;
            join->Props.StageId = joinStage;
            graph.Connect(
                leftStage,
                joinStage,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(leftStage)));
            graph.Connect(
                rightStage,
                joinStage,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(rightStage)));

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT(result.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "source co-partitioning assumption");
        }
    }

    Y_UNIT_TEST(MalformedStageMembershipAndProducerSinkFailClosed) {
        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            auto project = MakeCopyMap(ctx, read, "result", "a.k");
            TOpRoot root(project, TPositionHandle(), {"result"});
            auto& graph = root.PlanProps.StageGraph;
            const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const ui32 consumer = graph.AddStage();
            read->Props.StageId = producer;
            graph.Connect(
                producer,
                consumer,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT(result.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "missing or invalid stage");
        }

        {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            read->StorageType = NYql::EStorageType::ColumnStorage;
            const auto pos = TPositionHandle();
            auto left = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{});
            auto right = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{});
            auto unionAll = MakeIntrusive<TOpUnionAll>(
                left,
                right,
                pos,
                TVector<TInfoUnit>{TInfoUnit("a.k")});
            TOpRoot root(unionAll, pos, {"a.k"});

            auto& graph = root.PlanProps.StageGraph;
            const ui32 producer = graph.AddSourceStage(NYql::EStorageType::ColumnStorage);
            const ui32 consumer = graph.AddStage();
            read->Props.StageId = producer;
            left->Props.StageId = producer;
            right->Props.StageId = producer;
            unionAll->Props.StageId = consumer;
            graph.Connect(
                producer,
                consumer,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
            graph.Connect(
                producer,
                consumer,
                MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT(result.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "more than one logical sink");
        }
    }

    Y_UNIT_TEST(ConnectionContainerMismatchFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto project = MakeCopyMap(ctx, read, "result", "a.k");
        TOpRoot root(project, TPositionHandle(), {"result"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 consumer = graph.AddStage();
        read->Props.StageId = producer;
        project->Props.StageId = consumer;
        graph.Connect(
            producer,
            consumer,
            MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
        graph.StageOutputs[producer].clear();

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT(result.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(
            result.UnsupportedReason,
            "connection keys disagree");
    }

    Y_UNIT_TEST(ConsumerTaskCountsMatchExecutorAndChannelConstraints) {
        const auto exportDuplicate = [](TStringBuf mode) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            const auto pos = TPositionHandle();
            auto unionAll = MakeIntrusive<TOpUnionAll>(
                read,
                read,
                pos,
                TVector<TInfoUnit>{TInfoUnit("a.k")});
            TOpRoot root(unionAll, pos, {"a.k"});
            auto& graph = root.PlanProps.StageGraph;
            const ui32 producer = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const ui32 consumer = graph.AddStage();
            read->Props.StageId = producer;
            unionAll->Props.StageId = consumer;
            if (mode == "broadcast") {
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TBroadcastConnection>(graph.GetOutputIndex(producer)));
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TBroadcastConnection>(graph.GetOutputIndex(producer)));
            } else if (mode == "map-and-serial") {
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TUnionAllConnection>(
                        graph.GetOutputIndex(producer),
                        false));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(mode, "two-maps");
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
                graph.Connect(
                    producer,
                    consumer,
                    MakeIntrusive<TMapConnection>(graph.GetOutputIndex(producer)));
            }
            return ExportSemanticSnapshotV1(root, ctx.RboCtx);
        };

        const auto broadcast = exportDuplicate("broadcast");
        UNIT_ASSERT_C(broadcast.IsSupported(), broadcast.UnsupportedReason);

        const auto serialWithMap = exportDuplicate("map-and-serial");
        UNIT_ASSERT(!serialWithMap.IsSupported());
        UNIT_ASSERT(serialWithMap.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(serialWithMap.UnsupportedReason, "serial UnionAll");

        const auto twoMaps = exportDuplicate("two-maps");
        UNIT_ASSERT(!twoMaps.IsSupported());
        UNIT_ASSERT(twoMaps.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(twoMaps.UnsupportedReason, "more than one Map");
    }

    Y_UNIT_TEST(ParallelUnionUsesItsSingleTaskProducerCount) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto gather = MakeCopyMap(ctx, read, "middle", "a.k");
        const auto pos = TPositionHandle();
        auto unionAll = MakeIntrusive<TOpUnionAll>(
            gather,
            gather,
            pos,
            TVector<TInfoUnit>{TInfoUnit("middle")});
        TOpRoot root(unionAll, pos, {"middle"});

        auto& graph = root.PlanProps.StageGraph;
        const ui32 source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const ui32 gatherStage = graph.AddStage();
        const ui32 rootStage = graph.AddStage();
        read->Props.StageId = source;
        gather->Props.StageId = gatherStage;
        unionAll->Props.StageId = rootStage;
        graph.Connect(
            source,
            gatherStage,
            MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(source), false));
        graph.Connect(
            gatherStage,
            rootStage,
            MakeIntrusive<TMapConnection>(graph.GetOutputIndex(gatherStage)));
        graph.Connect(
            gatherStage,
            rootStage,
            MakeIntrusive<TUnionAllConnection>(
                graph.GetOutputIndex(gatherStage),
                true));

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT_C(result.IsSupported(), result.UnsupportedReason);
    }

    Y_UNIT_TEST(ChangedPhysicalTableIdentityFailsClosedAgainstInitialCatalog) {
        TExportTestContext ctx;
        auto& table = ctx.Tables->GetOrAddTable("ut", "/Root", "/Root/A");
        table.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/A");
        table.Metadata->DoesExist = true;
        table.Metadata->PathId = TKikimrPathId(1, 1);
        table.Metadata->SchemaVersion = 1;
        table.Metadata->Columns.emplace(
            "k", TKikimrColumnMetadata("k", 1, "Int32", true));
        table.Metadata->ColumnOrder = {"k"};
        UNIT_ASSERT(table.Load(ctx.ExprCtx));

        auto initialRead = MakeRead(ctx, table, "a", {"k"});
        TOpRoot initialRoot(initialRead, TPositionHandle(), {"a.k"});
        const auto catalog = CaptureSemanticSnapshotCatalogV1(initialRoot, ctx.RboCtx);
        UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);

        table.Metadata->SchemaVersion = 2;
        auto changedRead = MakeRead(ctx, table, "a", {"k"});
        TOpRoot changedRoot(changedRead, TPositionHandle(), {"a.k"});
        const auto result = ExportSemanticSnapshotV1(
            changedRoot, ctx.RboCtx, catalog.Catalog);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "table identity");
    }

    Y_UNIT_TEST(TransformationPrefixUsesTheInitialCatalogAndHasDistinctMetadata) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        const TVector<TRBOTransformationEventV1> events{
            {1, ERBOTransformationEventKindV1::RuleApplication, "Logical rewrites", "First rule"},
            {2, ERBOTransformationEventKindV1::RuleApplication, "Logical rewrites", "Push filter"},
        };
        capture.CaptureTransformationPrefix(root, ctx.RboCtx, events);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(sink.Results[0].TransformationEvents.empty());
        UNIT_ASSERT(
            sink.Results[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[0].Stage,
            "Logical rewrites");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[0].Name,
            "First rule");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[1].Stage,
            "Logical rewrites");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[1].Name,
            "Push filter");

        const auto initialSnapshot = ParseSupported(sink.Results[0]);
        const auto prefixSnapshot = ParseSupported(sink.Results[1]);
        UNIT_ASSERT(initialSnapshot["stage_graph"].IsNull());
        UNIT_ASSERT(prefixSnapshot["stage_graph"].IsNull());
        const auto& initialTables = initialSnapshot["schema"]["tables"].GetArraySafe();
        const auto& prefixTables = prefixSnapshot["schema"]["tables"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(initialTables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(prefixTables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            initialTables[0]["name"].GetStringSafe(),
            prefixTables[0]["name"].GetStringSafe());
    }

    Y_UNIT_TEST(TransformationPrefixConfigurationExceptionsAreContained) {
        TThrowingTransformationPrefixConfigurationSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        UNIT_ASSERT(!capture.GetTransformationPrefixTarget());

        TTransformationPrefixRecordingSink zeroSink(0);
        TSemanticSnapshotPairCaptureV1 zeroCapture(&zeroSink);
        UNIT_ASSERT(!zeroCapture.GetTransformationPrefixTarget());
        TExportTestContext ctx;
        ctx.RboCtx.TransformationDebug.Reset(
            zeroCapture.GetTransformationPrefixTarget());
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.OnRuleApplication("Stage", "Rule"));
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.OnAtomicStageCommit("Stage", "Commit"));
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.Stopped);
        UNIT_ASSERT(ctx.RboCtx.TransformationDebug.Events.empty());
    }

    Y_UNIT_TEST(FinalBoundaryCarriesTheCompleteSequenceWhenTargetDoesNotExist) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TTransformationPrefixRecordingSink sink(3);
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        ctx.RboCtx.TransformationDebug.Reset(
            capture.GetTransformationPrefixTarget());
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.OnRuleApplication("First", "R1"));
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.OnRuleApplication("Second", "R2"));
        const ui32 finalStage = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::RowStorage);
        read->Props.StageId = finalStage;
        capture.CaptureFinal(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Stage, "First");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Name, "R1");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[1].Stage, "Second");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[1].Name, "R2");
        ParseSupported(sink.Results[1]);
    }

    Y_UNIT_TEST(TransformationStopIsOptimizerWideAndPrecedesEverySuffix) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        ui32 repeatedAttempts = 0;
        ui32 wrapApplications = 0;
        ui32 laterStageRuns = 0;
        TVector<std::unique_ptr<IRule>> repeatedRules;
        repeatedRules.push_back(std::make_unique<TFixedApplicationRule>(
            "Apply twice",
            2,
            repeatedAttempts));
        TVector<std::unique_ptr<IRule>> wrappingRules;
        wrappingRules.push_back(std::make_unique<TWrapReadRule>(wrapApplications));

        TRuleBasedOptimizer optimizer;
        optimizer.AddStage(std::make_unique<TRuleBasedStage>(
            TString("Repeated stage"),
            std::move(repeatedRules)));
        optimizer.AddStage(std::make_unique<TRuleBasedStage>(
            TString("Wrapping stage"),
            std::move(wrappingRules)));
        optimizer.AddStage(std::make_unique<TCountingStage>(laterStageRuns));

        TTransformationPrefixRecordingSink sink(3);
        const auto output = optimizer.Optimize(root, ctx.RboCtx, &sink);

        UNIT_ASSERT(!output);
        UNIT_ASSERT_VALUES_EQUAL(repeatedAttempts, 3);
        UNIT_ASSERT_VALUES_EQUAL(wrapApplications, 1);
        UNIT_ASSERT_VALUES_EQUAL(laterStageRuns, 0);
        UNIT_ASSERT(ctx.RboCtx.TransformationDebug.Stopped);
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.TransformationDebug.Events.size(),
            3);
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.TransformationDebug.Events.back().Stage,
            "Wrapping stage");
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.TransformationDebug.Events.back().Name,
            "Wrap read");
        UNIT_ASSERT(!ctx.RboCtx.ExecutionJson);
        UNIT_ASSERT(!ctx.RboCtx.ExplainJson);
        UNIT_ASSERT(root.GetInput()->Kind == EOperator::Map);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            sink.Results[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[0].Name,
            "Apply twice");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[1].Name,
            "Apply twice");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[2].Ordinal, 3);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[2].Stage,
            "Wrapping stage");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].TransformationEvents[2].Name,
            "Wrap read");
        const auto prefixSnapshot = ParseSupported(sink.Results[1]);
        UNIT_ASSERT(prefixSnapshot["stage_graph"].IsNull());
        FindNode(prefixSnapshot, "project");
    }

    Y_UNIT_TEST(TransformationTargetsBeforeAtAndAfterAnAtomicStageAreExact) {
        for (ui64 target = 1; target <= 3; ++target) {
            TExportTestContext ctx;
            const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
            auto read = MakeRead(ctx, table, "a", {"k"});
            TOpRoot root(read, TPositionHandle(), {"a.k"});

            ui32 firstAttempts = 0;
            ui32 atomicRuns = 0;
            ui32 laterApplications = 0;
            ui32 suffixRuns = 0;
            TVector<std::unique_ptr<IRule>> firstRules;
            firstRules.push_back(std::make_unique<TFixedApplicationRule>(
                "First rule",
                1,
                firstAttempts));
            TVector<std::unique_ptr<IRule>> laterRules;
            laterRules.push_back(std::make_unique<TWrapReadRule>(laterApplications));

            TRuleBasedOptimizer optimizer;
            optimizer.AddStage(std::make_unique<TRuleBasedStage>(
                TString("First stage"),
                std::move(firstRules)));
            optimizer.AddStage(std::make_unique<TAtomicWrapStage>(atomicRuns));
            optimizer.AddStage(std::make_unique<TRuleBasedStage>(
                TString("Later stage"),
                std::move(laterRules)));
            optimizer.AddStage(std::make_unique<TCountingStage>(suffixRuns));

            TTransformationPrefixRecordingSink sink(target);
            const auto output = optimizer.Optimize(root, ctx.RboCtx, &sink);

            UNIT_ASSERT(!output);
            UNIT_ASSERT_VALUES_EQUAL(atomicRuns, target >= 2 ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(laterApplications, target >= 3 ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(suffixRuns, 0);
            UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
            UNIT_ASSERT(
                sink.Results[1].Boundary ==
                ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
            const auto& events = sink.Results[1].TransformationEvents;
            UNIT_ASSERT_VALUES_EQUAL(events.size(), target);
            UNIT_ASSERT_VALUES_EQUAL(events[0].Ordinal, 1);
            UNIT_ASSERT(
                events[0].Kind == ERBOTransformationEventKindV1::RuleApplication);
            UNIT_ASSERT_VALUES_EQUAL(events[0].Stage, "First stage");
            UNIT_ASSERT_VALUES_EQUAL(events[0].Name, "First rule");
            if (target >= 2) {
                UNIT_ASSERT_VALUES_EQUAL(events[1].Ordinal, 2);
                UNIT_ASSERT(
                    events[1].Kind ==
                    ERBOTransformationEventKindV1::AtomicStageCommit);
                UNIT_ASSERT_VALUES_EQUAL(events[1].Stage, "Atomic stage");
                UNIT_ASSERT_VALUES_EQUAL(events[1].Name, "Wrap root");
            }
            if (target == 3) {
                UNIT_ASSERT_VALUES_EQUAL(events[2].Ordinal, 3);
                UNIT_ASSERT(
                    events[2].Kind ==
                    ERBOTransformationEventKindV1::RuleApplication);
                UNIT_ASSERT_VALUES_EQUAL(events[2].Stage, "Later stage");
                UNIT_ASSERT_VALUES_EQUAL(events[2].Name, "Wrap read");
            }

            const auto prefixSnapshot = ParseSupported(sink.Results[1]);
            UNIT_ASSERT(prefixSnapshot["stage_graph"].IsNull());
            if (target == 1) {
                FindNode(prefixSnapshot, "scan");
            } else {
                FindNode(prefixSnapshot, "project");
            }
        }
    }

    Y_UNIT_TEST(NoOpAtomicStageStillEmitsACommittedCheckpoint) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        ui32 atomicRuns = 0;
        ui32 suffixRuns = 0;
        TRuleBasedOptimizer optimizer;
        optimizer.AddStage(std::make_unique<TNoOpAtomicStage>(atomicRuns));
        optimizer.AddStage(std::make_unique<TCountingStage>(suffixRuns));

        TTransformationPrefixRecordingSink sink(1);
        const auto output = optimizer.Optimize(root, ctx.RboCtx, &sink);

        UNIT_ASSERT(!output);
        UNIT_ASSERT_VALUES_EQUAL(atomicRuns, 1);
        UNIT_ASSERT_VALUES_EQUAL(suffixRuns, 0);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        const auto& events = sink.Results[1].TransformationEvents;
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT(
            events[0].Kind == ERBOTransformationEventKindV1::AtomicStageCommit);
        UNIT_ASSERT_VALUES_EQUAL(events[0].Stage, "No-op atomic stage");
        UNIT_ASSERT_VALUES_EQUAL(events[0].Name, "Commit no-op checkpoint");
        const auto prefixSnapshot = ParseSupported(sink.Results[1]);
        FindNode(prefixSnapshot, "scan");
    }

    Y_UNIT_TEST(TransformationPrefixDeliveryFailureDoesNotEscapeOrLoseCatalog) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TThrowOnceSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        capture.CaptureTransformationPrefix(
            root,
            ctx.RboCtx,
            TVector<TRBOTransformationEventV1>{{
                1,
                ERBOTransformationEventKindV1::RuleApplication,
                "Stage",
                "Rule"}});

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(
            sink.Results[0].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::TransformationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[0].TransformationEvents.size(), 1);
        ParseSupported(sink.Results[0]);
    }

    Y_UNIT_TEST(UnsupportedTransformationPrefixRetainsItsCompleteSequence) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        TOpRoot initialRoot(read, pos, {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(initialRoot, ctx.RboCtx);
        auto limit = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Int64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot prefixRoot(limit, pos, {"a.k"});
        capture.CaptureTransformationPrefix(
            prefixRoot,
            ctx.RboCtx,
            TVector<TRBOTransformationEventV1>{{
                1,
                ERBOTransformationEventKindV1::RuleApplication,
                "Stage",
                "Rule"}});

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(!sink.Results[1].IsSupported());
        UNIT_ASSERT(sink.Results[1].Json.empty());
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT_STRING_CONTAINS(sink.Results[1].UnsupportedReason, "Limit count");
    }

    Y_UNIT_TEST(NullPairSinkDoesNoSnapshotWork) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});
        UNIT_ASSERT(!read->Props.OutputIUs);

        TSemanticSnapshotPairCaptureV1 capture(nullptr);
        capture.CaptureInitial(root, ctx.RboCtx);
        capture.CaptureFinal(root, ctx.RboCtx);

        // Exporting either boundary asks the Read for its output IUs.  Keeping
        // this cache empty makes the disabled path observably lazy.
        UNIT_ASSERT(!read->Props.OutputIUs);
    }

    Y_UNIT_TEST(PairSinkReceivesInitialThenFinalWithOneSharedCatalog) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"flag", "Bool", false},
        }, {"k"});
        auto initialRead = MakeRead(ctx, table, "a", {"k", "flag"});
        TOpRoot initialRoot(initialRead, TPositionHandle(), {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(initialRoot, ctx.RboCtx);

        const auto pos = TPositionHandle();
        auto empty = MakeIntrusive<TOpEmptySource>(pos);
        auto replacement = MakeIntrusive<TOpMap>(
            empty,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("a.k"),
                MakeConstant("Int32", "0", pos, &ctx.ExprCtx))});
        TOpRoot finalRoot(replacement, pos, {"a.k"});
        const ui32 finalStage = finalRoot.PlanProps.StageGraph.AddStage();
        empty->Props.StageId = finalStage;
        replacement->Props.StageId = finalStage;
        capture.CaptureFinal(finalRoot, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

        const auto initialSnapshot = ParseSupported(sink.Results[0]);
        const auto finalSnapshot = ParseSupported(sink.Results[1]);
        UNIT_ASSERT(initialSnapshot["stage_graph"].IsNull());
        UNIT_ASSERT(!finalSnapshot["stage_graph"].IsNull());
        const auto& initialTables = initialSnapshot["schema"]["tables"].GetArraySafe();
        const auto& finalTables = finalSnapshot["schema"]["tables"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(initialTables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finalTables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            initialTables[0]["name"].GetStringSafe(),
            finalTables[0]["name"].GetStringSafe());
        UNIT_ASSERT_STRING_CONTAINS(finalTables[0]["name"].GetStringSafe(), "/Root/A");
        UNIT_ASSERT_VALUES_EQUAL(
            initialTables[0]["columns"].GetArraySafe().size(),
            finalTables[0]["columns"].GetArraySafe().size());
    }

    Y_UNIT_TEST(InitialPairCaptureMaterializesAggregateTypesInsideFailClosedPath) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int64", true},
            {"x", "Int32", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"), "count", TInfoUnit("result"))},
            TVector<TInfoUnit>{TInfoUnit("a.k")},
            EOpPhase::Undefined,
            false,
            pos);
        TOpRoot root(aggregate, pos, {"a.k", "result"});
        UNIT_ASSERT(!aggregate->GetTypeAnn());

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        const auto snapshot = ParseSupported(sink.Results[0]);
        UNIT_ASSERT(aggregate->GetTypeAnn());
        const auto& trait = FindNode(snapshot, "aggregate")["aggregates"][0];
        UNIT_ASSERT_VALUES_EQUAL(trait["type"].GetStringSafe(), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(trait["nullable"].GetBooleanSafe(), false);
    }

    Y_UNIT_TEST(InitialPairCaptureMaterializesNonAggregateScalarSubplanTypes) {
        TExportTestContext ctx;
        const auto& table = AddTable(
            ctx,
            "/Root/A",
            {{"k", "Int32", true}});
        const auto& scalarTable = AddTable(
            ctx,
            "/Root/Scalar",
            {{"value", "Int64", false}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        auto scalarRead = MakeRead(
            ctx,
            scalarTable,
            "scalar",
            {"value"});
        const auto pos = TPositionHandle();
        auto scalarLimit = MakeIntrusive<TOpLimit>(
            scalarRead,
            pos,
            MakeConstant("Uint64", "0", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);

        TOpRoot root(read, pos, {"result"});
        const TInfoUnit binding("_rbo_arg_0", true);
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                scalarLimit,
                {},
                ESubplanType::EXPR,
                binding,
                {}});
        auto consumer = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("result"),
                binding,
                pos,
                &ctx.ExprCtx,
                &root.PlanProps)});
        const auto* optionalInt64 =
            ScalarType(ctx, NUdf::EDataSlot::Int64, true);
        auto& consumerExpression =
            consumer->MapElements.front().GetExpressionRef();
        consumerExpression.GetExpressionBody()->SetTypeAnn(optionalInt64);
        consumerExpression.Node->SetTypeAnn(optionalInt64);
        auto* consumerArguments = consumerExpression.Node->Child(0);
        consumerArguments->Child(0)->SetTypeAnn(
            ctx.ExprCtx.MakeType<TStructExprType>(
                TVector<const TItemExprType*>{
                    ctx.ExprCtx.MakeType<TItemExprType>(
                        "a.k",
                        ScalarType(ctx, NUdf::EDataSlot::Int32)),
                    ctx.ExprCtx.MakeType<TItemExprType>(
                        "_rbo_arg_0",
                        optionalInt64),
                }));
        consumerArguments->SetTypeAnn(
            ctx.ExprCtx.MakeType<TUnitExprType>());
        root.SetInput(consumer);
        UNIT_ASSERT(!scalarLimit->GetTypeAnn());
        UNIT_ASSERT(!consumer->GetTypeAnn());

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        const auto snapshot = ParseSupported(sink.Results[0]);
        UNIT_ASSERT(scalarLimit->GetTypeAnn());
        UNIT_ASSERT(consumer->GetTypeAnn());
        const auto& descriptor =
            snapshot["plan"]["subplans"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["column"].GetStringSafe(),
            "scalar.value");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor["output"]["type"].GetStringSafe(),
            "Int64");
    }

    Y_UNIT_TEST(InitialPairCaptureMaterializesMapAndSortTypesInsideFailClosedPath) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto map = MakeIntrusive<TOpMap>(read, pos, TVector<TMapElement>{});
        auto sort = MakeIntrusive<TOpSort>(
            map,
            pos,
            TVector<TSortElement>{TSortElement(TInfoUnit("a.k"), true, false)});
        TOpRoot root(sort, pos, {"a.k"});
        UNIT_ASSERT(!map->GetTypeAnn());
        UNIT_ASSERT(!sort->GetTypeAnn());

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        const auto snapshot = ParseSupported(sink.Results[0]);
        UNIT_ASSERT(map->GetTypeAnn());
        UNIT_ASSERT(sort->GetTypeAnn());
        const auto& order = FindNode(snapshot, "sort")["order"][0];
        UNIT_ASSERT_VALUES_EQUAL(order["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(order["ascending"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order["nulls_first"].GetBooleanSafe(), false);
    }

    Y_UNIT_TEST(InitialPairCaptureValidatesTopologyBeforeMaterialization) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {
            {"k", "Int32", true},
            {"x", "Int64", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        const auto pos = TPositionHandle();
        auto scalarAggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"),
                "sum",
                TInfoUnit("scalar.value"))},
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        auto malformedSort = MakeIntrusive<TOpSort>(
            read,
            pos,
            TVector<TSortElement>{
                TSortElement(TInfoUnit("a.k"), true, true)});
        malformedSort->Children.clear();
        TOpRoot root(malformedSort, pos, {"a.k"});
        const TInfoUnit binding("_rbo_arg_0", true);
        root.PlanProps.Subplans.Add(
            binding,
            TSubplanEntry{
                scalarAggregate,
                {},
                ESubplanType::EXPR,
                binding,
                {}});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(!sink.Results[0].IsSupported());
        UNIT_ASSERT(sink.Results[0].Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(
            sink.Results[0].UnsupportedReason,
            "Sort must have one input");
    }

    Y_UNIT_TEST(PairCaptureRejectsAStagedInitialBoundary) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});
        const ui32 stage = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::RowStorage);
        read->Props.StageId = stage;

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(!sink.Results[0].IsSupported());
        UNIT_ASSERT(sink.Results[0].Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(
            sink.Results[0].UnsupportedReason,
            "Initial semantic snapshot boundary requires stage_graph:null");
    }

    Y_UNIT_TEST(PairCaptureRejectsALogicalFinalBoundary) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        capture.CaptureFinal(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(sink.Results[0].IsSupported());
        UNIT_ASSERT(!sink.Results[1].IsSupported());
        UNIT_ASSERT(sink.Results[1].Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(
            sink.Results[1].UnsupportedReason,
            "Final semantic snapshot boundary requires a non-null stage_graph");
    }

    Y_UNIT_TEST(UnsupportedFinalSnapshotIsDeliveredWithoutThrowing) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        TOpRoot initialRoot(read, pos, {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(initialRoot, ctx.RboCtx);
        ctx.RboCtx.TransformationDebug.Reset(2);
        UNIT_ASSERT(!ctx.RboCtx.TransformationDebug.OnRuleApplication("Stage", "Rule"));

        auto limit = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Int64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot finalRoot(limit, pos, {"a.k"});
        const ui32 finalStage = finalRoot.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::RowStorage);
        read->Props.StageId = finalStage;
        limit->Props.StageId = finalStage;
        capture.CaptureFinal(finalRoot, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(sink.Results[0].IsSupported());
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        UNIT_ASSERT(!sink.Results[1].IsSupported());
        UNIT_ASSERT(sink.Results[1].Json.empty());
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].TransformationEvents[0].Ordinal, 1);
        UNIT_ASSERT_STRING_CONTAINS(sink.Results[1].UnsupportedReason, "Limit count");
    }

    Y_UNIT_TEST(SinkFailureDoesNotDiscardTheSharedCatalog) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TThrowOnceSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        const ui32 finalStage = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::RowStorage);
        read->Props.StageId = finalStage;
        capture.CaptureFinal(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        ParseSupported(sink.Results[0]);
    }
}

} // namespace
