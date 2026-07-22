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
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_type_ops.h>

#include <algorithm>
#include <initializer_list>
#include <limits>
#include <stdexcept>

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
    const TTypeAnnotationNode* type)
{
    auto result = ctx.ExprCtx.NewCallable(
        TPositionHandle(),
        "DataType",
        {ctx.ExprCtx.NewAtom(TPositionHandle(), typeName)});
    result->SetTypeAnn(ctx.ExprCtx.MakeType<TTypeExprType>(type));
    return result;
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

TExprNode::TPtr MakeOlapComparisonProcess(
    TExportTestContext& ctx,
    TStringBuf operation,
    TStringBuf column,
    TStringBuf literal,
    bool coalesceFalse = false)
{
    const auto pos = TPositionHandle();
    const auto* intType = ScalarType(ctx, NUdf::EDataSlot::Int32);
    auto comparison = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
        .Operator().Value(operation).Build()
        .Left<TCoAtom>().Value(column).Build()
        .Right(TypedLiteral(ctx, "Int32", literal, intType))
        .Done();

    TExprNode::TPtr condition = comparison.Ptr();
    if (coalesceFalse) {
        condition = Build<TKqpOlapFilterBinaryOp>(ctx.ExprCtx, pos)
            .Operator().Value("??").Build()
            .Left(comparison)
            .Right(TypedLiteral(
                ctx,
                "Bool",
                "false",
                ScalarType(ctx, NUdf::EDataSlot::Bool)))
            .Done().Ptr();
    }

    const auto argument = ctx.ExprCtx.NewArgument(pos, "row");
    const auto filter = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
        .Input(TExprBase(argument))
        .Condition(TExprBase(condition))
        .Done();
    return ctx.ExprCtx.NewLambda(
        pos,
        ctx.ExprCtx.NewArguments(pos, {argument}),
        filter.Ptr());
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

    const auto argument = ctx.ExprCtx.NewArgument(pos, "row");
    const auto filter = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
        .Input(TExprBase(argument))
        .Condition(TExprBase(comparison.Ptr()))
        .Done();
    return ctx.ExprCtx.NewLambda(
        pos,
        ctx.ExprCtx.NewArguments(pos, {argument}),
        filter.Ptr());
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

Y_UNIT_TEST_SUITE(TSemanticSnapshotExporter) {
    Y_UNIT_TEST(OutputIsDeterministicAcrossEquivalentAllocations) {
        UNIT_ASSERT_VALUES_EQUAL(ExportDeterministicPlan(), ExportDeterministicPlan());
    }

    Y_UNIT_TEST(StageGraphOutputIgnoresRandomRuntimeGuids) {
        UNIT_ASSERT_VALUES_EQUAL(
            ExportDeterministicStageGraph(),
            ExportDeterministicStageGraph());
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
            "constant Decimal cast must have a non-nullable Decimal result");
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

    Y_UNIT_TEST(UnauditedDecimalCastsFailClosed) {
        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int32);
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
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "source is not a non-nullable integer literal");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = ScalarType(ctx, NUdf::EDataSlot::Int32);
            const auto* decimalType = DecimalType(ctx, "11", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedLiteral(ctx, "Int32", "2147483647", sourceType),
                        DecimalDataTypeDescriptor(ctx, "11", "2", decimalType),
                    },
                    decimalType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "cast is not complete");
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
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "non-nullable Decimal result");
        }

        {
            TExportTestContext ctx;
            const auto* sourceType = DecimalType(ctx, "5", "2");
            const auto* targetType = DecimalType(ctx, "12", "2");
            const auto result = ExportMapExpressionResult(
                ctx,
                "a",
                TypedCallable(
                    ctx,
                    "SafeCast",
                    {
                        TypedDecimalLiteral(ctx, "1", "5", "2", sourceType),
                        DecimalDataTypeDescriptor(ctx, "12", "2", targetType),
                    },
                    targetType));
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "source is not a non-nullable integer literal");
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

        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), "Int32");
        UNIT_ASSERT(!expression["nullable"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(expression["args"].GetArraySafe().size(), 2);
        for (const auto callable : {"SafeCast", "Exists", "Just", "Coalesce", "Convert", "If"}) {
            UNIT_ASSERT_STRING_CONTAINS(expression["fingerprint"].GetStringSafe(), callable);
        }

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

        TExportTestContext lossy;
        const auto* int8Type = ScalarType(lossy, NUdf::EDataSlot::Int8);
        const auto* uint8Type = ScalarType(lossy, NUdf::EDataSlot::Uint8);
        const auto* lossyBoolType = ScalarType(lossy, NUdf::EDataSlot::Bool);
        const auto lossyResult = ExportMapExpressionResult(
            lossy,
            "a",
            TypedCallable(
                lossy,
                "==",
                {
                    TypedMember(lossy, "a.x", int8Type),
                    TypedLiteral(lossy, "Uint8", "30", uint8Type),
                },
                lossyBoolType));
        UNIT_ASSERT(!lossyResult.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(
            lossyResult.UnsupportedReason,
            "comparison operand types differ");
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

        for (const auto callable : {"/", "Unwrap", "StrictCast", "Udf", "Apply", "Now", "CurrentActorId"}) {
            const auto result = exportCallable(callable);
            UNIT_ASSERT_C(!result.IsSupported(), callable);
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported scalar callable");
        }

        auto result = exportCallable("+", [](TExprNode& node) {
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
        const auto& predicate = joinJson["predicate"];
        UNIT_ASSERT_VALUES_EQUAL(predicate["kind"].GetStringSafe(), "and");
        const auto& conjuncts = predicate["args"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(conjuncts[0]),
            (std::pair<TString, TString>{"a.k", "b.k"}));
        UNIT_ASSERT_VALUES_EQUAL(
            EqualityColumns(conjuncts[1]),
            (std::pair<TString, TString>{"a.flag", "b.flag"}));
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

    Y_UNIT_TEST(PreservesNonDefaultAggregateTraitFlags) {
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
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(
                    TInfoUnit("a.x"),
                    "sum",
                    TInfoUnit("distinct_result"),
                    true,
                    false),
                TOpAggregationTraits(
                    TInfoUnit("a.y"),
                    "sum",
                    TInfoUnit("unwrap_result"),
                    false,
                    true),
            },
            TVector<TInfoUnit>{},
            EOpPhase::Undefined,
            false,
            pos);
        SetOutputType(ctx, *aggregate, {
            {"distinct_result", NUdf::EDataSlot::Int64, true},
            {"unwrap_result", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot root(aggregate, pos, {"distinct_result", "unwrap_result"});

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& node = FindNode(snapshot, "aggregate");
        const auto& traits = node["aggregates"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(traits.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["input"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["output"].GetStringSafe(), "distinct_result");
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["distinct"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(traits[0]["unwrap"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["input"].GetStringSafe(), "a.y");
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["output"].GetStringSafe(), "unwrap_result");
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["nullable"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["distinct"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(traits[1]["unwrap"].GetBooleanSafe(), true);

        auto distinctAll = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                TInfoUnit("a.x"), "distinct", TInfoUnit("a.x"))},
            TVector<TInfoUnit>{TInfoUnit("a.x")},
            EOpPhase::Undefined,
            true,
            pos);
        SetOutputType(ctx, *distinctAll, {
            {"a.x", NUdf::EDataSlot::Int64, true},
        });
        TOpRoot distinctRoot(distinctAll, pos, {"a.x"});
        const auto distinctSnapshot = ParseSupported(
            ExportSemanticSnapshotV1(distinctRoot, ctx.RboCtx));
        UNIT_ASSERT_VALUES_EQUAL(
            FindNode(distinctSnapshot, "aggregate")["distinct_all"].GetBooleanSafe(),
            true);
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

    Y_UNIT_TEST(ExportsDateAndDecimalSortAndRejectsTextOrdering) {
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
            auto sort = MakeIntrusive<TOpSort>(
                read,
                pos,
                TVector<TSortElement>{TSortElement(TInfoUnit("a.value"), true, false)});
            SetExactOutputType(ctx, *sort, {{"a.value", optionalType}});
            TOpRoot root(sort, pos, {"a.value"});

            const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT_C(!result.IsSupported(), typeName);
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                TStringBuilder()
                    << "Sort ordering column a.value has unsupported type "
                    << typeName);
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

    Y_UNIT_TEST(UnsupportedOlapFilterFormsFailClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto makeRead = [&]() {
            auto read = MakeRead(
                ctx,
                table,
                "a",
                {"k"},
                NYql::EStorageType::ColumnStorage);
            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Int32}});
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
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "unavailable physical column missing");

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
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.x", NUdf::EDataSlot::Date, true},
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
                },
                graph.GetOutputIndex(producer)));

        const auto snapshot = ParseSupported(ExportSemanticSnapshotV1(root, ctx.RboCtx));
        const auto& edge = snapshot["stage_graph"]["edges"][0];
        UNIT_ASSERT_VALUES_EQUAL(edge.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(edge["kind"].GetStringSafe(), "merge");
        const auto& order = edge["order"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(order.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(order[0].GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(order[0]["column"].GetStringSafe(), "a.k");
        UNIT_ASSERT_VALUES_EQUAL(order[0]["ascending"].GetBooleanSafe(), true);
        UNIT_ASSERT_VALUES_EQUAL(order[0]["nulls_first"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(order[1].GetMapSafe().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(order[1]["column"].GetStringSafe(), "a.x");
        UNIT_ASSERT_VALUES_EQUAL(order[1]["ascending"].GetBooleanSafe(), false);
        UNIT_ASSERT_VALUES_EQUAL(order[1]["nulls_first"].GetBooleanSafe(), true);
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
