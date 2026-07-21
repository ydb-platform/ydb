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

class TRulePrefixRecordingSink final : public IRBOSemanticSnapshotSink {
public:
    explicit TRulePrefixRecordingSink(ui64 target)
        : Target(target)
    {
    }

    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        Results.push_back(std::move(result));
    }

    std::optional<ui64> GetRuleApplicationPrefixTarget() const override {
        return Target;
    }

    ui64 Target;
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

class TThrowingRulePrefixConfigurationSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1) override {
    }

    std::optional<ui64> GetRuleApplicationPrefixTarget() const override {
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
        : IRBOStage(TString("Must not run"))
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

    Y_UNIT_TEST(DateLiteralFailsClosed) {
        TExportTestContext ctx;
        const auto result = ExportMapExpressionResult(
            ctx,
            "a",
            TypedLiteral(
                ctx,
                "Date",
                "0",
                ScalarType(ctx, NUdf::EDataSlot::Date)));
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported literal callable Date");
    }

    Y_UNIT_TEST(ExportsDecimalNothingAndTypeDescriptor) {
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
        const auto castExpression = ExportMapExpression(
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
        UNIT_ASSERT_VALUES_EQUAL(castExpression["kind"].GetStringSafe(), "opaque");
        UNIT_ASSERT_VALUES_EQUAL(castExpression["type"].GetStringSafe(), "Decimal(5,2)");
        UNIT_ASSERT(castExpression["nullable"].GetBooleanSafe());
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

    Y_UNIT_TEST(DecimalLiteralFailsClosed) {
        TExportTestContext ctx;
        const auto* decimalType = ctx.ExprCtx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal,
            "5",
            "2");
        const auto result = ExportMapExpressionResult(
            ctx,
            "a",
            TypedCallable(
                ctx,
                "Decimal",
                {
                    ctx.ExprCtx.NewAtom(TPositionHandle(), "12.34"),
                    ctx.ExprCtx.NewAtom(TPositionHandle(), "5"),
                    ctx.ExprCtx.NewAtom(TPositionHandle(), "2"),
                },
                decimalType));
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported scalar callable Decimal");
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

            SetOutputType(ctx, *read, {{"a.k", NUdf::EDataSlot::Date}});
            SetOutputType(ctx, *sort, {{"a.k", NUdf::EDataSlot::Date}});
            result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(
                result.UnsupportedReason,
                "Sort ordering is modeled only for integers");
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
            {"x", "Int32", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "x"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.x", NUdf::EDataSlot::Int32, true},
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

    Y_UNIT_TEST(RuleApplicationPrefixUsesTheInitialCatalogAndHasDistinctMetadata) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TRecordingSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        const TVector<TRBORuleApplicationV1> applications{
            {1, "Logical rewrites", "First rule"},
            {2, "Logical rewrites", "Push filter"},
        };
        capture.CaptureRuleApplicationPrefix(root, ctx.RboCtx, applications);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(sink.Results[0].RuleApplications.empty());
        UNIT_ASSERT(
            sink.Results[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[0].StageName,
            "Logical rewrites");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[0].RuleName,
            "First rule");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[1].StageName,
            "Logical rewrites");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[1].RuleName,
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

    Y_UNIT_TEST(RuleApplicationPrefixConfigurationExceptionsAreContained) {
        TThrowingRulePrefixConfigurationSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        UNIT_ASSERT(!capture.GetRuleApplicationPrefixTarget());

        TRulePrefixRecordingSink zeroSink(0);
        TSemanticSnapshotPairCaptureV1 zeroCapture(&zeroSink);
        UNIT_ASSERT(!zeroCapture.GetRuleApplicationPrefixTarget());
        TExportTestContext ctx;
        ctx.RboCtx.RuleApplicationDebug.Reset(
            zeroCapture.GetRuleApplicationPrefixTarget());
        UNIT_ASSERT(!ctx.RboCtx.RuleApplicationDebug.OnApplied("Stage", "Rule"));
        UNIT_ASSERT(ctx.RboCtx.RuleApplicationDebug.Applications.empty());
    }

    Y_UNIT_TEST(FinalBoundaryCarriesTheCompleteSequenceWhenTargetDoesNotExist) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TRulePrefixRecordingSink sink(3);
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        ctx.RboCtx.RuleApplicationDebug.Reset(
            capture.GetRuleApplicationPrefixTarget());
        UNIT_ASSERT(!ctx.RboCtx.RuleApplicationDebug.OnApplied("First", "R1"));
        UNIT_ASSERT(!ctx.RboCtx.RuleApplicationDebug.OnApplied("Second", "R2"));
        const ui32 finalStage = root.PlanProps.StageGraph.AddSourceStage(
            NYql::EStorageType::RowStorage);
        read->Props.StageId = finalStage;
        capture.CaptureFinal(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].StageName, "First");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].RuleName, "R1");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[1].StageName, "Second");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[1].RuleName, "R2");
        ParseSupported(sink.Results[1]);
    }

    Y_UNIT_TEST(RuleApplicationStopIsOptimizerWideAndPrecedesEverySuffix) {
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

        TRulePrefixRecordingSink sink(3);
        const auto output = optimizer.Optimize(root, ctx.RboCtx, &sink);

        UNIT_ASSERT(!output);
        UNIT_ASSERT_VALUES_EQUAL(repeatedAttempts, 3);
        UNIT_ASSERT_VALUES_EQUAL(wrapApplications, 1);
        UNIT_ASSERT_VALUES_EQUAL(laterStageRuns, 0);
        UNIT_ASSERT(ctx.RboCtx.RuleApplicationDebug.Stopped);
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.RuleApplicationDebug.Applications.size(),
            3);
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.RuleApplicationDebug.Applications.back().StageName,
            "Wrapping stage");
        UNIT_ASSERT_VALUES_EQUAL(
            ctx.RboCtx.RuleApplicationDebug.Applications.back().RuleName,
            "Wrap read");
        UNIT_ASSERT(!ctx.RboCtx.ExecutionJson);
        UNIT_ASSERT(!ctx.RboCtx.ExplainJson);
        UNIT_ASSERT(root.GetInput()->Kind == EOperator::Map);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            sink.Results[1].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].Ordinal, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[0].RuleName,
            "Apply twice");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[1].Ordinal, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[1].RuleName,
            "Apply twice");
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[2].Ordinal, 3);
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[2].StageName,
            "Wrapping stage");
        UNIT_ASSERT_VALUES_EQUAL(
            sink.Results[1].RuleApplications[2].RuleName,
            "Wrap read");
        const auto prefixSnapshot = ParseSupported(sink.Results[1]);
        UNIT_ASSERT(prefixSnapshot["stage_graph"].IsNull());
        FindNode(prefixSnapshot, "project");
    }

    Y_UNIT_TEST(RuleApplicationPrefixDeliveryFailureDoesNotEscapeOrLoseCatalog) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TThrowOnceSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        capture.CaptureRuleApplicationPrefix(
            root,
            ctx.RboCtx,
            TVector<TRBORuleApplicationV1>{{1, "Stage", "Rule"}});

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(
            sink.Results[0].Boundary ==
            ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[0].RuleApplications.size(), 1);
        ParseSupported(sink.Results[0]);
    }

    Y_UNIT_TEST(UnsupportedRuleApplicationPrefixRetainsItsCompleteSequence) {
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
        capture.CaptureRuleApplicationPrefix(
            prefixRoot,
            ctx.RboCtx,
            TVector<TRBORuleApplicationV1>{{1, "Stage", "Rule"}});

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(!sink.Results[1].IsSupported());
        UNIT_ASSERT(sink.Results[1].Json.empty());
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].Ordinal, 1);
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
        ctx.RboCtx.RuleApplicationDebug.Reset(2);
        UNIT_ASSERT(!ctx.RboCtx.RuleApplicationDebug.OnApplied("Stage", "Rule"));

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
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results[1].RuleApplications[0].Ordinal, 1);
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
