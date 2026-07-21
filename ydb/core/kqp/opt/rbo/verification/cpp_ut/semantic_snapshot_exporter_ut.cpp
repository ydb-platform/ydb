#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

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
        table.Metadata->Columns.emplace(
            column.Name,
            TKikimrColumnMetadata(column.Name, id++, column.Type, column.NotNull));
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
            {"payload", "Utf8", false},
        });
        auto read = MakeRead(ctx, table, "a", {"k", "payload"});
        SetOutputType(ctx, *read, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.payload", NUdf::EDataSlot::Utf8, true},
        });
        const auto pos = TPositionHandle();
        const TVector<TSortElement> order = {
            TSortElement(TInfoUnit("a.k"), true, false),
            TSortElement(TInfoUnit("a.payload"), false, true),
        };

        auto plainSort = MakeIntrusive<TOpSort>(read, pos, order);
        SetOutputType(ctx, *plainSort, {
            {"a.k", NUdf::EDataSlot::Int32},
            {"a.payload", NUdf::EDataSlot::Utf8, true},
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
                {"a.payload", NUdf::EDataSlot::Utf8, true},
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
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Unsupported scalar type Date");
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
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "pushdown or ordering semantics");
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
