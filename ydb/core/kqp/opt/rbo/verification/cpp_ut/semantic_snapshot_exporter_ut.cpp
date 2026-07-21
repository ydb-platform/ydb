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
    const TVector<TString>& columns)
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
        NYql::EStorageType::RowStorage,
        NOpt::BuildTableMeta(table, pos, ctx.ExprCtx).Ptr(),
        nullptr,
        nullptr,
        std::nullopt,
        std::nullopt,
        ESortDir::None,
        TPhysicalOpProps{},
        pos);
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

Y_UNIT_TEST_SUITE(TSemanticSnapshotExporter) {
    Y_UNIT_TEST(OutputIsDeterministicAcrossEquivalentAllocations) {
        UNIT_ASSERT_VALUES_EQUAL(ExportDeterministicPlan(), ExportDeterministicPlan());
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

        const auto& project = FindNode(snapshot, "project");
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

    Y_UNIT_TEST(UnsupportedOperatorFailsClosed) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        const auto pos = TPositionHandle();
        auto limit = MakeIntrusive<TOpLimit>(
            read,
            pos,
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot root(limit, pos, {"a.k"});

        const auto result = ExportSemanticSnapshotV1(root, ctx.RboCtx);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT(result.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "Limit");
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
        auto replacement = MakeIntrusive<TOpMap>(
            MakeIntrusive<TOpEmptySource>(pos),
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("a.k"),
                MakeConstant("Int32", "0", pos, &ctx.ExprCtx))});
        TOpRoot finalRoot(replacement, pos, {"a.k"});
        capture.CaptureFinal(finalRoot, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Initial);
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);

        const auto initialSnapshot = ParseSupported(sink.Results[0]);
        const auto finalSnapshot = ParseSupported(sink.Results[1]);
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
            MakeConstant("Uint64", "1", pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TOpRoot finalRoot(limit, pos, {"a.k"});
        capture.CaptureFinal(finalRoot, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT(sink.Results[0].IsSupported());
        UNIT_ASSERT(
            sink.Results[1].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        UNIT_ASSERT(!sink.Results[1].IsSupported());
        UNIT_ASSERT(sink.Results[1].Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(sink.Results[1].UnsupportedReason, "Limit");
    }

    Y_UNIT_TEST(SinkFailureDoesNotDiscardTheSharedCatalog) {
        TExportTestContext ctx;
        const auto& table = AddTable(ctx, "/Root/A", {{"k", "Int32", true}});
        auto read = MakeRead(ctx, table, "a", {"k"});
        TOpRoot root(read, TPositionHandle(), {"a.k"});

        TThrowOnceSemanticSnapshotSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, ctx.RboCtx);
        capture.CaptureFinal(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT(
            sink.Results[0].Boundary == ERBOSemanticSnapshotBoundaryV1::Final);
        ParseSupported(sink.Results[0]);
    }
}

} // namespace
