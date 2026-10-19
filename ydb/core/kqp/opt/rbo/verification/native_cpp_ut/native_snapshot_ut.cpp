#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/shellcommand.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

struct TFixture {
    TFixture()
        : Functions(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , Query(MakeIntrusive<TKikimrQueryContext>(
              Functions.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , Request(MakeIntrusive<TUserRequestContext>())
        , Optimize("ut", Config, Query, Tables, Request)
        , Rbo(Optimize, Expr, Types, TypeAnnotation, *Functions)
    {
        auto& table = Tables->GetOrAddTable("ut", "/Root", "/Root/NativeSnapshot");
        table.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot");
        table.Metadata->DoesExist = true;
        table.Metadata->PathId = TKikimrPathId(1, 1);
        table.Metadata->SchemaVersion = 1;
        table.Metadata->Kind = EKikimrTableKind::Datashard;
        table.Metadata->KeyColumnNames = {"k"};
        table.Metadata->Columns.emplace("k", TKikimrColumnMetadata("k", 1, "Int32", true));
        table.Metadata->Columns.emplace("x", TKikimrColumnMetadata("x", 2, "Int32", true));
        table.Metadata->Columns.emplace("g", TKikimrColumnMetadata("g", 3, "String", false));
        TKikimrColumnMetadata price("price", 4, "Decimal", false);
        price.TypeInfo = NScheme::TTypeInfo(NScheme::TDecimalType(15, 2));
        table.Metadata->Columns.emplace("price", std::move(price));
        table.Metadata->ColumnOrder = {"k", "x", "g", "price"};
        UNIT_ASSERT(table.Load(Expr));
        Table = &table;
        Key = Registry.Add(TInfoUnit("same", "k"));
        Other = Registry.Add(TInfoUnit("same", "x"));
        Group = Registry.Add(TInfoUnit("same", "g"));
        Price = Registry.Add(TInfoUnit("same", "price"));
    }

    TIntrusivePtr<TOpRead> Read() {
        auto read = MakeIntrusive<TOpRead>(
            "same", TUnorderedIUs{Key, Other}, NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, TPositionHandle(), Expr).Ptr(),
            nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None,
            TPhysicalOpProps{}, TPositionHandle());
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        SetType(*read, {{Key, int32}, {Other, int32}});
        return read;
    }

    void SetType(IOperator& op, const TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>>& columns) {
        TVector<const TItemExprType*> items;
        for (const auto& [id, type] : columns) {
            items.push_back(Expr.MakeType<TItemExprType>(ToString(id), type));
        }
        op.Type = Expr.MakeType<TListExprType>(Expr.MakeType<TStructExprType>(std::move(items)));
    }

    TSemanticSnapshotExportResult Export(TOpRoot& root) {
        root.PlanProps.InfoUnitRegistry = Registry;
        return ExportSemanticSnapshotV1(root, Rbo);
    }

    TIntrusivePtr<TOpWindow> Window(const TString& function, bool wholePartition) {
        const auto* decimal = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "35", "2"));
        // Leave accumulator headroom at the two-row proof bound. Full-width
        // Decimal sources intentionally fail the model's overflow admission.
        const auto* priceType = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "15", "2"));
        const auto* groupType = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String));
        auto read = MakeIntrusive<TOpRead>(
            "same", TUnorderedIUs{Key, Group, Price}, NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, TPositionHandle(), Expr).Ptr(),
            nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None,
            TPhysicalOpProps{}, TPositionHandle());
        SetType(*read, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Price, priceType}});
        Sum = Registry.Add(TInfoUnit("", "total"));
        WindowResult = Registry.Add(TInfoUnit("", "total"));
        auto aggregate = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{Sum, {Price, "sum", false, false}}},
            TOrderedIUs<>{Group, Key}, EOpPhase::Undefined, false, TPositionHandle());
        SetType(*aggregate, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Sum, decimal}});
        TOpWindowFrame frame;
        frame.EndKind = wholePartition ? EWindowFrameBound::UnboundedFollowing : EWindowFrameBound::CurrentRow;
        auto window = MakeIntrusive<TOpWindow>(aggregate, TPositionHandle(),
            TWindowIUs{{WindowResult, {function, EWindowFuncKind::Aggregate, TOrderedIUs<>{Sum}}}},
            TOrderedIUs<>{Group}, TSortIUs{}, frame);
        SetType(*window, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Sum, decimal}, {WindowResult, decimal}});
        return window;
    }

    TIntrusivePtr<TOpMap> StringConcat(bool copyColumn, TInfoUnitId& result) {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* string = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String);
        const auto* optionalString = Expr.MakeType<TOptionalExprType>(string);
        auto read = Read();
        read->GetColumns() = TUnorderedIUs{Key, Group};
        TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> types{{Key, int32}, {Group, optionalString}};
        SetType(*read, types);
        TIntrusivePtr<IOperator> input = read;
        auto source = Group;
        if (copyColumn) {
            source = Registry.Add(TInfoUnit("", "copy"));
            auto access = MakeColumnAccess(Group, pos, &Expr, &ExpressionProps);
            access.GetExpressionBody()->SetTypeAnn(optionalString);
            auto copy = MakeIntrusive<TOpMap>(input, pos, TMapIUs{{source, TMapElement(access)}});
            types.emplace_back(source, optionalString);
            SetType(*copy, types);
            input = copy;
        }
        auto row = Expr.NewArgument(pos, "row");
        row->SetTypeAnn(input->Type->Cast<TListExprType>()->GetItemType());
        auto member = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(source))});
        member->SetTypeAnn(optionalString);
        const auto literal = [&](TStringBuf value) {
            auto node = Expr.NewCallable(pos, "String", {Expr.NewAtom(pos, value)});
            node->SetTypeAnn(string);
            return node;
        };
        auto coalesce = Expr.NewCallable(pos, "Coalesce", {member, literal("")});
        coalesce->SetTypeAnn(string);
        auto concat = Expr.NewCallable(pos, "Concat", {coalesce, literal(":")});
        concat->SetTypeAnn(string);
        auto lambda = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(concat));
        result = Registry.Add(TInfoUnit("", "answer"));
        auto map = MakeIntrusive<TOpMap>(input, pos, TMapIUs{{result,
            TMapElement(TExpression(lambda, &Expr, &ExpressionProps))}});
        types.emplace_back(result, string);
        SetType(*map, types);
        return map;
    }

    void StageSerial(TOpRoot& root) {
        TVector<IOperator*> compute;
        auto* input = root.GetInput().Get();
        while (input->GetKind() != EOperator::Source) {
            UNIT_ASSERT_VALUES_EQUAL(input->GetChildCount(), 1);
            compute.push_back(input);
            input = input->GetChild(0).Get();
        }
        UNIT_ASSERT(!compute.empty());
        auto& graph = root.PlanProps.StageGraph;
        const auto source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const auto sink = graph.AddStage();
        input->Props.StageId = source;
        for (auto* op : compute) {
            op->Props.StageId = sink;
        }
        graph.Connect(source, sink, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(source)));
    }

    TSemanticSnapshotExportResult CorrelatedExists(
        TInfoUnitId captured, bool wrongType = false, bool withResidual = false)
    {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* boolean = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
        const auto innerKey = Registry.Add(TInfoUnit("inner", "k"));
        const auto local = Registry.Add(TInfoUnit("", "capture"));
        const auto binding = Registry.Add(TInfoUnit("", "exists"));
        auto inner = MakeIntrusive<TOpRead>("inner", TUnorderedIUs{innerKey},
            NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, pos, Expr).Ptr(), nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
        SetType(*inner, {{innerKey, int32}});
        auto capture = MakeIntrusive<TOpAddDependencies>(inner, pos,
            TDependencyIUs{{local, {captured, wrongType ? boolean : int32}}});
        SetType(*capture, {{innerKey, int32}, {local, int32}});
        auto row = Expr.NewArgument(pos, "row");
        row->SetTypeAnn(capture->Type->Cast<TListExprType>()->GetItemType());
        auto innerMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(innerKey))});
        auto localMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(local))});
        innerMember->SetTypeAnn(int32);
        localMember->SetTypeAnn(int32);
        auto equals = Expr.NewCallable(pos, "==", {innerMember, localMember});
        equals->SetTypeAnn(boolean);
        const auto typedComparison = equals;
        if (withResidual) {
            auto residual = Expr.NewCallable(pos, "Bool", {Expr.NewAtom(pos, "true")});
            residual->SetTypeAnn(boolean);
            equals = Expr.NewCallable(pos, "And", {equals, residual});
            equals->SetTypeAnn(boolean);
        }
        auto predicate = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(equals));
        auto filtered = MakeIntrusive<TOpFilter>(capture, pos,
            TExpression(predicate, &Expr, &ExpressionProps));
        SetType(*filtered, {{innerKey, int32}, {local, int32}});
        auto outer = Read();
        auto call = MakeColumnAccess(binding, pos, &Expr, &ExpressionProps);
        call.GetExpressionBody()->SetTypeAnn(boolean);
        auto consumer = MakeIntrusive<TOpFilter>(outer, pos, call);
        SetType(*consumer, {{Key, int32}, {Other, int32}});
        TOpRoot root(consumer, pos, {{Key, "key"}});
        root.PlanProps.Subplans.Add(binding, filtered, ESubplanType::EXISTS);
        root.PlanProps.Subplans.RefreshDependencies(binding);
        filtered->BindExpressionPlanProps(&root.PlanProps);
        consumer->BindExpressionPlanProps(&root.PlanProps);
        auto exported = Export(root);
        UNIT_ASSERT_VALUES_EQUAL(localMember->Child(1)->Content(), ToString(local));
        UNIT_ASSERT(typedComparison->GetTypeAnn() == boolean);
        UNIT_ASSERT(localMember->GetTypeAnn() == int32);
        return exported;
    }

    TExprContext Expr;
    TTypeAnnotationContext Types;
    TNullTransformer TypeAnnotation;
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> Functions;
    TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<TKikimrQueryContext> Query;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> Request;
    NOpt::TKqpOptimizeContext Optimize;
    TRBOContext Rbo;
    TInfoUnitRegistry Registry;
    TPlanProps ExpressionProps;
    const TKikimrTableDescription* Table = nullptr;
    TInfoUnitId Key;
    TInfoUnitId Other;
    TInfoUnitId Group;
    TInfoUnitId Price;
    TInfoUnitId Sum = TUnorderedIUs::InvalidBit;
    TInfoUnitId WindowResult = TUnorderedIUs::InvalidBit;
};

NJson::TJsonValue Parse(const TSemanticSnapshotExportResult& exported) {
    UNIT_ASSERT_C(exported.IsSupported(), exported.UnsupportedReason);
    NJson::TJsonValue result;
    UNIT_ASSERT(NJson::ReadJsonTree(exported.Json, &result, true));
    return result;
}

TString Verify(const TString& before, const TString& after, bool diagnosticPrefix = false) {
    TTempDir directory;
    const auto beforePath = directory.Path() / "before.json";
    const auto afterPath = directory.Path() / "after.json";
    TFileOutput(beforePath.GetPath()).Write(before);
    TFileOutput(afterPath.GetPath()).Write(after);
    TShellCommand command(BinaryPath("ydb/core/kqp/opt/rbo/verification/bin/kqp_rbo_verify"));
    command << beforePath.GetPath() << afterPath.GetPath()
        << "--rows" << "2" << "--timeout-ms" << "10000"
        << "--solver" << BinaryPath("contrib/tools/z3/z3");
    if (diagnosticPrefix) {
        command << "--diagnostic-transformation-prefix";
    }
    command.Run();
    NJson::TJsonValue verdict;
    UNIT_ASSERT_C(NJson::ReadJsonTree(command.GetOutput(), &verdict, true),
        command.GetError() << command.GetOutput());
    if (diagnosticPrefix) {
        UNIT_ASSERT_VALUES_EQUAL(verdict["comparison_scope"].GetStringSafe(), "OPTIMIZER_TRANSFORMATION_PREFIX");
    }
    const TString status = verdict["status"].GetStringSafe();
    UNIT_ASSERT(command.GetExitCode().Defined());
    UNIT_ASSERT_VALUES_EQUAL_C(command.GetExitCode().GetRef(), status == "VERIFIED_BOUNDED" ? 0 : 1,
        command.GetError() << command.GetOutput());
    return status;
}

TSemanticSnapshotExportResult StagedRead(ui32 firstStage) {
    TFixture fixture;
    auto read = fixture.Read();
    TOpRoot root(read, TPositionHandle(), {{fixture.Key, "key"}});
    auto& graph = root.PlanProps.StageGraph;
    graph.StageCounter = firstStage;
    read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
    auto exported = fixture.Export(root);
    UNIT_ASSERT_VALUES_EQUAL(graph.StageIds.front(), firstStage);
    UNIT_ASSERT_VALUES_EQUAL(*read->Props.StageId, firstStage);
    return exported;
}

} // namespace

Y_UNIT_TEST_SUITE(TNativeSemanticSnapshot) {
    Y_UNIT_TEST(ResultProjectionRetainsRepeatedBindingsAndLabels) {
        TFixture fixture;
        TOpRoot root(fixture.Read(), TPositionHandle(), {
            {fixture.Other, "duplicate"}, {fixture.Key, "duplicate"}, {fixture.Key, "last"}});
        const auto snapshot = Parse(fixture.Export(root));
        UNIT_ASSERT_VALUES_EQUAL(snapshot["plan"]["root"].GetStringSafe(), "result");
        const auto& projection = snapshot["plan"]["nodes"].GetArraySafe().back();
        const auto& columns = projection["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["output"].GetStringSafe(), "result[0]:duplicate");
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["output"].GetStringSafe(), "result[1]:duplicate");
        UNIT_ASSERT_VALUES_EQUAL(columns[2]["output"].GetStringSafe(), "result[2]:last");
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["expression"]["column"].GetStringSafe(), ToString(fixture.Other));
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["expression"]["column"].GetStringSafe(), ToString(fixture.Key));
        UNIT_ASSERT_VALUES_EQUAL(columns[2]["expression"]["column"].GetStringSafe(), ToString(fixture.Key));
    }

    Y_UNIT_TEST(StageNumberingIsLocalAndDoesNotMutateThePlan) {
        const auto zero = StagedRead(0);
        const auto offset = StagedRead(37);
        Parse(zero);
        const auto snapshot = Parse(offset);
        UNIT_ASSERT_VALUES_EQUAL(zero.Json, offset.Json);
        UNIT_ASSERT_VALUES_EQUAL(snapshot["stage_graph"]["root_stage"].GetStringSafe(), "s0");
    }

    Y_UNIT_TEST(UnknownStageReferencesAreRejected) {
        TFixture fixture;
        auto read = fixture.Read();
        TOpRoot root(read, TPositionHandle(), {{fixture.Key, "key"}});
        auto& graph = root.PlanProps.StageGraph;
        read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        graph.StageInputs.emplace(900, TVector<ui32>{});
        const auto exported = fixture.Export(root);
        UNIT_ASSERT(!exported.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "unknown stage");
    }

    Y_UNIT_TEST(RowSourceProofObservesEveryResultPosition) {
        TFixture fixture;
        auto read = fixture.Read();
        TOpRoot initial(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Key, "second"}});
        const auto before = fixture.Export(initial);
        Parse(before);
        TOpRoot final(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Key, "second"}});
        auto& graph = final.PlanProps.StageGraph;
        graph.StageCounter = 37;
        read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const auto after = fixture.Export(final);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        TOpRoot changed(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Other, "second"}});
        changed.PlanProps.StageGraph = graph;
        const auto mutation = fixture.Export(changed);
        Parse(mutation);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, mutation.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeWholePartitionDecimalWindowsUseTheExistingModel) {
        for (const TString& function : {TString("sum"), TString("avg")}) {
            TFixture fixture;
            auto window = fixture.Window(function, true);
            TOpRoot root(window, TPositionHandle(), {{fixture.WindowResult, "answer"}});
            const auto exported = fixture.Export(root);
            const auto snapshot = Parse(exported);
            size_t matches = 0;
            for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
                if (node["op"].GetStringSafe() != "project") {
                    continue;
                }
                for (const auto& column : node["columns"].GetArraySafe()) {
                    if (column["expression"]["kind"].GetStringSafe() == "window_" + function) {
                        ++matches;
                        UNIT_ASSERT_VALUES_EQUAL(column["expression"]["input"].GetStringSafe(), ToString(fixture.Sum));
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(matches, 1);
            fixture.StageSerial(root);
            const auto staged = fixture.Export(root);
            Parse(staged);
            UNIT_ASSERT_VALUES_EQUAL(Verify(exported.Json, staged.Json), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeWindowProofDetectsSumChangedToAverage) {
        TFixture sumFixture;
        auto sum = sumFixture.Window("sum", true);
        TOpRoot before(sum, TPositionHandle(), {{sumFixture.WindowResult, "answer"}});
        const auto initial = sumFixture.Export(before);
        Parse(initial);
        TFixture averageFixture;
        auto average = averageFixture.Window("avg", true);
        TOpRoot after(average, TPositionHandle(), {{averageFixture.WindowResult, "answer"}});
        averageFixture.StageSerial(after);
        const auto changed = averageFixture.Export(after);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeColumnCopiesRetainStoredStringProvenance) {
        TFixture directFixture;
        TInfoUnitId directResult;
        auto direct = directFixture.StringConcat(false, directResult);
        TOpRoot before(direct, TPositionHandle(), {{directResult, "answer"}});
        const auto initial = directFixture.Export(before);
        Parse(initial);
        TFixture copyFixture;
        TInfoUnitId copiedResult;
        auto copied = copyFixture.StringConcat(true, copiedResult);
        TOpRoot after(copied, TPositionHandle(), {{copiedResult, "answer"}});
        copyFixture.StageSerial(after);
        const auto final = copyFixture.Export(after);
        Parse(final);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeCorrelatedExistsUsesTheCallerBinding) {
        TFixture fixture;
        const auto exported = fixture.CorrelatedExists(fixture.Key);
        const auto snapshot = Parse(exported);
        const auto& subplan = snapshot["plan"]["subplans"].GetArraySafe().front();
        UNIT_ASSERT_VALUES_EQUAL(subplan["dependencies"][0].GetStringSafe(), ToString(fixture.Key));
        UNIT_ASSERT_VALUES_EQUAL(subplan["predicate"]["right"]["column"].GetStringSafe(), ToString(fixture.Key));
    }

    Y_UNIT_TEST(NativeCorrelatedExistsDetectsAChangedCallerBinding) {
        TFixture keyFixture;
        const auto before = keyFixture.CorrelatedExists(keyFixture.Key);
        Parse(before);
        TFixture otherFixture;
        const auto after = otherFixture.CorrelatedExists(otherFixture.Other);
        Parse(after);
        // Both boundaries are logical: explicitly exercise the diagnostic
        // prefix theorem, without relaxing the normal Final-stage requirement.
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, before.Json, true), "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, true), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeCorrelationAuditPreservesTypedConjuncts) {
        TFixture fixture;
        const auto plain = fixture.CorrelatedExists(fixture.Key);
        Parse(plain);
        TFixture residualFixture;
        const auto conjunction = residualFixture.CorrelatedExists(residualFixture.Key, false, true);
        Parse(conjunction);
        UNIT_ASSERT_VALUES_EQUAL(Verify(plain.Json, conjunction.Json, true), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeCorrelatedExistsRejectsInconsistentCaptureTypes) {
        TFixture fixture;
        const auto exported = fixture.CorrelatedExists(fixture.Key, true);
        UNIT_ASSERT(!exported.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "type");
    }

    Y_UNIT_TEST(NativeWindowRejectsUnmodeledFramesAndFunctions) {
        for (const auto& [function, whole] : TVector<std::pair<TString, bool>>{{"sum", false}, {"count", true}}) {
            TFixture fixture;
            auto window = fixture.Window(function, whole);
            TOpRoot root(window, TPositionHandle(), {{fixture.WindowResult, "answer"}});
            const auto exported = fixture.Export(root);
            UNIT_ASSERT(!exported.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "Native Window");
        }
    }
}

} // namespace NKikimr::NKqp
