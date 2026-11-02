#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_plan_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

size_t CountOperatorInTraversal(TOpRoot& root, const IOperator* expected) {
    size_t count = 0;
    for (const auto& item : root) {
        count += item.Current == expected;
    }
    return count;
}

TString LoadTPCHYqlQuery(ui32 queryId) {
    const TString toDecimal = R"($to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };)";
    const TString toDecimalMax = R"($to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };)";
    return toDecimal + "\n" + toDecimalMax + "\n"
        + GetFullPath(BenchmarkQueryPath[EBenchType::TPCH], ToString(queryId) + ".yql");
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(OperatorIteratorMovePreservesDeepTraversal) {
        const TPositionHandle pos;
        TIntrusivePtr<IOperator> op = MakeIntrusive<TOpEmptySource>(pos);
        for (size_t i = 0; i < 30; ++i) {
            op = MakeIntrusive<TOpJoin>(std::move(op), MakeIntrusive<TOpEmptySource>(pos), pos, "Cross", TPairedIUs{});
        }
        auto* expected = op.get();
        TOpRoot root(std::move(op), pos, {});
        auto source = root.begin();
        TOpIterator moved(std::move(source));
        UNIT_ASSERT(source == TOpEnd{});
        ++source;
        UNIT_ASSERT(source == TOpEnd{});
        auto assigned = root.begin();
        assigned = std::move(moved);
        UNIT_ASSERT(moved == TOpEnd{});
        ++moved;
        UNIT_ASSERT(moved == TOpEnd{});
        size_t count = 0;
        IOperator* last = nullptr;
        for (; assigned != TOpEnd{}; ++assigned) {
            last = assigned->Current;
            ++count;
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 61);
        UNIT_ASSERT_VALUES_EQUAL(last, expected);
    }

    Y_UNIT_TEST(MapUniqueRawInputIUsFollowExpressionMutations) {
        NTests::TIdTestContext f;
        const auto first = f.Id(), replacement = f.Id(), output = f.Id(), duplicate = f.Id(), again = f.Id();
        auto map = f.Copies(MakeIntrusive<TOpEmptySource>(f.Pos), {{output, first}, {duplicate, first}});
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{first});
        map->SetMapElementExpression(output, f.Column(replacement));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == (TUnorderedIUs{first, replacement}));
        map->RemoveMapElement(duplicate);
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{replacement});
        map->AddMapElement(again, TMapElement(f.Column(first)));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == (TUnorderedIUs{first, replacement}));
        TMapIUs definitions;
        definitions.Add(again, f.Column(first));
        map->SetMapElements(std::move(definitions));
        UNIT_ASSERT(map->GetUniqueRawInputIUs() == TUnorderedIUs{first});
    }

    Y_UNIT_TEST(MapElementClassifiesOnlyExactCopies) {
        NTests::TIdTestContext f;
        const auto source = f.Id();
        const TMapElement copy(f.Column(source));
        const TMapElement calculation(f.Constant());
        const TMapElement conversion(MakeUnaryCallable("Just", f.Column(source)));
        UNIT_ASSERT(copy.IsColumnAccess());
        UNIT_ASSERT_VALUES_EQUAL(copy.GetColumnAccess(), source);
        UNIT_ASSERT(!calculation.IsColumnAccess());
        UNIT_ASSERT(!conversion.IsColumnAccess());
    }

    Y_UNIT_TEST(SubplanRegistryMutationInvariants) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), dependency = f.Id(), local = f.Id(), secondLocal = f.Id(), missing = f.Id();
        auto& subplans = f.Props.Subplans;
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.Add(binding, {}, ESubplanType::EXISTS), yexception, "null subplan");
        subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS),
            yexception, "Duplicate subplan binding");
        TDependencyIUs captures;
        const auto* type = f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
        captures.Add(local, TCapturedIU{dependency, type});
        captures.Add(secondLocal, TCapturedIU{dependency, type});
        subplans.ReplacePlan(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)));
        subplans.RefreshDependencies(binding);
        UNIT_ASSERT(subplans.At(binding).DependentIUs == TUnorderedIUs{dependency});
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.ReplacePlan(binding, {}), yexception, "null subplan");
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.Remove(missing), yexception, "Unknown subplan binding");
    }

    Y_UNIT_TEST(SubplanBindingsCannotBeRenamed) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), replacement = f.Id();
        auto& subplans = f.Props.Subplans;
        subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.RenameExternalReferences({{binding, replacement}}),
            yexception, "cannot be rebound as parameters");
        UNIT_ASSERT_EXCEPTION_CONTAINS(subplans.RenameExternalReferences({{replacement, binding}}),
            yexception, "cannot be rebound as parameters");
    }

    Y_UNIT_TEST(ExpressionInputIUsFollowSubplanRegistryMutations) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), dependency = f.Id(), local = f.Id();
        auto expression = f.Column(binding);
        UNIT_ASSERT(expression.GetRawInputIUs() == TUnorderedIUs{binding});
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{binding});
        f.Props.Subplans.Add(binding, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        UNIT_ASSERT(expression.GetInputIUs(false, true).Empty());
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{dependency, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.ReplacePlan(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)));
        f.Props.Subplans.RefreshDependencies(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{dependency});
        f.Props.Subplans.Remove(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, true) == TUnorderedIUs{binding});
    }

    Y_UNIT_TEST(ExpressionInputIUBufferSupportsAllResolutionModes) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), tuple = f.Id(), dependency = f.Id(), local = f.Id();
        auto expression = f.Column(binding);
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{dependency, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.Add(binding, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)), ESubplanType::IN_SUBPLAN, {tuple}, local);
        f.Props.Subplans.RefreshDependencies(binding);
        UNIT_ASSERT(expression.GetInputIUs(false, false).Empty());
        UNIT_ASSERT(expression.GetInputIUs(true, false) == TUnorderedIUs{binding});
        UNIT_ASSERT(expression.GetInputIUs(false, true) == (TUnorderedIUs{tuple, dependency}));
        UNIT_ASSERT(expression.GetInputIUs(true, true) == (TUnorderedIUs{binding, tuple, dependency}));
        UNIT_ASSERT(expression.GetInputIUs(false, false).Empty());
    }

    Y_UNIT_TEST(SubplanTraversalFollowsRegistryMutations) {
        NTests::TIdTestContext f;
        const auto binding = f.Id(), replacement = f.Id();
        auto filter = MakeIntrusive<TOpFilter>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, f.Column(binding));
        auto* consumer = filter.get();
        filter->SetFilterExpression(f.Column(replacement));
        UNIT_ASSERT(filter->GetUniqueRawInputIUs() == TUnorderedIUs{replacement});
        filter->SetFilterExpression(f.Column(binding));
        auto root = f.Root(std::move(filter), {});
        auto& subplans = root->PlanProps.Subplans;
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans).Empty());
        auto original = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto substitute = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* originalPtr = original.get();
        auto* substitutePtr = substitute.get();
        subplans.Add(binding, std::move(original), ESubplanType::EXISTS);
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans) == TUnorderedIUs{binding});
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, originalPtr), 1);
        subplans.ReplacePlan(binding, std::move(substitute));
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, originalPtr), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, substitutePtr), 1);
        subplans.Remove(binding);
        UNIT_ASSERT(consumer->GetSubplanIUs(subplans).Empty());
        UNIT_ASSERT_VALUES_EQUAL(CountOperatorInTraversal(*root, substitutePtr), 0);
    }

    Y_UNIT_TEST(SubplanTraversalResumesRawIUResolutionAfterMove) {
        NTests::TIdTestContext f;
        const auto first = f.Id(), second = f.Id();
        auto firstPlan = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto secondPlan = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* firstPtr = firstPlan.get();
        auto* secondPtr = secondPlan.get();
        f.Props.Subplans.Add(first, std::move(firstPlan), ESubplanType::EXISTS);
        f.Props.Subplans.Add(second, std::move(secondPlan), ESubplanType::EXISTS);
        auto root = f.Root(MakeIntrusive<TOpFilter>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos,
            MakeBinaryPredicate("And", f.Column(first), f.Column(second))), {});
        auto iterator = root->begin();
        UNIT_ASSERT(iterator->Current == firstPtr);
        UNIT_ASSERT(iterator->SubplanIU && *iterator->SubplanIU == first);
        TOpIterator moved(std::move(iterator));
        UNIT_ASSERT(iterator == TOpEnd{});
        ++moved;
        UNIT_ASSERT(moved != TOpEnd{});
        UNIT_ASSERT(moved->Current == secondPtr);
        UNIT_ASSERT(moved->SubplanIU && *moved->SubplanIU == second);
    }

    Y_UNIT_TEST(ComputeParentsHandlesSharedDagAndIgnoresInactiveSubplans) {
        NTests::TIdTestContext f;
        auto hub = TReplicate::Create(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto* leftPtr = left.get();
        auto* rightPtr = right.get();
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "Cross", TPairedIUs{});
        auto* joinPtr = join.get();
        auto inactiveChild = MakeIntrusive<TOpEmptySource>(f.Pos);
        auto* inactivePtr = inactiveChild.get();
        f.Props.Subplans.Add(f.Id(), MakeIntrusive<TOpJoin>(std::move(inactiveChild),
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, "Cross", TPairedIUs{}), ESubplanType::EXISTS);
        auto root = f.Root(std::move(join), {});
        root->ComputeParents();
        UNIT_ASSERT_VALUES_EQUAL(joinPtr->Parents.size(), 1);
        UNIT_ASSERT(joinPtr->Parents[0].first == root.get());
        UNIT_ASSERT_VALUES_EQUAL(hub->GetOutputs().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(hub->GetInput()->Parents.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(leftPtr->Parents[0].first, joinPtr);
        UNIT_ASSERT_VALUES_EQUAL(leftPtr->Parents[0].second, 0);
        UNIT_ASSERT_VALUES_EQUAL(rightPtr->Parents[0].first, joinPtr);
        UNIT_ASSERT_VALUES_EQUAL(rightPtr->Parents[0].second, 1);
        UNIT_ASSERT(inactivePtr->Parents.empty());
        UNIT_ASSERT(&leftPtr->GetChild(0) == &rightPtr->GetChild(0));
        size_t visits = 0;
        for (const auto& item : *root) {
            visits += item.Current == hub->GetInput().Get();
        }
        UNIT_ASSERT_VALUES_EQUAL(visits, 1);
        auto replacement = MakeIntrusive<TOpEmptySource>(f.Pos);
        leftPtr->SetChild(0, replacement);
        UNIT_ASSERT(rightPtr->GetChild(0) == replacement);
        UNIT_ASSERT(hub->GetInput() == replacement);
        root->ComputeParents();
        UNIT_ASSERT_VALUES_EQUAL(replacement->Parents.size(), 2);
    }

    Y_UNIT_TEST(ProjectedKqpOpMapPreservesAvailableInputIds) {
        NTests::TIdTestContext f;
        auto& ctx = f.ExprCtx;
        const auto empty = ctx.NewCallable(f.Pos, "KqpOpEmptySource", {});
        auto constant = f.Constant().Node;
        constant->TailPtr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        const auto first = ctx.NewCallable(f.Pos, "KqpOpMap", {empty, ctx.NewList(f.Pos, {
            ctx.NewCallable(f.Pos, "KqpOpMapElementLambda", {empty, ctx.NewAtom(f.Pos, "payload"), constant, ctx.NewAtom(f.Pos, "false")}),
            ctx.NewCallable(f.Pos, "KqpOpMapElementLambda", {empty, ctx.NewAtom(f.Pos, "unmentioned"), constant, ctx.NewAtom(f.Pos, "false")})
        })});
        auto project = [&](TExprNode::TPtr input, TStringBuf from, TStringBuf to) {
            return ctx.NewCallable(f.Pos, "KqpOpMap", {input, ctx.NewList(f.Pos, {
                ctx.NewCallable(f.Pos, "KqpOpMapElementRename", {input, ctx.NewAtom(f.Pos, to), ctx.NewAtom(f.Pos, from)})
            }), ctx.NewAtom(f.Pos, "true")});
        };
        PlanConverter converter(f.TypeCtx, ctx);
        auto plan = converter.ExprNodeToOperator(project(project(first, "payload", "projected"), "projected", "out"));
        auto& outer = CastOperator<TOpMap>(*plan);
        auto& inner = CastOperator<TOpMap>(*outer.GetInput());
        auto& initial = CastOperator<TOpMap>(*inner.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(initial.GetOutputIUs().Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(inner.GetOutputIUs().Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(outer.GetOutputIUs().Size(), 4);
        UNIT_ASSERT(initial.GetOutputIUs().IsSubsetOf(outer.GetOutputIUs()));
        const auto outerId = *outer.GetMapElements().Keys().begin();
        const auto innerId = *inner.GetMapElements().Keys().begin();
        UNIT_ASSERT_VALUES_EQUAL(outer.GetMapElements().Find(outerId)->GetColumnAccess(), innerId);
        UNIT_ASSERT_VALUES_EQUAL(converter.PlanProps.InfoUnitRegistry.Get(outerId).GetFullName(), "out");
    }

    Y_UNIT_TEST(ReplaceAliasSubqueryDoesNotDuplicateVisibleColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/test` (
                D Utf8 NOT NULL,
                S Utf8 NOT NULL,
                V Utf8,
                PRIMARY KEY(D, S)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';

            SELECT t1.S AS result
            FROM (
                SELECT D, S, V
                FROM `/Root/test`
            ) AS t1
            WHERE t1.D = 'd'
            ORDER BY result;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DistinctAllTypeMatchesLogicalOutputColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/dups` (
                id Int64 NOT NULL,
                k Int64 NOT NULL,
                v Int64 NOT NULL,
                PRIMARY KEY(id)
            ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(1)
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(100)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(2)
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(100)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(3)
            .AddMember("k").Int64(10)
            .AddMember("v").Int64(101)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").Int64(4)
            .AddMember("k").Int64(20)
            .AddMember("v").Int64(200)
            .EndStruct();
        rows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/dups", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';

            SELECT d.k
            FROM (
                SELECT DISTINCT k, v
                FROM `/Root/dups`
            ) AS d
            ORDER BY d.k;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings())
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[10];[10];[20]])");
    }

    Y_UNIT_TEST(TPCH_YQL_CBO4_ColumnLineageMapping) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.SetKqpSettings({MakeTPCHStatsSetting()});
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], /*useColumnStore*/ true);

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        for (const ui32 queryId : {2, 3, 10}) {
            auto result = querySession.ExecuteQuery(LoadTPCHYqlQuery(queryId),
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
            ).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), "Expected TPCH q" << queryId << " explain to succeed: " << result.GetIssues().ToString());
        }
    }

    

    Y_UNIT_TEST(MapAliasCleanupSemanticRenameAndDeadSortKey) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], /*useColumnStore*/ true);

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $cte = (
                SELECT a1.id2 AS id2, a1.join_id AS join_id
                FROM (
                    SELECT c.c_custkey AS id2, c.c_nationkey AS join_id
                    FROM `/Root/customer` AS c
                ) AS a1
            );

            $joined = (
                SELECT X1.id2 AS left_id, X2.id2 AS right_id, X1.id2 + X2.id2 AS sort_key
                FROM
                   (
                       SELECT cte.id2 AS id2
                       FROM `/Root/supplier` AS supplier, $cte AS cte
                       WHERE supplier.s_nationkey == cte.join_id
                   ) AS X1,
                   (
                       SELECT cte.id2 AS id2
                       FROM `/Root/nation` AS nation, $cte AS cte
                       WHERE nation.n_nationkey == cte.join_id
                   ) AS X2
            );

            SELECT left_id, right_id
            FROM $joined
            ORDER BY sort_key
            LIMIT 10;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "TopSort") || FindOperatorByStringField(simplifiedPlan, "Name", "Sort"), plan);
        UNIT_ASSERT_C(plan.Contains("Join"), plan);
    }

    Y_UNIT_TEST(MapMetadataAliasFanout) {
        NTests::TIdTestContext f;
        const auto id = f.Id("id"), payload = f.Id("payload"), alias = f.Id("id_alias");
        auto read = f.Read({id, payload});
        read->Props.Metadata.emplace();
        read->Props.Metadata->KeyColumns = {id};
        read->Props.Metadata->ShuffledByColumns = {id};
        auto map = f.Copies(std::move(read), {{alias, id}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(map->Props.Metadata);
        UNIT_ASSERT(map->Props.Metadata->KeyColumns == TOrderedIUs<>{id});
        UNIT_ASSERT(map->Props.Metadata->ShuffledByColumns == TOrderedIUs<>{id});
    }

    Y_UNIT_TEST(AggregateSeparatesSourceStatisticsAndHintIdentityFromProvenance) {
        NTests::TIdTestContext f;
        const auto key = f.Id("key"), copy = f.Id("copy");
        auto read = f.Read({key});
        read->Props.Metadata.emplace();
        read->Props.Metadata->SourceStatsColumns.Add(key);
        auto& lineage = f.Props.ColumnLineage;
        const auto relation = lineage.AddRelation("t", "/Root/table");
        lineage.Add(key, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        read->Props.Metadata->HintRelations.Add(key, relation);
        using TStatsMap = NYql::TOptimizerStatistics::TColumnStatMap;
        f.TypeCtx.ColumnStatisticsByTableName["/Root/table"] = MakeIntrusive<TStatsMap>(
            THashMap<TString, NYql::TColumnStatistics>{{"key", NYql::TColumnStatistics{}}});
        UNIT_ASSERT(BuildOptimizerStatistics(*read, lineage, false, f.TypeCtx).ColumnStatistics);

        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(read), TAggregationIUs{},
            TOrderedIUs<>{key}, EOpPhase::Undefined, false, f.Pos);
        aggregate->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT_VALUES_EQUAL(lineage.Find(key)->TableName, "/Root/table");
        UNIT_ASSERT(aggregate->Props.Metadata->SourceStatsColumns.Empty());
        UNIT_ASSERT(!BuildOptimizerStatistics(*aggregate, lineage, false, f.TypeCtx).ColumnStatistics);
        UNIT_ASSERT(lineage.GetAliases(aggregate->Props.Metadata->HintRelations) == TVector<TString>{"_aggregate"});

        auto map = f.Copies(std::move(aggregate), {{copy, key}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT_VALUES_EQUAL(lineage.Find(copy)->TableName, "/Root/table");
        UNIT_ASSERT(map->Props.Metadata->SourceStatsColumns.Empty());
        auto hub = TReplicate::Create(std::move(map), f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        second->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(second->Props.Metadata->SourceStatsColumns.Empty());
        UNIT_ASSERT(!BuildOptimizerStatistics(*second, lineage, false, f.TypeCtx).ColumnStatistics);
        UNIT_ASSERT(lineage.GetAliases(second->Props.Metadata->HintRelations) == TVector<TString>{"_aggregate"});
    }

    Y_UNIT_TEST(CopiesAndReplicatePortsTranslateSourceStatisticsEligibility) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), copy = f.Id();
        auto read = f.Read({source});
        read->Props.Metadata.emplace();
        read->Props.Metadata->SourceStatsColumns.Add(source);
        const auto relation = f.Props.ColumnLineage.AddRelation("t", "/Root/table");
        read->Props.Metadata->HintRelations.Add(source, relation);
        f.Props.ColumnLineage.Add(source, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        auto map = f.Copies(std::move(read), {{copy, source}});
        map->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(map->Props.Metadata->SourceStatsColumns == (TUnorderedIUs{source, copy}));
        auto hub = TReplicate::Create(std::move(map), f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        second->ComputeMetadata(f.RboCtx, f.Props);
        UNIT_ASSERT(second->Props.Metadata->SourceStatsColumns == second->GetOutputIUs());
        UNIT_ASSERT(!second->Props.Metadata->SourceStatsColumns.HasAny({source, copy}));
    }

    Y_UNIT_TEST(StatisticsBuilderRetainsEmptyTableBoundaryForMixedOutputs) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), grouped = f.Id();
        auto boundary = f.Read({source, grouped});
        boundary->Props.Metadata.emplace();
        boundary->Props.Metadata->SourceStatsColumns.Add(source);
        const auto relation = f.Props.ColumnLineage.AddRelation("t", "/Root/table");
        for (const auto id : {source, grouped}) {
            f.Props.ColumnLineage.Add(id, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        }
        using TStatsMap = NYql::TOptimizerStatistics::TColumnStatMap;
        f.TypeCtx.ColumnStatisticsByTableName["/Root/table"] = MakeIntrusive<TStatsMap>(
            THashMap<TString, NYql::TColumnStatistics>{{"key", NYql::TColumnStatistics{}}});
        UNIT_ASSERT(!BuildOptimizerStatistics(*boundary, f.Props.ColumnLineage, false, f.TypeCtx).ColumnStatistics);
    }

    Y_UNIT_TEST(CBOTreeRecomputesPackedOutputIUs) {
        NTests::TIdTestContext f;
        const auto left = f.Id(), stale = f.Id(), current = f.Id();
        auto join = MakeIntrusive<TOpJoin>(f.Read({left}), f.Read({stale}), f.Pos, "Inner", TPairedIUs{});
        UNIT_ASSERT_VALUES_EQUAL(join->GetOutputIUs().Size(), 2);
        join->SetRightInput(f.Read({current}));
        auto tree = MakeIntrusive<TOpCBOTree>(std::move(join), f.Pos);
        auto* treePtr = tree.get();
        auto root = f.Root(std::move(tree), {{left, "left"}, {current, "current"}});
        UNIT_ASSERT(treePtr->GetOutputIUs() == (TUnorderedIUs{left, current}));
    }

    Y_UNIT_TEST(CopiedOperatorPropsDoNotReuseOutputIUs) {
        NTests::TIdTestContext f;
        const auto source = f.Id(), old = f.Id(), fresh = f.Id();
        TMapIUs oldDefinitions;
        oldDefinitions.Add(old, f.Constant());
        auto oldMap = MakeIntrusive<TOpMap>(f.Read({source}), f.Pos, std::move(oldDefinitions));
        UNIT_ASSERT_VALUES_EQUAL(oldMap->GetOutputIUs().Size(), 2);
        TMapIUs newDefinitions;
        newDefinitions.Add(fresh, f.Constant());
        auto newMap = MakeIntrusive<TOpMap>(oldMap->GetInput(), f.Pos, oldMap->Props, std::move(newDefinitions));
        UNIT_ASSERT(newMap->GetOutputIUs() == (TUnorderedIUs{source, fresh}));
    }

    Y_UNIT_TEST(MergeUnionAllFlattensLeftDeepChain) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), p = f.Id(), b = f.Id(), q = f.Id(), c = f.Id(), r = f.Id();
        const auto innerA = f.Id(), innerP = f.Id(), outA = f.Id(), outP = f.Id();
        auto first = f.Read({a, p}), second = f.Read({b, q}), third = f.Read({c, r});
        const TVector<IOperator*> leaves{first.get(), second.get(), third.get()};
        auto inner = f.Union(std::move(first), std::move(second), {{innerA, a, b}, {innerP, p, q}});
        auto outer = f.Union(std::move(inner), std::move(third), {{outA, innerA, c}, {outP, innerP, r}});
        auto* merged = outer.get();
        auto root = f.Root(std::move(outer), {{outA, "a"}, {outP, "payload"}});
        UNIT_ASSERT(TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_C(root->GetInput().Get() == merged, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(merged->GetChildCount(), 3);
        for (size_t i = 0; i < leaves.size(); ++i) {
            UNIT_ASSERT_C(merged->GetChild(i).Get() == leaves[i], root->PlanToString(f.ExprCtx));
        }
        UNIT_ASSERT(merged->GetColumns().Keys() == (TUnorderedIUs{outA, outP}));
        UNIT_ASSERT(merged->GetColumns().Find(outA)->Inputs == (TUnionInputRow{{a, b, c}}.Inputs));
        UNIT_ASSERT(merged->GetColumns().Find(outP)->Inputs == (TUnionInputRow{{p, q, r}}.Inputs));
    }

    Y_UNIT_TEST(MergeUnionAllFlattensNestedBranches) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), c = f.Id(), d = f.Id(), left = f.Id(), right = f.Id(), output = f.Id();
        auto first = f.Read({a}), second = f.Read({b}), third = f.Read({c}), fourth = f.Read({d});
        const TVector<IOperator*> leaves{first.get(), second.get(), third.get(), fourth.get()};
        auto outer = f.Union(f.Union(std::move(first), std::move(second), {{left, a, b}}),
            f.Union(std::move(third), std::move(fourth), {{right, c, d}}), {{output, left, right}});
        auto* merged = outer.get();
        auto root = f.Root(std::move(outer), {{output, "a"}});
        UNIT_ASSERT(TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_C(root->GetInput().Get() == merged, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_VALUES_EQUAL(merged->GetChildCount(), 4);
        for (size_t i = 0; i < leaves.size(); ++i) {
            UNIT_ASSERT_C(merged->GetChild(i).Get() == leaves[i], root->PlanToString(f.ExprCtx));
        }
        UNIT_ASSERT(merged->GetColumns().Find(output)->Inputs == (TUnionInputRow{{a, b, c, d}}.Inputs));
    }

    Y_UNIT_TEST(MergeUnionAllKeepsOrderedUnion) {
        for (const bool innerOrdered : {false, true}) {
            NTests::TIdTestContext f;
            const auto a = f.Id(), b = f.Id(), c = f.Id(), innerId = f.Id(), output = f.Id();
            auto inner = f.Union(f.Read({a}), f.Read({b}), {{innerId, a, b}}, innerOrdered);
            auto* innerPtr = inner.get();
            auto outer = f.Union(std::move(inner), f.Read({c}), {{output, innerId, c}}, !innerOrdered);
            auto root = f.Root(std::move(outer), {{output, "a"}});
            UNIT_ASSERT(!TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
            UNIT_ASSERT_VALUES_EQUAL(root->GetInput()->GetChildCount(), 2);
            UNIT_ASSERT_C(root->GetInput()->GetChild(0).Get() == innerPtr, root->PlanToString(f.ExprCtx));
        }
    }

    Y_UNIT_TEST(MergeUnionAllKeepsSharedUnion) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), innerId = f.Id(), output = f.Id();
        auto hub = TReplicate::Create(f.Union(f.Read({a}), f.Read({b}), {{innerId, a, b}}),
            f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto local = *right->GetRebindings().Find(innerId);
        auto root = f.Root(f.Union(std::move(left), std::move(right), {{output, innerId, local}}), {{output, "a"}});
        UNIT_ASSERT(!TMergeUnionAllRule().MatchAndApply(root->MutableChild(0), f.RboCtx, root->PlanProps));
        UNIT_ASSERT_VALUES_EQUAL(root->GetInput()->GetChildCount(), 2);
        UNIT_ASSERT_C(root->GetInput()->GetChild(0)->Kind == EOperator::Replicate, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT_C(root->GetInput()->GetChild(1)->Kind == EOperator::Replicate, root->PlanToString(f.ExprCtx));
        UNIT_ASSERT(hub->GetInput()->Kind == EOperator::UnionAll);
    }

    Y_UNIT_TEST_TWIN(ColumnStatistics, ColumnStore) {
        auto enableNewRbo = [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableTableServiceConfig()->SetEnableNewRBO(true);
            // Fallback is enabled, because analyze uses UDAF which are not supported in NEW RBO.
            settings.AppConfig->MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
            settings.AppConfig->MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            settings.AppConfig->MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            settings.AppConfig->MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        };

        TTestEnv env(1, 1, true, enableNewRbo);
        CreateDatabase(env, "Database");
        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        TString schemaQ = R"(
            CREATE TABLE `/Root/Database/t1` (
                a Int64 NOT NULL,
                b Int64,
                primary key(a)
            )
        )";

        if (ColumnStore) {
            schemaQ += R"(WITH (STORE = column))";
        }
        schemaQ += ";";

        auto result = session.ExecuteSchemeQuery(schemaQ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rowsTable;
        rowsTable.BeginList();
        for (size_t i = 0, e = (1 << 4); i < e; ++i) {
            rowsTable.AddListItem()
                .BeginStruct()
                .AddMember("a").Int64(i)
                .AddMember("b").Int64(i + 1)
                .EndStruct();
        }
        rowsTable.EndList();

        auto resultUpsert = client.BulkUpsert("/Root/Database/t1", rowsTable.Build()).GetValueSync();
        UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "t1")).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        std::vector<std::string> queries = {
            R"(
                PRAGMA YqlSelect = 'force';
                select t1.a, t1.b from `/Root/Database/t1` as t1 where t1.a > 10;
            )",
        };

        auto session2 = client.GetSession().GetValueSync().GetSession();
        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto result = session2.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    

    // Regression: Aggregate::ComputeMetadata was copying input metadata instead of starting
    // fresh, leaking its KeyColumns into the input Map. Repros with TPCH Q21 pattern.
    Y_UNIT_TEST(AggregateKeyColumnsNotLeakedToInputMap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/items` (
                order_id Int64 NOT NULL,
                line_id  Int64 NOT NULL,
                sup_id   Int64 NOT NULL,
                late     Int64 NOT NULL,
                PRIMARY KEY (order_id, line_id)
            ) WITH (Store = Column);
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(R"(
            SELECT i1.sup_id, COUNT(*) AS cnt
            FROM `/Root/items` AS i1
            WHERE i1.late = 1
              AND EXISTS (
                  SELECT * FROM `/Root/items` AS i2
                  WHERE i2.order_id = i1.order_id AND i2.sup_id != i1.sup_id
              )
              AND NOT EXISTS (
                  SELECT * FROM `/Root/items` AS i3
                  WHERE i3.order_id = i1.order_id AND i3.sup_id != i1.sup_id AND i3.late = 1
              )
            GROUP BY i1.sup_id
            ORDER BY cnt DESC, i1.sup_id;
        )", NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

/*
    Y_UNIT_TEST(MapAliasCleanupComplexQuery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        CreateTablesFromPath(tableSession, BenchmarkSchemaPathPrefix[EBenchType::TPCH], BenchmarkSchemaPath[EBenchType::TPCH], true);

        const TString query = R"(
            PRAGMA YqlSelect = 'force';
            PRAGMA AnsiImplicitCrossJoin;

            $zero_i32 = cast(0 as Int32);
            $zero_i64 = cast(0 as Int64);
            $one_i64 = cast(1 as Int64);
            $two_i64 = cast(2 as Int64);

            $c0 = (
                SELECT
                    c.c_custkey AS c_k,
                    c.c_nationkey AS c_nkey,
                    c.c_custkey AS c_amount,
                    c.c_custkey + $one_i64 AS c_metric
                FROM `/Root/customer` AS c
                WHERE c.c_custkey > $zero_i64
                ORDER BY c.c_custkey
                LIMIT 1000
            );

            $s0 = (
                SELECT
                    s.s_suppkey AS s_k,
                    s.s_nationkey AS s_nkey,
                    s.s_suppkey AS s_amount,
                    s.s_suppkey + $two_i64 AS s_metric
                FROM `/Root/supplier` AS s
                WHERE EXISTS (
                    SELECT *
                    FROM `/Root/nation` AS n
                    WHERE n.n_nationkey == s.s_nationkey
                        AND n.n_name IS NOT NULL
                )
            );

            $inner_j = (
                SELECT
                    c_k AS k,
                    s_k AS supplier_key,
                    c_nkey AS nkey,
                    c_amount + s_amount AS amount,
                    c_metric + s_metric AS metric
                FROM $c0, $s0
                WHERE c_nkey == s_nkey
            );

            $left_j = (
                SELECT
                    k,
                    nkey,
                    amount,
                    metric + cast(coalesce(ps_availqty, $zero_i32) as Int64) AS metric2
                FROM $inner_j
                LEFT JOIN `/Root/partsupp` AS partsupp
                    ON supplier_key == partsupp.ps_suppkey
            );

            $agg = (
                SELECT
                    nkey AS k,
                    MAX(metric2 + amount) AS sort_metric
                FROM $left_j
                GROUP BY nkey
            );

            $unioned = (
                SELECT k, sort_metric
                FROM $agg

                UNION ALL

                SELECT
                    cast(n.n_nationkey as Int32?) AS k,
                    cast(n.n_nationkey as Int64) AS sort_metric
                FROM `/Root/nation` AS n
            );

            SELECT
                k AS result_key,
                k + $one_i64 AS result_amount
            FROM $unioned
            ORDER BY sort_metric DESC
            LIMIT 10;
        )";

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        auto result = querySession.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing explain plan");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        const auto simplifiedPlan = GetSimplifiedPlan(plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinKind", "Inner"), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "JoinKind", "Left"), plan);
        UNIT_ASSERT_C(FindOperatorByStringFieldContaining(simplifiedPlan, "Aggregation", ": max("), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "UnionAll"), plan);
        UNIT_ASSERT_C(FindOperatorByStringField(simplifiedPlan, "Name", "TopSort") || FindOperatorByStringField(simplifiedPlan, "Name", "Sort"), plan);
        UNIT_ASSERT_C(plan.Contains("Join"), plan);
    }
    */

}

} // namespace NKikimr::NKqp
