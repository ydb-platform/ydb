#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include "kqp_rbo_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <regex>

namespace NKikimr::NKqp {
using NTests::TIdTestContext;

namespace {

void ExpandReplicates(TOpRoot& root, TRBOContext& ctx) {
    TVector<std::unique_ptr<IRule>> rules;
    rules.emplace_back(std::make_unique<TExpandReplicateRule>());
    TRuleBasedStage("Expand Replicate", std::move(rules)).RunStage(root, ctx);
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
}

size_t CountOperators(TOpRoot& root, EOperator kind) {
    size_t count = 0;
    for (const auto& item : root) {
        count += item.Current->Kind == kind;
    }
    return count;
}

enum class EMapUse { None, JoinKey, JoinFilter };

void CheckMapPullup(const TString& kind, bool fromLeft, EMapUse use, bool expectedPullup) {
    NTests::TIdTestContext f;
    const auto a = f.Id(), b = f.Id(), c = f.Id(), computed = f.Id();
    auto inner = MakeIntrusive<TOpJoin>(f.Read({a}), f.Read({b}), f.Pos, "Inner", TJoinIUs{{a, b}});
    auto cbo = MakeIntrusive<TOpCBOTree>(std::move(inner), f.Pos);
    TMapIUs definitions;
    definitions.Add(computed, f.Constant());
    auto map = MakeIntrusive<TOpMap>(std::move(cbo), f.Pos, std::move(definitions));
    TIntrusivePtr<IOperator> left = fromLeft ? TIntrusivePtr<IOperator>(map) : f.Read({c});
    TIntrusivePtr<IOperator> right = fromLeft ? TIntrusivePtr<IOperator>(f.Read({c})) : map;
    const auto key = use == EMapUse::JoinKey ? computed : a;
    TJoinIUs keys;
    if (kind != "Cross") {
        keys.Add(fromLeft ? key : c, fromLeft ? c : key);
    }
    TVector<TExpression> filters;
    if (use == EMapUse::JoinFilter) {
        auto row = f.ExprCtx.NewArgument(f.Pos, "row");
        auto member = f.ExprCtx.NewCallable(f.Pos, "Member", {row, f.ExprCtx.NewAtom(f.Pos, computed)});
        auto value = f.ExprCtx.NewCallable(f.Pos, "Uint64", {f.ExprCtx.NewAtom(f.Pos, "1")});
        filters.emplace_back(f.ExprCtx.NewLambda(f.Pos, f.ExprCtx.NewArguments(f.Pos, {row}),
            f.ExprCtx.NewCallable(f.Pos, "==", {member, value})), &f.ExprCtx, &f.Props);
    }
    auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, kind, std::move(keys), std::move(filters));
    const auto outputs = join->GetOutputIUs();
    TPullUpMapOverCBORule rule;
    const auto result = rule.SimpleMatchAndApply(join, f.RboCtx, f.Props);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Kind == EOperator::Map, expectedPullup,
        kind << ", fromLeft=" << fromLeft << ", use=" << static_cast<int>(use));
    // The rule pipeline rebuilds output caches after replacing the subtree.
    auto root = f.Root(result, {});
    UNIT_ASSERT(result->GetOutputIUs() == outputs);
    TUnorderedIUs available;
    available.UnionWith(join->GetLeftInput()->GetOutputIUs());
    available.UnionWith(join->GetRightInput()->GetOutputIUs());
    UNIT_ASSERT(join->GetUsedIUs(root->PlanProps).IsSubsetOf(available));
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboGlobalIUs) {
    Y_UNIT_TEST(ExpandReplicateReplacesAllPorts) {
        TIdTestContext f;
        f.Config->_KqpEnableSpilling = false;
        const auto source = f.Id(), call = f.Id(), result = f.Id();
        TIntrusivePtr<IOperator> producer = f.Read({source});
        auto hub = TReplicate::Create(producer, f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput(), subplan = hub->AddOutput();
        const auto rightId = right->GetRebindings().At(source);
        const auto subplanId = subplan->GetRebindings().At(source);
        f.Props.Subplans.Add(call, std::move(subplan), ESubplanType::EXPR, {}, subplanId);
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "Cross", TJoinIUs{});
        auto root = f.Root(f.Copies(join, {{result, call}}), {{source, "left"}, {rightId, "right"}, {result, "subplan"}});

        ExpandReplicates(*root, f.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), 0);
        UNIT_ASSERT(join->GetLeftInput() == producer);
        UNIT_ASSERT(join->GetRightInput() != producer);
        UNIT_ASSERT(join->GetRightInput()->GetOutputIUs() == TUnorderedIUs{rightId});
        UNIT_ASSERT(root->PlanProps.Subplans.At(call).Plan->GetOutputIUs() == TUnorderedIUs{subplanId});
        NTests::AssertIdInvariants(*join, root->PlanProps);
    }

    Y_UNIT_TEST(ExpandReplicatePreservesNestedConsumerBindings) {
        // Direct rule application is independent of pipeline configuration.
        for (const bool spilling : {false, true}) {
            for (const bool keepPrimary : {false, true}) {
                TIdTestContext f;
                f.Config->_KqpEnableSpilling = spilling;
                f.QueryCtx->Type = EKikimrQueryType::Query;
                const auto a = f.Id(), b = f.Id(), mapped = f.Id(), total = f.Id();
                const auto read = f.Read({a, b});
                auto source = TReplicate::Create(read, f.Pos, f.Props.InfoUnitRegistry);
                auto primary = source->AddOutput(), secondary = source->AddOutput();
                auto mapInput = keepPrimary ? primary : source->AddOutput();
                const auto inputA = keepPrimary ? a : mapInput->GetRebindings().At(a);
                const auto key = keepPrimary ? b : mapInput->GetRebindings().At(b);
                auto map = f.Copies(mapInput, {{mapped, inputA}});
                TAggregationIUs aggregations;
                aggregations.Add(total, TOpAggregationTraits{mapped, "count"});
                auto aggregate = MakeIntrusive<TOpAggregate>(map, std::move(aggregations), TOrderedIUs<>{key},
                    EOpPhase::Final, false, f.Pos);
                auto shared = TReplicate::Create(aggregate, f.Pos, f.Props.InfoUnitRegistry);
                auto nested = TReplicate::Create(shared->AddOutput(), f.Pos, f.Props.InfoUnitRegistry);
                auto left = nested->AddOutput(), right = nested->AddOutput();
                const auto rightTotal = right->GetRebindings().At(total);
                auto pair = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Inner",
                    TJoinIUs{{key, right->GetRebindings().At(key)}});
                auto root = f.Root(MakeIntrusive<TOpJoin>(pair, secondary, f.Pos, "Cross", TJoinIUs{}),
                    {{total, "left"}, {rightTotal, "right"}});
                const auto outputs = root->GetInput()->GetOutputIUs();

                ExpandReplicates(*root, f.RboCtx);
                // Consumers keep their IDs; a copy Map may also forward the producer's.
                UNIT_ASSERT(outputs.IsSubsetOf(root->GetInput()->GetOutputIUs()));
                NTests::AssertIdInvariants(*root, root->PlanProps);
                // A consumer keeps the original read, even without the primary port.
                bool keepsRead = false;
                for (const auto& item : *root) {
                    keepsRead |= item.Current == read.Get();
                    UNIT_ASSERT_VALUES_EQUAL(item.Current->Parents.size(), 1);
                }
                UNIT_ASSERT(keepsRead);
                UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), 0);
                UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Source), 3);
                const auto before = root->PlanToString(f.ExprCtx);
                ExpandReplicates(*root, f.RboCtx);
                UNIT_ASSERT_VALUES_EQUAL(root->PlanToString(f.ExprCtx), before);
            }
        }
    }

    Y_UNIT_TEST(ExpandReplicateCopiesOnlyRepeatableProducers) {
        enum class EProducer { Sum, Some, RandomNumber, Limit, TopSort, Window };
        for (const auto kind : {EProducer::Sum, EProducer::Some, EProducer::RandomNumber,
                               EProducer::Limit, EProducer::TopSort, EProducer::Window}) {
            TIdTestContext f;
            f.Config->_KqpEnableSpilling = false;
            const auto k = f.Id("k"), v = f.Id("v"), x = f.Id("x");
            TIntrusivePtr<IOperator> producer = f.Read({k, v});
            if (kind == EProducer::Sum || kind == EProducer::Some) {
                TAggregationIUs aggregations;
                aggregations.Add(x, TOpAggregationTraits{v, kind == EProducer::Sum ? "sum" : "some"});
                producer = MakeIntrusive<TOpAggregate>(producer, std::move(aggregations), TOrderedIUs<>{k},
                    EOpPhase::Undefined, false, f.Pos);
            } else if (kind == EProducer::RandomNumber) {
                TMapIUs definitions;
                definitions.Add(x, MakeUnaryCallable("RandomNumber", f.Column(v)));
                producer = MakeIntrusive<TOpMap>(producer, f.Pos, std::move(definitions));
            } else if (kind == EProducer::Limit) {
                producer = MakeIntrusive<TOpLimit>(producer, f.Pos, f.Constant(), EOpPhase::Undefined);
            } else if (kind == EProducer::TopSort) {
                producer = MakeIntrusive<TOpSort>(producer, f.Pos, TPhysicalOpProps{},
                    TSortIUs{{k, {true, false}}}, f.Constant(), EOpPhase::Undefined);
            } else {
                TWindowIUs functions;
                functions.Add(x, TOpWindowFunc{"sum", EWindowFuncKind::Aggregate, {v}});
                producer = MakeIntrusive<TOpWindow>(producer, f.Pos, std::move(functions),
                    TOrderedIUs<>{k}, TSortIUs{}, TOpWindowFrame{});
            }
            auto hub = TReplicate::Create(producer, f.Pos, f.Props.InfoUnitRegistry);
            auto left = hub->AddOutput(), right = hub->AddOutput();
            const auto out = f.Id();
            auto root = f.Root(f.Union(left, right, {{out, k, right->GetRebindings().At(k)}}), {{out, "k"}});
            ExpandReplicates(*root, f.RboCtx);
            const bool duplicated = kind == EProducer::Sum;
            UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), duplicated ? 0 : 2);
            UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, producer->Kind), duplicated ? 2 : 1);
            NTests::AssertIdInvariants(*root, root->PlanProps);
        }
    }

    Y_UNIT_TEST(ExpandReplicateRejectsLargeProducers) {
        TIdTestContext f;
        f.Config->_KqpEnableSpilling = false;
        const auto key = f.Id("key");
        TIntrusivePtr<IOperator> producer = f.Read({key});
        for (size_t i = 0; i < 1001; ++i) {
            producer = MakeIntrusive<TOpFilter>(producer, f.Pos,
                MakeBinaryPredicate("==", f.Column(key), f.Constant()));
        }
        auto hub = TReplicate::Create(producer, f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto root = f.Root(MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{}),
            {{key, "left"}, {right->GetRebindings().At(key), "right"}});

        ExpandReplicates(*root, f.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), 2);
        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Source), 1);
        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Filter), 1001);
        NTests::AssertIdInvariants(*root, root->PlanProps);
    }

    Y_UNIT_TEST(ExpandReplicateCopiesRepeatableSubplansWithFreshCaptures) {
        TIdTestContext f;
        f.Config->_KqpEnableSpilling = false;
        const auto source = f.Id("source"), local = f.Id("local"), call = f.Id("call"), result = f.Id("result");
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{source, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        auto subplan = MakeIntrusive<TOpAddDependencies>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures));
        f.Props.Subplans.Add(call, subplan, ESubplanType::EXPR, {}, local);
        f.Props.Subplans.RefreshDependencies(call);
        auto producer = f.Copies(f.Read({source}), {{result, call}});
        auto hub = TReplicate::Create(producer, f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto rightResult = right->GetRebindings().At(result);
        const auto rightSource = right->GetRebindings().At(source);
        auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{});
        auto root = f.Root(join, {{result, "left"}, {rightResult, "right"}});

        ExpandReplicates(*root, f.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), 0);
        UNIT_ASSERT(join->GetLeftInput() == producer);
        auto& copy = CastOperator<TOpMap>(*join->GetRightInput());
        const auto copiedCall = copy.GetMapElements().At(rightResult).GetColumnAccess();
        UNIT_ASSERT(copiedCall != call);
        const auto& copiedEntry = root->PlanProps.Subplans.At(copiedCall);
        UNIT_ASSERT(copiedEntry.Plan != subplan);
        UNIT_ASSERT(copiedEntry.DependentIUs == TUnorderedIUs{rightSource});
        auto& copiedCaptures = CastOperator<TOpAddDependencies>(*copiedEntry.Plan);
        UNIT_ASSERT_VALUES_EQUAL(copiedCaptures.GetDependencies().At(*copiedEntry.ResultIU).Outer, rightSource);
        UNIT_ASSERT(root->PlanProps.Subplans.At(call).Plan == subplan);
        UNIT_ASSERT(root->PlanProps.Subplans.At(call).DependentIUs == TUnorderedIUs{source});
        NTests::AssertIdInvariants(*root, root->PlanProps);
    }

    Y_UNIT_TEST(StageAssignmentRejectsSharingAfterExpandingSafeSubtrees) {
        TIdTestContext f;
        f.Config->_KqpEnableSpilling = false;
        const auto safe = f.Id(), unsafe = f.Id();
        auto safeHub = TReplicate::Create(f.Read({safe}), f.Pos, f.Props.InfoUnitRegistry);
        auto left = safeHub->AddOutput(), right = safeHub->AddOutput();
        auto safeJoin = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{});
        auto limit = MakeIntrusive<TOpLimit>(f.Read({unsafe}), f.Pos, f.Constant(), EOpPhase::Undefined);
        auto unsafeHub = TReplicate::Create(limit, f.Pos, f.Props.InfoUnitRegistry);
        auto first = unsafeHub->AddOutput(), second = unsafeHub->AddOutput();
        auto unsafeJoin = MakeIntrusive<TOpJoin>(first, second, f.Pos, "Cross", TJoinIUs{});
        auto root = f.Root(MakeIntrusive<TOpJoin>(safeJoin, unsafeJoin, f.Pos, "Cross", TJoinIUs{}),
            {{safe, "safe"}, {unsafe, "unsafe"}});

        ExpandReplicates(*root, f.RboCtx);

        UNIT_ASSERT(safeJoin->GetLeftInput()->Kind == EOperator::Source);
        UNIT_ASSERT(safeJoin->GetRightInput()->Kind == EOperator::Source);
        UNIT_ASSERT(safeJoin->GetLeftInput() != safeJoin->GetRightInput());
        UNIT_ASSERT(unsafeJoin->GetLeftInput() == first);
        UNIT_ASSERT(unsafeJoin->GetRightInput() == second);
        UNIT_ASSERT_EXCEPTION_CONTAINS(TAssignStagesStage().RunStage(*root, f.RboCtx),
            yexception, "Cannot execute shared Limit with channel spilling disabled");
    }

    Y_UNIT_TEST(ExpansionRejectsUnsafeCalledSubplansBeforeCopying) {
        TIdTestContext f;
        const auto call = f.Id(), value = f.Id(), source = f.Id(), result = f.Id();
        TMapIUs definitions;
        definitions.Add(value, MakeUnaryCallable("RandomNumber", f.Constant()));
        auto subplan = MakeIntrusive<TOpMap>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(definitions));
        f.Props.Subplans.Add(call, subplan, ESubplanType::EXPR, {}, value);
        auto producer = f.Copies(f.Read({source}), {{result, call}});
        auto hub = TReplicate::Create(producer, f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto root = f.Root(MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{}),
            {{result, "left"}, {right->GetRebindings().At(result), "right"}});
        const auto size = root->PlanProps.InfoUnitRegistry.Size();

        ExpandReplicates(*root, f.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(CountOperators(*root, EOperator::Replicate), 2);
        UNIT_ASSERT_VALUES_EQUAL(root->PlanProps.InfoUnitRegistry.Size(), size);
        UNIT_ASSERT(root->PlanProps.Subplans.At(call).Plan == subplan);
        NTests::AssertIdInvariants(*root, root->PlanProps);
    }

    Y_UNIT_TEST(StageAssignmentRequiresChannelSpillingForSharedInputs) {
        for (const auto& [serviceSpilling, querySpilling, type, allowed] :
                TVector<std::tuple<bool, bool, EKikimrQueryType, bool>>{
                    {true, true, EKikimrQueryType::Query, true},
                    {true, true, EKikimrQueryType::Scan, true},
                    {true, true, EKikimrQueryType::Dml, false},
                    {false, true, EKikimrQueryType::Query, false},
                    {true, false, EKikimrQueryType::Query, false}}) {
            TIdTestContext f;
            f.Config->SetEnableQueryServiceSpilling(serviceSpilling);
            f.Config->_KqpEnableSpilling = querySpilling;
            f.QueryCtx->Type = type;
            const auto source = f.Id();
            auto hub = TReplicate::Create(f.Read({source}), f.Pos, f.Props.InfoUnitRegistry);
            auto left = hub->AddOutput(), right = hub->AddOutput();
            auto root = f.Root(MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{}), {{source, "source"}});
            // Also covers sharing introduced after the expansion stage.
            if (allowed) {
                TAssignStagesStage().RunStage(*root, f.RboCtx);
                UNIT_ASSERT(left->Props.StageId == right->Props.StageId);
                UNIT_ASSERT(left->Props.StageOutputIndex != right->Props.StageOutputIndex);
            } else {
                UNIT_ASSERT_EXCEPTION_CONTAINS(TAssignStagesStage().RunStage(*root, f.RboCtx),
                    yexception, "with channel spilling disabled");
            }
        }
    }

    Y_UNIT_TEST(ExpandReplicateExecutesSharedSubqueries) {
        for (const auto& [serviceSpilling, querySpilling] : {std::pair{true, false}, std::pair{true, true}, std::pair{false, true}}) {
            const bool spilling = serviceSpilling && querySpilling;
            NKikimrConfig::TAppConfig config;
            config.MutableTableServiceConfig()->SetEnableNewRBO(true);
            config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
            config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
            config.MutableTableServiceConfig()->SetEnableQueryServiceSpilling(serviceSpilling);
            NKikimrKqp::TKqpSetting setting;
            setting.SetName("_KqpEnableSpilling");
            setting.SetValue(querySpilling ? "true" : "false");
            TKikimrRunner kikimr(TKikimrSettings(config).SetKqpSettings({setting}).SetWithSampleTables(false));
            auto client = kikimr.GetQueryClient();
            auto created = client.ExecuteQuery("CREATE TABLE `/Root/KeyValue` (Key Uint64, PRIMARY KEY (Key));",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (const ui64 key : {1, 2}) {
                rows.AddListItem().BeginStruct().AddMember("Key").Uint64(key).EndStruct();
            }
            auto inserted = kikimr.GetTableClient().BulkUpsert("/Root/KeyValue", rows.EndList().Build()).ExtractValueSync();
            UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
            for (const auto& [query, expected] : TVector<std::pair<TString, TString>>{
                {R"(
                    PRAGMA YqlSelect = 'force';
                    $shared = (SELECT Key, SUM(Key) AS Value FROM `/Root/KeyValue` WHERE Key > 0 GROUP BY Key);
                    SELECT Unwrap(SUM(l.Value + r.Value)) FROM $shared AS l
                    INNER JOIN $shared AS r ON l.Key = r.Key;
                )", "[[6u]]"},
                {R"(
                    PRAGMA YqlSelect = 'force';
                    $shared = (SELECT Key, SUM(CAST(Key AS Double)) AS Value FROM `/Root/KeyValue` GROUP BY Key);
                    SELECT Unwrap(CAST(SUM(l.Value + r.Value) AS Uint64)) FROM $shared AS l
                    INNER JOIN $shared AS r ON l.Key = r.Key;
                )", "[[6u]]"},
                {R"(
                    PRAGMA YqlSelect = 'force';
                    $shared = (SELECT Key, SUM(CAST(Key AS Decimal(22, 9))) AS Value FROM `/Root/KeyValue` GROUP BY Key);
                    SELECT Unwrap(CAST(SUM(l.Value + r.Value) AS Uint64)) FROM $shared AS l
                    INNER JOIN $shared AS r ON l.Key = r.Key;
                )", "[[6u]]"},
                // Copy elimination renames the read column in the second consumer.
                {R"(
                    PRAGMA YqlSelect = 'force';
                    $shared = (SELECT key, COUNT(*) AS c FROM (SELECT Key AS key FROM `/Root/KeyValue`) GROUP BY key);
                    SELECT Unwrap(SUM(l.c + r.c)) FROM $shared AS l INNER JOIN $shared AS r ON l.key = r.key;
                )", "[[4u]]"},
            }) {
                auto explain = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).ExtractValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                const TString ast{*explain.GetStats()->GetAst()};
                // A stage output other than the first, quoted or not.
                const bool shared = std::regex_search(ast.c_str(), std::regex(R"(\(TDqOutput [^ ]+ '"?[1-9][0-9]*"?\))"));
                UNIT_ASSERT_VALUES_EQUAL_C(shared, spilling, query << ast);
                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), expected);
            }
            const TString query = R"(
                PRAGMA YqlSelect = 'force';
                $shared = (SELECT Key, AVG(Key) AS Value FROM `/Root/KeyValue` GROUP BY Key);
                SELECT SUM(l.Value + r.Value) FROM $shared AS l INNER JOIN $shared AS r ON l.Key = r.Key;
            )";
            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), spilling, query << result.GetIssues().ToString());
            if (!spilling) {
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "with channel spilling disabled");
            }
        }
    }

    Y_UNIT_TEST(SingletonReplicateCollapsesEitherPortAndIgnoresHubReferences) {
        for (const bool secondary : {false, true}) {
            TIdTestContext f;
            const auto source = f.Id();
            auto hub = TReplicate::Create(f.Read({source}), f.Pos, f.Props.InfoUnitRegistry);
            TIntrusivePtr<IOperator> primary = hub->AddOutput();
            TIntrusivePtr<IOperator> other = hub->AddOutput();
            UNIT_ASSERT(!TOpReplicate::TryCollapse(primary, f.ExprCtx, f.Props));
            auto retained = secondary ? other : primary;
            const auto result = *retained->GetOutputIUs().begin();
            auto root = f.Root(std::move(retained), {{result, "result"}});
            FinishLogicalRewrite(*root, f.ExprCtx);
            UNIT_ASSERT(root->GetInput()->Kind == (secondary ? EOperator::Map : EOperator::Source));
            UNIT_ASSERT(root->GetInput()->GetOutputIUs().Contains(result));
            TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
            UNIT_ASSERT(root->GetInput()->Kind == EOperator::Source);
            UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items().front().first, source);
            NTests::AssertIdInvariants(*root, root->PlanProps);
        }

        // Collapsing the input of another replication must preserve its port
        // list while the sibling consumer is still waiting to be visited.
        for (const bool secondary : {false, true}) {
            TIdTestContext f;
            const auto source = f.Id();
            auto inner = TReplicate::Create(f.Read({source}), f.Pos, f.Props.InfoUnitRegistry);
            auto unused = inner->AddOutput();
            auto retained = secondary ? inner->AddOutput() : unused;
            auto outer = TReplicate::Create(retained, f.Pos, f.Props.InfoUnitRegistry);
            auto left = outer->AddOutput(), right = outer->AddOutput();
            const auto leftId = *left->GetOutputIUs().begin();
            const auto rightId = *right->GetOutputIUs().begin();
            auto root = f.Root(MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{}),
                {{leftId, "left"}, {rightId, "right"}});
            // The outer input now owns the inner port alone; collapse destroys
            // that operator while the shared binding itself remains alive.
            retained.Reset();
            unused.Reset();
            FinishLogicalRewrite(*root, f.ExprCtx);
            UNIT_ASSERT(outer->GetInput()->Kind == (secondary ? EOperator::Map : EOperator::Source));
            UNIT_ASSERT_VALUES_EQUAL(outer->GetOutputs().size(), 2);
            UNIT_ASSERT(left->GetOutputIUs().Contains(leftId));
            UNIT_ASSERT(right->GetOutputIUs().Contains(rightId));
            UNIT_ASSERT(!left->GetOutputIUs().HasAny(right->GetOutputIUs()));
            NTests::AssertIdInvariants(*root, root->PlanProps);
        }
    }

    Y_UNIT_TEST(SharedSubplansAccumulateProducerDemand) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), callA = f.Id(), callB = f.Id(), x = f.Id(), y = f.Id();
        auto hub = TReplicate::Create(f.Read({a, b}), f.Pos, f.Props.InfoUnitRegistry);
        auto first = hub->AddOutput(), second = hub->AddOutput();
        const auto secondB = *second->GetRebindings().Find(b);
        f.Props.Subplans.Add(callA, std::move(first), ESubplanType::EXPR, {}, a);
        f.Props.Subplans.Add(callB, std::move(second), ESubplanType::EXPR, {}, secondB);
        auto root = f.Root(f.Copies(MakeIntrusive<TOpEmptySource>(f.Pos), {{x, callA}, {y, callB}}),
            {{x, "a"}, {y, "b"}});
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(hub->GetInput()->GetOutputIUs() == (TUnorderedIUs{a, b}));
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b}));
    }

    Y_UNIT_TEST(CopyChainsPreserveFixedResultLabelsAndRepeatedPositions) {
        TIdTestContext f;
        const auto a = f.Id("source"), b = f.Id("alias"), c = f.Id("alias");
        auto root = f.Root(f.Copies(f.Copies(f.Read({a}), {{b, a}}), {{c, b}}),
            {{c, "first"}, {b, "second"}, {c, "again"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(root->GetInput()->Kind == EOperator::Source);
        UNIT_ASSERT(root->GetOutputIUs() == TUnorderedIUs{a});
        const auto& columns = root->GetColumns().Items();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(columns[0].second, "first");
        UNIT_ASSERT_VALUES_EQUAL(columns[1].second, "second");
        UNIT_ASSERT_VALUES_EQUAL(columns[2].second, "again");
    }

    Y_UNIT_TEST(CopyEliminationKeepsCalculationsAndSimultaneousInputs) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), x = f.Id(), y = f.Id(), z = f.Id();
        auto copies = f.Copies(f.Read({a, b}), {{x, b}, {y, a}});
        TMapIUs definitions;
        definitions.Add(z, MakeBinaryPredicate("-", f.Column(x), f.Column(y)));
        auto root = f.Root(MakeIntrusive<TOpMap>(std::move(copies), f.Pos, std::move(definitions)), {{z, "result"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        auto& map = CastOperator<TOpMap>(*root->GetInput());
        UNIT_ASSERT(map.GetInput()->Kind == EOperator::Source);
        UNIT_ASSERT(map.GetMapElements().Keys() == TUnorderedIUs{z});
        UNIT_ASSERT(map.GetUniqueRawInputIUs() == (TUnorderedIUs{a, b}));
        const auto body = map.GetMapElements().Find(z)->GetExpression().GetExpressionBody();
        UNIT_ASSERT_VALUES_EQUAL(body->Head().Tail().Content(), f.ExprCtx.GetIndexAsString(b));
        UNIT_ASSERT_VALUES_EQUAL(body->Tail().Tail().Content(), f.ExprCtx.GetIndexAsString(a));
    }

    Y_UNIT_TEST(CopiesRebindSortAndJoinWithoutDroppingUnmentionedInputs) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), copy = f.Id(), payload = f.Id();
        auto join = MakeIntrusive<TOpJoin>(f.Copies(f.Read({a, payload}), {{copy, a}}), f.Read({b}),
            f.Pos, "Inner", TPairedIUs{{copy, b}});
        TSortIUs order{{copy, {true, true}}, {copy, {false, false}}};
        auto root = f.Root(MakeIntrusive<TOpSort>(std::move(join), f.Pos, std::move(order)), {{payload, "p"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        auto& sort = CastOperator<TOpSort>(*root->GetInput());
        auto& joined = CastOperator<TOpJoin>(*sort.GetInput());
        UNIT_ASSERT(joined.JoinKeys.Contains(a, b));
        UNIT_ASSERT(joined.GetOutputIUs().Contains(payload));
        UNIT_ASSERT_VALUES_EQUAL(sort.GetSortElements().Items().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sort.GetSortElements().Items()[0].first, a);
        UNIT_ASSERT_VALUES_EQUAL(sort.GetSortElements().Items()[1].first, a);
        UNIT_ASSERT(!sort.GetSortElements().Items()[1].second.Ascending);
    }

    Y_UNIT_TEST(CopyEliminationDoesNotMergeReplicateNamespaces) {
        TIdTestContext f;
        const auto a = f.Id(), copy = f.Id();
        auto hub = TReplicate::Create(f.Copies(f.Read({a}), {{copy, a}}), f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        const auto localA = *right->GetRebindings().Find(a);
        const auto localCopy = *right->GetRebindings().Find(copy);
        auto root = f.Root(MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "Inner",
            TPairedIUs{{copy, localCopy}}), {{copy, "left"}, {localCopy, "right"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        auto& join = CastOperator<TOpJoin>(*root->GetInput());
        UNIT_ASSERT(join.JoinKeys.Contains(a, localA));
        UNIT_ASSERT(!join.GetLeftInput()->GetOutputIUs().HasAny(join.GetRightInput()->GetOutputIUs()));
        UNIT_ASSERT(hub->GetInput()->Kind == EOperator::Source);
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[1].first, localA);
    }

    Y_UNIT_TEST(LocalLivenessRetainsDefinitionsGlobalPruningRemovesDeadChains) {
        TIdTestContext f;
        const auto a = f.Id(), payload = f.Id(), dead = f.Id(), deadCopy = f.Id();
        TMapIUs definitions;
        definitions.Add(dead, MakeBinaryPredicate("+", f.Column(a), f.Constant()));
        auto map = MakeIntrusive<TOpMap>(f.Read({a, payload}), f.Pos, std::move(definitions));
        auto* read = map->GetInput().Get();
        auto root = f.Root(f.Copies(std::move(map), {{deadCopy, dead}}), {{payload, "result"}});
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(read) == (TUnorderedIUs{a, payload}));
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(root->GetInput().Get() == read);
        UNIT_ASSERT(read->GetOutputIUs() == TUnorderedIUs{payload});
        UNIT_ASSERT(!read->Props.Analysis.LiveOut);
    }

    Y_UNIT_TEST(CopyEliminationPreservesUnionDefinitionsAndAggregateResults) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), aCopy = f.Id(), merged = f.Id(), alias = f.Id();
        const auto result = f.Id(), resultAlias = f.Id();
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(merged, TUnionInputRow{{aCopy, b}});
        auto merge = MakeIntrusive<TOpUnionAll>(f.Copies(f.Read({a}), {{aCopy, a}}), f.Read({b}), f.Pos, std::move(columns));
        auto* mergePtr = merge.get();
        TAggregationIUs traits;
        traits.Add(result, TOpAggregationTraits{alias, "sum"});
        auto aggregate = MakeIntrusive<TOpAggregate>(f.Copies(std::move(merge), {{alias, merged}}),
            std::move(traits), TOrderedIUs<>{}, EOpPhase::Undefined, false, f.Pos);
        auto root = f.Root(f.Copies(std::move(aggregate), {{resultAlias, result}}), {{resultAlias, "total"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        auto& agg = CastOperator<TOpAggregate>(*root->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(agg.GetAggregationTraits().Find(result)->Input, merged);
        UNIT_ASSERT(mergePtr->GetColumns().Keys() == TUnorderedIUs{merged});
        UNIT_ASSERT_VALUES_EQUAL(mergePtr->GetColumns().Find(merged)->Inputs[0], a);
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[0].first, result);
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[0].second, "total");
    }

    Y_UNIT_TEST(CopyEliminationRebindsCapturesWithoutRemovingSubplanEvaluation) {
        TIdTestContext f;
        const auto source = f.Id(), copy = f.Id(), local = f.Id(), call = f.Id(), result = f.Id();
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{copy, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.Add(call, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)), ESubplanType::EXPR, {}, local);
        f.Props.Subplans.RefreshDependencies(call);
        auto root = f.Root(f.Copies(f.Copies(f.Read({source}), {{copy, source}}), {{result, call}}), {{result, "result"}});
        TGlobalInliningStage("copies").RunStage(*root, f.RboCtx);
        const auto& subplan = root->PlanProps.Subplans.At(call);
        UNIT_ASSERT(subplan.DependentIUs == TUnorderedIUs{source});
        UNIT_ASSERT_VALUES_EQUAL(*subplan.ResultIU, local);
        const auto& deps = CastOperator<TOpAddDependencies>(*subplan.Plan).GetDependencies();
        UNIT_ASSERT_VALUES_EQUAL(deps.Find(local)->Outer, source);
        auto& map = CastOperator<TOpMap>(*root->GetInput());
        UNIT_ASSERT(map.GetMapElements().Keys() == TUnorderedIUs{result});
        UNIT_ASSERT_VALUES_EQUAL(map.GetMapElements().Find(result)->GetColumnAccess(), call);
    }

    Y_UNIT_TEST(GlobalPruningRemovesInactiveSubplansAndKeepsCaptureSources) {
        TIdTestContext f;
        const auto source = f.Id(), local = f.Id(), active = f.Id(), inactive = f.Id();
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{source, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
        f.Props.Subplans.Add(active, MakeIntrusive<TOpAddDependencies>(
            MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures)), ESubplanType::EXISTS);
        f.Props.Subplans.RefreshDependencies(active);
        f.Props.Subplans.Add(inactive, MakeIntrusive<TOpEmptySource>(f.Pos), ESubplanType::EXISTS);
        auto root = f.Root(MakeIntrusive<TOpFilter>(f.Read({source}), f.Pos, f.Column(active)), {});
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(!root->PlanProps.Subplans.Contains(inactive));
        const auto& subplan = root->PlanProps.Subplans.At(active);
        // Captures are never pruned, even when unused inside the subplan.
        UNIT_ASSERT(subplan.Plan->Kind == EOperator::AddDependencies);
        UNIT_ASSERT(subplan.DependentIUs == TUnorderedIUs{source});
        UNIT_ASSERT(root->GetInput()->GetChild(0)->GetOutputIUs() == TUnorderedIUs{source});
    }

    Y_UNIT_TEST(PruningPreservesAggregationBoundaries) {
        // Grouping keys determine multiplicity even when no result is consumed.
        {
            TIdTestContext f;
            const auto key = f.Id(), value = f.Id(), result = f.Id();
            TAggregationIUs traits;
            traits.Add(result, TOpAggregationTraits{value, "sum"});
            auto root = f.Root(MakeIntrusive<TOpAggregate>(f.Read({key, value}), std::move(traits),
                TOrderedIUs<>{key}, EOpPhase::Undefined, false, f.Pos), {});
            TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
            auto& aggregate = CastOperator<TOpAggregate>(*root->GetInput());
            UNIT_ASSERT(aggregate.GetAggregationTraits().Keys().Empty());
            UNIT_ASSERT(aggregate.GetKeyColumns().Unordered() == TUnorderedIUs{key});
            UNIT_ASSERT(aggregate.GetInput()->GetOutputIUs() == TUnorderedIUs{key});
        }
        // DistinctAll preserves its complete tuple even when only one result is consumed.
        {
            TIdTestContext f;
            const auto a = f.Id(), b = f.Id(), x = f.Id(), y = f.Id();
            TAggregationIUs traits;
            traits.Add(x, TOpAggregationTraits{a, "distinct"});
            traits.Add(y, TOpAggregationTraits{b, "distinct"});
            auto root = f.Root(MakeIntrusive<TOpAggregate>(f.Read({a, b}), std::move(traits),
                TOrderedIUs<>{a, b}, EOpPhase::Undefined, true, f.Pos), {{x, "result"}});
            TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
            UNIT_ASSERT(root->GetInput()->GetOutputIUs() == (TUnorderedIUs{x, y}));
            UNIT_ASSERT(root->GetInput()->GetChild(0)->GetOutputIUs() == (TUnorderedIUs{a, b}));
        }
        // A scalar aggregate without used results is one row whatever its input,
        // also below a projection that becomes empty.
        for (const bool projected : {false, true}) {
            TIdTestContext f;
            const auto input = f.Id(), count = f.Id(), copy = f.Id();
            TAggregationIUs traits;
            traits.Add(count, TOpAggregationTraits{input, "count"});
            TIntrusivePtr<IOperator> plan = MakeIntrusive<TOpAggregate>(f.Read({input}), std::move(traits),
                TOrderedIUs<>{}, EOpPhase::Undefined, false, f.Pos);
            if (projected) {
                TMapIUs projection;
                projection.Add(copy, MakeColumnAccess(count, f.Pos, &f.ExprCtx, &f.Props));
                plan = MakeIntrusive<TOpMap>(std::move(plan), f.Pos, std::move(projection));
            }
            auto root = f.Root(std::move(plan), {});
            TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
            auto& source = CastOperator<TOpEmptySource>(*root->GetInput());
            UNIT_ASSERT(!source.Input);
            UNIT_ASSERT(source.GetOutputIUs().Empty());
        }
    }

    Y_UNIT_TEST(UnionAllCanRetainOnlyRowMultiplicity) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), result = f.Id();
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(result, TUnionInputRow{{a, b}});
        auto root = f.Root(MakeIntrusive<TOpUnionAll>(f.Read({a}), f.Read({b}), f.Pos, std::move(columns)), {});
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        auto& merge = CastOperator<TOpUnionAll>(*root->GetInput());
        UNIT_ASSERT(merge.GetColumns().Keys().Empty());
        UNIT_ASSERT_VALUES_EQUAL(merge.GetChildCount(), 2);
        UNIT_ASSERT(merge.GetChild(0)->GetOutputIUs().Empty());
        UNIT_ASSERT(merge.GetChild(1)->GetOutputIUs().Empty());
    }

    Y_UNIT_TEST(ReadRetainsOlapProgramDependenciesWithoutRewritingIt) {
        TIdTestContext f;
        const auto used = f.Id(), unused = f.Id();
        auto read = f.Read({used, unused});
        auto row = f.ExprCtx.NewArgument(f.Pos, "row");
        auto condition = f.ExprCtx.NewList(f.Pos, {f.ExprCtx.NewAtom(f.Pos, used)});
        auto lambda = f.ExprCtx.NewLambda(f.Pos, f.ExprCtx.NewArguments(f.Pos, {row}),
            f.ExprCtx.NewCallable(f.Pos, "KqpOlapFilter", {row, condition}));
        read->OlapFilterLambda = lambda;
        auto root = f.Root(std::move(read), {});
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(root->GetInput()->GetOutputIUs() == TUnorderedIUs{used});
        UNIT_ASSERT(CastOperator<TOpRead>(*root->GetInput()).OlapFilterLambda == lambda);
    }

    Y_UNIT_TEST(OlapPruningAllowsAnEmptyLogicalPayload) {
        TIdTestContext f;
        const auto first = f.Id(), second = f.Id();
        const auto table = f.Read({first, second})->TableCallable;
        auto read = MakeIntrusive<TOpRead>("t", TUnorderedIUs{first, second},
            NYql::EStorageType::ColumnStorage, table, nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, f.Pos);
        auto root = f.Root(std::move(read), {});
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(root->GetInput()->GetOutputIUs().Empty());
    }

    Y_UNIT_TEST(KeyPreservationRemainsAnExplicitPruningPolicy) {
        for (const bool pruneKeys : {false, true}) {
            TIdTestContext f;
            const auto key = f.Id(), payload = f.Id();
            auto root = f.Root(f.Read({key, payload}), {{payload, "result"}});
            root->GetInput()->Props.Metadata.emplace();
            root->GetInput()->Props.Metadata->KeyColumns.Append(key);
            TGlobalPruningStage("prune", pruneKeys).RunStage(*root, f.RboCtx);
            UNIT_ASSERT_VALUES_EQUAL(root->GetInput()->GetOutputIUs().Contains(key), !pruneKeys);
        }
    }

    Y_UNIT_TEST(MapComputationsMoveToPreservedJoinInputs) {
        for (const TString kind : {"Inner", "Left"}) {
            TIdTestContext f;
            const auto a = f.Id(), b = f.Id();
            const auto left = f.Id(), right = f.Id(), constant = f.Id(), both = f.Id();
            TMapIUs definitions;
            definitions.Add(left, MakeUnaryCallable("Just", f.Column(a)));
            definitions.Add(right, MakeUnaryCallable("Just", f.Column(b)));
            definitions.Add(constant, f.Constant());
            definitions.Add(both, MakeBinaryPredicate("==", f.Column(a), f.Column(b)));
            auto join = MakeIntrusive<TOpJoin>(f.Read({a}), f.Read({b}), f.Pos, kind, TPairedIUs{{a, b}});
            auto map = MakeIntrusive<TOpMap>(std::move(join), f.Pos, std::move(definitions));

            TPushMapElementsThroughInputRule rule;
            UNIT_ASSERT(!rule.QuickMatch(map));
            UNIT_ASSERT(rule.SimpleMatchAndApply(map, f.RboCtx, f.Props).Get() == map.Get());

            map->NeedToPush = true;
            UNIT_ASSERT(rule.QuickMatch(map));
            auto result = rule.SimpleMatchAndApply(std::move(map), f.RboCtx, f.Props);
            const bool inner = kind == "Inner";
            auto& kept = CastOperator<TOpMap>(*result);
            UNIT_ASSERT(kept.GetMapElements().Keys() == (inner ? TUnorderedIUs{both} : TUnorderedIUs{right, both}));
            auto& rewritten = CastOperator<TOpJoin>(*kept.GetInput());
            UNIT_ASSERT(CastOperator<TOpMap>(*rewritten.GetLeftInput()).GetMapElements().Keys() == (TUnorderedIUs{left, constant}));
            UNIT_ASSERT(rewritten.GetRightInput()->Kind == (inner ? EOperator::Map : EOperator::Source));
            UNIT_ASSERT(result->GetOutputIUs() == (TUnorderedIUs{a, b, left, right, constant, both}));
        }
    }

    Y_UNIT_TEST(MapComputationsMoveIntoLowerMap) {
        TIdTestContext f;
        const auto a = f.Id(), lower = f.Id(), independent = f.Id(), dependent = f.Id();
        TMapIUs bottom;
        bottom.Add(lower, MakeUnaryCallable("Just", f.Column(a)));
        TMapIUs top;
        top.Add(independent, MakeBinaryPredicate("+", f.Column(a), f.Constant()));
        top.Add(dependent, MakeUnaryCallable("Just", f.Column(lower)));
        auto map = MakeIntrusive<TOpMap>(MakeIntrusive<TOpMap>(f.Read({a}), f.Pos, std::move(bottom)), f.Pos, std::move(top));

        TPushMapElementsIntoMapRule rule;
        auto result = rule.SimpleMatchAndApply(std::move(map), f.RboCtx, f.Props);
        auto& kept = CastOperator<TOpMap>(*result);
        UNIT_ASSERT(kept.GetMapElements().Keys() == TUnorderedIUs{dependent});
        auto& merged = CastOperator<TOpMap>(*kept.GetInput());
        UNIT_ASSERT(merged.GetMapElements().Keys() == (TUnorderedIUs{lower, independent}));
        UNIT_ASSERT(result->GetOutputIUs() == (TUnorderedIUs{a, lower, independent, dependent}));
    }

    Y_UNIT_TEST(MapPullupKeepsComputationsOfNullExtendedSides) {
        CheckMapPullup("Full", true, EMapUse::None, false);
        CheckMapPullup("Full", false, EMapUse::None, false);
        CheckMapPullup("Left", false, EMapUse::None, false);
        CheckMapPullup("Right", true, EMapUse::None, false);
    }

    Y_UNIT_TEST(MapPullupFromPreservedSides) {
        for (const TString kind : {"Inner", "Cross", "Left", "LeftSemi", "LeftOnly"}) {
            CheckMapPullup(kind, true, EMapUse::None, true);
        }
        CheckMapPullup("Inner", false, EMapUse::None, true);
    }

    Y_UNIT_TEST(MapPullupKeepsComputedJoinKeysBelowJoin) {
        for (const bool fromLeft : {false, true}) {
            CheckMapPullup("Inner", fromLeft, EMapUse::JoinKey, false);
        }
    }

    Y_UNIT_TEST(MapPullupKeepsComputedJoinFiltersBelowJoin) {
        for (const bool fromLeft : {false, true}) {
            CheckMapPullup("Inner", fromLeft, EMapUse::JoinFilter, false);
        }
    }

    Y_UNIT_TEST(AggregateSplitKeepsResultsAndAllocatesFreshIntermediates) {
        for (const bool distinctAll : {false, true}) {
            TIdTestContext f;
            const auto key = f.Id(), value = f.Id(), count = f.Id(), sum = f.Id();
            TAggregationIUs functions;
            functions.Add(count, TOpAggregationTraits{value, distinctAll ? "distinct" : "count"});
            functions.Add(sum, TOpAggregationTraits{key, distinctAll ? "distinct" : "sum"});
            const TOrderedIUs<> keys = distinctAll ? TOrderedIUs<>{value, key} : TOrderedIUs<>{key};
            auto aggregate = MakeIntrusive<TOpAggregate>(f.Read({key, value}), std::move(functions), keys,
                EOpPhase::Undefined, distinctAll, f.Pos);
            const auto outputs = aggregate->GetOutputIUs();
            TPropagateAggregateThroughStageRule rule;
            auto result = rule.SimpleMatchAndApply(std::move(aggregate), f.RboCtx, f.Props);
            auto& final = CastOperator<TOpAggregate>(*result);
            auto& partial = CastOperator<TOpAggregate>(*final.GetInput());
            UNIT_ASSERT(result->GetOutputIUs() == outputs);
            UNIT_ASSERT(final.GetAggregationPhase() == EOpPhase::Final);
            UNIT_ASSERT(partial.GetAggregationPhase() == EOpPhase::Intermediate);
            UNIT_ASSERT(final.GetAggregationTraits().Keys() == (TUnorderedIUs{count, sum}));
            UNIT_ASSERT(!partial.GetAggregationTraits().Keys().HasAny({key, value, count, sum}));
            UNIT_ASSERT(partial.GetKeyColumns().Items() == keys.Items());
            for (const auto output : final.GetAggregationTraits().Keys()) {
                const auto* finish = final.GetAggregationTraits().Find(output);
                const auto* start = partial.GetAggregationTraits().Find(finish->Input);
                UNIT_ASSERT(start);
                UNIT_ASSERT_VALUES_EQUAL(start->Input, output == count ? value : key);
                UNIT_ASSERT_VALUES_EQUAL(finish->AggFunction, distinctAll ? "distinct" : "sum");
            }
            if (distinctAll) {
                UNIT_ASSERT(final.GetKeyColumns().Unordered() == partial.GetAggregationTraits().Keys());
            } else {
                UNIT_ASSERT(final.GetKeyColumns().Items() == keys.Items());
            }
        }
    }

    Y_UNIT_TEST(SortPushThroughCopiesPreservesPositionsAndDirections) {
        TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), copy = f.Id();
        auto read = f.Read({a, b});
        read->Props.StageId = 0;
        TMapIUs definitions;
        definitions.Add(copy, f.Column(a));
        auto map = MakeIntrusive<TOpMap>(std::move(read), f.Pos, std::move(definitions));
        map->Props.StageId = 0;
        TSortIUs order;
        order.Append(copy, TSortOrder{false, true});
        order.Append(b, TSortOrder{true, false});
        order.Append(copy, TSortOrder{true, true});
        TPhysicalOpProps props;
        props.StageId = 0;
        auto sort = MakeIntrusive<TOpSort>(std::move(map), f.Pos, props, order, std::nullopt, EOpPhase::Intermediate);
        f.SetType(*sort);
        TPropagateTopSortThroughStageRule rule;
        auto result = rule.SimpleMatchAndApply(std::move(sort), f.RboCtx, f.Props);
        auto& outer = CastOperator<TOpMap>(*result);
        auto& pushed = CastOperator<TOpSort>(*outer.GetInput());
        const auto& keys = pushed.GetSortElements().Items();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(keys[0].first, a);
        UNIT_ASSERT_VALUES_EQUAL(keys[1].first, b);
        UNIT_ASSERT_VALUES_EQUAL(keys[2].first, a);
        for (size_t i = 0; i < keys.size(); ++i) {
            UNIT_ASSERT(keys[i].second.Ascending == order.Items()[i].second.Ascending);
            UNIT_ASSERT(keys[i].second.NullsFirst == order.Items()[i].second.NullsFirst);
        }
        UNIT_ASSERT(outer.GetMapElements().Keys() == TUnorderedIUs{copy});
        UNIT_ASSERT(outer.GetOutputIUs() == (TUnorderedIUs{a, b, copy}));
    }
}

namespace {

struct TPlan {
    TExprContext Ctx;
    const TPositionHandle Pos;
    TOpRoot Root{MakeIntrusive<TOpEmptySource>(Pos), Pos, {}};

    TInfoUnitId Id() { return Root.PlanProps.InfoUnitRegistry.AddGenerated(); }

    TIntrusivePtr<TOpMap> Source(std::initializer_list<TInfoUnitId> ids) {
        TMapIUs definitions;
        for (const auto id : ids) {
            definitions.Add(id, MakeConstant("Uint64", "1", Pos, &Ctx));
        }
        return MakeIntrusive<TOpMap>(MakeIntrusive<TOpEmptySource>(Pos), Pos, std::move(definitions));
    }

    TExpression Column(TInfoUnitId id) {
        return MakeColumnAccess(id, Pos, &Ctx, &Root.PlanProps);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpRboRebindConsumers) {
    Y_UNIT_TEST(ForwardedKeysAndFixedResults) {
        TPlan plan;
        const auto key = plan.Id(), value = plan.Id(), newKey = plan.Id(), newValue = plan.Id(), result = plan.Id();
        auto source = plan.Source({key, value});
        auto* old = source.get();
        TAggregationIUs functions;
        functions.Add(result, TOpAggregationTraits{value, "sum"});
        auto aggregate = MakeIntrusive<TOpAggregate>(std::move(source), std::move(functions),
            TOrderedIUs<>{key}, EOpPhase::Undefined, false, plan.Pos);
        auto* agg = aggregate.get();
        TOpRoot root(std::move(aggregate), plan.Pos, {{key, "key"}, {result, "total"}, {key, "again"}});
        root.ComputeParents();

        RebindConsumers(*old, {{key, newKey}, {value, newValue}}, root.PlanProps.Subplans);
        agg->SetInput(plan.Source({newKey, newValue}));

        UNIT_ASSERT(agg->GetKeyColumns().Unordered() == TUnorderedIUs{newKey});
        UNIT_ASSERT_VALUES_EQUAL(agg->GetAggregationTraits().Find(result)->Input, newValue);
        UNIT_ASSERT_VALUES_EQUAL(root.GetColumns().Items()[0].first, newKey);
        UNIT_ASSERT_VALUES_EQUAL(root.GetColumns().Items()[1].first, result);
        UNIT_ASSERT_VALUES_EQUAL(root.GetColumns().Items()[2].first, newKey);
        UNIT_ASSERT_VALUES_EQUAL(root.GetColumns().Items()[2].second, "again");
    }

    Y_UNIT_TEST(UnionAllStopsIdsButNotInvalidation) {
        TPlan plan;
        const auto left = plan.Id(), right = plan.Id(), replacement = plan.Id(), output = plan.Id();
        auto source = plan.Source({left});
        auto* old = source.get();
        TUnionAllIUs columns(TUnionInputPolicy{2});
        columns.Add(output, TUnionInputRow{{left, right}});
        auto unionAll = MakeIntrusive<TOpUnionAll>(std::move(source), plan.Source({right}), plan.Pos, std::move(columns));
        auto* merge = unionAll.get();
        auto filter = MakeIntrusive<TOpFilter>(std::move(unionAll), plan.Pos, plan.Column(output));
        auto* consumer = filter.get();
        plan.Root.SetInput(std::move(filter));
        plan.Root.ComputeParents();
        consumer->Props.OutputIUs = TUnorderedIUs{output};
        consumer->Props.Analysis.LiveOut = TUnorderedIUs{output};
        consumer->Props.Cost = 1;
        consumer->Type = plan.Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);

        RebindConsumers(*old, {{left, replacement}}, plan.Root.PlanProps.Subplans);
        merge->SetChild(0, plan.Source({replacement}));

        UNIT_ASSERT_VALUES_EQUAL(merge->GetColumns().Find(output)->Inputs[0], replacement);
        UNIT_ASSERT_VALUES_EQUAL(merge->GetColumns().Find(output)->Inputs[1], right);
        UNIT_ASSERT(consumer->GetFilterExpression().GetRawInputIUs() == TUnorderedIUs{output});
        UNIT_ASSERT(!consumer->Props.OutputIUs && !consumer->Props.Analysis.LiveOut);
        UNIT_ASSERT(!consumer->Props.Cost && !consumer->Type);
    }

    Y_UNIT_TEST(ReplicateSwapPreservesLocalsAtReconvergence) {
        TPlan plan;
        const auto first = plan.Id(), second = plan.Id();
        auto hub = TReplicate::Create(plan.Source({first, second}), plan.Pos, plan.Root.PlanProps.InfoUnitRegistry);
        auto left = hub->AddOutput();
        auto right = hub->AddOutput();
        auto* port = right.get();
        const auto localFirst = *port->GetRebindings().Find(first);
        const auto localSecond = *port->GetRebindings().Find(second);
        auto join = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), plan.Pos, "Inner",
            TPairedIUs{{first, localFirst}, {second, localSecond}});
        auto* consumer = join.get();
        plan.Root.SetInput(std::move(join));
        plan.Root.ComputeParents();

        RebindConsumers(*hub->GetInput(), {{first, second}, {second, first}}, plan.Root.PlanProps.Subplans);
        hub->SetInput(plan.Source({first, second}));

        UNIT_ASSERT_VALUES_EQUAL(*port->GetRebindings().Find(second), localFirst);
        UNIT_ASSERT_VALUES_EQUAL(*port->GetRebindings().Find(first), localSecond);
        UNIT_ASSERT(consumer->JoinKeys.Contains(second, localFirst));
        UNIT_ASSERT(consumer->JoinKeys.Contains(first, localSecond));
        UNIT_ASSERT(consumer->GetOutputIUs() == (TUnorderedIUs{first, second, localFirst, localSecond}));
    }

    Y_UNIT_TEST(OnlyAffectedSubplanCaptureSourcesAndTupleChange) {
        TPlan plan;
        auto& subplans = plan.Root.PlanProps.Subplans;
        const auto outer = plan.Id(), replacement = plan.Id(), local = plan.Id();
        const auto call = plan.Id(), nestedLocal = plan.Id(), nestedCall = plan.Id();
        const auto* type = plan.Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        TDependencyIUs innerCaptures;
        innerCaptures.Add(nestedLocal, TCapturedIU{local, type});
        subplans.Add(nestedCall, MakeIntrusive<TOpAddDependencies>(MakeIntrusive<TOpEmptySource>(plan.Pos),
            plan.Pos, std::move(innerCaptures)), ESubplanType::EXPR, {}, nestedLocal);
        subplans.RefreshDependencies(nestedCall);
        TDependencyIUs captures;
        captures.Add(local, TCapturedIU{outer, type});
        auto capture = MakeIntrusive<TOpAddDependencies>(MakeIntrusive<TOpEmptySource>(plan.Pos), plan.Pos, std::move(captures));
        auto* captureOp = capture.get();
        auto nestedCaller = MakeIntrusive<TOpFilter>(std::move(capture), plan.Pos, plan.Column(nestedCall));
        auto* body = nestedCaller.get();
        subplans.Add(call, std::move(nestedCaller), ESubplanType::IN_SUBPLAN, {outer}, local);
        subplans.RefreshDependencies(call);
        auto source = plan.Source({outer});
        auto* old = source.get();
        auto caller = MakeIntrusive<TOpFilter>(std::move(source), plan.Pos, plan.Column(call));
        auto* filter = caller.get();
        plan.Root.SetInput(std::move(caller));
        plan.Root.ComputeParents();
        auto originalBody = body->GetFilterExpression().Node;

        RebindConsumers(*old, {{outer, replacement}}, subplans);
        filter->SetInput(plan.Source({replacement}));

        UNIT_ASSERT_VALUES_EQUAL(captureOp->GetDependencies().Find(local)->Outer, replacement);
        UNIT_ASSERT_VALUES_EQUAL(subplans.At(call).Tuple.Items()[0], replacement);
        UNIT_ASSERT(subplans.At(call).DependentIUs == TUnorderedIUs{replacement});
        UNIT_ASSERT_VALUES_EQUAL(*subplans.At(call).ResultIU, local);
        UNIT_ASSERT(subplans.At(nestedCall).DependentIUs == TUnorderedIUs{local});
        UNIT_ASSERT(body->GetFilterExpression().Node == originalBody);
        UNIT_ASSERT(filter->GetFilterExpression().GetRawInputIUs() == TUnorderedIUs{call});
    }
}

} // namespace NKikimr::NKqp
