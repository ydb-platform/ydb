#include "kqp_rbo_test_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
using NTests::TIdTestContext;

Y_UNIT_TEST_SUITE(KqpRboGlobalIUs) {
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
