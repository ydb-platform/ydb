#include "kqp_rbo_test_helpers.h"

#include <ydb/core/kqp/opt/rbo/copy_logical_subtree.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using NTests::TIdTestContext;

void AssertFreshGraph(IOperator& original, IOperator& copy, TPlanProps& props) {
    THashSet<const IOperator*> originals;
    TUnorderedIUs originalOutputs;
    for (const auto& item : IterateSubtree(&original)) {
        originals.insert(item.Current);
        originalOutputs.UnionWith(item.Current->GetOutputIUs());
    }
    for (const auto& item : IterateSubtree(&copy)) {
        UNIT_ASSERT(!originals.contains(item.Current));
        UNIT_ASSERT(!originalOutputs.HasAny(item.Current->GetOutputIUs()));
        UNIT_ASSERT(!item.Current->Type);
        UNIT_ASSERT(!item.Current->Props.StageId);
        UNIT_ASSERT(item.Current->Parents.empty());
    }
    NTests::AssertIdInvariants(copy, props);
}

void AssertSortKeys(const TSortIUs& original, const TSortIUs& copy, const TSubstitutions& renames) {
    UNIT_ASSERT_VALUES_EQUAL(original.Items().size(), copy.Items().size());
    for (size_t i = 0; i < original.Items().size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(copy.Items()[i].first, renames.At(original.Items()[i].first));
        UNIT_ASSERT_VALUES_EQUAL(copy.Items()[i].second.Ascending, original.Items()[i].second.Ascending);
        UNIT_ASSERT_VALUES_EQUAL(copy.Items()[i].second.NullsFirst, original.Items()[i].second.NullsFirst);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpRboCopy) {
    Y_UNIT_TEST(DuplicationRejectsVolatileExpressionsAndOpaqueFunctions) {
        for (const TString callable : {"Random", "RandomNumber", "RandomUuid", "Now", "CurrentUtcDate",
                "CurrentUtcDatetime", "CurrentUtcTimestamp", "CurrentTzDate", "CurrentTzDatetime",
                "CurrentTzTimestamp", "Udf", "ScriptUdf", "SqlCall"}) {
            TIdTestContext f;
            const auto source = f.Id(), result = f.Id();
            TMapIUs definitions;
            definitions.Add(result, MakeUnaryCallable(callable, f.Column(source)));
            auto map = MakeIntrusive<TOpMap>(f.Read({source}), f.Pos, std::move(definitions));
            UNIT_ASSERT_C(!CanDuplicateOperator(*map), callable);
            auto empty = MakeIntrusive<TOpEmptySource>(f.Pos, map->GetMapElements().At(result).GetExpression().Node);
            UNIT_ASSERT_C(!CanDuplicateOperator(*empty), callable);
        }
    }

    Y_UNIT_TEST(DuplicationChecksAggregateNamesWithoutTypes) {
        for (const auto& [function, allowed] : TVector<std::pair<TString, bool>>{
                {"count", true}, {"distinct", true}, {"sum", true}, {"min", true}, {"max", true},
                {"avg", false}, {"variance_1_1", false}, {"some", false}, {"unknown_aggregate", false}}) {
            TIdTestContext f;
            const auto source = f.Id(), result = f.Id();
            auto input = f.Read({source});
            UNIT_ASSERT(!input->Type);
            TAggregationIUs functions;
            functions.Add(result, TOpAggregationTraits{source, function});
            auto aggregate = MakeIntrusive<TOpAggregate>(input, std::move(functions), TOrderedIUs<>{},
                EOpPhase::Undefined, false, f.Pos);
            UNIT_ASSERT_VALUES_EQUAL_C(CanDuplicateOperator(*aggregate), allowed, function);
            UNIT_ASSERT_VALUES_EQUAL_C(CanDuplicateSubtree(*aggregate, f.Props.Subplans), allowed, function);
        }
    }

    Y_UNIT_TEST(DuplicatingOneOperatorDoesNotRequireDuplicatingItsInputs) {
        TIdTestContext f;
        const auto source = f.Id(), random = f.Id();
        TMapIUs definitions;
        definitions.Add(random, MakeUnaryCallable("RandomNumber", f.Column(source)));
        auto map = MakeIntrusive<TOpMap>(f.Read({source}), f.Pos, std::move(definitions));
        auto filter = MakeIntrusive<TOpFilter>(map, f.Pos, MakeBinaryPredicate("==", f.Column(source), f.Constant()));
        UNIT_ASSERT(CanDuplicateOperator(*filter));
        UNIT_ASSERT(!CanDuplicateSubtree(*filter, f.Props.Subplans));
        auto read = f.Read({source});
        UNIT_ASSERT(CanDuplicateOperator(*read));
        read->Limit = f.Constant().Node;
        UNIT_ASSERT(!CanDuplicateOperator(*read));
    }

    Y_UNIT_TEST(PushedReadProgramsCannotBeCopied) {
        for (const bool savedPredicate : {false, true}) {
            TIdTestContext f;
            const auto source = f.Id();
            auto read = f.Read({source});
            const auto predicate = MakeBinaryPredicate("==", f.Column(source), f.Constant());
            if (savedPredicate) {
                read->OriginalPredicate = predicate;
            } else {
                const auto row = f.ExprCtx.NewArgument(f.Pos, "row");
                read->OlapFilterLambda = f.ExprCtx.NewLambda(f.Pos,
                    f.ExprCtx.NewArguments(f.Pos, {row}), TExprNode::TPtr(row));
            }

            UNIT_ASSERT(!CanDuplicateOperator(*read));
            const auto size = f.Props.InfoUnitRegistry.Size();
            TSubstitutions renames;
            UNIT_ASSERT(!read->Copy(f.Props, renames));
            UNIT_ASSERT_VALUES_EQUAL(f.Props.InfoUnitRegistry.Size(), size);
            UNIT_ASSERT(renames.Keys().Empty());
        }
    }

    Y_UNIT_TEST(ReadRangesMustBeRepeatableEvenWithoutAnOlapProgram) {
        for (const bool points : {false, true}) {
            TIdTestContext f;
            auto read = f.Read({f.Id()});
            read->RangeInfo.emplace();
            auto& expression = points ? read->RangeInfo->Points : read->RangeInfo->ComputeNode;
            expression = f.Constant().Node;
            UNIT_ASSERT(CanDuplicateOperator(*read));
            expression = f.ExprCtx.NewCallable(f.Pos, "RandomNumber", {});
            UNIT_ASSERT(!CanDuplicateOperator(*read));
        }
    }

    Y_UNIT_TEST(DuplicationChecksNestedSubplansAndCountsThemInTheBudget) {
        for (const bool random : {false, true}) {
            TIdTestContext f;
            const auto innerCall = f.Id(), outerCall = f.Id(), value = f.Id(), nested = f.Id(), result = f.Id();
            TMapIUs definitions;
            definitions.Add(value, random ? MakeUnaryCallable("RandomNumber", f.Constant()) : f.Constant());
            auto inner = MakeIntrusive<TOpMap>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(definitions));
            f.Props.Subplans.Add(innerCall, inner, ESubplanType::EXPR, {}, value);
            auto outer = f.Copies(MakeIntrusive<TOpEmptySource>(f.Pos), {{nested, innerCall}});
            f.Props.Subplans.Add(outerCall, outer, ESubplanType::EXPR, {}, nested);
            auto producer = f.Copies(MakeIntrusive<TOpEmptySource>(f.Pos), {{result, outerCall}});

            UNIT_ASSERT_VALUES_EQUAL(CanDuplicateSubtree(*producer, f.Props.Subplans, 6), !random);
            UNIT_ASSERT(!CanDuplicateSubtree(*producer, f.Props.Subplans, 5));
        }
    }

    Y_UNIT_TEST(CopiesSharedProducerOnceIncludingSecondaryFirstTraversal) {
        for (const bool secondaryFirst : {false, true}) {
            TIdTestContext f;
            const auto key = f.Id("Key"), value = f.Id("Value"), output = f.Id("result");
            auto read = f.Read({key, value});
            read->Alias = "table";
            read->SortDir = ESortDir::Desc;
            read->Limit = f.Constant().Node;
            read->Props.StageId = 7;
            f.SetType(*read);
            auto hub = TReplicate::Create(read, f.Pos, f.Props.InfoUnitRegistry);
            auto left = hub->AddOutput(), right = hub->AddOutput(), third = hub->AddOutput();
            const auto rightKey = right->GetRebindings().At(key);
            const auto thirdKey = third->GetRebindings().At(key);
            if (secondaryFirst) {
                left.Swap(right);
            }
            TJoinIUs keys{{secondaryFirst ? rightKey : key, secondaryFirst ? key : rightKey, true}};
            const auto condition = MakeBinaryPredicate("==", f.Column(key), f.Column(rightKey));
            auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Full", keys, TVector<TExpression>{condition});
            auto root = f.Root(f.Union(join, third, {{output, key, thirdKey}}, true), {{output, "result"}});
            const auto before = root->PlanToString(f.ExprCtx);

            TSubstitutions renames;
            auto copy = root->GetInput()->Copy(root->PlanProps.InfoUnitRegistry, renames);
            UNIT_ASSERT(copy);
            AssertFreshGraph(*root->GetInput(), *copy, root->PlanProps);
            auto& merge = CastOperator<TOpUnionAll>(*copy);
            auto& copiedJoin = CastOperator<TOpJoin>(*merge.GetInput(0));
            auto& copiedLeft = CastOperator<TOpReplicate>(*copiedJoin.GetLeftInput());
            auto& copiedRight = CastOperator<TOpReplicate>(*copiedJoin.GetRightInput());
            auto& copiedThird = CastOperator<TOpReplicate>(*merge.GetInput(1));
            UNIT_ASSERT(&copiedLeft.GetReplicate() == &copiedRight.GetReplicate());
            UNIT_ASSERT(&copiedLeft.GetReplicate() == &copiedThird.GetReplicate());
            UNIT_ASSERT(&copiedLeft.GetReplicate() != hub.Get());
            UNIT_ASSERT_VALUES_EQUAL(copiedLeft.GetIndex(), left->GetIndex());
            UNIT_ASSERT_VALUES_EQUAL(copiedRight.GetIndex(), right->GetIndex());
            UNIT_ASSERT(copiedLeft.GetInput() == copiedRight.GetInput());
            auto& copiedRead = CastOperator<TOpRead>(*copiedLeft.GetInput());
            UNIT_ASSERT_VALUES_EQUAL(copiedRead.Alias, read->Alias);
            UNIT_ASSERT(copiedRead.SortDir == read->SortDir);
            UNIT_ASSERT(copiedRead.Limit == read->Limit);
            UNIT_ASSERT(copiedRead.TableCallable == read->TableCallable);
            UNIT_ASSERT(copiedRead.StorageType == read->StorageType);
            UNIT_ASSERT(copiedJoin.JoinKind == "Full");
            UNIT_ASSERT(copiedJoin.JoinKeys.Items()[0].EqualNulls);
            UNIT_ASSERT_VALUES_EQUAL(copiedJoin.JoinKeys.Items()[0].first, renames.At(keys.Items()[0].first));
            UNIT_ASSERT_VALUES_EQUAL(copiedJoin.JoinKeys.Items()[0].second, renames.At(keys.Items()[0].second));
            UNIT_ASSERT_VALUES_EQUAL(copiedJoin.JoinFilters.size(), 1);
            UNIT_ASSERT(copiedJoin.JoinFilters[0].GetRawInputIUs() == (TUnorderedIUs{renames.At(key), renames.At(rightKey)}));
            UNIT_ASSERT(merge.Ordered);
            const auto& unionInputs = merge.GetColumns().At(renames.At(output)).Inputs;
            UNIT_ASSERT_VALUES_EQUAL(unionInputs.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(unionInputs[0], renames.At(key));
            UNIT_ASSERT_VALUES_EQUAL(unionInputs[1], renames.At(thirdKey));
            size_t sources = 0;
            for (const auto& item : IterateSubtree(copy.Get())) {
                sources += item.Current->Kind == EOperator::Source;
            }
            UNIT_ASSERT_VALUES_EQUAL(sources, 1);
            UNIT_ASSERT_VALUES_EQUAL(root->PlanToString(f.ExprCtx), before);
        }
    }

    Y_UNIT_TEST(CopiesSecondaryOnlyPortWithoutMergingItsNamespaceWithTheProducer) {
        TIdTestContext f;
        const auto source = f.Id("Key");
        auto hub = TReplicate::Create(f.Read({source}), f.Pos, f.Props.InfoUnitRegistry);
        auto primary = hub->AddOutput(), unused = hub->AddOutput(), retained = hub->AddOutput();
        const auto result = retained->GetRebindings().At(source);
        auto root = f.Root(retained, {{result, "result"}});
        const auto before = root->PlanToString(f.ExprCtx);

        TSubstitutions renames;
        auto copy = retained->Copy(root->PlanProps.InfoUnitRegistry, renames);
        UNIT_ASSERT(copy);
        AssertFreshGraph(*retained, *copy, root->PlanProps);
        auto& port = CastOperator<TOpReplicate>(*copy);
        UNIT_ASSERT_VALUES_EQUAL(port.GetIndex(), retained->GetIndex());
        UNIT_ASSERT(!port.IsPrimary());
        UNIT_ASSERT(port.GetOutputIUs() == TUnorderedIUs{renames.At(result)});
        UNIT_ASSERT(port.GetInput()->GetOutputIUs() == TUnorderedIUs{renames.At(source)});
        UNIT_ASSERT_VALUES_EQUAL(port.GetRebindings().At(renames.At(source)), renames.At(result));
        UNIT_ASSERT(renames.At(source) != renames.At(result));
        UNIT_ASSERT_VALUES_EQUAL(root->PlanToString(f.ExprCtx), before);
    }

    Y_UNIT_TEST(RequestedDefinitionsDoNotChangePhysicalSourceColumns) {
        for (const bool parameterSource : {false, true}) {
            TIdTestContext f;
            const auto key = f.Id("Key"), value = f.Id("Value"), mapped = f.Id("mapped");
            const auto wantedKey = f.Id("Key"), wrongValueLabel = f.Id("Alias"), wantedMap = f.Id("renamed_result");
            TIntrusivePtr<IOperator> source;
            if (parameterSource) {
                const auto rows = f.ExprCtx.NewArgument(f.Pos, "rows");
                source = MakeIntrusive<TOpEmptySource>(f.Pos, rows, TUnorderedIUs{key, value});
            } else {
                source = f.Read({key, value});
            }
            auto map = f.Copies(source, {{mapped, value}});
            TSubstitutions renames{{key, wantedKey}, {value, wrongValueLabel}, {mapped, wantedMap}};

            auto copy = map->Copy(f.Props.InfoUnitRegistry, renames);
            UNIT_ASSERT(copy);
            auto& copiedMap = CastOperator<TOpMap>(*copy);
            UNIT_ASSERT_VALUES_EQUAL(renames.At(key), wantedKey);
            UNIT_ASSERT(renames.At(value) != wrongValueLabel);
            UNIT_ASSERT_VALUES_EQUAL(f.Props.InfoUnitRegistry.Get(renames.At(value)).GetColumnName(), "Value");
            UNIT_ASSERT(copiedMap.GetInput()->GetOutputIUs() == (TUnorderedIUs{wantedKey, renames.At(value)}));
            UNIT_ASSERT_VALUES_EQUAL(copiedMap.GetMapElements().At(wantedMap).GetColumnAccess(), renames.At(value));
            UNIT_ASSERT(!copy->GetOutputIUs().Contains(wrongValueLabel));
            UNIT_ASSERT(source->GetOutputIUs() == (TUnorderedIUs{key, value}));
            UNIT_ASSERT_VALUES_EQUAL(map->GetMapElements().At(mapped).GetColumnAccess(), value);
            if (parameterSource) {
                UNIT_ASSERT(CastOperator<TOpEmptySource>(*copiedMap.GetInput()).Input == CastOperator<TOpEmptySource>(*source).Input);
            }
            NTests::AssertIdInvariants(*copy, f.Props);
        }
    }

    Y_UNIT_TEST(CopiesBoundAndExternalCapturesInTheirOwnScopes) {
        for (const bool explicitDomainColumn : {false, true}) {
            TIdTestContext f;
            const auto parameter = f.Id("parameter"), external = f.Id("external"), replacement = f.Id("external_copy");
            const auto domainColumn = explicitDomainColumn ? f.Id("domain") : parameter;
            const auto value = f.Id("value"), local = f.Id("local"), externalLocal = f.Id("external_local"), result = f.Id("result");
            const auto* type = f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
            TDependencyIUs captures;
            captures.Add(local, TCapturedIU{parameter, type});
            captures.Add(externalLocal, TCapturedIU{external, type});
            auto capture = MakeIntrusive<TOpAddDependencies>(f.Read({value}), f.Pos, std::move(captures));
            TSubstitutions domainColumns;
            if (explicitDomainColumn) {
                domainColumns.Add(parameter, domainColumn);
            }
            auto dependent = MakeIntrusive<TOpDependentJoin>(f.Read({domainColumn}), f.Copies(capture, {{result, local}}),
                TUnorderedIUs{parameter}, f.Pos, domainColumns);
            auto root = f.Root(dependent, {{result, "result"}});
            const auto before = root->PlanToString(f.ExprCtx);
            TSubstitutions renames{{external, replacement}};

            auto copy = dependent->Copy(root->PlanProps.InfoUnitRegistry, renames);
            UNIT_ASSERT(copy);
            AssertFreshGraph(*dependent, *copy, root->PlanProps);
            auto& copiedJoin = CastOperator<TOpDependentJoin>(*copy);
            auto& copiedMap = CastOperator<TOpMap>(*copiedJoin.GetInput());
            auto& copiedCapture = CastOperator<TOpAddDependencies>(*copiedMap.GetInput());
            const auto copiedParameter = Substitute(parameter, renames);
            UNIT_ASSERT(copiedJoin.Dependencies == TUnorderedIUs{copiedParameter});
            UNIT_ASSERT_VALUES_EQUAL(copiedJoin.GetDomainColumn(copiedParameter), renames.At(domainColumn));
            UNIT_ASSERT_VALUES_EQUAL(copiedCapture.GetDependencies().At(renames.At(local)).Outer, copiedParameter);
            UNIT_ASSERT_VALUES_EQUAL(copiedCapture.GetDependencies().At(renames.At(externalLocal)).Outer, replacement);
            UNIT_ASSERT(copiedCapture.GetDependencies().At(renames.At(local)).Type == type);
            UNIT_ASSERT_VALUES_EQUAL(copiedMap.GetMapElements().At(renames.At(result)).GetColumnAccess(), renames.At(local));
            UNIT_ASSERT_VALUES_EQUAL(capture->GetDependencies().At(local).Outer, parameter);
            UNIT_ASSERT_VALUES_EQUAL(capture->GetDependencies().At(externalLocal).Outer, external);
            UNIT_ASSERT_VALUES_EQUAL(root->PlanToString(f.ExprCtx), before);
        }
    }

    Y_UNIT_TEST(CopiesGroupingWindowAndOrderingContractsTogether) {
        TIdTestContext f;
        const auto key = f.Id("Key"), value = f.Id("Value"), mapped = f.Id("mapped"), total = f.Id("total");
        const auto groupedKey = f.Id("grouped_key"), groupedTotal = f.Id("grouped_total"), indicator = f.Id("indicator");
        const auto running = f.Id("running");
        auto map = f.Copies(f.Read({key, value}), {{mapped, value}});
        auto filter = MakeIntrusive<TOpFilter>(map, f.Pos, TPhysicalOpProps{},
            MakeBinaryPredicate(">", f.Column(mapped), f.Constant()), true);
        TAggregationIUs aggregations;
        aggregations.Add(total, TOpAggregationTraits{mapped, "sum", true, true});
        auto aggregate = MakeIntrusive<TOpAggregate>(filter, std::move(aggregations), TOrderedIUs<>{key},
            EOpPhase::Final, false, f.Pos);
        TMappedIUs<TInfoUnitId> columns{{groupedKey, key}, {groupedTotal, total}};
        TMappedIUs<TInfoUnitId> indicators{{indicator, key}};
        auto grouping = MakeIntrusive<TOpGroupingSets>(aggregate, TVector<TUnorderedIUs>{{key}, {}},
            std::move(columns), f.Pos, std::move(indicators));
        TWindowIUs functions;
        functions.Add(running, TOpWindowFunc{"sum", EWindowFuncKind::Aggregate, {groupedTotal}});
        TSortIUs ordering{{groupedTotal, {false, true}}, {groupedKey, {true, false}}, {groupedTotal, {true, true}}};
        TOpWindowFrame frame{EWindowFrameType::Groups, EWindowFrameBound::Preceding, 3, EWindowFrameBound::Following, 2};
        auto window = MakeIntrusive<TOpWindow>(grouping, f.Pos, std::move(functions), TOrderedIUs<>{groupedKey}, ordering, frame);
        auto sort = MakeIntrusive<TOpSort>(window, f.Pos, TPhysicalOpProps{}, ordering, f.Column(running), EOpPhase::Intermediate);
        auto limit = MakeIntrusive<TOpLimit>(sort, f.Pos, f.Column(running), f.Column(indicator), EOpPhase::Final);
        auto root = f.Root(limit, {{running, "result"}});
        const auto before = root->PlanToString(f.ExprCtx);

        TSubstitutions renames;
        auto copy = limit->Copy(root->PlanProps.InfoUnitRegistry, renames);
        UNIT_ASSERT(copy);
        AssertFreshGraph(*limit, *copy, root->PlanProps);
        auto& copiedLimit = CastOperator<TOpLimit>(*copy);
        auto& copiedSort = CastOperator<TOpSort>(*copiedLimit.GetInput());
        auto& copiedWindow = CastOperator<TOpWindow>(*copiedSort.GetInput());
        auto& copiedGrouping = CastOperator<TOpGroupingSets>(*copiedWindow.GetInput());
        auto& copiedAggregate = CastOperator<TOpAggregate>(*copiedGrouping.GetInput());
        auto& copiedFilter = CastOperator<TOpFilter>(*copiedAggregate.GetInput());
        UNIT_ASSERT(copiedLimit.GetLimitPhase() == EOpPhase::Final);
        UNIT_ASSERT(copiedLimit.GetLimitCond().GetRawInputIUs() == TUnorderedIUs{renames.At(running)});
        UNIT_ASSERT(copiedLimit.GetOffsetCond()->GetRawInputIUs() == TUnorderedIUs{renames.At(indicator)});
        UNIT_ASSERT(copiedSort.GetSortPhase() == EOpPhase::Intermediate);
        UNIT_ASSERT(copiedSort.LimitCond->GetRawInputIUs() == TUnorderedIUs{renames.At(running)});
        AssertSortKeys(ordering, copiedSort.GetSortElements(), renames);
        AssertSortKeys(ordering, copiedWindow.GetSortElements(), renames);
        UNIT_ASSERT(copiedWindow.GetPartitionKeys().Items() == TOrderedIUs<>{renames.At(groupedKey)}.Items());
        const auto& function = copiedWindow.GetWindowFuncs().At(renames.At(running));
        UNIT_ASSERT_VALUES_EQUAL(function.Function, "sum");
        UNIT_ASSERT(function.Kind == EWindowFuncKind::Aggregate);
        UNIT_ASSERT(function.Arguments.Items() == TOrderedIUs<>{renames.At(groupedTotal)}.Items());
        UNIT_ASSERT(copiedWindow.GetFrame().Type == frame.Type);
        UNIT_ASSERT(copiedWindow.GetFrame().BeginKind == frame.BeginKind);
        UNIT_ASSERT_VALUES_EQUAL(copiedWindow.GetFrame().BeginValue, frame.BeginValue);
        UNIT_ASSERT(copiedWindow.GetFrame().EndKind == frame.EndKind);
        UNIT_ASSERT_VALUES_EQUAL(copiedWindow.GetFrame().EndValue, frame.EndValue);
        UNIT_ASSERT_VALUES_EQUAL(copiedGrouping.GetGroupingSets().size(), 2);
        UNIT_ASSERT(copiedGrouping.GetGroupingSets()[0] == TUnorderedIUs{renames.At(key)});
        UNIT_ASSERT(copiedGrouping.GetGroupingSets()[1].Empty());
        UNIT_ASSERT_VALUES_EQUAL(copiedGrouping.GetColumns().At(renames.At(groupedKey)), renames.At(key));
        UNIT_ASSERT_VALUES_EQUAL(copiedGrouping.GetColumns().At(renames.At(groupedTotal)), renames.At(total));
        UNIT_ASSERT_VALUES_EQUAL(copiedGrouping.GetGroupingIndicators().At(renames.At(indicator)), renames.At(key));
        const auto& traits = copiedAggregate.GetAggregationTraits().At(renames.At(total));
        UNIT_ASSERT_VALUES_EQUAL(traits.Input, renames.At(mapped));
        UNIT_ASSERT_VALUES_EQUAL(traits.AggFunction, "sum");
        UNIT_ASSERT(traits.Distinct && traits.Unwrap);
        UNIT_ASSERT(copiedAggregate.GetAggregationPhase() == EOpPhase::Final);
        UNIT_ASSERT(copiedAggregate.GetKeyColumns().Unordered() == TUnorderedIUs{renames.At(key)});
        UNIT_ASSERT(copiedFilter.PartiallyPushedDown);
        UNIT_ASSERT(copiedFilter.GetFilterExpression().GetRawInputIUs() == TUnorderedIUs{renames.At(mapped)});
        UNIT_ASSERT_VALUES_EQUAL(root->PlanToString(f.ExprCtx), before);
    }

    Y_UNIT_TEST(CopyWithInputsRebuildsOnlyTheRequestedOperator) {
        TIdTestContext f;
        const auto source = f.Id("source"), result = f.Id("result"), replacement = f.Id("replacement");
        auto original = f.Copies(f.Read({source}), {{result, source}});
        auto input = f.Read({replacement});
        TSubstitutions renames{{source, replacement}};
        const auto previousSize = f.Props.InfoUnitRegistry.Size();

        auto copy = original->CopyWithInputs({input}, f.Props.InfoUnitRegistry, renames);
        UNIT_ASSERT(copy);
        auto& map = CastOperator<TOpMap>(*copy);
        UNIT_ASSERT(map.GetInput() == input);
        UNIT_ASSERT_VALUES_EQUAL(f.Props.InfoUnitRegistry.Size(), previousSize + 1);
        UNIT_ASSERT_VALUES_EQUAL(map.GetMapElements().At(renames.At(result)).GetColumnAccess(), replacement);
        UNIT_ASSERT_VALUES_EQUAL(original->GetMapElements().At(result).GetColumnAccess(), source);
        UNIT_ASSERT(original->GetInput()->GetOutputIUs() == TUnorderedIUs{source});
        NTests::AssertIdInvariants(*copy, f.Props);
    }

    Y_UNIT_TEST(CopiesEffectBindingsWithoutChangingStorageNames) {
        TIdTestContext f;
        const auto source = f.Id("input"), result = f.Id("returned");
        auto read = f.Read({source});
        TEffectOptions options;
        options.Columns = TVector<TString>{"Key"};
        options.ReturningColumns = TVector<TString>{"Key"};
        options.OnConflict = "revert";
        options.IsBatch = true;
        auto effect = MakeIntrusive<TOpTableEffect>(read, f.Pos, read->TableCallable, EEffectType::UpsertRows,
            options, TOrderedIUs<TString>{{source, "Key"}}, TOrderedIUs<TString>{{result, "Key"}, {result, "KeyAgain"}});

        TSubstitutions renames;
        auto copy = effect->Copy(f.Props.InfoUnitRegistry, renames);
        UNIT_ASSERT(copy);
        AssertFreshGraph(*effect, *copy, f.Props);
        auto& copiedEffect = CastOperator<TOpTableEffect>(*copy);
        UNIT_ASSERT(copiedEffect.Table == effect->Table);
        UNIT_ASSERT(copiedEffect.EffectType == effect->EffectType);
        UNIT_ASSERT(copiedEffect.Options.Columns == options.Columns);
        UNIT_ASSERT(copiedEffect.Options.ReturningColumns == options.ReturningColumns);
        UNIT_ASSERT(copiedEffect.Options.OnConflict == options.OnConflict);
        UNIT_ASSERT(copiedEffect.Options.IsBatch == options.IsBatch);
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetColumns().Items()[0].first, renames.At(source));
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetColumns().Items()[0].second, "Key");
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetReturningColumns().Items().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetReturningColumns().Items()[0].first, renames.At(result));
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetReturningColumns().Items()[1].first, renames.At(result));
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetReturningColumns().Items()[0].second, "Key");
        UNIT_ASSERT_VALUES_EQUAL(copiedEffect.GetReturningColumns().Items()[1].second, "KeyAgain");
        UNIT_ASSERT_VALUES_EQUAL(effect->GetColumns().Items()[0].first, source);
        UNIT_ASSERT(effect->GetOutputIUs() == TUnorderedIUs{result});
    }

    Y_UNIT_TEST(ReplicateExpansionDoesNotDuplicateEffects) {
        TIdTestContext f;
        f.Config->_KqpEnableSpilling = false;
        const auto key = f.Id("Key"), result = f.Id("result");
        auto read = f.Read({key});
        auto effect = MakeIntrusive<TOpTableEffect>(read, f.Pos, read->TableCallable, EEffectType::InsertRows,
            TEffectOptions{}, TOrderedIUs<TString>{{key, "Key"}}, TOrderedIUs<TString>{{result, "Key"}});
        auto hub = TReplicate::Create(effect, f.Pos, f.Props.InfoUnitRegistry);
        auto left = hub->AddOutput(), right = hub->AddOutput();
        auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{});
        auto root = f.Root(join, {{result, "result"}});
        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TExpandReplicateRule>());

        TRuleBasedStage("Expand Replicate", std::move(rules)).RunStage(*root, f.RboCtx);

        UNIT_ASSERT(join->GetLeftInput() == left);
        UNIT_ASSERT(join->GetRightInput() == right);
        UNIT_ASSERT(hub->GetInput() == effect);
        NTests::AssertIdInvariants(*root, root->PlanProps);
    }

    Y_UNIT_TEST(CopiesSubplanCallsWithTheirOwnCapturesAndResultBindings) {
        for (const auto kind : {ESubplanType::EXPR, ESubplanType::IN_SUBPLAN, ESubplanType::EXISTS}) {
            TIdTestContext f;
            const auto source = f.Id("source"), local = f.Id("local"), call = f.Id("call"), result = f.Id("result");
            TDependencyIUs captures;
            captures.Add(local, TCapturedIU{source, f.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)});
            auto producer = MakeIntrusive<TOpAddDependencies>(MakeIntrusive<TOpEmptySource>(f.Pos), f.Pos, std::move(captures));
            const auto tuple = kind == ESubplanType::IN_SUBPLAN ? TOrderedIUs<>{source} : TOrderedIUs<>{};
            f.Props.Subplans.Add(call, producer, kind, tuple, kind == ESubplanType::EXISTS ? std::nullopt : std::optional{local});
            f.Props.Subplans.RefreshDependencies(call);
            auto root = f.Root(f.Copies(f.Read({source}), {{result, call}}), {{result, "result"}});

            TSubstitutions renames;
            auto copy = root->GetInput()->Copy(root->PlanProps, renames);
            UNIT_ASSERT(copy);
            auto& map = CastOperator<TOpMap>(*copy);
            const auto copiedCall = map.GetMapElements().At(renames.At(result)).GetColumnAccess();
            UNIT_ASSERT(copiedCall != call);
            UNIT_ASSERT_VALUES_EQUAL(copiedCall, renames.At(call));
            const auto& entry = root->PlanProps.Subplans.At(copiedCall);
            UNIT_ASSERT(entry.Type == kind);
            UNIT_ASSERT(entry.Plan != producer);
            UNIT_ASSERT(entry.DependentIUs == TUnorderedIUs{renames.At(source)});
            auto& copiedCaptures = CastOperator<TOpAddDependencies>(*entry.Plan);
            UNIT_ASSERT_VALUES_EQUAL(copiedCaptures.GetDependencies().Items().size(), 1);
            const auto copiedLocal = copiedCaptures.GetDependencies().Items().begin()->first;
            UNIT_ASSERT(copiedLocal != local);
            UNIT_ASSERT_VALUES_EQUAL(copiedCaptures.GetDependencies().At(copiedLocal).Outer, renames.At(source));
            UNIT_ASSERT_VALUES_EQUAL(entry.Tuple.Items().size(), tuple.Items().size());
            if (kind == ESubplanType::IN_SUBPLAN) {
                UNIT_ASSERT_VALUES_EQUAL(entry.Tuple.Items()[0], renames.At(source));
            }
            if (kind == ESubplanType::EXISTS) {
                UNIT_ASSERT(!entry.ResultIU);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(*entry.ResultIU, copiedLocal);
            }
            const auto& originalEntry = root->PlanProps.Subplans.At(call);
            UNIT_ASSERT(originalEntry.Plan == producer);
            UNIT_ASSERT(originalEntry.DependentIUs == TUnorderedIUs{source});
            UNIT_ASSERT_VALUES_EQUAL(producer->GetDependencies().At(local).Outer, source);
            UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpMap>(*root->GetInput()).GetMapElements().At(result).GetColumnAccess(), call);
            NTests::AssertIdInvariants(*copy, root->PlanProps);
        }
    }
}

} // namespace NKikimr::NKqp
