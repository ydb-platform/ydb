#include <ydb/core/kqp/ut/rbo/kqp_rbo_copy_helpers.h>
#include <ydb/core/kqp/opt/rbo/copy_logical_subtree.h>

namespace NKikimr::NKqp {

using NTests::TIdTestContext;
using NTests::AssertFreshGraph;

Y_UNIT_TEST_SUITE(KqpRboCopy) {
    Y_UNIT_TEST(SequencerIsAnAllocationBarrier) {
        TIdTestContext f;
        const auto source = f.Id("input"), generated = f.Id("id"), result = f.Id("dependent");
        auto read = f.Read({source});
        auto sequencer = MakeIntrusive<TOpSequencer>(read, f.Pos, read->TableCallable,
            TOrderedIUs<TString>{{source, "value"}}, TOrderedIUs<TString>{{generated, "id"}});
        TMapIUs expressions;
        expressions.Add(result, MakeBinaryPredicate("+", f.Column(generated), f.Constant()));
        auto map = MakeIntrusive<TOpMap>(sequencer, f.Pos, std::move(expressions));
        map->NeedToPush = true;
        auto root = f.Root(map, {{result, "dependent"}});
        UNIT_ASSERT(sequencer->GetOutputIUs() == (TUnorderedIUs{source, generated}));
        UNIT_ASSERT(sequencer->GetUsedIUs(root->PlanProps) == TUnorderedIUs{source});
        NTests::AssertIdInvariants(*root, root->PlanProps);

        UNIT_ASSERT(!CanDuplicateOperator(*sequencer));
        UNIT_ASSERT(!CanDuplicateSubtree(*map, root->PlanProps.Subplans));
        TSubstitutions copyRenames;
        UNIT_ASSERT(!map->Copy(root->PlanProps.InfoUnitRegistry, copyRenames));
        TPushMapElementsThroughInputRule push;
        UNIT_ASSERT(!push.QuickMatch(map));

        // Even when only the generated expression is requested, pruning must
        // retain the sequencer's input row schema and the allocation itself.
        TGlobalPruningStage("prune").RunStage(*root, f.RboCtx);
        UNIT_ASSERT(map->GetInput() == sequencer);
        UNIT_ASSERT(read->GetOutputIUs().Contains(source));
        UNIT_ASSERT(sequencer->GetGeneratedColumns().Unordered().Contains(generated));

        const auto replacement = root->PlanProps.InfoUnitRegistry.AddCopy(source);
        TSubstitutions renames;
        renames.Add(source, replacement);
        sequencer->RenameUsedIUs(renames);
        UNIT_ASSERT_VALUES_EQUAL(sequencer->GetInputColumns().Items()[0].first, replacement);
        UNIT_ASSERT_VALUES_EQUAL(sequencer->GetInputColumns().Items()[0].second, "value");
        UNIT_ASSERT_VALUES_EQUAL(sequencer->GetGeneratedColumns().Items()[0].first, generated);
    }

    Y_UNIT_TEST(CopiesEffectBindingsWithoutChangingStorageNames) {
        TIdTestContext f;
        const auto source = f.Id("input"), result = f.Id("returned");
        auto read = f.Read({source});
        TEffectOptions options;
        options.Columns = TOrderedIUs<TString>{{source, "Key"}};
        options.ReturningColumns = TOrderedIUs<TString>{{result, "Key"}, {result, "KeyAgain"}};
        options.DefaultColumns = TUnorderedIUs{source};
        options.OnConflict = "revert";
        options.IsBatch = true;
        auto effect = MakeIntrusive<TOpTableEffect>(read, f.Pos, read->TableCallable, EEffectType::UpsertRows,
            options);

        TSubstitutions renames;
        auto copy = effect->Copy(f.Props.InfoUnitRegistry, renames);
        UNIT_ASSERT(copy);
        AssertFreshGraph(*effect, *copy, f.Props);
        auto& copiedEffect = CastOperator<TOpTableEffect>(*copy);
        UNIT_ASSERT(copiedEffect.Table == effect->Table);
        UNIT_ASSERT(copiedEffect.EffectType == effect->EffectType);
        UNIT_ASSERT(copiedEffect.Options.DefaultColumns == TUnorderedIUs{renames.At(source)});
        UNIT_ASSERT(effect->Options.DefaultColumns == TUnorderedIUs{source});
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
        TEffectOptions options;
        options.Columns = TOrderedIUs<TString>{{key, "Key"}};
        options.ReturningColumns = TOrderedIUs<TString>{{result, "Key"}};
        auto effect = MakeIntrusive<TOpTableEffect>(read, f.Pos, read->TableCallable, EEffectType::InsertRows,
            std::move(options));
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
}

} // namespace NKikimr::NKqp
