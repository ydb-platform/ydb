#include <ydb/core/tablet_flat/flat_comp.h>
#include <ydb/core/tablet_flat/flat_compaction_context.h>
#include <ydb/core/tablet_flat/flat_compaction_prepare.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <initializer_list>

namespace NKikimr::NTable {
namespace {

    struct TFixture {
        NTest::TLayoutCook Layout;
        ui32 Serial = 1;

        TFixture() {
            Layout.Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(0, 8, NScheme::NTypeIds::Uint32)
                .Key({0});
        }

        TPartView Part(i64 epoch, ui32 first = 10, ui32 last = 30, bool bloom = false) {
            NPage::TConf conf;
            if (bloom) {
                conf.ByKeyFilterPrefixes.push_back({1, 0.000001});
            }
            NTest::TPartCook cook(Layout, conf,
                TLogoBlobID(1, 1, Serial++, 1, 0, 0), TEpoch::FromIndex(epoch));
            cook.AddN(first, first);
            if (last != first) {
                cook.AddN(last, last);
            }
            return cook.Finish().ToPartView();
        }

        TIntrusiveConstPtr<TColdPart> Cold(i64 epoch) {
            return new TColdPart(TLogoBlobID(2, 1, Serial++, 1, 0, 0), TEpoch::FromIndex(epoch));
        }
    };

    NLocalDb::TCompactionPolicyPtr TieredPolicy() {
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        const auto generation = policy->Generations.front();
        policy->Generations.assign(6, generation);
        const ui32 tiers[] = {0, 0, 42, 42, 7, 7};
        for (size_t index = 0; index < policy->Generations.size(); ++index) {
            policy->Generations[index].TierId = tiers[index];
        }
        return policy;
    }

    TIntrusiveConstPtr<TCompactionContext> MakeContext(
            TVector<TCompactionPartInfo> parts, std::initializer_list<TLogoBlobID> selected,
            TVector<TEpoch> extraEpochs = {}) {
        const TVector<TLogoBlobID> labels(selected);
        return TCompactionContext::Build(std::move(parts), labels, extraEpochs);
    }

    bool Contains(TConstArrayRef<TCompactionPartInfo> parts, const TLogoBlobID& label) {
        return std::any_of(parts.begin(), parts.end(), [&](const auto& part) {
            return part.Label() == label;
        });
    }

    bool Has(NPage::IKeySpace& mask, ui32 value) {
        const auto key = TCell::Make(value);
        return mask.HasKey({&key, 1});
    }

    TIntrusiveConstPtr<TSlices> Slice(ui32 first, ui32 last,
            bool firstInclusive = true, bool lastInclusive = true) {
        TIntrusivePtr<TSlices> result = new TSlices;
        const TVector<TCell> firstCells{TCell::Make(first)};
        const TVector<TCell> lastCells{TCell::Make(last)};
        result->emplace_back(TSerializedCellVec(firstCells), TSerializedCellVec(lastCells),
            0, 10, firstInclusive, lastInclusive);
        return result;
    }

    TPartView WithBlooms(TPartView part,
            TVector<std::pair<ui32, TIntrusiveConstPtr<NPage::TBloom>>> blooms) {
        const auto* original = part.As<NTest::TPartStore>();
        UNIT_ASSERT(original);
        TPart::TParams params{
            .Epoch = original->Epoch,
            .Scheme = original->Scheme,
            .IndexPages = original->IndexPages,
            .Blobs = original->Blobs,
            .ByKeyPrefixes = std::move(blooms),
            .Large = original->Large,
            .Small = original->Small,
            .IndexesRawSize = original->IndexesRawSize,
            .MinRowVersion = original->MinRowVersion,
            .MaxRowVersion = original->MaxRowVersion,
            .GarbageStats = original->GarbageStats,
            .TxIdStats = original->TxIdStats,
        };
        part.Part = new NTest::TPartStore(original->Store, original->Label,
            std::move(params), original->Stat, original->Slices);
        return part;
    }

}

Y_UNIT_TEST_SUITE(TCompactionContextTest) {

    Y_UNIT_TEST(ClosesTheGlobalIntervalAcrossTiers) {
        TFixture f;
        auto old = f.Part(5);
        auto first = f.Part(10);
        auto middle = f.Part(20);
        auto last = f.Part(30);
        auto newer = f.Part(40);
        auto context = MakeContext({{old, 255, 0}, {last, 5, 7}, {middle, 2, 0},
            {newer, 1, 0}, {first, 4, 42}}, {first->Label, last->Label});

        UNIT_ASSERT(context->Epochs);
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Min, TEpoch::FromIndex(10));
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Max, TEpoch::FromIndex(30));
        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT(Contains(context->Inputs, middle->Label));
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.size(), 2);
        UNIT_ASSERT(Contains(context->Remaining, old->Label));
        UNIT_ASSERT(Contains(context->Remaining, newer->Label));
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.size(), 3);
        UNIT_ASSERT(context->RequiresTierAware());
    }

    Y_UNIT_TEST(IncludesAllEqualEpochPartsAtBothBounds) {
        TFixture f;
        auto lower = f.Part(10);
        auto lowerPeer = f.Part(10);
        auto upper = f.Part(30);
        auto upperPeer = f.Cold(30);
        auto context = MakeContext({{lower, 1, 0}, {lowerPeer, 3, 42},
            {upper, 4, 7}, {upperPeer, 255, 0}}, {lower->Label, upper->Label});

        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 4);
        UNIT_ASSERT(context->Remaining.empty());
        UNIT_ASSERT(Contains(context->Inputs, lowerPeer->Label));
        UNIT_ASSERT(Contains(context->Inputs, upperPeer->Label));
    }

    Y_UNIT_TEST(AColdCompactionIncludesTheNewerHotVersion) {
        TFixture f;
        auto oldKey = f.Part(10, 1, 1);
        auto newKey = f.Part(20, 1, 1);
        auto otherKey = f.Part(30, 2, 2);
        auto context = MakeContext({{oldKey, 4, 42}, {newKey, 2, 0}, {otherKey, 3, 42}},
            {oldKey->Label, otherKey->Label});

        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT(Contains(context->Inputs, newKey->Label));
        UNIT_ASSERT(context->RequiresTierAware());
    }

    Y_UNIT_TEST(IncludesTheHotPeerOfAColdOutputEpoch) {
        TFixture f;
        auto oldKey = f.Part(10, 1, 1);
        auto hotOutput = f.Part(30, 1, 1);
        auto coldOutput = f.Part(30, 2, 2);
        auto context = MakeContext({{oldKey, 4, 42}, {hotOutput, 2, 0}, {coldOutput, 3, 42}},
            {oldKey->Label, coldOutput->Label});

        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT(Contains(context->Inputs, hotOutput->Label));
    }

    Y_UNIT_TEST(OnlyCompactsTheClosedInterval) {
        TFixture f;
        auto older = f.Cold(9);
        auto chosen = f.Part(10);
        auto newer = f.Cold(11);
        auto context = MakeContext({{older, 255, 0}, {chosen, 3, 42}, {newer, 1, 0}},
            {chosen->Label});

        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.size(), 2);
        UNIT_ASSERT(!context->RequiresTierAware());
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.front(), 42);
    }

    Y_UNIT_TEST(EmptyInputsDoNotSelectTheWholeTable) {
        TFixture f;
        auto part = f.Part(10);
        auto context = MakeContext({{part, 255, 0}}, {});

        UNIT_ASSERT(!context->Epochs);
        UNIT_ASSERT(context->Inputs.empty());
        UNIT_ASSERT(context->InputTiers.empty());
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.size(), 1);
        UNIT_ASSERT(!context->RequiresTierAware());
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        context->ValidateSubset(subset);
    }

    Y_UNIT_TEST(RejectsUnknownInputsAndDuplicateInventory) {
        TFixture f;
        auto part = f.Part(10);
        const auto unknown = TLogoBlobID(9, 9, 9, 1, 0, 0);
        UNIT_ASSERT_EXCEPTION(MakeContext({{part, 1, 0}}, {unknown}), yexception);
        UNIT_ASSERT_EXCEPTION(MakeContext({{part, 1, 0}, {part, 2, 42}}, {part->Label}), yexception);
    }

    Y_UNIT_TEST(RejectsUnknownSourceEpochs) {
        TFixture f;
        const auto part = f.Part(10).CloneWithEpoch(TEpoch::Max());
        UNIT_ASSERT_EXCEPTION(MakeContext({{part, 1, 0}}, {part->Label}), yexception);
        UNIT_ASSERT_EXCEPTION(MakeContext({}, {}, {TEpoch::Max()}), yexception);
    }

    Y_UNIT_TEST(ResolvesLoadedBindingsAndKeepsUnloadedSourcesInTheDefaultTier) {
        TFixture f;
        auto defaultPart = f.Part(10);
        auto coldPart = f.Part(20);
        auto unloadedArchive = f.Cold(30);
        auto unloadedDefault = f.Cold(35);
        auto finalPart = f.Part(40);
        auto borrowed = f.Cold(50);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        UNIT_ASSERT(policy->Generations.size() >= 3);
        policy->Generations[1].TierId = 42;
        policy->Generations[2].TierId = 7;
        TCompactionState state;
        state.PartLevels[defaultPart->Label] = 1;
        state.PartLevels[coldPart->Label] = 2;
        state.PartLevels[unloadedArchive->Label] = 3;
        state.PartLevels[unloadedDefault->Label] = 1;
        state.PartLevels[finalPart->Label] = 255;
        const TVector<TPartView> loaded{defaultPart, coldPart, finalPart};
        const TVector<TIntrusiveConstPtr<TColdPart>> unloaded{unloadedArchive, unloadedDefault, borrowed};
        const auto parts = DescribeCompactionParts(loaded, unloaded, state, *policy);

        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 6);
        for (const auto& part : parts) {
            UNIT_ASSERT_VALUES_EQUAL(part.HasUnknownPlacement, part.Label() == unloadedArchive->Label);
            if (part.Label() == coldPart->Label) {
                UNIT_ASSERT_VALUES_EQUAL(part.TierId, 42);
                UNIT_ASSERT_VALUES_EQUAL(part.Level, 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(part.TierId, 0);
            }
            if (part.Label() == unloadedArchive->Label || part.Label() == borrowed->Label) {
                UNIT_ASSERT_VALUES_EQUAL(part.Level, 255);
            }
            if (part.Label() == unloadedDefault->Label) {
                // Preserve its default-generation position for epoch placement.
                UNIT_ASSERT_VALUES_EQUAL(part.Level, 1);
            }
        }
    }

    Y_UNIT_TEST(RejectsInvalidGenerationBindings) {
        TFixture f;
        auto part = f.Part(10);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        TCompactionState state;
        state.PartLevels[part->Label] = 0;
        const TVector<TPartView> loaded{part};
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts(loaded, {}, state, *policy), yexception);
        state.PartLevels[part->Label] = policy->Generations.size() + 1;
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts(loaded, {}, state, *policy), yexception);

        const auto unloaded = f.Cold(20);
        const TVector<TIntrusiveConstPtr<TColdPart>> coldParts{unloaded};
        state.PartLevels[unloaded->Label] = 0;
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts({}, coldParts, state, *policy), yexception);
        state.PartLevels[unloaded->Label] = policy->Generations.size() + 1;
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts({}, coldParts, state, *policy), yexception);
        for (ui32 level : {ui32(254), ui32(256), Max<ui32>()}) {
            state.PartLevels[part->Label] = level;
            UNIT_ASSERT_EXCEPTION(DescribeCompactionParts(loaded, {}, state, *policy), yexception);
            state.PartLevels[unloaded->Label] = level;
            UNIT_ASSERT_EXCEPTION(DescribeCompactionParts({}, coldParts, state, *policy), yexception);
        }
    }

    Y_UNIT_TEST(DecodesEveryTierFinalAndNormalizesUnloadedNonDefaultFinals) {
        TFixture f;
        const auto policy = TieredPolicy();
        struct TCase {
            ui32 Level;
            ui32 TierId;
        };
        const TCase cases[] = {
            {1, 0}, {2, 0}, {3, 42}, {4, 42}, {5, 7}, {6, 7},
            {255, 0}, {254, 42}, {253, 7},
        };
        TVector<TPartView> loaded;
        TVector<TIntrusiveConstPtr<TColdPart>> unloaded;
        TCompactionState state;
        for (const auto& test : cases) {
            auto part = f.Part(10);
            auto cold = f.Cold(20);
            state.PartLevels[part->Label] = test.Level;
            state.PartLevels[cold->Label] = test.Level;
            loaded.push_back(part);
            unloaded.push_back(cold);
        }
        const auto inventory = DescribeCompactionParts(loaded, unloaded, state, *policy);
        UNIT_ASSERT_VALUES_EQUAL(inventory.size(), loaded.size() + unloaded.size());
        for (size_t index = 0; index < loaded.size(); ++index) {
            const auto& test = cases[index];
            const auto& part = inventory[index];
            UNIT_ASSERT_VALUES_EQUAL(part.Label(), loaded[index]->Label);
            UNIT_ASSERT_VALUES_EQUAL(part.Level, test.Level);
            UNIT_ASSERT_VALUES_EQUAL(part.TierId, test.TierId);
            UNIT_ASSERT(!part.HasUnknownPlacement);

            const auto& cold = inventory[loaded.size() + index];
            UNIT_ASSERT_VALUES_EQUAL(cold.Label(), unloaded[index]->Label);
            UNIT_ASSERT_VALUES_EQUAL(cold.TierId, 0);
            UNIT_ASSERT_VALUES_EQUAL(cold.Level, test.TierId == 0 ? test.Level : 255);
            UNIT_ASSERT_VALUES_EQUAL(cold.HasUnknownPlacement, test.TierId != 0);
        }
        state.PartLevels[loaded.front()->Label] = 252;
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts(loaded, unloaded, state, *policy), yexception);
        state.PartLevels[loaded.front()->Label] = 1;
        state.PartLevels[unloaded.front()->Label] = 252;
        UNIT_ASSERT_EXCEPTION(DescribeCompactionParts(loaded, unloaded, state, *policy), yexception);
    }

    Y_UNIT_TEST(UnloadedColdBindingsDoNotHideDefaultInputsFromTheEpochClosure) {
        TFixture f;
        auto first = f.Part(10);
        auto middle = f.Cold(20);
        auto last = f.Part(30);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        UNIT_ASSERT(policy->Generations.size() >= 3);
        policy->Generations[1].TierId = 42;
        policy->Generations[2].TierId = 7;
        TCompactionState state;
        state.PartLevels[first->Label] = 2;
        state.PartLevels[middle->Label] = 3;
        state.PartLevels[last->Label] = 2;
        const TVector<TPartView> loaded{first, last};
        const TVector<TIntrusiveConstPtr<TColdPart>> unloaded{middle};
        const auto inventory = DescribeCompactionParts(loaded, unloaded, state, *policy);
        auto context = MakeContext(inventory, {first->Label, last->Label});

        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Min, first->Epoch);
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Max, last->Epoch);
        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT(context->Remaining.empty());
        UNIT_ASSERT(context->RequiresTierAware());
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.size(), 2);
        UNIT_ASSERT(std::find(context->InputTiers.begin(), context->InputTiers.end(), 0) != context->InputTiers.end());
        UNIT_ASSERT(std::find(context->InputTiers.begin(), context->InputTiers.end(), 42) != context->InputTiers.end());
        for (const auto& input : context->Inputs) {
            if (input.Label() == middle->Label) {
                UNIT_ASSERT_VALUES_EQUAL(input.Level, 255);
                UNIT_ASSERT_VALUES_EQUAL(input.TierId, 0);
            }
        }
        TCompactionParams params;
        context->CopyInputs(params);
        UNIT_ASSERT_VALUES_EQUAL(params.Parts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.front().Get(), middle.Get());
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = params.Parts;
        subset.ColdParts = params.ColdParts;
        context->ValidateSubset(subset);
        UNIT_ASSERT(context->IsCurrent(inventory));
    }

    Y_UNIT_TEST(UnloadedUnknownPlacementKeepsCapAndGarbageConservative) {
        TFixture f;
        const auto unloaded = f.Cold(10);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        UNIT_ASSERT(policy->Generations.size() >= 3);
        policy->Generations[1].TierId = 42;
        policy->Generations[2].TierId = 7;
        TCompactionState state;
        state.PartLevels[unloaded->Label] = 2;
        const TVector<TIntrusiveConstPtr<TColdPart>> coldParts{unloaded};
        const auto inventory = DescribeCompactionParts({}, coldParts, state, *policy);
        auto context = MakeContext(inventory, {});
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.front().TierId, 0);
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.front().Level, 255);
        UNIT_ASSERT(context->Remaining.front().HasUnknownPlacement);

        TCompactionTierCap exact(f.Layout.RowScheme()->Keys, context->Remaining, {0, 42, 7});
        TCompactionTierCap parallel(f.Layout.RowScheme()->Keys, context->Remaining, {0, 42, 7}, true);
        const auto key = TCell::Make(ui32(20));
        // Its logical tier is default, but the conflicting recorded placement
        // cannot prove that the key is absent from an intermediate tier.
        UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 0, 7), 42);
        UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 0, 7), 42);
        exact.Reset();
        parallel.Reset();
        UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 7, 0), 42);
        UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 7, 0), 42);

        auto garbage = MakeCompactionGarbageConf(f.Layout.RowScheme()->Keys, context->Remaining, false);
        UNIT_ASSERT(!garbage.Final);
        UNIT_ASSERT(garbage.UnderlayMask);
        UNIT_ASSERT(Has(*garbage.UnderlayMask, 0));
        UNIT_ASSERT(Has(*garbage.UnderlayMask, Max<ui32>()));
    }

    Y_UNIT_TEST(DefaultUnloadedRemainingSourcesDoNotBlockExactTierJumps) {
        TFixture f;
        const auto unloaded = f.Cold(10);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        UNIT_ASSERT(policy->Generations.size() >= 3);
        policy->Generations[1].TierId = 42;
        policy->Generations[2].TierId = 7;
        const TVector<TIntrusiveConstPtr<TColdPart>> coldParts{unloaded};

        for (ui32 level : {ui32(255), ui32(1)}) {
            TCompactionState state;
            state.PartLevels[unloaded->Label] = level;
            const auto inventory = DescribeCompactionParts({}, coldParts, state, *policy);
            UNIT_ASSERT_VALUES_EQUAL(inventory.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inventory.front().TierId, 0);
            UNIT_ASSERT_VALUES_EQUAL(inventory.front().Level, level);
            UNIT_ASSERT(!inventory.front().HasUnknownPlacement);

            TCompactionTierCap exact(f.Layout.RowScheme()->Keys, inventory, {0, 42, 7});
            TCompactionTierCap parallel(f.Layout.RowScheme()->Keys, inventory, {0, 42, 7}, true);
            const auto key = TCell::Make(ui32(20));
            UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 0, 7), 7);
            UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 0, 7), 42);
            exact.Reset();
            parallel.Reset();
            UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 7, 0), 0);
            UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 7, 0), 42);
        }
    }

    Y_UNIT_TEST(UnloadedPlacementUncertaintyInvalidatesContext) {
        TFixture f;
        const auto unloaded = f.Cold(10);
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        UNIT_ASSERT(policy->Generations.size() >= 2);
        policy->Generations[1].TierId = 42;
        TCompactionState state;
        state.PartLevels[unloaded->Label] = 2;
        const TVector<TIntrusiveConstPtr<TColdPart>> coldParts{unloaded};
        const auto inventory = DescribeCompactionParts({}, coldParts, state, *policy);
        auto context = MakeContext(inventory, {});
        UNIT_ASSERT(context->IsCurrent(inventory));
        UNIT_ASSERT(inventory.front().HasUnknownPlacement);

        state.PartLevels[unloaded->Label] = 255;
        const auto updated = DescribeCompactionParts({}, coldParts, state, *policy);
        UNIT_ASSERT_VALUES_EQUAL(updated.front().Level, inventory.front().Level);
        UNIT_ASSERT_VALUES_EQUAL(updated.front().TierId, inventory.front().TierId);
        UNIT_ASSERT_VALUES_EQUAL(updated.front().ColdPart.Get(), inventory.front().ColdPart.Get());
        UNIT_ASSERT(!updated.front().HasUnknownPlacement);
        UNIT_ASSERT(!context->IsCurrent(updated));
    }

    Y_UNIT_TEST(ExtraSourcesExtendBothIntervalBounds) {
        TFixture f;
        auto lower = f.Part(5);
        auto selected = f.Part(10);
        auto upper = f.Cold(30);
        auto outside = f.Part(40);
        auto context = MakeContext({{selected, 2, 0}, {lower, 255, 0},
            {upper, 4, 42}, {outside, 1, 0}}, {selected->Label},
            {TEpoch::FromIndex(5), TEpoch::FromIndex(30)});

        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Min, TEpoch::FromIndex(5));
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Max, TEpoch::FromIndex(30));
        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT(Contains(context->Inputs, upper->Label));
        UNIT_ASSERT_VALUES_EQUAL(context->Remaining.size(), 1);
    }

    Y_UNIT_TEST(SubsetMemtableAndTxStatusContributeToTheInterval) {
        TFixture f;
        auto lower = f.Part(5);
        auto selected = f.Part(10);
        auto upper = f.Part(30);
        TIntrusivePtr<TMemTable> mem = new TMemTable(f.Layout.RowScheme(), TEpoch::FromIndex(5), 0);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {selected};
        subset.Frozen.emplace_back(mem, mem->Immediate());
        subset.TxStatus.emplace_back(new TTxStatusPart(TLogoBlobID(3, 1, 1, 1, 0, 0),
            TEpoch::FromIndex(30), nullptr));
        auto context = TCompactionContext::Build({{lower, 255, 0}, {selected, 2, 0}, {upper, 4, 42}}, subset);

        UNIT_ASSERT_VALUES_EQUAL(context->Inputs.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Min, TEpoch::FromIndex(5));
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Max, subset.Epoch());
        TCompactionParams params;
        context->CopyInputs(params);
        subset.Flatten = params.Parts;
        subset.ColdParts = params.ColdParts;
        context->ValidateSubset(subset);
    }

    Y_UNIT_TEST(FrozenDataAddsTheDefaultInputTier) {
        TFixture f;
        auto cold = f.Part(20);
        TIntrusivePtr<TMemTable> mem = new TMemTable(f.Layout.RowScheme(), TEpoch::FromIndex(10), 0);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {cold};
        subset.Frozen.emplace_back(mem, mem->Immediate());
        auto context = TCompactionContext::Build({{cold, 4, 42}}, subset);

        UNIT_ASSERT(context->RequiresTierAware());
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.size(), 2);
        UNIT_ASSERT(std::find(context->InputTiers.begin(), context->InputTiers.end(), 0) != context->InputTiers.end());
        UNIT_ASSERT(std::find(context->InputTiers.begin(), context->InputTiers.end(), 42) != context->InputTiers.end());
        context->ValidateSubset(subset);
    }

    Y_UNIT_TEST(TxStatusDoesNotIntroduceADefaultDataTier) {
        TFixture f;
        auto cold = f.Part(10);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {cold};
        subset.TxStatus.emplace_back(new TTxStatusPart(TLogoBlobID(3, 1, 1, 1, 0, 0),
            TEpoch::FromIndex(20), nullptr));
        auto context = TCompactionContext::Build({{cold, 4, 42}}, subset);

        UNIT_ASSERT(!context->RequiresTierAware());
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context->InputTiers.front(), 42);
        UNIT_ASSERT_VALUES_EQUAL(context->Epochs->Max, TEpoch::FromIndex(20));
        context->ValidateSubset(subset);
    }

    Y_UNIT_TEST(CopiesTheExpandedLoadedAndUnloadedInputs) {
        TFixture f;
        auto first = f.Part(10);
        auto middle = f.Cold(20);
        auto last = f.Part(30);
        auto context = MakeContext({{first, 1, 0}, {middle, 3, 42}, {last, 255, 0}},
            {first->Label, last->Label});
        TCompactionParams params;
        context->CopyInputs(params);

        UNIT_ASSERT_VALUES_EQUAL(params.Parts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.front()->Label, middle->Label);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = params.Parts;
        subset.ColdParts = params.ColdParts;
        context->ValidateSubset(subset);
        subset.ColdParts.clear();
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
    }

    Y_UNIT_TEST(RejectsReplacedExtraSourcesWithTheSameEpoch) {
        TFixture f;
        TIntrusivePtr<TMemTable> memory = new TMemTable(f.Layout.RowScheme(), TEpoch::FromIndex(10), 0);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Frozen.emplace_back(memory, memory->Immediate());
        subset.TxStatus.emplace_back(new TTxStatusPart(TLogoBlobID(3, 1, 1, 1, 0, 0),
            TEpoch::FromIndex(20), nullptr));
        auto context = TCompactionContext::Build({}, subset);
        context->ValidateSubset(subset);
        const auto status = subset.TxStatus.front();
        subset.TxStatus.front() = new TTxStatusPart(TLogoBlobID(3, 1, 2, 1, 0, 0),
            TEpoch::FromIndex(20), nullptr);
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
        subset.TxStatus.front() = status;
        TIntrusivePtr<TMemTable> replacement = new TMemTable(f.Layout.RowScheme(), TEpoch::FromIndex(10), 0);
        subset.Frozen.clear();
        subset.Frozen.emplace_back(replacement, replacement->Immediate());
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
    }

    Y_UNIT_TEST(ValidatesInputViewsAndActualEpoch) {
        TFixture f;
        auto part = f.Part(10);
        auto context = MakeContext({{part, 1, 0}}, {part->Label});
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {part};
        context->ValidateSubset(subset);
        subset.Flatten.front().Slices = TSlices::All();
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
        subset.Flatten = {part.CloneWithEpoch(TEpoch::FromIndex(11))};
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
        subset.Flatten = {part};
        subset.TxStatus.emplace_back(new TTxStatusPart(TLogoBlobID(3, 1, 1, 1, 0, 0),
            TEpoch::FromIndex(20), nullptr));
        UNIT_ASSERT_EXCEPTION(context->ValidateSubset(subset), yexception);
    }

    Y_UNIT_TEST(DetectsChangesOutsideTheSelectedInterval) {
        TFixture f;
        auto selected = f.Part(10);
        auto remaining = f.Part(20);
        TVector<TCompactionPartInfo> inventory{{selected, 1, 0}, {remaining, 3, 42}};
        auto context = MakeContext(inventory, {selected->Label});
        UNIT_ASSERT(context->IsCurrent(inventory));
        std::reverse(inventory.begin(), inventory.end());
        UNIT_ASSERT(context->IsCurrent(inventory));
        inventory.front().TierId = 7;
        UNIT_ASSERT(!context->IsCurrent(inventory));
        inventory.front().TierId = 42;
        inventory.front().Level = 4;
        UNIT_ASSERT(!context->IsCurrent(inventory));
        inventory.front().Level = 3;
        inventory.front().Part.Slices = TSlices::All();
        UNIT_ASSERT(!context->IsCurrent(inventory));
        inventory.front().Part = remaining;
        inventory.front().Part = remaining.CloneWithEpoch(TEpoch::FromIndex(21));
        UNIT_ASSERT(!context->IsCurrent(inventory));
        inventory.front().Part = remaining;
        inventory.emplace_back(f.Part(30), 5, 7);
        UNIT_ASSERT(!context->IsCurrent(inventory));
        inventory.pop_back();
        inventory.pop_back();
        UNIT_ASSERT(!context->IsCurrent(inventory));
    }

}

Y_UNIT_TEST_SUITE(TCompactionPlacementTest) {

    Y_UNIT_TEST(ForeignOutputsTryEveryDestinationGenerationBeforeItsFinal) {
        TFixture f;
        const auto policy = TieredPolicy();
        // The same destination rule applies when cooling into 42 or 7, and
        // when warming into 42 or default. Other tiers' epochs do not constrain it.
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(90), 1, 0}, {f.Part(50), 2, 0}, {f.Part(30), 255, 0},
            {f.Part(90), 3, 42}, {f.Part(50), 4, 42}, {f.Part(30), 254, 42},
            {f.Part(90), 5, 7}, {f.Part(50), 6, 7}, {f.Part(30), 253, 7},
        };
        struct TCase {
            ui32 TierId;
            ui32 FirstLevel;
            ui32 SecondLevel;
            ui32 FinalLevel;
        };
        const TCase cases[] = {{0, 1, 2, 255}, {42, 3, 4, 254}, {7, 5, 6, 253}};
        for (const auto& test : cases) {
            const auto first = FindTierCompactionLevel(*policy, remaining,
                test.TierId, TEpoch::FromIndex(70));
            const auto second = FindTierCompactionLevel(*policy, remaining,
                test.TierId, TEpoch::FromIndex(40));
            const auto final = FindTierCompactionLevel(*policy, remaining,
                test.TierId, TEpoch::FromIndex(20));
            UNIT_ASSERT(first);
            UNIT_ASSERT(second);
            UNIT_ASSERT(final);
            UNIT_ASSERT_VALUES_EQUAL(*first, test.FirstLevel);
            UNIT_ASSERT_VALUES_EQUAL(*second, test.SecondLevel);
            UNIT_ASSERT_VALUES_EQUAL(*final, test.FinalLevel);
        }
        UNIT_ASSERT(!FindTierCompactionLevel(*policy, remaining, 999, TEpoch::FromIndex(20)));
    }

    Y_UNIT_TEST(PreferredFinalMustBelongToTheDestinationTier) {
        TFixture f;
        const auto policy = TieredPolicy();
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(90), 1, 0}, {f.Part(90), 3, 42}, {f.Part(90), 5, 7},
        };
        const ui32 tiers[] = {0, 42, 7};
        const ui32 firstLevels[] = {1, 3, 5};
        const ui32 finals[] = {255, 254, 253};
        for (size_t index = 0; index < 3; ++index) {
            for (size_t preferred = 0; preferred < 3; ++preferred) {
                const auto level = FindTierCompactionLevel(*policy, remaining,
                    tiers[index], TEpoch::FromIndex(20), finals[preferred]);
                UNIT_ASSERT(level);
                UNIT_ASSERT_VALUES_EQUAL(*level,
                    index == preferred ? finals[index] : firstLevels[index]);
            }
        }
    }

    Y_UNIT_TEST(FinalPlacementChecksEveryRemainingGenerationOfItsTier) {
        TFixture f;
        const auto policy = TieredPolicy();
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(90), 1, 0}, {f.Part(50), 2, 0},
            {f.Part(90), 3, 42}, {f.Part(50), 4, 42},
            {f.Part(90), 5, 7}, {f.Part(50), 6, 7},
        };
        const ui32 tiers[] = {0, 42, 7};
        const ui32 firstLevels[] = {1, 3, 5};
        const ui32 finals[] = {255, 254, 253};
        for (size_t index = 0; index < 3; ++index) {
            const auto newer = FindTierCompactionLevel(*policy, remaining,
                tiers[index], TEpoch::FromIndex(70), finals[index]);
            const auto equal = FindTierCompactionLevel(*policy, remaining,
                tiers[index], TEpoch::FromIndex(50), finals[index]);
            const auto older = FindTierCompactionLevel(*policy, remaining,
                tiers[index], TEpoch::FromIndex(20), finals[index]);
            UNIT_ASSERT(newer);
            UNIT_ASSERT(equal);
            UNIT_ASSERT(older);
            UNIT_ASSERT_VALUES_EQUAL(*newer, firstLevels[index]);
            UNIT_ASSERT_VALUES_EQUAL(*equal, finals[index]);
            UNIT_ASSERT_VALUES_EQUAL(*older, finals[index]);
        }
    }

}

Y_UNIT_TEST_SUITE(TCompactionKeySpaceTest) {

    Y_UNIT_TEST(RespectsInclusiveAndExclusiveSliceBounds) {
        TFixture f;
        auto part = f.Part(10);
        part.Slices = Slice(10, 30, false, false);
        const TVector<TCompactionPartInfo> remaining{{part, 2, 42}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining);

        UNIT_ASSERT(!Has(*mask, 9));
        UNIT_ASSERT(!Has(*mask, 10));
        UNIT_ASSERT(Has(*mask, 11));
        UNIT_ASSERT(Has(*mask, 29));
        UNIT_ASSERT(!Has(*mask, 30));
        UNIT_ASSERT(!Has(*mask, 31));
        part.Slices = Slice(10, 30);
        const TVector<TCompactionPartInfo> inclusive{{part, 2, 42}};
        mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, inclusive);
        UNIT_ASSERT(Has(*mask, 10));
        UNIT_ASSERT(Has(*mask, 30));
    }

    Y_UNIT_TEST(HandlesMissingEmptyAndUnboundedSlices) {
        TFixture f;
        auto part = f.Part(10);
        part.Slices = nullptr;
        const TVector<TCompactionPartInfo> missing{{part, 2, 42}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, missing);
        UNIT_ASSERT(Has(*mask, 1));
        UNIT_ASSERT(Has(*mask, 1000));
        part.Slices = new TSlices;
        const TVector<TCompactionPartInfo> empty{{part, 2, 42}};
        mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, empty);
        UNIT_ASSERT(!Has(*mask, 20));
        part.Slices = TSlices::All();
        const TVector<TCompactionPartInfo> unbounded{{part, 2, 42}};
        mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, unbounded);
        UNIT_ASSERT(Has(*mask, 1));
        UNIT_ASSERT(Has(*mask, 1000));
    }

    Y_UNIT_TEST(ExtendsSavedBoundsWithSchemaDefaults) {
        NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::Uint32, TCell::Make(ui32(7)))
            .Col(0, 8, NScheme::NTypeIds::Uint32)
            .Key({0, 1});
        auto part = NTest::TPartCook(layout, {}).AddN(ui32(10), ui32(7), ui32(1))
            .AddN(ui32(20), ui32(7), ui32(2)).Finish().ToPartView();
        part.Slices = Slice(10, 20);
        const TVector<TCompactionPartInfo> remaining{{part, 3, 42}};
        auto mask = MakeCompactionKeySpace(layout.RowScheme()->Keys, remaining);
        const auto query = [&](ui32 first, ui32 second) {
            const TCell key[] = {TCell::Make(first), TCell::Make(second)};
            return mask->HasKey(key);
        };
        UNIT_ASSERT(!query(10, 6));
        UNIT_ASSERT(query(10, 7));
        UNIT_ASSERT(query(20, 7));
        UNIT_ASSERT(!query(20, 8));
    }

    Y_UNIT_TEST(UsesBloomNegativesWithoutReadingRows) {
        TFixture f;
        auto part = f.Part(10, 10, 30, true);
        part.Slices = TSlices::All();
        UNIT_ASSERT(!part->ByKeyPrefixes.empty());
        const TVector<TCompactionPartInfo> remaining{{part, 3, 42}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining);
        UNIT_ASSERT(Has(*mask, 10));
        UNIT_ASSERT(Has(*mask, 30));
        bool checkedNegative = false;
        for (ui32 value = 31; value < 1000; ++value) {
            const auto cell = TCell::Make(value);
            const NBloom::TPrefix prefix({&cell, 1});
            if (!part->MightHaveKeyPrefix(prefix)) {
                UNIT_ASSERT(!Has(*mask, value));
                checkedNegative = true;
                break;
            }
        }
        UNIT_ASSERT(checkedNegative);
    }

    Y_UNIT_TEST(PartBloomChecksRespectAvailableCellsAndExplicitLimit) {
        NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::Uint32)
            .Col(0, 8, NScheme::NTypeIds::Uint32)
            .Key({0, 1});
        NPage::TConf conf;
        conf.ByKeyFilterPrefixes.push_back({1, 0.000001});
        conf.ByKeyFilterPrefixes.push_back({2, 0.000001});
        const auto part = NTest::TPartCook(layout, conf).AddN(ui32(10), ui32(100), ui32(1))
            .Finish().ToPartView();
        UNIT_ASSERT_VALUES_EQUAL(part->ByKeyPrefixes.size(), 2);
        const auto& [shortLength, shortBloom] = part->ByKeyPrefixes.front();
        const auto& [fullLength, fullBloom] = part->ByKeyPrefixes.back();
        UNIT_ASSERT_VALUES_EQUAL(shortLength, 1);
        UNIT_ASSERT_VALUES_EQUAL(fullLength, 2);
        UNIT_ASSERT(shortBloom);
        UNIT_ASSERT(fullBloom);

        const TCell storedKey[] = {TCell::Make(ui32(10)), TCell::Make(ui32(100))};
        const NBloom::TPrefix stored(storedKey);
        UNIT_ASSERT_VALUES_EQUAL(stored.Size(), 2);
        UNIT_ASSERT(part->MightHaveKeyPrefix(stored));
        const NBloom::TPrefix shortStored({storedKey, 1});
        UNIT_ASSERT_VALUES_EQUAL(shortStored.Size(), 1);
        // The full-key bloom cannot be queried with just one key cell.
        UNIT_ASSERT(part->MightHaveKeyPrefix(shortStored));

        bool checkedShortNegative = false;
        for (ui32 value = 11; value < 1000; ++value) {
            const auto cell = TCell::Make(value);
            const NBloom::TPrefix prefix({&cell, 1});
            if (!shortBloom->MightHave(prefix.Get(1))) {
                UNIT_ASSERT(!part->MightHaveKeyPrefix(prefix));
                UNIT_ASSERT(part->MightHaveKeyPrefix(prefix, 0));
                checkedShortNegative = true;
                break;
            }
        }
        UNIT_ASSERT(checkedShortNegative);

        bool checkedFullNegative = false;
        for (ui32 value = 101; value < 1000; ++value) {
            const TCell key[] = {TCell::Make(ui32(10)), TCell::Make(value)};
            const NBloom::TPrefix prefix(key);
            if (!fullBloom->MightHave(prefix.Get(2))) {
                // The first key cell is present, so only the full-key bloom
                // can reject this key unless its length exceeds the limit.
                UNIT_ASSERT(!part->MightHaveKeyPrefix(prefix));
                UNIT_ASSERT(part->MightHaveKeyPrefix(prefix, 1));
                UNIT_ASSERT(part->MightHaveKeyPrefix(prefix, 0));
                checkedFullNegative = true;
                break;
            }
        }
        UNIT_ASSERT(checkedFullNegative);
    }

    Y_UNIT_TEST(PrefixBloomPositiveRemainsConservative) {
        NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::Uint32)
            .Col(0, 8, NScheme::NTypeIds::Uint32)
            .Key({0, 1});
        NPage::TConf conf;
        conf.ByKeyFilterPrefixes.push_back({1, 0.000001});
        auto part = NTest::TPartCook(layout, conf).AddN(ui32(1), ui32(10), ui32(1))
            .Finish().ToPartView();
        part.Slices = TSlices::All();
        const TVector<TCompactionPartInfo> remaining{{part, 3, 42}};
        auto mask = MakeCompactionKeySpace(layout.RowScheme()->Keys, remaining);
        const TCell key[] = {TCell::Make(ui32(1)), TCell::Make(ui32(11))};
        UNIT_ASSERT(mask->HasKey(key));
    }

    Y_UNIT_TEST(IgnoresUnavailableAndIncompatibleBloomPrefixes) {
        TFixture f;
        auto source = f.Part(10, 10, 30, true);
        auto nullBloom = WithBlooms(source, {{1, nullptr}});
        const TVector<TCompactionPartInfo> nullParts{{nullBloom, 3, 42}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, nullParts);
        UNIT_ASSERT(Has(*mask, 20));
        auto incompatible = WithBlooms(source, {{2, source->ByKeyPrefixes.front().second}});
        const TVector<TCompactionPartInfo> incompatibleParts{{incompatible, 3, 42}};
        mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, incompatibleParts);
        UNIT_ASSERT(Has(*mask, 20));
    }

    Y_UNIT_TEST(UnloadedMetadataAlwaysMeansMaybe) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Cold(10), 255, 0}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining);
        UNIT_ASSERT(Has(*mask, 0));
        UNIT_ASSERT(Has(*mask, Max<ui32>()));
    }

    Y_UNIT_TEST(FiltersByTierAndGeneration) {
        TFixture f;
        auto one = f.Part(10, 10, 10);
        auto two = f.Part(20, 20, 20);
        auto three = f.Part(30, 30, 30);
        const TVector<TCompactionPartInfo> remaining{{one, 2, 42}, {two, 3, 42}, {three, 4, 7}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining, 42, 3);
        UNIT_ASSERT(!Has(*mask, 10));
        UNIT_ASSERT(Has(*mask, 20));
        UNIT_ASSERT(!Has(*mask, 30));
    }

    Y_UNIT_TEST(PerTierFinalsParticipateInTierAndGenerationMasks) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(10, 10, 10), 255, 0},
            {f.Part(20, 20, 20), 254, 42},
            {f.Part(30, 30, 30), 253, 7},
        };
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining, 42, 254);
        UNIT_ASSERT(!Has(*mask, 10));
        UNIT_ASSERT(Has(*mask, 20));
        UNIT_ASSERT(!Has(*mask, 30));
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7});
        const auto key = TCell::Make(ui32(20));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 7), 42);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 7, 0), 42);
    }

    Y_UNIT_TEST(UnknownUnloadedPlacementIsIncludedInEveryTierAndGenerationMask) {
        TFixture f;
        const auto unloaded = f.Cold(10);
        const TVector<TCompactionPartInfo> remaining{{unloaded, 255, 0, true}};
        for (ui32 tierId : {ui32(0), ui32(42), ui32(7)}) {
            auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining, tierId, 2);
            UNIT_ASSERT(Has(*mask, 0));
            UNIT_ASSERT(Has(*mask, Max<ui32>()));
        }

        const TVector<TCompactionPartInfo> defaultRemaining{{unloaded, 255, 0}};
        auto mask = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, defaultRemaining, 42, 2);
        UNIT_ASSERT(!Has(*mask, 20));
    }

    Y_UNIT_TEST(IndependentCursorsCanResetSeparately) {
        TFixture f;
        auto part = f.Part(10);
        TIntrusivePtr<TSlices> slices = new TSlices;
        slices->push_back(Slice(10, 10)->front());
        slices->push_back(Slice(30, 30)->front());
        part.Slices = slices;
        const TVector<TCompactionPartInfo> remaining{{part, 3, 42}};
        auto first = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining);
        auto second = MakeCompactionKeySpace(f.Layout.RowScheme()->Keys, remaining);
        UNIT_ASSERT(Has(*first, 30));
        UNIT_ASSERT(Has(*second, 10));
        UNIT_ASSERT(!Has(*second, 20));
        first->Reset();
        UNIT_ASSERT(Has(*second, 30));
        UNIT_ASSERT(Has(*first, 10));
    }

    Y_UNIT_TEST(CapStopsInBothDirectionsUsingPhysicalTierOrder) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10), 3, 42}};
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7});
        const auto key = TCell::Make(ui32(20));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 7), 42);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 7, 0), 42);
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 42, 42), 42);
    }

    Y_UNIT_TEST(CapSkipsOnlyProvenEmptyIntermediateTiers) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10, 10, 10), 2, 42},
            {f.Part(20, 30, 30), 3, 42}};
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7});
        const auto key = TCell::Make(ui32(20));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 7), 7);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 7, 0), 0);
        const auto present = TCell::Make(ui32(30));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&present, 1}, 0, 7), 42);
    }

    Y_UNIT_TEST(CapIncludesEveryGenerationAndUnknownMetadata) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10, 10, 10), 2, 42},
            {f.Part(20, 20, 20), 3, 42}, {f.Cold(30), 4, 7}};
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7, 9});
        const auto key = TCell::Make(ui32(20));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 9), 42);
        const auto absent = TCell::Make(ui32(30));
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&absent, 1}, 0, 9), 7);
    }

    Y_UNIT_TEST(CapChecksAllGenerationsInBothDirections) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(80, 1, 1), 1, 0}, {f.Part(70, 2, 2), 2, 0},
            {f.Part(60, 10, 10), 3, 42}, {f.Part(50, 50, 50), 4, 42},
            {f.Part(40, 30, 30), 5, 7}, {f.Part(30, 50, 50), 6, 7},
            {f.Part(20, 70, 70), 7, 9}, {f.Part(10, 80, 80), 8, 9},
        };
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7, 9});
        struct TCase {
            ui32 Key;
            ui32 CoolingTier;
            ui32 WarmingTier;
        };
        const TCase cases[] = {
            {10, 42, 42},
            {30, 7, 7},
            // The same key in both intermediate tiers stops at the nearest one.
            {50, 42, 7},
            {90, 9, 0},
        };
        for (const auto& test : cases) {
            const auto key = TCell::Make(test.Key);
            cap.Reset();
            UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 9), test.CoolingTier);
            cap.Reset();
            UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 9, 0), test.WarmingTier);
        }
    }

    Y_UNIT_TEST(ParallelCapCoversKeysThatAnotherCompactionMayRelocate) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10, 20, 20), 1, 0}};
        TCompactionTierCap exact(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7});
        TCompactionTierCap parallel(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7}, true);
        const auto key = TCell::Make(ui32(20));

        // Although the intermediate tier is empty now, a concurrent operation
        // may put this remaining key there before the current scan publishes.
        UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 0, 7), 7);
        UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 0, 7), 42);
        exact.Reset();
        parallel.Reset();
        UNIT_ASSERT_VALUES_EQUAL(exact.LimitTier({&key, 1}, 7, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(parallel.LimitTier({&key, 1}, 7, 0), 42);
    }

    Y_UNIT_TEST(ParallelCapStillSkipsWhenNoRemainingSourceCanContainTheKey) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10, 10, 10), 1, 0},
            {f.Part(20, 30, 30), 3, 7}};
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7}, true);
        const auto key = TCell::Make(ui32(20));

        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 7), 7);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 7, 0), 0);
    }

    Y_UNIT_TEST(UnknownConcurrentKeysStopAtTheFirstIntermediateTier) {
        TFixture f;
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, {}, {0, 42, 7, 9}, true, true);
        const auto key = TCell::Make(ui32(20));

        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 0, 9), 42);
        cap.Reset();
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 9, 0), 7);
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 42, 7), 7);
        UNIT_ASSERT_VALUES_EQUAL(cap.LimitTier({&key, 1}, 7, 7), 7);
    }

    Y_UNIT_TEST(CapRejectsAmbiguousOrUnknownTierOrders) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10), 3, 42}};
        UNIT_ASSERT_EXCEPTION(TCompactionTierCap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 0}), yexception);
        UNIT_ASSERT_EXCEPTION(TCompactionTierCap(f.Layout.RowScheme()->Keys, remaining, {0, 7}), yexception);
        TCompactionTierCap cap(f.Layout.RowScheme()->Keys, remaining, {0, 42, 7});
        const auto key = TCell::Make(ui32(20));
        UNIT_ASSERT_EXCEPTION(cap.LimitTier({&key, 1}, 0, 99), yexception);
    }

    Y_UNIT_TEST(GarbageConfCoversAllRemainingTiersAndEpochs) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(5, 10, 10), 255, 0},
            {f.Part(100, 30, 30), 3, 42}};
        auto conf = MakeCompactionGarbageConf(f.Layout.RowScheme()->Keys, remaining, false);
        UNIT_ASSERT(!conf.Final);
        UNIT_ASSERT(conf.UnderlayMask);
        UNIT_ASSERT(Has(*conf.UnderlayMask, 10));
        UNIT_ASSERT(!Has(*conf.UnderlayMask, 20));
        UNIT_ASSERT(Has(*conf.UnderlayMask, 30));
        auto final = MakeCompactionGarbageConf(f.Layout.RowScheme()->Keys, {}, false);
        UNIT_ASSERT(final.Final);
        UNIT_ASSERT(!final.UnderlayMask);
    }

    Y_UNIT_TEST(KeepEraseMarkersDisablesBothKindsOfFinalization) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{{f.Part(10), 3, 42}};
        for (const bool withRemaining : {false, true}) {
            auto garbage = MakeCompactionGarbageConf(f.Layout.RowScheme()->Keys,
                withRemaining ? TConstArrayRef<TCompactionPartInfo>(remaining) : TConstArrayRef<TCompactionPartInfo>{}, true);
            UNIT_ASSERT(!garbage.Final);
            UNIT_ASSERT(!garbage.UnderlayMask);
            NPage::TConf conf;
            conf.Final = garbage.Final;
            conf.UnderlayMask = garbage.UnderlayMask.Get();
            const auto eggs = NTest::TPartCook(f.Layout, conf)
                .AddOpN(ERowOp::Erase, ui32(20)).Finish();
            UNIT_ASSERT(eggs.Written);
            UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Rows, 1);
            UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Drops, 1);
        }
    }

    Y_UNIT_TEST(NewerKeysInNonDefaultFinalsRetainEraseMarkers) {
        TFixture f;
        const TVector<TCompactionPartInfo> remaining{
            {f.Part(5, 10, 10), 255, 0},
            {f.Part(100, 30, 30), 254, 42},
            {f.Part(200, 50, 50), 253, 7},
        };
        auto garbage = MakeCompactionGarbageConf(f.Layout.RowScheme()->Keys, remaining, false);
        UNIT_ASSERT(!garbage.Final);
        UNIT_ASSERT(garbage.UnderlayMask);
        NPage::TConf conf;
        conf.Final = garbage.Final;
        conf.UnderlayMask = garbage.UnderlayMask.Get();
        const auto eggs = NTest::TPartCook(f.Layout, conf)
            .AddOpN(ERowOp::Erase, ui32(20))
            .AddOpN(ERowOp::Erase, ui32(30))
            .AddOpN(ERowOp::Erase, ui32(50)).Finish();
        UNIT_ASSERT(eggs.Written);
        UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Rows, 2);
        UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Drops, 2);
    }

}

}
