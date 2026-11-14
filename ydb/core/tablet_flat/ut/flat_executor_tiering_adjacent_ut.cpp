#include "flat_executor_tiering_ut_common.h"

namespace NKikimr::NTabletFlatExecutor {
namespace {
    using namespace NTieringTest;

    enum class ERemaining {
        SameKey,
        DifferentKey,
        Erase,
    };

    constexpr ui32 DeepTier = 99;

    NLocalDb::TCompactionPolicyPtr CasePolicy(bool withDeepTier) {
        auto policy = MakePolicy();
        if (withDeepTier) {
            auto generation = policy->Generations.back();
            generation.TierId = DeepTier;
            policy->Generations.push_back(generation);
            policy->Generations.push_back(generation);
        }
        return policy;
    }

    struct TObservedCompaction {
        struct TOutput {
            NTable::TPartView Part;
            ui32 Id;
            ui32 Tier;
        };
        bool Seen = false;
        NTable::TCompactionParams::EMode Mode = NTable::TCompactionParams::EMode::Regular;
        TVector<TLogoBlobID> Inputs;
        TVector<TLogoBlobID> Remaining;
        TVector<TOutput> Outputs;
    };

    void CheckAdjacentTransition(bool warming, ui32 remainingGeneration, ERemaining kind,
            bool restartBefore = false, bool adjacent = false, bool withDeepTier = false) {
        TEnv env;
        if (withDeepTier) {
            auto policy = CasePolicy(true);
            env.Exec([&](TTransactionContext& txc) {
                auto tiering = MakeTierPolicy();
                tiering.Rules.insert(tiering.Rules.begin(), {
                    .TierId = DeepTier, .Ranges = {{
                        .To = NTable::TTierRelativeBound{TDuration::Days(730)}, .ToInclusive = true}}});
                txc.DB.Alter()
                    .SetRoom(TableId, DeepTier, 3, {3}, 3)
                    .AddFamily(TableId, DeepTier, DeepTier)
                    .SetCompactionPolicy(TableId, *policy)
                    .SetTieringPolicy(TableId, tiering);
                return true;
            });
            env.Policy(*policy);
        }
        constexpr ui64 key = 20;
        const TRowVersion oldVersion(1, 1);
        const TRowVersion newVersion(2, 2);
        // Exercise the cold family's external-blob channel as well as its
        // descriptor/data layout when the routed result lands in tier1.
        const TString sourceValue(1024, 's');
        const TString remainingValue = "remaining-newer-version";
        const ui64 sourceTime = warming ? Max<ui64>() : 0;
        const ui32 sourceGeneration = warming ? (withDeepTier ? 8 : 6) : 2;
        const TRowVersion sourceVersion = warming ? oldVersion : newVersion;
        const TRowVersion remainingVersion = warming ? newVersion : oldVersion;
        if (kind == ERemaining::Erase) {
            auto policy = CasePolicy(withDeepTier);
            policy->KeepEraseMarkers = true;
            env.Policy(*policy);
        }

        TVector<TRow> remainingRows;
        if (kind == ERemaining::SameKey) {
            remainingRows.push_back({key, std::nullopt, remainingValue, 0, remainingVersion});
        } else if (kind == ERemaining::Erase) {
            remainingRows.push_back({key, std::nullopt, {}, 0, remainingVersion, true});
        } else {
            remainingRows.push_back({10, std::nullopt, "other", 0, remainingVersion});
        }

        NTable::TPartView source;
        NTable::TPartView remaining;
        if (warming) {
            source = env.FlushPart({{key, sourceTime, sourceValue, 0, sourceVersion}});
            remaining = env.FlushPart(std::move(remainingRows));
            UNIT_ASSERT(source->Epoch < remaining->Epoch);
        } else {
            remaining = env.FlushPart(std::move(remainingRows));
            source = env.FlushPart({{key, sourceTime, sourceValue, 0, sourceVersion}});
            UNIT_ASSERT(remaining->Epoch < source->Epoch);
        }

        THashMap<TLogoBlobID, ui32> placement{
            {source->Label, sourceGeneration},
            {remaining->Label, remainingGeneration},
        };
        env.SeedLevels(placement);
        if (restartBefore) {
            env.Restart();
            env.CheckLevels(placement, "after staging/restart, before TAC");
        }

        if (adjacent) {
            // The requested tier is already adjacent and must be reached.
            env.Exec([&](TTransactionContext& txc) {
                auto policy = MakeTierPolicy();
                UNIT_ASSERT(!warming && !withDeepTier);
                policy.Rules.erase(policy.Rules.begin());
                txc.DB.Alter().SetTieringPolicy(TableId, policy);
                return true;
            });
        }

        TObservedCompaction observed;
        auto observer = env.Env.AddObserver<NOps::TEvResult>([&](auto& event) {
            auto* result = dynamic_cast<TProdCompact*>(event->Get()->Result.Get());
            if (!result || !result->Params) {
                return;
            }
            const auto& params = static_cast<const NTable::NCompGen::TGenCompactionParams&>(*result->Params);
            if (params.Generation != sourceGeneration) {
                return;
            }
            UNIT_ASSERT(result->Success);
            UNIT_ASSERT(!observed.Seen);
            observed.Seen = true;
            observed.Mode = params.Mode;
            for (const auto& part : params.Context->Inputs) {
                observed.Inputs.push_back(part.Label());
            }
            for (const auto& part : params.Context->Remaining) {
                observed.Remaining.push_back(part.Label());
            }
            for (const auto& output : result->Results) {
                observed.Outputs.push_back({output.Part, output.OutputId,
                    params.Tiering->OutputTiers.at(output.OutputId)});
            }
        });

        auto policy = CasePolicy(withDeepTier);
        if (kind == ERemaining::Erase) {
            policy->KeepEraseMarkers = true;
        }
        policy->Generations[sourceGeneration - 1].ForceCountToCompact = 1;
        env.Enable();
        env.Policy(*policy);
        env.Env.WaitFor("adjacent tier compaction", [&] { return observed.Seen; });
        env.WaitFor<NFake::TEvCompacted>();
        observer.Remove();

        UNIT_ASSERT(observed.Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT_VALUES_EQUAL(observed.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observed.Inputs.front(), source->Label);
        UNIT_ASSERT_VALUES_EQUAL(observed.Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observed.Remaining.front(), remaining->Label);
        UNIT_ASSERT_VALUES_EQUAL(observed.Outputs.size(), 1);

        const ui32 expectedTier = warming && withDeepTier ? ArchiveTier : ColdTier;
        const auto& output = observed.Outputs.front();
        UNIT_ASSERT_VALUES_EQUAL(output.Tier, expectedTier);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, source->Epoch);
        const auto levels = env.Levels();
        UNIT_ASSERT(!levels.contains(source->Label));
        UNIT_ASSERT_VALUES_EQUAL(levels.at(remaining->Label), remainingGeneration);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(output.Part->Label)), expectedTier);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}),
            expectedTier == 0 ? 1 : expectedTier == ColdTier ? 2 : 3);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->GetSlotChannel(output.Part->DescriptorSlot()), 1);

        const auto expected = warming && kind == ERemaining::SameKey ? remainingValue : sourceValue;
        env.Read(key, expected);
        env.Read(key, sourceValue, sourceVersion);
        env.Restart();
        env.CheckLevels(levels, "after TAC/restart");
        env.Read(key, expected);
        env.Read(key, sourceValue, sourceVersion);
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutor_TieringAdjacentTransitions) {
    Y_UNIT_TEST(CoolingWithAVersionInTheFirstGenerationOfTheAdjacentTier) {
        CheckAdjacentTransition(false, 4, ERemaining::SameKey, true);
    }
    Y_UNIT_TEST(CoolingWithAVersionInTheLastGenerationOfTheAdjacentTier) {
        CheckAdjacentTransition(false, 5, ERemaining::SameKey);
    }
    Y_UNIT_TEST(WarmingWithAVersionInTheFirstGenerationOfTheAdjacentTier) {
        CheckAdjacentTransition(true, 4, ERemaining::SameKey, true);
    }
    Y_UNIT_TEST(WarmingWithAVersionInTheLastGenerationOfTheAdjacentTier) {
        CheckAdjacentTransition(true, 5, ERemaining::SameKey);
    }
    Y_UNIT_TEST(CoolingMovesOneTierWhenTheAdjacentTierHasOnlyADifferentKey) {
        CheckAdjacentTransition(false, 5, ERemaining::DifferentKey, true);
    }
    Y_UNIT_TEST(WarmingMovesOneTierWhenTheAdjacentTierHasOnlyADifferentKey) {
        CheckAdjacentTransition(true, 4, ERemaining::DifferentKey, true);
    }
    Y_UNIT_TEST(CoolingKeepsTheAdjacentTierEraseAndMvccHistory) {
        CheckAdjacentTransition(false, 5, ERemaining::Erase);
    }
    Y_UNIT_TEST(CoolingReachesAnAdjacentPolicyTarget) {
        CheckAdjacentTransition(false, 5, ERemaining::SameKey, false, true);
    }
    Y_UNIT_TEST(CoolingTowardsADistantTargetStillMovesOnlyOneTier) {
        CheckAdjacentTransition(false, 5, ERemaining::SameKey, true, false, true);
    }
    Y_UNIT_TEST(WarmingTowardsADistantTargetStillMovesOnlyOneTier) {
        CheckAdjacentTransition(true, 7, ERemaining::SameKey, true, false, true);
    }
}
}
