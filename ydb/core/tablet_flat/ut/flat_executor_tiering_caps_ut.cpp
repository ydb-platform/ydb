#include "flat_executor_tiering_ut_common.h"

namespace NKikimr::NTabletFlatExecutor {
namespace {
    using namespace NTieringTest;

    enum class EBlocker {
        SameKey,
        OutsideBounds,
        MissingBloom,
        Erase,
    };

    constexpr ui32 DeepTier = 99;

    NLocalDb::TCompactionPolicyPtr CasePolicy(bool multipleIntermediate) {
        auto policy = MakePolicy();
        if (multipleIntermediate) {
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

    void CheckCap(bool warming, ui32 blockerGeneration, EBlocker kind,
            bool restartBefore = false, bool adjacent = false, bool multipleIntermediate = false) {
        TEnv env;
        if (multipleIntermediate) {
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
        const TString blockerValue = "remaining-newer-version";
        const ui64 sourceTime = warming ? Max<ui64>() : 0;
        const ui32 sourceGeneration = warming ? multipleIntermediate ? 8 : 6 : 2;
        const TRowVersion sourceVersion = warming ? oldVersion : newVersion;
        const TRowVersion blockerVersion = warming ? newVersion : oldVersion;
        if (kind == EBlocker::Erase) {
            auto policy = CasePolicy(multipleIntermediate);
            policy->KeepEraseMarkers = true;
            env.Policy(*policy);
        }

        TVector<TRow> blockerRows;
        if (kind == EBlocker::SameKey) {
            blockerRows.push_back({key, std::nullopt, blockerValue, 0, blockerVersion});
        } else if (kind == EBlocker::MissingBloom) {
            // The searched key is absent, but bounds [10,30] alone cannot
            // prove that. No bloom filter has been configured for this table.
            blockerRows.push_back({10, std::nullopt, "left", 0, blockerVersion});
            blockerRows.push_back({30, std::nullopt, "right", 0, blockerVersion});
        } else if (kind == EBlocker::Erase) {
            blockerRows.push_back({key, std::nullopt, {}, 0, blockerVersion, true});
        } else {
            blockerRows.push_back({10, std::nullopt, "outside", 0, blockerVersion});
        }

        NTable::TPartView source;
        NTable::TPartView blocker;
        if (warming) {
            source = env.FlushPart({{key, sourceTime, sourceValue, 0, sourceVersion}});
            blocker = env.FlushPart(std::move(blockerRows));
            UNIT_ASSERT(source->Epoch < blocker->Epoch);
        } else {
            blocker = env.FlushPart(std::move(blockerRows));
            source = env.FlushPart({{key, sourceTime, sourceValue, 0, sourceVersion}});
            UNIT_ASSERT(blocker->Epoch < source->Epoch);
        }
        if (kind == EBlocker::MissingBloom) {
            UNIT_ASSERT(blocker->ByKeyPrefixes.empty());
        }

        THashMap<TLogoBlobID, ui32> placement{
            {source->Label, sourceGeneration},
            {blocker->Label, blockerGeneration},
        };
        env.SeedLevels(placement);
        if (restartBefore) {
            env.Restart();
            env.CheckLevels(placement, "after staging/restart, before TAC");
        }

        if (adjacent) {
            // Restrict the policy target to the adjacent tier. A matching key
            // in that target tier is not an intermediate-tier cap.
            env.Exec([&](TTransactionContext& txc) {
                auto policy = MakeTierPolicy();
                UNIT_ASSERT(!warming && !multipleIntermediate);
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

        auto policy = CasePolicy(multipleIntermediate);
        if (kind == EBlocker::Erase) {
            policy->KeepEraseMarkers = true;
        }
        policy->Generations[sourceGeneration - 1].ForceCountToCompact = 1;
        env.Enable();
        env.Policy(*policy);
        env.Env.WaitFor("tiering cap compaction", [&] { return observed.Seen; });
        env.WaitFor<NFake::TEvCompacted>();
        observer.Remove();

        UNIT_ASSERT(observed.Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT_VALUES_EQUAL(observed.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observed.Inputs.front(), source->Label);
        UNIT_ASSERT_VALUES_EQUAL(observed.Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observed.Remaining.front(), blocker->Label);
        UNIT_ASSERT_VALUES_EQUAL(observed.Outputs.size(), 1);

        const bool canSkip = kind == EBlocker::OutsideBounds;
        const ui32 intermediate = warming && multipleIntermediate ? ArchiveTier : ColdTier;
        const ui32 expectedTier = adjacent || !canSkip ? intermediate
            : warming ? 0 : multipleIntermediate ? DeepTier : ArchiveTier;
        const auto& output = observed.Outputs.front();
        UNIT_ASSERT_VALUES_EQUAL(output.Tier, expectedTier);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, source->Epoch);
        const auto levels = env.Levels();
        UNIT_ASSERT(!levels.contains(source->Label));
        UNIT_ASSERT_VALUES_EQUAL(levels.at(blocker->Label), blockerGeneration);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(output.Part->Label)), expectedTier);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}),
            expectedTier == 0 ? 1 : expectedTier == ColdTier ? 2 : 3);
        UNIT_ASSERT_VALUES_EQUAL(output.Part->GetSlotChannel(output.Part->DescriptorSlot()), 1);

        const auto expected = warming && kind == EBlocker::SameKey ? blockerValue : sourceValue;
        env.Read(key, expected);
        env.Read(key, sourceValue, sourceVersion);
        env.Restart();
        env.CheckLevels(levels, "after TAC/restart");
        env.Read(key, expected);
        env.Read(key, sourceValue, sourceVersion);
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutor_TieringCaps) {
    Y_UNIT_TEST(CoolingStopsAtTheFirstGenerationOfTheIntermediateTier) {
        CheckCap(false, 4, EBlocker::SameKey, true);
    }
    Y_UNIT_TEST(CoolingStopsAtTheLastGenerationOfTheIntermediateTier) {
        CheckCap(false, 5, EBlocker::SameKey);
    }
    Y_UNIT_TEST(WarmingStopsAtTheFirstGenerationOfTheIntermediateTier) {
        CheckCap(true, 4, EBlocker::SameKey, true);
    }
    Y_UNIT_TEST(WarmingStopsAtTheLastGenerationOfTheIntermediateTier) {
        CheckCap(true, 5, EBlocker::SameKey);
    }
    Y_UNIT_TEST(CoolingSkipsTheIntermediateTierWhenAllRemainingBoundsExcludeTheKey) {
        CheckCap(false, 5, EBlocker::OutsideBounds, true);
    }
    Y_UNIT_TEST(WarmingSkipsTheIntermediateTierWhenAllRemainingBoundsExcludeTheKey) {
        CheckCap(true, 4, EBlocker::OutsideBounds, true);
    }
    Y_UNIT_TEST(CoolingDoesNotInferAbsenceFromBoundsWithoutABloom) {
        CheckCap(false, 4, EBlocker::MissingBloom);
    }
    Y_UNIT_TEST(WarmingDoesNotInferAbsenceFromBoundsWithoutABloom) {
        CheckCap(true, 5, EBlocker::MissingBloom);
    }
    Y_UNIT_TEST(AnIntermediateEraseMarkerStopsCooling) {
        CheckCap(false, 5, EBlocker::Erase);
    }
    Y_UNIT_TEST(AnAdjacentCoolingTargetDoesNotActAsAnIntermediateTier) {
        CheckCap(false, 5, EBlocker::SameKey, false, true);
    }
    Y_UNIT_TEST(CoolingAcrossSeveralTiersStopsAtTheFirstPossibleIntermediateTier) {
        CheckCap(false, 5, EBlocker::SameKey, true, false, true);
    }
    Y_UNIT_TEST(WarmingAcrossSeveralTiersStopsAtTheFirstPossibleIntermediateTier) {
        CheckCap(true, 7, EBlocker::SameKey, true, false, true);
    }
}
}
