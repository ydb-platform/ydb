#include "flat_executor_tiering_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_gen.h>
#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_scan_events.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <algorithm>

namespace NKikimr::NTabletFlatExecutor {
namespace {
    using namespace NTieringTest;

    const TProdCompact& Product(const NOps::TEvResult::TPtr& event) {
        const auto* product = dynamic_cast<const TProdCompact*>(event->Get()->Result.Get());
        UNIT_ASSERT(product);
        UNIT_ASSERT(product->Success);
        return *product;
    }

    ui32 Generation(const NOps::TEvResult::TPtr& event) {
        return static_cast<const NTable::NCompGen::TGenCompactionParams&>(*Product(event).Params).Generation;
    }

    void CheckAdjacentOutputs(const TProdCompact& product, ui32 tier) {
        UNIT_ASSERT(!product.Results.empty());
        UNIT_ASSERT(product.Params->Tiering);
        for (const auto& output : product.Results) {
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Tiering->OutputTiers.at(output.OutputId), tier);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}), tier == ColdTier ? 2 : 3);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetSlotChannel(output.Part->DescriptorSlot()), 1);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, product.Params->Context->Epochs->Max);
        }
    }

    void CheckRows(TEnv& env) {
        env.Exec([](TTransactionContext& txc) {
            for (ui64 key : {10, 11, 20, 21}) {
                const auto typedKey = NScheme::TUint64::TInstance(key);
                NTable::TRowState row;
                const auto ready = txc.DB.Select(TableId, {typedKey}, {ValueTag}, row);
                if (ready == NTable::EReady::Page) {
                    return false;
                }
                UNIT_ASSERT(ready == NTable::EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(row.Get(0).AsBuf(), ToString(key));
            }
            return true;
        });
    }

    void CheckInventory(TEnv& env, const THashMap<TLogoBlobID, ui32>& expected, TStringBuf stage) {
        env.CheckLevels(expected, stage);
        const auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), expected.size());
        for (const auto& part : parts) {
            UNIT_ASSERT_C(expected.contains(part->Label), stage);
        }
    }

    NLocalDb::TCompactionPolicy PrepareTwoGenerations(TEnv& env) {
        for (ui64 key : {10, 11, 20, 21}) {
            env.Write({TRow{.Key = key, .Time = 0, .Value = ToString(key)}});
            env.Flush();
        }
        auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 4);
        std::sort(parts.begin(), parts.end(), [](const auto& a, const auto& b) {
            return a->Epoch < b->Epoch;
        });
        auto levels = env.Levels();
        for (size_t i = 0; i < parts.size(); ++i) {
            // Adjacent hot outputs enter the upper cold generation, leaving
            // this lower generation's epoch interval independently runnable.
            levels[parts[i]->Label] = i < 2 ? 5 : 1;
        }
        env.SeedLevels(levels);
        NLocalDb::TCompactionPolicy policy;
        env.Exec([&](TTransactionContext& txc) {
            policy = *txc.DB.GetScheme().GetTableInfo(TableId)->CompactionPolicy;
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(policy.Generations.size(), 7);
        policy.Generations[0].ForceCountToCompact = 2;
        policy.Generations[4].ForceCountToCompact = 2;
        return policy;
    }

    void CheckEmptyAdjacentTierIsNotSkipped(bool warming) {
        TEnv env;
        constexpr ui64 key = 20;
        const TRowVersion oldVersion(1, 1);
        const TRowVersion newVersion(2, 2);
        const ui64 time = warming ? Max<ui64>() : 0;
        const auto older = env.FlushPart({{key, time, "older", 0, oldVersion}});
        const auto newer = env.FlushPart({{key, time, "newer", 0, newVersion}});
        const auto source = warming ? older : newer;
        const auto remaining = warming ? newer : older;
        const ui32 sourceGeneration = warming ? 7 : 2;
        const ui32 remainingGeneration = warming ? 6 : 3;
        const ui32 expectedTier = ColdTier;
        const THashMap<TLogoBlobID, ui32> placement{
            {source->Label, sourceGeneration},
            {remaining->Label, remainingGeneration},
        };
        env.SeedLevels(placement);

        auto policy = MakePolicy();
        policy->Generations[sourceGeneration - 1].ForceCountToCompact = 1;
        env.Enable();
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(*policy);
        env->WaitFor("held TAC entering the empty adjacent tier", [&] {
            return blocked.size() == 1;
        });
        const auto& product = Product(blocked.front());
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), sourceGeneration);
        UNIT_ASSERT(product.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Inputs.front().Label(), source->Label);
        UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Remaining.front().Label(), remaining->Label);
        UNIT_ASSERT(!product.Results.empty());
        TVector<TLogoBlobID> outputs;
        for (const auto& output : product.Results) {
            // An empty neighboring tier is still the only allowed step,
            // even when another version remains in the source tier.
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Tiering->OutputTiers.at(output.OutputId), expectedTier);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}), 2);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, source->Epoch);
            outputs.push_back(output.Part->Label);
        }
        CheckInventory(env, placement, "before adjacent TAC publication");

        auto* state = env.State;
        UNIT_ASSERT(state);
        blocked.Stop().Unblock();
        env->WaitFor("adjacent TAC publication", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(outputs.begin(), outputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        const auto levels = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(levels.size(), outputs.size() + 1);
        UNIT_ASSERT(!levels.contains(source->Label));
        UNIT_ASSERT_VALUES_EQUAL(levels.at(remaining->Label), remainingGeneration);
        for (const auto& label : outputs) {
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(label)), expectedTier);
        }
        CheckInventory(env, levels, "after adjacent TAC publication");
        env.Read(key, "newer");
        env.Read(key, "older", oldVersion);
        env.Restart();
        env.CheckLevels(levels, "after adjacent TAC/restart");
        env.Read(key, "newer");
        env.Read(key, "older", oldVersion);
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutor_TieringConcurrency) {
    Y_UNIT_TEST(ParallelTacPublicationsKeepBothInputsAndReadsValid) {
        TEnv env;
        const auto policy = PrepareTwoGenerations(env);
        const auto originalLevels = env.Levels();
        env.Enable(true);
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("two held TAC results", [&] { return blocked.size() == 2; });

        THashSet<ui32> generations;
        THashSet<TLogoBlobID> inputs;
        TVector<TLogoBlobID> outputs;
        THashMap<TLogoBlobID, ui32> outputTiers;
        THashSet<TLogoBlobID> firstInputs;
        TVector<TLogoBlobID> firstOutputs;
        const auto& first = Product(blocked.front());
        for (const auto& input : first.Params->Context->Inputs) {
            firstInputs.insert(input.Label());
        }
        for (const auto& output : first.Results) {
            firstOutputs.push_back(output.Part->Label);
        }
        for (const auto& event : blocked) {
            const auto& product = Product(event);
            UNIT_ASSERT(generations.insert(Generation(event)).second);
            UNIT_ASSERT(product.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Inputs.size(), 2);
            const ui32 tier = Generation(event) == 1 ? ColdTier : ArchiveTier;
            CheckAdjacentOutputs(product, tier);
            for (const auto& input : product.Params->Context->Inputs) {
                UNIT_ASSERT(inputs.insert(input.Label()).second);
            }
            for (const auto& output : product.Results) {
                outputs.push_back(output.Part->Label);
                outputTiers.emplace(output.Part->Label, tier);
            }
        }
        UNIT_ASSERT(generations.contains(1));
        UNIT_ASSERT(generations.contains(5));
        CheckInventory(env, originalLevels, "before first TAC publication");
        CheckRows(env);

        // Publish one operation while the other retains its prepared inputs.
        // The known replacement must leave the held second result usable.
        auto* state = env.State;
        UNIT_ASSERT(state);
        blocked.Unblock(1);
        env->WaitFor("first TAC publication", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(firstOutputs.begin(), firstOutputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        const auto afterFirst = env.Levels();
        auto expectedAfterFirst = originalLevels;
        for (const auto& label : firstInputs) {
            UNIT_ASSERT_VALUES_EQUAL(expectedAfterFirst.erase(label), 1);
        }
        for (const auto& label : firstOutputs) {
            UNIT_ASSERT(afterFirst.contains(label));
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(afterFirst.at(label)), outputTiers.at(label));
            expectedAfterFirst.emplace(label, afterFirst.at(label));
        }
        CheckInventory(env, expectedAfterFirst, "before second TAC publication");
        CheckRows(env);

        blocked.Stop().Unblock();
        env->WaitFor("both TAC publications", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(outputs.begin(), outputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        const auto levels = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(levels.size(), outputs.size());
        for (const auto& label : inputs) {
            UNIT_ASSERT(!levels.contains(label));
        }
        for (const auto& label : outputs) {
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(label)), outputTiers.at(label));
        }
        CheckInventory(env, levels, "after both TAC publications");
        CheckRows(env);
        env.Restart();
        env.CheckLevels(levels, "after restart");
        CheckRows(env);
    }

    Y_UNIT_TEST(RegularCompactionsOfDisjointGenerationsRemainParallel) {
        TEnv env;
        const auto policy = PrepareTwoGenerations(env);
        const auto originalLevels = env.Levels();
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("two held regular results", [&] { return blocked.size() == 2; });

        THashSet<ui32> generations;
        THashSet<TLogoBlobID> inputs;
        TVector<TLogoBlobID> outputs;
        for (const auto& event : blocked) {
            const auto& product = Product(event);
            const ui32 generation = Generation(event);
            UNIT_ASSERT(generations.insert(generation).second);
            UNIT_ASSERT(product.Params->Mode == NTable::TCompactionParams::EMode::Regular);
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Inputs.size(), 2);
            for (const auto& input : product.Params->Context->Inputs) {
                UNIT_ASSERT(inputs.insert(input.Label()).second);
            }
            UNIT_ASSERT(!product.Results.empty());
            for (const auto& output : product.Results) {
                UNIT_ASSERT_VALUES_EQUAL(product.Params->Tiering->OutputTiers.at(output.OutputId),
                    generation == 1 ? 0 : ColdTier);
                outputs.push_back(output.Part->Label);
            }
        }
        UNIT_ASSERT(generations.contains(1));
        UNIT_ASSERT(generations.contains(5));
        CheckInventory(env, originalLevels, "before parallel regular publications");
        CheckRows(env);

        auto* state = env.State;
        UNIT_ASSERT(state);
        blocked.Stop().Unblock();
        env->WaitFor("both regular publications", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(outputs.begin(), outputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        const auto levels = env.Levels();
        for (const auto& label : inputs) {
            UNIT_ASSERT(!levels.contains(label));
        }
        CheckInventory(env, levels, "after parallel regular publications");
        CheckRows(env);
        env.Restart();
        env.CheckLevels(levels, "after restart");
        CheckRows(env);
    }

    Y_UNIT_TEST(CoolingEntersAnEmptyAdjacentTierWithAnOlderVersionInTheSourceTier) {
        CheckEmptyAdjacentTierIsNotSkipped(false);
    }

    Y_UNIT_TEST(WarmingEntersAnEmptyAdjacentTierWithANewerVersionInTheSourceTier) {
        CheckEmptyAdjacentTierIsNotSkipped(true);
    }

    Y_UNIT_TEST(DisablingTieringRejectsAHeldTacAndPublishesItsRegularRetryAtomically) {
        TEnv env;
        auto policy = PrepareTwoGenerations(env);
        policy.Generations[4].ForceCountToCompact = 1000;
        const auto originalLevels = env.Levels();
        env.Enable(true);
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("one held cooling result", [&] { return blocked.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 1);
        const auto& tac = Product(blocked.front());
        UNIT_ASSERT(tac.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(tac.Params->Tiering->AllowCooling);
        CheckAdjacentOutputs(tac, ColdTier);
        TVector<TLogoBlobID> discardedOutputs;
        TVector<TLogoBlobID> originalInputs;
        for (const auto& output : tac.Results) {
            discardedOutputs.push_back(output.Part->Label);
        }
        for (const auto& input : tac.Params->Context->Inputs) {
            originalInputs.push_back(input.Label());
        }
        UNIT_ASSERT(!discardedOutputs.empty());
        UNIT_ASSERT_VALUES_EQUAL(originalInputs.size(), 2);

        env.Enable(false);
        // Keep the observer active: only the stale cooling result is released.
        // Its replacement must be held before the executor can publish it.
        blocked.Unblock(1);
        env->WaitFor("held regular replacement", [&] {
            return blocked.size() == 1
                && Product(blocked.front()).Params->Mode == NTable::TCompactionParams::EMode::Regular;
        });
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 1);
        const auto& regular = Product(blocked.front());
        UNIT_ASSERT(!regular.Params->Tiering->AllowCooling);
        TVector<TLogoBlobID> regularOutputs;
        for (const auto& output : regular.Results) {
            UNIT_ASSERT_VALUES_EQUAL(regular.Params->Tiering->OutputTiers.at(output.OutputId), 0);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}), 1);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetSlotChannel(output.Part->DescriptorSlot()), 1);
            regularOutputs.push_back(output.Part->Label);
        }
        UNIT_ASSERT(!regularOutputs.empty());

        // Neither the abandoned TAC nor the replacement awaiting publication
        // may make even one output visible; the complete old inventory remains.
        env.CheckLevels(originalLevels, "before regular retry publication");
        const auto staged = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(staged.size(), originalLevels.size());
        for (const auto& part : staged) {
            UNIT_ASSERT(originalLevels.contains(part->Label));
        }
        for (const auto& label : discardedOutputs) {
            UNIT_ASSERT(!originalLevels.contains(label));
        }
        for (const auto& label : regularOutputs) {
            UNIT_ASSERT(!originalLevels.contains(label));
        }
        CheckRows(env);

        auto* state = env.State;
        UNIT_ASSERT(state);
        blocked.Stop().Unblock();
        env->WaitFor("regular retry publication", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(regularOutputs.begin(), regularOutputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        const auto levels = env.Levels();
        for (const auto& label : discardedOutputs) {
            UNIT_ASSERT(!levels.contains(label));
        }
        for (const auto& label : originalInputs) {
            UNIT_ASSERT(!levels.contains(label));
        }
        for (const auto& label : regularOutputs) {
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(label)), 0);
        }
        CheckRows(env);
        env.Restart();
        env.CheckLevels(levels, "after restart");
        CheckRows(env);
    }

}
}
