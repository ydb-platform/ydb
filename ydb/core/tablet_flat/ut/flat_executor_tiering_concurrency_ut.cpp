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

    NLocalDb::TCompactionPolicy PrepareTwoGenerations(TEnv& env, ui64 newestRowTime = 0) {
        for (ui64 key : {10, 11, 20, 21}) {
            env.Write({TRow{.Key = key, .Time = key == 21 ? newestRowTime : 0, .Value = ToString(key)}});
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
    Y_UNIT_TEST(AddingTiersPreservesTheLegacyFinalLevelAndWakesTheNextFlush) {
        TEnv env(/*withTiering*/ false);
        env.Enable();
        const auto input = env.FlushPart({{1, 0, "written before ALTER"}});
        auto previous = MakePolicy(/*withTiering*/ false);
        const ui32 lastLevel = previous->Generations.size();
        env.SeedLevels({{input->Label, lastLevel}});
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        previous->Generations.back().ForceCountToCompact = 1;
        env.Policy(*previous);
        env->WaitFor("held last-generation compaction", [&] { return blocked.size() == 1; }, TDuration::Seconds(10));
        const auto& legacy = Product(blocked.front());
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), lastLevel);
        UNIT_ASSERT(!legacy.Params->Tiering);
        UNIT_ASSERT_VALUES_EQUAL(legacy.Results.size(), 1);
        const auto legacyLabel = legacy.Results.front().Part->Label;

        const auto policy = MakePolicy();
        UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(lastLevel + 1), ColdTier);
        env.Exec([&](TTransactionContext& txc) {
            txc.DB.Alter().SetCompactionPolicy(TableId, *policy)
                .SetTieringPolicy(TableId, MakeTierPolicy());
            return true;
        });
        env.Write({{2, 0, "written after ALTER"}});
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env->WaitFor("flush waiting for the legacy scan", [&] {
            const auto& mem = env.State->Tables.at(TableId).InMem;
            return mem.State == ECompactionState::Compaction && mem.CompactionTask.TaskId == 0;
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1u);
        blocked.Unblock(1);
        env->WaitFor("tier-aware flush after legacy publication", [&] { return blocked.size() == 1; },
            TDuration::Seconds(10));
        const auto& flush = Product(blocked.front());
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 0u);
        CheckAdjacentOutputs(flush, ColdTier);
        UNIT_ASSERT_VALUES_EQUAL(flush.Results.size(), 1u);
        const auto cooledLabel = flush.Results.front().Part->Label;
        UNIT_ASSERT_VALUES_EQUAL(env.Levels().at(legacyLabel), previous->GetFinalLevel(0));
        blocked.Stop().Unblock();
        for (ui32 count = 0; count < 2; ++count) {
            UNIT_ASSERT(env.GrabEdgeEvent<NFake::TEvCompacted>(TDuration::Seconds(10)));
        }
        const auto levels = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(levels.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(levels.at(cooledLabel)), ColdTier);
        env.Restart();
        env.CheckLevels(levels, "after enabling tiering during the last-generation compaction");
        env.Read(1, "written before ALTER");
        env.Read(2, "written after ALTER");
    }

    Y_UNIT_TEST(AlteringTierRulesAndPlacementPreservesTheRunningDiskCompaction) {
        TEnv env;
        const ui64 time = env.NowSeconds() - TDuration::Days(30).Seconds();
        env.FlushPart({{1, time, "old rule"}});
        env.Enable();
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Tick();
        env->WaitFor("held disk TAC", [&] { return blocked.size() == 1; }, TDuration::Seconds(10));
        const auto& disk = Product(blocked.front());
        UNIT_ASSERT(Generation(blocked.front()) != 0);
        CheckAdjacentOutputs(disk, ColdTier);
        UNIT_ASSERT_VALUES_EQUAL(disk.Results.size(), 1);
        const auto oldLabel = disk.Results.front().Part->Label;

        constexpr ui32 deepTier = 99;
        auto policy = MakePolicy();
        auto generation = policy->Generations.back();
        generation.TierId = deepTier;
        policy->Generations.push_back(generation);
        env.Exec([&](TTransactionContext& txc) {
            auto rules = MakeTierPolicy();
            rules.Rules.front().TierId = deepTier;
            rules.Rules.back().Ranges.front().To = NTable::TTierRelativeBound{TDuration::Days(100)};
            txc.DB.Alter().SetRoom(TableId, ColdTier, 3, {3}, 3)
                .AddFamily(TableId, deepTier, ArchiveTier)
                .SetCompactionPolicy(TableId, *policy)
                .SetTieringPolicy(TableId, rules);
            return true;
        });
        blocked.Stop().Unblock();
        UNIT_ASSERT(env.GrabEdgeEvent<NFake::TEvCompacted>(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(env.Levels().at(oldLabel)), ColdTier);

        const auto hot = env.FlushPart({{2, time, "new rule"}});
        const auto cold = env.FlushPart({{3, 0, "new channel"}});
        const auto levels = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(levels.at(hot->Label)), 0u);
        UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(levels.at(cold->Label)), ColdTier);
        UNIT_ASSERT_VALUES_EQUAL(cold->GetDataChannel({}), 3u);
        env.Restart();
        env.CheckLevels(levels, "after changing tier rules and placement");
        env.Read(1, "old rule");
        env.Read(2, "new rule");
        env.Read(3, "new channel");
    }

    Y_UNIT_TEST(AlteringAnotherTablePreservesTheRunningFlushAndDiskCompaction) {
        TEnv env;
        constexpr ui32 otherTable = TableId + 1;
        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().AddTable("other", otherTable)
                .AddColumn(otherTable, "key", KeyTag, NScheme::NTypeIds::Uint64, false, false)
                .AddColumnToKey(otherTable, KeyTag);
            return true;
        });

        env.FlushPart({{1, 0, "on disk"}});
        env.Enable();
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Tick();
        env->WaitFor("held disk TAC", [&] { return blocked.size() == 1; }, TDuration::Seconds(10));
        UNIT_ASSERT(Generation(blocked.front()) != 0);
        UNIT_ASSERT(Product(blocked.front()).Params->Mode == NTable::TCompactionParams::EMode::TierAware);

        env.Write({{2, 0, "in memory"}});
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env->WaitFor("held MemTable flush", [&] { return blocked.size() == 2; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.back()), 0);
        const auto& disk = Product(blocked.front());
        const auto& flush = Product(blocked.back());
        UNIT_ASSERT_VALUES_EQUAL(disk.Results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(flush.Results.size(), 1);
        const auto diskOutput = disk.Results.front().Part->Label;
        const auto flushOutput = flush.Results.front().Part->Label;

        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().AddColumn(otherTable, "value", ValueTag, NScheme::NTypeIds::String, false, false);
            return true;
        });
        blocked.Stop().Unblock();
        for (size_t i = 0; i < 2; ++i) {
            const auto done = env->GrabEdgeEventRethrow<NFake::TEvCompacted>(env.Edge, TDuration::Seconds(10));
            UNIT_ASSERT(done);
            UNIT_ASSERT_VALUES_EQUAL(done->Get()->Table, TableId);
        }
        const auto levels = env.Levels();
        UNIT_ASSERT(levels.contains(diskOutput));
        UNIT_ASSERT(levels.contains(flushOutput));
    }

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

    Y_UNIT_TEST(DisablingTieringPublishesThePreparedTacAndPreventsCoolingOnTheNextCompaction) {
        TEnv env;
        auto policy = PrepareTwoGenerations(env, env.NowSeconds());
        policy.Generations[4].ForceCountToCompact = 1000;
        const auto originalLevels = env.Levels();
        env.Enable(true);
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("prepared cooling result", [&] { return blocked.size() == 1; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 1);
        const auto& tac = Product(blocked.front());
        UNIT_ASSERT(tac.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(tac.Params->Tiering->AllowCooling);
        auto afterTac = originalLevels;
        UNIT_ASSERT_VALUES_EQUAL(tac.Params->Context->Inputs.size(), 2);
        for (const auto& input : tac.Params->Context->Inputs) {
            UNIT_ASSERT_VALUES_EQUAL(originalLevels.at(input.Label()), 1);
            UNIT_ASSERT_VALUES_EQUAL(afterTac.erase(input.Label()), 1);
        }
        THashSet<ui32> outputTiers;
        for (const auto& output : tac.Results) {
            const ui32 tier = tac.Params->Tiering->OutputTiers.at(output.OutputId);
            UNIT_ASSERT(outputTiers.insert(tier).second);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}), tier == ColdTier ? 2 : 1);
            // The hot row advances from generation 1 to 2; the old row enters the first cold generation.
            UNIT_ASSERT(afterTac.emplace(output.Part->Label, tier == ColdTier ? 4 : 2).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(outputTiers.size(), 2);
        UNIT_ASSERT(outputTiers.contains(0));
        UNIT_ASSERT(outputTiers.contains(ColdTier));

        env.Enable(false);
        CheckInventory(env, originalLevels, "before publishing the prepared TAC");
        CheckRows(env);
        blocked.Unblock(1);
        UNIT_ASSERT(env.GrabEdgeEvent<NFake::TEvCompacted>(TDuration::Seconds(10)));
        CheckInventory(env, afterTac, "after publishing both prepared TAC outputs");
        CheckRows(env);

        // These two older cold parts qualify for the archive tier when cooling is enabled.
        policy.Generations[4].ForceCountToCompact = 2;
        env.Policy(policy);
        env->WaitFor("prepared compaction with tiering disabled", [&] { return blocked.size() == 1; },
            TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 5);
        const auto& regular = Product(blocked.front());
        UNIT_ASSERT(regular.Params->Mode == NTable::TCompactionParams::EMode::Regular);
        UNIT_ASSERT(!regular.Params->Tiering->AllowCooling);
        CheckAdjacentOutputs(regular, ColdTier);
        auto afterRegular = afterTac;
        UNIT_ASSERT_VALUES_EQUAL(regular.Params->Context->Inputs.size(), 2);
        for (const auto& input : regular.Params->Context->Inputs) {
            UNIT_ASSERT_VALUES_EQUAL(originalLevels.at(input.Label()), 5);
            UNIT_ASSERT_VALUES_EQUAL(afterRegular.erase(input.Label()), 1);
        }
        for (const auto& output : regular.Results) {
            UNIT_ASSERT(afterRegular.emplace(output.Part->Label, policy.GetFinalLevel(ColdTier)).second);
        }
        CheckInventory(env, afterTac, "before publishing the regular compaction");
        blocked.Stop().Unblock();
        UNIT_ASSERT(env.GrabEdgeEvent<NFake::TEvCompacted>(TDuration::Seconds(10)));
        CheckInventory(env, afterRegular, "after publishing the regular compaction");
        CheckRows(env);
        env.Restart();
        env.CheckLevels(afterRegular, "after restart");
        CheckRows(env);
    }

}
}
