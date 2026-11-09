#include "flat_executor_tiering_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_gen.h>
#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_scan_events.h>
#include <ydb/core/tablet/resource_broker.h>
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

    void CheckArchiveOutputs(const TProdCompact& product) {
        UNIT_ASSERT(!product.Results.empty());
        UNIT_ASSERT(product.Params->Tiering);
        for (const auto& output : product.Results) {
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Tiering->OutputTiers.at(output.OutputId), ArchiveTier);
            UNIT_ASSERT_VALUES_EQUAL(output.Part->GetDataChannel({}), 3);
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
            levels[parts[i]->Label] = i < 2 ? 4 : 1;
        }
        env.SeedLevels(levels);
        NLocalDb::TCompactionPolicy policy;
        env.Exec([&](TTransactionContext& txc) {
            policy = *txc.DB.GetScheme().GetTableInfo(TableId)->CompactionPolicy;
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(policy.Generations.size(), 7);
        policy.Generations[0].ForceCountToCompact = 2;
        policy.Generations[3].ForceCountToCompact = 2;
        return policy;
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutor_TieringConcurrency) {
    Y_UNIT_TEST(ParallelTacPublicationsKeepBothInputsAndReadsValid) {
        TEnv env;
        const auto policy = PrepareTwoGenerations(env);
        env.Enable(true);
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("two disjoint real TAC results", [&] { return blocked.size() == 2; });

        THashSet<ui32> generations;
        THashSet<TLogoBlobID> inputs;
        TVector<TLogoBlobID> outputs;
        for (const auto& event : blocked) {
            const auto& product = Product(event);
            generations.insert(Generation(event));
            UNIT_ASSERT(product.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
            UNIT_ASSERT_VALUES_EQUAL(product.Params->Context->Inputs.size(), 2);
            CheckArchiveOutputs(product);
            for (const auto& input : product.Params->Context->Inputs) {
                UNIT_ASSERT(inputs.insert(input.Label()).second);
            }
            for (const auto& output : product.Results) {
                UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, product.Params->Context->Epochs->Max);
                outputs.push_back(output.Part->Label);
            }
        }
        UNIT_ASSERT(generations.contains(1));
        UNIT_ASSERT(generations.contains(4));
        CheckRows(env);

        // Publish one operation while the other still holds its original masks.
        // The second result must remain usable after that known replacement.
        const TLogoBlobID firstOutput = Product(blocked.front()).Results.front().Part->Label;
        blocked.Unblock(1);
        TCompactionLogicState* state = nullptr;
        env.Call([&](TExecutor&, TCompactionLogicState& current) { state = &current; });
        env->WaitFor("first TAC publication", [&] {
            return state->Tables.at(TableId).Strategy->SnapshotState().PartLevels.contains(firstOutput);
        });
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(env.Levels().at(firstOutput)), ArchiveTier);
        CheckRows(env);

        blocked.Stop().Unblock();
        env->WaitFor("both TAC publications", [&] {
            const auto levels = state->Tables.at(TableId).Strategy->SnapshotState().PartLevels;
            return std::all_of(outputs.begin(), outputs.end(), [&](const auto& label) {
                return levels.contains(label);
            });
        });
        const auto levels = env.Levels();
        for (const auto& label : inputs) {
            UNIT_ASSERT(!levels.contains(label));
        }
        for (const auto& label : outputs) {
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(label)), ArchiveTier);
        }
        CheckRows(env);
        env.Restart();
        env.CheckLevels(levels, "after restart");
        CheckRows(env);
    }

    Y_UNIT_TEST(DisablingTieringRejectsAHeldTacAndPublishesItsRegularRetryAtomically) {
        TEnv env;
        auto policy = PrepareTwoGenerations(env);
        policy.Generations[3].ForceCountToCompact = 1000;
        const auto originalLevels = env.Levels();
        env.Enable(true);
        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        env.Policy(policy);
        env->WaitFor("one held cooling result", [&] { return blocked.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 1);
        const auto& tac = Product(blocked.front());
        UNIT_ASSERT(tac.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(tac.Params->Tiering->AllowCooling);
        CheckArchiveOutputs(tac);
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

    Y_UNIT_TEST(AConflictingEpochClosureDrainsToAnExclusiveTac) {
        TEnv env;
        // The cold generation brackets the hot generation in epoch order.
        // Its initial inputs therefore need both already reserved hot parts.
        for (ui64 key : {10, 20, 21, 11}) {
            env.Write({TRow{.Key = key, .Time = 0, .Value = ToString(key)}});
            env.Flush();
        }
        auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 4);
        std::sort(parts.begin(), parts.end(), [](const auto& a, const auto& b) {
            return a->Epoch < b->Epoch;
        });
        auto levels = env.Levels();
        levels[parts[0]->Label] = 4;
        levels[parts[1]->Label] = 1;
        levels[parts[2]->Label] = 1;
        levels[parts[3]->Label] = 4;
        env.SeedLevels(levels);
        auto policy = MakePolicy();
        policy->TieringCompactionConflictLimit = 1;
        policy->Generations[0].ForceCountToCompact = 2;
        policy->Generations[3].ForceCountToCompact = 2;
        env.Enable(true);

        TBlockEvents<NOps::TEvResult> blocked(env.Env);
        // Delay submission of the cold task until the hot scan has finished
        // writing but still owns its inputs. This makes the conflict deterministic.
        TBlockEvents<NResourceBroker::TEvResourceBroker::TEvSubmitTask> coldSubmits(env.Env,
            [](const NResourceBroker::TEvResourceBroker::TEvSubmitTask::TPtr& event) {
                return event->Get()->Task.Name.StartsWith("gen4-table-101-");
            });
        env.Policy(*policy);
        env->WaitFor("hot result and pending cold task", [&] {
            return blocked.size() == 1 && coldSubmits.size() == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(Generation(blocked.front()), 1);
        CheckArchiveOutputs(Product(blocked.front()));
        TVector<TLogoBlobID> hotOutputs;
        for (const auto& output : Product(blocked.front()).Results) {
            hotOutputs.push_back(output.Part->Label);
        }
        UNIT_ASSERT(!hotOutputs.empty());
        auto& strategy = static_cast<NTable::NCompGen::TGenCompactionStrategy&>(
            *env.State->Tables.at(TableId).Strategy);

        coldSubmits.Stop().Unblock();
        env->WaitFor("cold input conflict enters exclusive retry", [&] {
            return strategy.GetTierCompactionConflicts(4) == 1
                && strategy.GetSerialTierCompactionGeneration() == 4;
        });
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        env.CheckLevels(levels, "after restart");
        CheckRows(env);

        // The active hot operation publishes normally. Its success must not
        // reset the cold generation's counter or bypass its priority.
        blocked.Unblock(1);
        env->WaitFor("exclusive cold retry result", [&] {
            return blocked.size() == 1 && Generation(blocked.front()) == 4;
        });
        UNIT_ASSERT_VALUES_EQUAL(strategy.GetTierCompactionConflicts(4), 1);
        UNIT_ASSERT(strategy.GetSerialTierCompactionGeneration() == 4);
        const auto current = env.Levels();
        for (const auto& label : hotOutputs) {
            UNIT_ASSERT(current.contains(label));
        }
        const auto& exclusive = Product(blocked.front());
        UNIT_ASSERT(exclusive.Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        CheckArchiveOutputs(exclusive);
        UNIT_ASSERT(!exclusive.Params->Tiering->UseConservativeMasks);
        UNIT_ASSERT_VALUES_EQUAL(exclusive.Params->Context->Inputs.size(), 3);
        for (const auto& label : hotOutputs) {
            UNIT_ASSERT(std::any_of(exclusive.Params->Context->Inputs.begin(),
                exclusive.Params->Context->Inputs.end(), [&](const auto& part) {
                    return part.Label() == label;
                }));
        }
        TVector<TLogoBlobID> finalOutputs;
        for (const auto& output : exclusive.Results) {
            UNIT_ASSERT_VALUES_EQUAL(output.Part->Epoch, parts[3]->Epoch);
            finalOutputs.push_back(output.Part->Label);
        }
        UNIT_ASSERT(!finalOutputs.empty());
        CheckRows(env);

        blocked.Stop().Unblock();
        env->WaitFor("exclusive TAC publication releases serialized mode", [&] {
            return strategy.GetTierCompactionConflicts(4) == 0
                && !strategy.GetSerialTierCompactionGeneration();
        });
        const auto finalLevels = env.Levels();
        for (const auto& label : finalOutputs) {
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(finalLevels.at(label)), ArchiveTier);
        }
        CheckRows(env);
        env.Restart();
        UNIT_ASSERT(env.Levels() == finalLevels);
        CheckRows(env);
    }
}
}
