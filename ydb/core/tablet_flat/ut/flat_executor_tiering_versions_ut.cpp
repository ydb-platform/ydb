#include "flat_executor_tiering_ut_common.h"

#include <ydb/core/testlib/actors/block_events.h>

#include <algorithm>

namespace NKikimr::NTabletFlatExecutor::NTieringTest {
namespace {

    struct TReadRow {
        ui64 Key;
        std::optional<ui64> Time;
        TString Value;
        ui64 Extra = 0;
    };

    TVector<TReadRow> ReadRows(TEnv& env, TRowVersion version = TRowVersion::Max(),
            bool reverse = false, bool follower = false) {
        TVector<TReadRow> result;
        env.Exec([&](TTransactionContext& txc) {
            const TVector<NTable::TTag> tags{KeyTag, TimeTag, ValueTag, ExtraTag};
            TVector<TReadRow> rows;
            const auto read = [&](auto& iter) {
                for (;;) {
                    const auto ready = iter->Next(NTable::ENext::Data);
                    if (ready == NTable::EReady::Page) {
                        return false;
                    }
                    if (ready == NTable::EReady::Gone) {
                        return true;
                    }
                    const auto& row = iter->Row();
                    const auto time = row.Get(1);
                    const auto value = row.Get(2);
                    rows.push_back({
                        row.Get(0).template AsValue<ui64>(),
                        time.IsNull() ? std::nullopt : std::optional<ui64>(time.template AsValue<ui64>()),
                        TString(value.Data(), value.Size()),
                        row.Get(3).template AsValue<ui64>(),
                    });
                }
            };
            if (reverse) {
                auto iter = txc.DB.IterateRangeReverse(TableId, {}, tags, version);
                if (!read(iter)) {
                    return false;
                }
            } else {
                auto iter = txc.DB.IterateRange(TableId, {}, tags, version);
                if (!read(iter)) {
                    return false;
                }
            }
            result = std::move(rows);
            return true;
        }, follower);
        return result;
    }

    void CheckRows(TEnv& env, TVector<TReadRow> expected,
            TRowVersion version = TRowVersion::Max(), bool follower = false) {
        for (bool reverse : {false, true}) {
            if (reverse) {
                std::reverse(expected.begin(), expected.end());
            }
            const auto actual = ReadRows(env, version, reverse, follower);
            UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
            for (size_t index = 0; index < expected.size(); ++index) {
                UNIT_ASSERT_VALUES_EQUAL(actual[index].Key, expected[index].Key);
                UNIT_ASSERT(actual[index].Time == expected[index].Time);
                UNIT_ASSERT_VALUES_EQUAL(actual[index].Value, expected[index].Value);
                UNIT_ASSERT_VALUES_EQUAL(actual[index].Extra, expected[index].Extra);
            }
        }
    }

    void CheckMissing(TEnv& env, ui64 key, TRowVersion version = TRowVersion::Max()) {
        env.Exec([=](TTransactionContext& txc) {
            const auto rawKey = NScheme::TUint64::TInstance(key);
            const TVector<NTable::TTag> tags{ValueTag};
            NTable::TRowState row;
            const auto ready = txc.DB.Select(TableId, {rawKey}, tags, row, 0, version);
            if (ready == NTable::EReady::Page) {
                return false;
            }
            UNIT_ASSERT(ready == NTable::EReady::Gone);
            return true;
        });
    }

    struct TCompacted {
        TIntrusiveConstPtr<NTable::TCompactionContext> Context;
        TVector<NTable::TPartView> Outputs;
        THashMap<TLogoBlobID, ui32> OutputTiers;
    };

    TCompacted CompactGeneration(TEnv& env, ui32 generation) {
        NActors::TBlockEvents<NOps::TEvResult> blocked(env.Env,
            [generation](const NOps::TEvResult::TPtr& ev) {
                const auto* compact = dynamic_cast<const TProdCompact*>(ev->Get()->Result.Get());
                return compact && compact->Params && compact->Params->Tiering
                    && static_cast<const NTable::NCompGen::TGenCompactionParams&>(
                        *compact->Params).Generation == generation;
            });
        auto policy = MakePolicy();
        policy->Generations[generation - 1].ForceCountToCompact = 1;
        env.Policy(*policy);
        env->WaitFor("tiering version compaction result", [&] { return !blocked.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        const auto* compact = dynamic_cast<const TProdCompact*>(blocked.front()->Get()->Result.Get());
        UNIT_ASSERT(compact);
        UNIT_ASSERT(compact->Success);
        UNIT_ASSERT(compact->Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        TCompacted result;
        result.Context = compact->Params->Context;
        for (const auto& output : compact->Results) {
            result.Outputs.push_back(output.Part);
            result.OutputTiers.emplace(output.Part->Label,
                compact->Params->Tiering->OutputTiers.at(output.OutputId));
        }
        blocked.Stop().Unblock();

        // Each call gives the executor a chance to publish the unblocked
        // result; do not let an unchanged value hide an abandoned switch.
        for (ui32 attempt = 0; attempt != 100; ++attempt) {
            THashSet<TLogoBlobID> current;
            for (const auto& part : env.Parts()) {
                current.insert(part->Label);
            }
            bool published = true;
            for (const auto& output : result.Outputs) {
                published &= current.contains(output->Label);
            }
            for (const auto& input : result.Context->Inputs) {
                published &= !current.contains(input.Label());
            }
            if (published) {
                return result;
            }
        }
        UNIT_FAIL("The tiering result was produced but was not published");
        return result;
    }

    bool Includes(const TCompacted& result, const TLogoBlobID& label) {
        return std::any_of(result.Context->Inputs.begin(), result.Context->Inputs.end(),
            [&](const auto& input) { return input.Label() == label; });
    }

    void CheckOutputEpochAndTiers(TEnv& env, const TCompacted& result, NTable::TEpoch epoch) {
        const auto levels = env.Levels();
        THashSet<ui32> tiers;
        UNIT_ASSERT(!result.Outputs.empty());
        for (const auto& part : result.Outputs) {
            const ui32 tier = result.OutputTiers.at(part->Label);
            tiers.insert(tier);
            UNIT_ASSERT_VALUES_EQUAL(part->Epoch, epoch);
            UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(part->Label)), tier);
            UNIT_ASSERT_VALUES_EQUAL(part->GetDataChannel({}), tier == 0 ? 1 : 3);
            UNIT_ASSERT_VALUES_EQUAL(part->GetSlotChannel(part->DescriptorSlot()), 1);
        }
        // Every scenario has a fresh key for default and an old key for
        // archive. Correct values alone would also pass if TAC kept both hot.
        UNIT_ASSERT_VALUES_EQUAL(tiers.size(), 2);
        UNIT_ASSERT(tiers.contains(0));
        UNIT_ASSERT(tiers.contains(ArchiveTier));
    }

    void CheckEpochClosure(TEnv& env, const TCompacted& result,
            TConstArrayRef<TLogoBlobID> required, NTable::TEpoch lower, NTable::TEpoch upper) {
        UNIT_ASSERT(result.Context->Epochs);
        UNIT_ASSERT_VALUES_EQUAL(result.Context->Epochs->Min, lower);
        UNIT_ASSERT_VALUES_EQUAL(result.Context->Epochs->Max, upper);
        UNIT_ASSERT_VALUES_EQUAL(result.Context->Inputs.size(), required.size());
        for (const auto& label : required) {
            UNIT_ASSERT(Includes(result, label));
        }
        CheckOutputEpochAndTiers(env, result, upper);
    }

    void CheckEqualEpochBoundary(bool lowerBoundary) {
        TEnv env;
        env.Enable();
        constexpr ui64 fresh = Max<ui64>();
        const auto old = env.FlushPart({{1, 0, "old", 10, TRowVersion(1, 10)}});
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(env.Levels().at(old->Label)), ArchiveTier);
        THashSet<TLogoBlobID> before;
        for (const auto& part : env.Parts()) {
            before.insert(part->Label);
        }
        env.Write({{1, fresh, "new", 20, TRowVersion(2, 20)},
            {2, 0, "archive", 20, TRowVersion(2, 20)}});
        env.Flush();
        NTable::TPartView hot;
        NTable::TPartView cold;
        const auto levels = env.Levels();
        for (const auto& part : env.Parts()) {
            if (!before.contains(part->Label)) {
                if (env.Tier(levels.at(part->Label)) == 0) {
                    hot = part;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(part->Label)), ArchiveTier);
                    cold = part;
                }
            }
        }
        UNIT_ASSERT(hot && cold);
        UNIT_ASSERT_VALUES_EQUAL(hot->Epoch, cold->Epoch);
        UNIT_ASSERT(old->Epoch < hot->Epoch);

        NTable::TPartView last;
        if (lowerBoundary) {
            last = env.FlushPart({{3, 0, "later", 30, TRowVersion(3, 30)}});
            auto staged = env.Levels();
            // The older archive part remains outside [sharedEpoch, lastEpoch].
            staged[old->Label] = 7;
            staged[cold->Label] = 6;
            staged[last->Label] = 6;
            env.SeedLevels(staged);
        }
        const auto compacted = CompactGeneration(env, 6);
        if (lowerBoundary) {
            const TVector<TLogoBlobID> required{hot->Label, cold->Label, last->Label};
            CheckEpochClosure(env, compacted, required, hot->Epoch, last->Epoch);
            UNIT_ASSERT(!Includes(compacted, old->Label));
        } else {
            const TVector<TLogoBlobID> required{old->Label, hot->Label, cold->Label};
            CheckEpochClosure(env, compacted, required, old->Epoch, hot->Epoch);
        }

        env.Read(1, "new");
        env.Read(1, "old", TRowVersion(1, 15));
        CheckRows(env, {{1, 0, "old", 10}}, TRowVersion(1, 15));
        CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "archive", 20}}, TRowVersion(2, 25));
        if (lowerBoundary) {
            CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "archive", 20}, {3, 0, "later", 30}});
        }
        const auto placement = env.Levels();
        env.Restart();
        env.CheckLevels(placement, "after restart");
        env.Read(1, "new");
        env.Read(1, "old", TRowVersion(1, 15));
        CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "archive", 20}}, TRowVersion(2, 25));
    }
}

Y_UNIT_TEST_SUITE(TFlatTableExecutor_TieringVersions) {

    Y_UNIT_TEST(ColdEpochIntervalIncludesTheNewerHotVersionBeforePromotion) {
        TEnv env;
        env.Enable();
        constexpr ui64 fresh = Max<ui64>();
        const auto old = env.FlushPart({{1, 0, "old", 10, TRowVersion(1, 10)}});
        const auto hot = env.FlushPart({{1, fresh, "new", 20, TRowVersion(2, 20)}});
        const auto last = env.FlushPart({{2, 0, "other", 30, TRowVersion(3, 30)}});
        UNIT_ASSERT(old->Epoch < hot->Epoch && hot->Epoch < last->Epoch);
        const auto before = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(old->Label)), ArchiveTier);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(hot->Label)), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(last->Label)), ArchiveTier);

        const auto compacted = CompactGeneration(env, 6);
        const TVector<TLogoBlobID> required{old->Label, hot->Label, last->Label};
        CheckEpochClosure(env, compacted, required, old->Epoch, last->Epoch);
        CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "other", 30}});
        CheckRows(env, {{1, 0, "old", 10}}, TRowVersion(1, 15));
        env.Read(1, "new");
        env.Read(1, "old", TRowVersion(1, 15));

        const auto placement = env.Levels();
        env.Restart();
        env.CheckLevels(placement, "after restart");
        CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "other", 30}});
        CheckRows(env, {{1, 0, "old", 10}}, TRowVersion(1, 15));
        env.FireDummyFollower(1);
        CheckRows(env, {{1, fresh, "new", 20}, {2, 0, "other", 30}}, TRowVersion::Max(), true);
        CheckRows(env, {{1, 0, "old", 10}}, TRowVersion(1, 15), true);
    }

    Y_UNIT_TEST(EqualEpochPeersAtTheUpperBoundAreCompactedTogether) {
        CheckEqualEpochBoundary(false);
    }

    Y_UNIT_TEST(EqualEpochPeersAtTheLowerBoundAreCompactedTogether) {
        CheckEqualEpochBoundary(true);
    }

    Y_UNIT_TEST(PartialUpdatesEraseAndResurrectionKeepMvccHistoryAfterTacAndRestart) {
        TEnv env;
        constexpr ui64 fresh = Max<ui64>();
        const auto base = env.FlushPart({{1, fresh, "base", 7, TRowVersion(1, 10)}});
        env.Exec([](TTransactionContext& txc) {
            const auto key = NScheme::TUint64::TInstance(1);
            const auto extra = NScheme::TUint64::TInstance(9);
            const NTable::TUpdateOp update{ExtraTag, NTable::ECellOp::Set, extra};
            txc.DB.Update(TableId, NTable::ERowOp::Upsert, {key}, {update}, TRowVersion(2, 20));
            return true;
        });
        env.Flush();
        NTable::TPartView partial;
        for (const auto& part : env.Parts()) {
            if (part->Label != base->Label) {
                partial = part;
            }
        }
        UNIT_ASSERT(partial);
        const auto last = env.FlushPart({{2, 0, "other", 30, TRowVersion(3, 30)}});
        auto staged = env.Levels();
        staged[base->Label] = 6;
        staged[partial->Label] = 3;
        staged[last->Label] = 6;
        env.SeedLevels(staged);
        env.Enable();
        const auto compacted = CompactGeneration(env, 6);
        const TVector<TLogoBlobID> required{base->Label, partial->Label, last->Label};
        CheckEpochClosure(env, compacted, required, base->Epoch, last->Epoch);
        CheckRows(env, {{1, fresh, "base", 7}}, TRowVersion(1, 15));
        CheckRows(env, {{1, fresh, "base", 9}}, TRowVersion(2, 25));

        env.Write({{1, std::nullopt, {}, 0, TRowVersion(4, 40), true}});
        env.Flush();
        CheckMissing(env, 1);
        env.Read(1, "base", TRowVersion(2, 25));
        env.Write({{1, fresh, "resurrected", 11, TRowVersion(5, 50)}});
        env.Flush();
        env.Read(1, "resurrected");
        CheckMissing(env, 1, TRowVersion(4, 45));

        env.Restart();
        CheckRows(env, {{1, fresh, "base", 7}}, TRowVersion(1, 15));
        CheckRows(env, {{1, fresh, "base", 9}}, TRowVersion(2, 25));
        CheckMissing(env, 1, TRowVersion(4, 45));
        CheckRows(env, {{1, fresh, "resurrected", 11}, {2, 0, "other", 30}});
    }
}
}
