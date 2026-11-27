#include "flat_executor_tiering_ut_common.h"

#include <ydb/core/tablet_flat/flat_boot_cookie.h>
#include <ydb/core/tablet_flat/flat_sausage_slicer.h>
#include <ydb/core/tablet_flat/protos/flat_executor.pb.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
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

    TCompacted CompactGeneration(TEnv& env, ui32 generation,
            NTable::TCompactionParams::EMode mode = NTable::TCompactionParams::EMode::TierAware) {
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
        UNIT_ASSERT(compact->Params->Mode == mode);
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
            UNIT_ASSERT_VALUES_EQUAL(part->GetDataChannel({}), tier == 0 ? 1 : tier == ColdTier ? 2 : 3);
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
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(env.Levels().at(old->Label)), ColdTier);
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
                    UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(part->Label)), ColdTier);
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
        const auto compacted = CompactGeneration(env, lowerBoundary ? 6 : 4);
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

    Y_UNIT_TEST(SchemaChangesUseTheTierDataChannel) {
        TEnv env;
        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().AddFamily(TableId, 1, ArchiveTier)
                .AddColumnToFamily(TableId, ValueTag, 1);
            return true;
        });
        env.Enable();
        const auto part = env.FlushPart({{1, env.NowSeconds() - TDuration::Days(30).Seconds(), "cold"}});
        const auto level = env.Levels().at(part->Label);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(level), ColdTier);
        UNIT_ASSERT_VALUES_EQUAL(part->GroupsCount, 2u);
        UNIT_ASSERT_VALUES_EQUAL(part->GetDataChannel({}), 2u);
        UNIT_ASSERT_VALUES_EQUAL(part->GetDataChannel(NTable::NPage::TGroupId(1)), 2u);

        const auto checkSchemaChanges = [&](bool expected) {
            env.RawCall([&](TExecutor& executor) {
                UNIT_ASSERT_VALUES_EQUAL(executor.HasSchemaChanges(TableId), expected);
            });
        };
        checkSchemaChanges(false);
        env.Restart();
        checkSchemaChanges(false);

        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().SetRoom(TableId, ColdTier, 3, {2}, 2);
            return true;
        });
        checkSchemaChanges(true);
        const auto compacted = CompactGeneration(env, level);
        UNIT_ASSERT_VALUES_EQUAL(compacted.Outputs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(compacted.OutputTiers.at(compacted.Outputs.front()->Label), ColdTier);
        checkSchemaChanges(false);
    }

    Y_UNIT_TEST(PageCollectionCacheModeFollowsTheDestinationTier) {
        using NTable::NPage::ECacheMode;
        TEnv env;
        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter()
                .SetFamilyCacheMode(TableId, 0, ECacheMode::TryKeepInMemory)
                .AddFamily(TableId, 1, 0)
                .AddColumnToFamily(TableId, ValueTag, 1);
            return true;
        });

        THashMap<TLogoBlobID, THashMap<TActorId, ECacheMode>> attachments;
        auto observer = env.Env.AddObserver<NSharedCache::TEvAttach>([&](auto& event) {
            const auto* attach = event->Get();
            attachments[attach->PageCollection->Label()][event->Sender] = attach->CacheMode;
        });
        const auto check = [&](const NTable::TPartView& part, ECacheMode mainMode, ECacheMode valueMode, size_t owners) {
            UNIT_ASSERT_VALUES_EQUAL(part->GroupsCount, 2);
            const auto checkSlot = [&](ui32 slot, ECacheMode mode) {
                const auto& label = part->GetPageCollection(slot)->Label();
                env.Env.WaitFor(TStringBuilder() << "cache mode " << ui32(mode) << " for collection " << label,
                    [&] {
                        const auto* modes = attachments.FindPtr(label);
                        return modes && modes->size() == owners && std::all_of(modes->begin(), modes->end(),
                            [&](const auto& owner) { return owner.second == mode; });
                    }, TDuration::Seconds(10));
            };
            checkSlot(part->DescriptorSlot(), mainMode);
            checkSlot(part->DataSlot({}), mainMode);
            checkSlot(part->DataSlot(NTable::NPage::TGroupId(1)), valueMode);
        };

        env.Enable();
        const ui64 now = env.NowSeconds();
        env.Write({{1, now, "hot"}, {2, now - TDuration::Days(30).Seconds(), "will warm"},
            {3, now - TDuration::Days(180).Seconds(), "stays cold"}});
        env.Flush();
        auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);
        const auto levels = env.Levels();
        if (env.Tier(levels.at(parts[0]->Label)) != 0) {
            std::swap(parts[0], parts[1]);
        }
        const auto& hot = parts[0];
        const auto& cold = parts[1];
        const ui32 coldLevel = levels.at(cold->Label);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(hot->Label)), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(coldLevel), ColdTier);
        const auto checkBoth = [&](size_t owners) {
            check(hot, ECacheMode::TryKeepInMemory, ECacheMode::Regular, owners);
            check(cold, ECacheMode::Regular, ECacheMode::Regular, owners);
        };
        checkBoth(1);
        attachments.clear();
        env.Restart();
        checkBoth(1);
        env.FireDummyFollower(1);
        checkBoth(2);

        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().SetFamilyCacheMode(TableId, ColdTier, ECacheMode::TryKeepInMemory);
            return true;
        });
        check(cold, ECacheMode::TryKeepInMemory, ECacheMode::TryKeepInMemory, 2);
        check(hot, ECacheMode::TryKeepInMemory, ECacheMode::Regular, 2);

        env.Exec([](TTransactionContext& txc) {
            auto rules = MakeTierPolicy();
            rules.Rules.back().Ranges.front().To = NTable::TTierRelativeBound{TDuration::Days(100)};
            txc.DB.Alter().SetTieringPolicy(TableId, rules);
            return true;
        });
        const auto compacted = CompactGeneration(env, coldLevel);
        THashSet<ui32> outputTiers;
        for (const auto& output : compacted.Outputs) {
            const ui32 tier = compacted.OutputTiers.at(output->Label);
            UNIT_ASSERT(tier == 0 || tier == ColdTier);
            outputTiers.insert(tier);
            check(output, ECacheMode::TryKeepInMemory,
                tier == 0 ? ECacheMode::Regular : ECacheMode::TryKeepInMemory, 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(outputTiers.size(), 2);
    }

    Y_UNIT_TEST(ColdTierIndexPlacementSurvivesDisablingBTree) {
        TEnv env;
        auto& flags = env.Env.GetAppData().FeatureFlags;
        flags.SetEnableLocalDBFlatIndex(true);
        flags.SetEnableLocalDBBtreeIndexV2(false);
        env.Exec([](TTransactionContext& txc) {
            txc.DB.Alter().SetFamilyIndexPlacement(TableId, ColdTier, false, false);
            return true;
        });
        env.Enable();

        const TRowVersion oldVersion(1, 10);
        env.Write({{1, 0, "old", 0, oldVersion}});
        const auto mixed = env.FlushPart({{1, 0, "new", 0, TRowVersion(2, 20)}});
        const auto checkPlacement = [](const NTable::TPartView& part) {
            UNIT_ASSERT_VALUES_EQUAL(ui32(part->GetSlotChannel(part->DescriptorSlot())), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ui32(part->GetDataChannel({})), 2u);
            UNIT_ASSERT_VALUES_EQUAL(part->IndexSlot({}), part->DataSlot({}));
        };
        checkPlacement(mixed);
        UNIT_ASSERT(mixed->IndexPages.HasFlat() && mixed->IndexPages.HasBTree());

        flags.SetEnableLocalDBBtreeIndex(false);
        env.Restart();
        const auto restored = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(restored.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.front()->Label, mixed->Label);
        checkPlacement(restored.front());
        UNIT_ASSERT(restored.front()->IndexPages.HasFlat() && !restored.front()->IndexPages.HasBTree());
        env.Read(1, "old", oldVersion);
        env.Read(1, "new");

        const auto flat = env.FlushPart({{2, 0, "flat", 0, TRowVersion(3, 30)}});
        checkPlacement(flat);
        UNIT_ASSERT(flat->IndexPages.HasFlat() && !flat->IndexPages.HasBTree());
        env.Read(2, "flat");
    }

    Y_UNIT_TEST(RegularTierFinalizationKeepsErasesForOtherTierFinals) {
        for (ui32 tier : {0u, ColdTier, ArchiveTier}) {
            TEnv env;
            auto policy = MakePolicy();
            policy->KeepEraseMarkers = true;
            env.Policy(*policy);
            const auto older = env.FlushPart({{1, 0, "older-value", 7, TRowVersion(1, 10)}});
            const auto erased = env.FlushPart({{1, std::nullopt, {}, 0, TRowVersion(2, 20), true}});
            UNIT_ASSERT(older->Epoch < erased->Epoch);
            const ui32 generation = tier == 0 ? 3 : tier == ColdTier ? 5 : 7;
            const ui32 olderTier = tier == ArchiveTier ? ColdTier : ArchiveTier;
            const ui32 olderFinal = policy->GetFinalLevel(olderTier);
            const ui32 ownFinal = policy->GetFinalLevel(tier);
            env.SeedLevels({{older->Label, olderFinal}, {erased->Label, generation}});

            // Only the erase's tier is compacted. The older value in another
            // final is still capable of resurrecting the key if it is dropped.
            const auto compacted = CompactGeneration(env, generation,
                NTable::TCompactionParams::EMode::Regular);
            UNIT_ASSERT_VALUES_EQUAL(compacted.Context->Inputs.size(), 1);
            UNIT_ASSERT(Includes(compacted, erased->Label));
            UNIT_ASSERT(!Includes(compacted, older->Label));
            UNIT_ASSERT_VALUES_EQUAL(compacted.Context->Remaining.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(compacted.Context->Remaining.front().Label(), older->Label);
            UNIT_ASSERT_VALUES_EQUAL(compacted.Context->Remaining.front().Level, olderFinal);
            UNIT_ASSERT_VALUES_EQUAL(compacted.Outputs.size(), 1);
            const auto& output = compacted.Outputs.front();
            UNIT_ASSERT_VALUES_EQUAL(compacted.OutputTiers.at(output->Label), tier);
            const auto placement = env.Levels();
            UNIT_ASSERT_VALUES_EQUAL(placement.at(older->Label), olderFinal);
            UNIT_ASSERT_VALUES_EQUAL(placement.at(output->Label), ownFinal);
            CheckMissing(env, 1);
            env.Read(1, "older-value", TRowVersion(1, 15));

            env.Restart();
            env.CheckLevels(placement, "after erase finalization/restart");
            CheckMissing(env, 1);
            env.Read(1, "older-value", TRowVersion(1, 15));
        }
    }

    Y_UNIT_TEST(ColdEpochIntervalIncludesTheNewerHotVersionBeforePromotion) {
        TEnv env;
        env.Enable();
        constexpr ui64 fresh = Max<ui64>();
        const auto old = env.FlushPart({{1, 0, "old", 10, TRowVersion(1, 10)}});
        const auto hot = env.FlushPart({{1, fresh, "new", 20, TRowVersion(2, 20)}});
        const auto last = env.FlushPart({{2, 0, "other", 30, TRowVersion(3, 30)}});
        UNIT_ASSERT(old->Epoch < hot->Epoch && hot->Epoch < last->Epoch);
        const auto before = env.Levels();
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(old->Label)), ColdTier);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(hot->Label)), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(before.at(last->Label)), ColdTier);

        const auto compacted = CompactGeneration(env, 4);
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

    Y_UNIT_TEST(FollowerAppliesForcedDiskCompactionWithAnEpochGap) {
        TEnv env;
        const TVector<TReadRow> rows{{1, 0, "old", 10}};
        const auto input = env.FlushPart({{1, 0, "old", 10}});
        env.FireDummyFollower(1);

        // Erasing a key beyond the existing part advances the head on flush,
        // but the erase marker is discarded and leaves no newer part.
        env.Write({{2, std::nullopt, {}, 0, TRowVersion::Min(), true}});
        env.Flush();
        const auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parts.front()->Label, input->Label);

        CheckRows(env, rows, TRowVersion::Max(), true);
        env.Enable();

        NActors::TBlockEvents<NOps::TEvResult> blocked(env.Env, [](const auto& ev) {
            const auto* compact = dynamic_cast<const TProdCompact*>(ev->Get()->Result.Get());
            if (!compact || !compact->Params || compact->Params->Table != TableId || !compact->Params->Tiering) {
                return false;
            }
            const auto& params = static_cast<const NTable::NCompGen::TGenCompactionParams&>(*compact->Params);
            return params.ForcedMemRequestId != 0 || params.TieringForced;
        });
        env.SendSync(new NFake::TEvCompact(TableId));
        env->WaitFor("memory flush before forced disk compaction",
            [&] { return !blocked.empty(); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        {
            const auto* result = blocked.front()->Get();
            const auto* compact = static_cast<const TProdCompact*>(result->Result.Get());
            UNIT_ASSERT(compact->Success);
            UNIT_ASSERT(compact->Params->Mode == NTable::TCompactionParams::EMode::Regular);
            UNIT_ASSERT(result->Subset->Head > input->Epoch + 1);
            UNIT_ASSERT(result->Subset->Frozen.empty());
            UNIT_ASSERT(result->Subset->Flatten.empty());
            UNIT_ASSERT(compact->Results.empty());
        }
        blocked.Unblock();
        env->WaitFor("forced disk compaction result",
            [&] { return !blocked.empty(); }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        const auto* result = blocked.front()->Get();
        const auto* compact = static_cast<const TProdCompact*>(result->Result.Get());
        UNIT_ASSERT(compact->Success);
        UNIT_ASSERT(static_cast<const NTable::NCompGen::TGenCompactionParams&>(*compact->Params).TieringForced);
        UNIT_ASSERT(compact->Params->Mode == NTable::TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(result->Subset->Frozen.empty());
        UNIT_ASSERT(result->Subset->TxStatus.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Subset->Epoch(), input->Epoch);
        UNIT_ASSERT_VALUES_EQUAL(result->Subset->Head, NTable::TEpoch::Zero());
        UNIT_ASSERT_VALUES_EQUAL(result->Subset->Flatten.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Subset->Flatten.front()->Label, input->Label);
        UNIT_ASSERT_VALUES_EQUAL(compact->Results.size(), 1);
        const auto output = compact->Results.front().Part->Label;

        // Match the ack to the commit that introduces our output, rather than
        // accepting an ack for another snapshot or part switch.
        std::optional<TTxStamp> switchStamp;
        auto commit = env.Env.AddObserver<TEvTablet::TEvCommit>([&](auto& ev) {
            const auto* msg = ev->Get();
            if (msg->TabletID == env.Tablet && msg->WaitFollowerGcAck
                    && std::find(msg->GcDiscovered.begin(), msg->GcDiscovered.end(), output)
                        != msg->GcDiscovered.end()) {
                switchStamp.emplace(msg->Generation, msg->Step);
            }
        });
        bool acknowledged = false;
        auto ack = env.Env.AddObserver<TEvTablet::TEvFGcAck>([&](auto& ev) {
            const auto* msg = ev->Get();
            if (msg->TabletID == env.Tablet && switchStamp
                    && msg->Generation == switchStamp->Gen() && msg->Step == switchStamp->Step()) {
                acknowledged = true;
            }
        });
        blocked.Stop().Unblock();
        env->WaitFor("follower acknowledgement of forced disk compaction",
            [&] { return acknowledged; }, TDuration::Seconds(10));

        const auto levels = env.Levels();
        UNIT_ASSERT(!levels.contains(input->Label));
        UNIT_ASSERT_VALUES_EQUAL(env.Tier(levels.at(output)), ColdTier);
        env.Exec([&](TTransactionContext& txc) {
            const auto subset = txc.DB.Subset(TableId, NTable::TEpoch::Max(), {}, {});
            UNIT_ASSERT_VALUES_EQUAL(subset->Flatten.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(subset->Flatten.front()->Label, output);
            return true;
        }, true);
        CheckRows(env, rows, TRowVersion::Max(), true);
    }

    Y_UNIT_TEST(StatusOnlyFlushPreservesDataAtTheStatusEpoch) {
        TEnv env;
        env.FireDummyFollower(1);
        constexpr ui64 txId = 123;
        env.Exec([](TTransactionContext& txc) {
            const auto key = NScheme::TUint64::TInstance(1);
            const auto value = NScheme::TString::TInstance("delta");
            const NTable::TUpdateOp op{ValueTag, NTable::ECellOp::Set, value};
            txc.DB.UpdateTx(TableId, NTable::ERowOp::Upsert, {key}, {op}, txId);
            return true;
        });
        env.Flush();
        auto parts = env.Parts();
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 1);
        const auto delta = parts.front()->Label;

        // The commit status and an unrelated row share a flush and an epoch.
        env.Exec([](TTransactionContext& txc) {
            txc.DB.CommitTx(TableId, txId, TRowVersion(10, 1));
            return true;
        });
        const auto peer = env.FlushPart({{2, 0, "peer"}});
        env.FlushPart({{3, 0, "later"}});

        // Resolve only the original delta on disk. Its commit status is now
        // garbage, while the row written at the status epoch remains live.
        auto levels = env.Levels();
        levels.at(delta) = 2;
        env.SeedLevels(levels);
        const auto resolved = CompactGeneration(env, 2, NTable::TCompactionParams::EMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(resolved.Context->Inputs.size(), 1);
        UNIT_ASSERT(Includes(resolved, delta));
        const auto before = env.Levels();
        TLogoBlobID status;
        NTable::TEpoch head = NTable::TEpoch::Zero();
        env.Exec([&](TTransactionContext& txc) {
            const auto subset = txc.DB.Subset(TableId, NTable::TEpoch::Max(), {}, {});
            UNIT_ASSERT(subset->Frozen.empty());
            UNIT_ASSERT(subset->GarbageTransactions.Contains(txId));
            UNIT_ASSERT_VALUES_EQUAL(subset->TxStatus.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(subset->TxStatus.front()->Epoch, peer->Epoch);
            status = subset->TxStatus.front()->Label;
            head = subset->Head;
            UNIT_ASSERT(head > peer->Epoch + 1);
            return true;
        });

        std::optional<TTxStamp> published;
        auto commit = env.Env.AddObserver<TEvTablet::TEvCommit>([&](auto& ev) {
            const auto* msg = ev->Get();
            if (msg->TabletID != env.Tablet || !msg->WaitFollowerGcAck) {
                return;
            }
            for (const auto& ref : msg->References) {
                if (NBoot::TCookie(ref.Id.Cookie()).Index() != NBoot::TCookie::EIdx::TurnLz4) {
                    continue;
                }
                NKikimrExecutorFlat::TTablePartSwitch change;
                UNIT_ASSERT(change.ParseFromString(NPageCollection::TSlicer::Lz4()->Decode(ref.Buffer)));
                if (change.GetTableId() != TableId || change.LeavingTxStatusSize() == 0) {
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(change.LeavingTxStatusSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(change.GetLeavingTxStatus(0)), status);
                UNIT_ASSERT_VALUES_EQUAL(change.GetTableSnapshoted().GetHead(), head.ToProto());
                UNIT_ASSERT_VALUES_EQUAL(change.LeavingBundlesSize(), 0);
                UNIT_ASSERT(!change.HasIntroducedParts());
                published.emplace(msg->Generation, msg->Step);
            }
        });
        bool acknowledged = false;
        auto ack = env.Env.AddObserver<TEvTablet::TEvFGcAck>([&](auto& ev) {
            const auto* msg = ev->Get();
            if (published && msg->TabletID == env.Tablet &&
                    msg->Generation == published->Gen() && msg->Step == published->Step()) {
                acknowledged = true;
            }
        });
        env.SendSync(new NFake::TEvCompact(TableId, true));
        env->WaitFor("follower acknowledgement of status-only flush",
            [&] { return acknowledged; }, TDuration::Seconds(10));
        env.CheckLevels(before);
        for (bool follower : {false, true}) {
            env.Exec([&](TTransactionContext& txc) {
                const auto subset = txc.DB.Subset(TableId, NTable::TEpoch::Max(), {}, {});
                UNIT_ASSERT(subset->TxStatus.empty());
                UNIT_ASSERT(std::any_of(subset->Flatten.begin(), subset->Flatten.end(),
                    [&](const auto& part) { return part->Label == peer->Label; }));
                return true;
            }, follower);
        }
        env.Read(1, "delta");
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
