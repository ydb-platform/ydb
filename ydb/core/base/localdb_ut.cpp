#include "localdb.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NLocalDb {

namespace {

    TCompactionPolicyPtr PolicyWithTiers(const TVector<std::optional<ui32>>& tiers) {
        auto policy = CreateDefaultTablePolicy();
        for (const auto tierId : tiers) {
            auto& generation = policy->Generations.emplace_back(
                0, 8, 8, 128 * 1024 * 1024, LegacyQueueIdToTaskName(1), false);
            generation.TierId = tierId;
        }
        return policy;
    }

} // namespace

    Y_UNIT_TEST_SUITE(TLocalDbTest) {

        Y_UNIT_TEST(GenerationTierIdRoundTrip) {
            auto policy = PolicyWithTiers({std::nullopt, 0, 42});
            NKikimrCompaction::TCompactionPolicy proto;
            policy->Serialize(proto);

            UNIT_ASSERT(!proto.GetGeneration(0).HasTierId());
            UNIT_ASSERT(proto.GetGeneration(1).HasTierId());
            UNIT_ASSERT_VALUES_EQUAL(proto.GetGeneration(1).GetTierId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(proto.GetGeneration(2).GetTierId(), 42);

            TCompactionPolicy restored(proto);
            UNIT_ASSERT(!restored.Generations[0].TierId);
            UNIT_ASSERT(restored.Generations[1].TierId);
            UNIT_ASSERT_VALUES_EQUAL(*restored.Generations[1].TierId, 0);
            UNIT_ASSERT(restored.Generations[2].TierId);
            UNIT_ASSERT_VALUES_EQUAL(*restored.Generations[2].TierId, 42);
            UNIT_ASSERT(*policy == restored);
        }

        Y_UNIT_TEST(GenerationTierIdSerializationClearsUnsetField) {
            auto policy = PolicyWithTiers({42});
            auto& generation = policy->Generations.front();
            NKikimrCompaction::TCompactionPolicy::TGenerationPolicy proto;
            generation.Serialize(proto);
            UNIT_ASSERT_VALUES_EQUAL(proto.GetTierId(), 42);

            generation.TierId.reset();
            generation.Serialize(proto);
            UNIT_ASSERT(!proto.HasTierId());
        }

        Y_UNIT_TEST(GenerationTiersFormContiguousBlocks) {
            for (const auto& tiers : TVector<TVector<std::optional<ui32>>>{
                {},
                {std::nullopt, std::nullopt, std::nullopt},
                {0, 0, 0},
                {std::nullopt, 0, std::nullopt, 42, 42, 7, 7},
                {42, 42, 7, 7},
                {42, std::nullopt, 0, 7},
            }) {
                const auto policy = PolicyWithTiers(tiers);
                TString err;
                UNIT_ASSERT_C(ValidateCompactionPolicy(*policy, err), err);
                UNIT_ASSERT_C(ValidateCompactionPolicyChange(*CreateDefaultTablePolicy(), *policy, err), err);
            }
        }

        Y_UNIT_TEST(GenerationTiersCannotInterleave) {
            for (const auto& tiers : TVector<TVector<std::optional<ui32>>>{
                {42, 7, 42},
                {0, 42, 7, 42},
                {std::nullopt, 42, std::nullopt},
                {std::nullopt, 42, 0},
                {0, 42, std::nullopt},
                {42, 0, 7, 0},
            }) {
                const auto policy = PolicyWithTiers(tiers);
                TString err;
                UNIT_ASSERT(!ValidateCompactionPolicy(*policy, err));
                UNIT_ASSERT_C(err.Contains("contiguous block"), err);
                err.clear();
                UNIT_ASSERT(!ValidateCompactionPolicyChange(*CreateDefaultTablePolicy(), *policy, err));
                UNIT_ASSERT_C(err.Contains("contiguous block"), err);
            }
        }

        Y_UNIT_TEST(ExistingGenerationTiersCannotChange) {
            const auto before = PolicyWithTiers({0, 0, 42, 42, 7, 7});
            for (const auto& tiers : TVector<TVector<std::optional<ui32>>>{
                    {0, 42, 42, 42, 7, 7},
                    {0, 0, 0, 42, 7, 7},
                    {0, 0, 42, 7, 7, 7},
                    {0, 0, 42, 42, 42, 7}}) {
                const auto after = PolicyWithTiers(tiers);
                TString error;
                UNIT_ASSERT_C(ValidateCompactionPolicy(*after, error), error);
                UNIT_ASSERT(before->GetTierIds() == after->GetTierIds());
                UNIT_ASSERT(!ValidateCompactionPolicyChange(*before, *after, error));
                UNIT_ASSERT_STRING_CONTAINS(error, "Changing TierId of existing generation");
            }

            const auto after = PolicyWithTiers({0, 42, 42, 42, 7, 7});
            TString error;
            UNIT_ASSERT(!ValidateCompactionPolicyChange(*before, *after, error));
            UNIT_ASSERT_VALUES_EQUAL(error,
                "Changing TierId of existing generation 2 is not supported: old TierId 0, new TierId 42");
        }

        Y_UNIT_TEST(GenerationTierChangesTreatOmittedAndZeroAsDefault) {
            const auto before = PolicyWithTiers({std::nullopt, 0, 42, 42});
            const auto after = PolicyWithTiers({0, std::nullopt, 42, 42});
            TString error;
            UNIT_ASSERT_C(ValidateCompactionPolicyChange(*before, *after, error), error);
            UNIT_ASSERT_C(ValidateCompactionPolicyChange(*after, *before, error), error);
        }

        Y_UNIT_TEST(GenerationTierChangesAllowAppendsAndOtherSettings) {
            const auto before = PolicyWithTiers({0, 0, 42});
            const auto after = PolicyWithTiers({0, 0, 42, 42, 7, 7});
            after->Generations[0].CountToCompact += 1;
            after->Generations[1].ForceSizeToCompact += 1;
            after->Generations[2].KeepInCache = !before->Generations[2].KeepInCache;
            TString error;
            UNIT_ASSERT_C(ValidateCompactionPolicyChange(*before, *after, error), error);
        }

        Y_UNIT_TEST(TierFinalLevelsUseTheGenerationOrder) {
            const auto policy = PolicyWithTiers({42, 42, 0, 0, 7, 7});
            UNIT_ASSERT((policy->GetTierIds() == TVector<ui32>{0, 42, 7}));
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(0), 255);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(42), 254);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(7), 253);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(0), 0);
            for (ui32 level : {1u, 2u, 254u}) {
                UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(level), 42);
            }
            for (ui32 level : {3u, 4u, 255u}) {
                UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(level), 0);
            }
            for (ui32 level : {5u, 6u, 253u}) {
                UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(level), 7);
            }
            for (ui32 level : {255u, 254u, 253u}) {
                UNIT_ASSERT(policy->IsFinalLevel(level));
            }
            for (ui32 level : {0u, 1u, 6u, 252u, Max<ui32>()}) {
                UNIT_ASSERT(!policy->IsFinalLevel(level));
            }
            UNIT_ASSERT_EXCEPTION(policy->GetTierForLevel(252), yexception);
            UNIT_ASSERT_EXCEPTION(policy->GetTierForLevel(Max<ui32>()), yexception);
            UNIT_ASSERT_EXCEPTION(policy->GetFinalLevel(99), yexception);
        }

        Y_UNIT_TEST(TierFinalLevelsSurvivePolicyRoundTripAndAppends) {
            const auto before = PolicyWithTiers({0, 42, 42});
            const auto after = PolicyWithTiers({0, 42, 42, 42, 7, 7});
            TString error;
            UNIT_ASSERT_C(ValidateCompactionPolicyChange(*before, *after, error), error);
            NKikimrCompaction::TCompactionPolicy proto;
            after->Serialize(proto);
            const TCompactionPolicy restored(proto);
            for (ui32 tierId : before->GetTierIds()) {
                const ui32 level = before->GetFinalLevel(tierId);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetFinalLevel(tierId), level);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetTierForLevel(level), tierId);
            }
            UNIT_ASSERT_VALUES_EQUAL(restored.GetFinalLevel(7), 253);
        }

        Y_UNIT_TEST(TierFinalSlotsCannotBeRemovedOrReordered) {
            const auto before = PolicyWithTiers({0, 42, 7});
            for (const auto& tiers : TVector<TVector<std::optional<ui32>>>{
                    {0, 7, 42}, {0, 99, 42, 7}, {0, 7, 7},
                    {0, 42, 99}, {0, 0, 0}}) {
                const auto after = PolicyWithTiers(tiers);
                TString error;
                UNIT_ASSERT_C(ValidateCompactionPolicy(*after, error), error);
                UNIT_ASSERT(!ValidateCompactionPolicyChange(*before, *after, error));
                UNIT_ASSERT_STRING_CONTAINS(error, "slots must not be removed or reordered");
            }
        }

        Y_UNIT_TEST(GenerationLevelsCannotOverlapTierFinalLevels) {
            TVector<std::optional<ui32>> tiers(254, 0);
            auto policy = PolicyWithTiers(tiers);
            TString error;
            UNIT_ASSERT_C(ValidateCompactionPolicy(*policy, error), error);
            UNIT_ASSERT(!policy->IsFinalLevel(254));
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(0), 255);

            tiers.back() = 42;
            policy = PolicyWithTiers(tiers);
            UNIT_ASSERT(!ValidateCompactionPolicy(*policy, error));
            UNIT_ASSERT_STRING_CONTAINS(error, "levels 1..255");

            tiers.pop_back();
            tiers.back() = 42;
            policy = PolicyWithTiers(tiers);
            error.clear();
            UNIT_ASSERT_C(ValidateCompactionPolicy(*policy, error), error);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(42), 254);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(253), 42);
            UNIT_ASSERT(!policy->IsFinalLevel(253));
            UNIT_ASSERT(policy->IsFinalLevel(254));
        }

        Y_UNIT_TEST(DefaultFinalLevelWorksWithoutGenerations) {
            const auto policy = PolicyWithTiers({});
            UNIT_ASSERT((policy->GetTierIds() == TVector<ui32>{0}));
            UNIT_ASSERT_VALUES_EQUAL(policy->GetFinalLevel(0), 255);
            UNIT_ASSERT_VALUES_EQUAL(policy->GetTierForLevel(255), 0);
            UNIT_ASSERT(policy->IsFinalLevel(255));
            UNIT_ASSERT(!policy->IsFinalLevel(254));
            UNIT_ASSERT_EXCEPTION(policy->GetTierForLevel(254), yexception);
        }

        Y_UNIT_TEST(BackupTaskNameChangedAtLoadTime) {
            NKikimrCompaction::TCompactionPolicy policyPb;
            CreateDefaultUserTablePolicy()->Serialize(policyPb);

            // We expect default task name to be "scan" at the moment
            UNIT_ASSERT_VALUES_EQUAL(policyPb.GetBackupResourceBrokerTask(), "scan");

            // There are a lot of policies out there with "compaction_gen1" as the backup task name
            // We expect it to automatically change to "scan" at load time
            {
                policyPb.SetBackupResourceBrokerTask("compaction_gen1");
                TCompactionPolicy policy(policyPb);
                UNIT_ASSERT_VALUES_EQUAL(policy.BackupResourceBrokerTask, "scan");
            }

            // For other non-legacy task names we expect it to stay unchanged
            {
                policyPb.SetBackupResourceBrokerTask("backup");
                TCompactionPolicy policy(policyPb);
                UNIT_ASSERT_VALUES_EQUAL(policy.BackupResourceBrokerTask, "backup");
            }
        }

    } // Y_UNIT_TEST_SUITE(TLocalDbTest)

} // namespace NLocalDb
} // namespace NKikimr
