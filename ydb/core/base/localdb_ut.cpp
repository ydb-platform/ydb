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
