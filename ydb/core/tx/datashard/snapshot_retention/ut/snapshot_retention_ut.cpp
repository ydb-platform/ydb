#include <ydb/core/tx/datashard/snapshot_retention/snapshot_retention.h>

#include <ydb/core/protos/long_tx_service_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>

namespace NKikimr::NDataShard {
namespace {

const TTableId TABLE_A(100, 200);
const TTableId TABLE_B(100, 201);

struct TSnapshotRegistration {
    TVector<TTableId> Tables;
    TRowVersion Version;
};

NKikimrConfig::TLongTxServiceConfig MakeConfig() {
    NKikimrConfig::TLongTxServiceConfig config;
    config.SetLocalSnapshotPromotionTimeSeconds(30);
    config.SetMaxClockSkewMs(5000);
    return config;
}

void PublishRegistry(const IImmutableSnapshotRegistryHolderPtr& holder,
    TInstant collectionTime = TInstant::MilliSeconds(1000000),
    TRowVersion border = TRowVersion::Max(),
    std::initializer_list<TSnapshotRegistration> snapshots = {})
{
    auto builder = CreateImmutableSnapshotRegistryBuilder();
    builder->SetOldestCollectionTime(collectionTime);
    builder->SetSnapshotBorder(border);
    for (const auto& snapshot : snapshots) {
        builder->AddSnapshot(snapshot.Tables, snapshot.Version);
    }
    holder->Set(std::move(*builder).Build());
}

TSet<TRowVersion> CollectRetainedSnapshots(const TSnapshotRetentionPolicy& policy, const TTableId& tableId,
    const TRowVersion& from = TRowVersion::Min(), const TRowVersion& to = TRowVersion::Max())
{
    TSet<TRowVersion> snapshots;
    policy.ForEachRetainedSnapshot(tableId, from, to, [&](const TRowVersion& version) {
        snapshots.emplace(version);
    });
    return snapshots;
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardSnapshotRetention) {
    Y_UNIT_TEST(DisabledIgnoresRegistry) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion snapshot(100, 5);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion(500, 10), {
            {{TABLE_A}, snapshot},
        });

        const TSnapshotRetentionPolicy policy(false, holder, MakeConfig());
        UNIT_ASSERT(!policy.IsWaitingForRegistry());
        const TRowVersion proposed(2000000, 10);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(proposed), proposed);
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, TRowVersion::Max()));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).empty());
    }

    Y_UNIT_TEST(DisabledDoesNotRequireRegistry) {
        const TSnapshotRetentionPolicy policy(false, {}, MakeConfig());
        UNIT_ASSERT(!policy.IsWaitingForRegistry());
        const TRowVersion proposed(2000000, 10);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(proposed), proposed);
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, TRowVersion::Min()));
    }

    Y_UNIT_TEST(MissingHolderPreventsCleanup) {
        const TSnapshotRetentionPolicy policy(true, {}, MakeConfig());
        UNIT_ASSERT(policy.IsWaitingForRegistry());
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion::Min());
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, TRowVersion::Min()));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, TRowVersion::Max()));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).empty());
    }

    Y_UNIT_TEST(MissingRegistryPreservesPointsBelowWatermark) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());
        const TRowVersion lowWatermark(1000, 0);
        const TRowVersion retainedSnapshot(500, 1);

        UNIT_ASSERT(policy.IsWaitingForRegistry());
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(lowWatermark), TRowVersion::Min());
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, retainedSnapshot));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_B, retainedSnapshot));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).empty());
    }

    Y_UNIT_TEST(ReadyEmptyRegistryAllowsCleanup) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder);
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT(!policy.IsWaitingForRegistry());
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion(965000, 0));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, TRowVersion(100, 1)));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).empty());
    }

    Y_UNIT_TEST(FreshnessIncludesPromotionDelayAndClockSkew) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder);
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());
        const TRowVersion boundary(965000, 0);

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), boundary);
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, boundary.Prev()));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, boundary));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, boundary.Next()));
    }

    Y_UNIT_TEST(ExistingCleanupCandidateIsNotAdvanced) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder);
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());
        const TRowVersion proposed(900000, 123);

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(proposed), proposed);
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Min()), TRowVersion::Min());
    }

    Y_UNIT_TEST(StaleRegistryDoesNotFollowAdvancingCleanupCandidate) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder);
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion(1100000, 0)), TRowVersion(965000, 0));
        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion(2000000, 0)), TRowVersion(965000, 0));

        PublishRegistry(holder, TInstant::MilliSeconds(2000000));
        const TSnapshotRetentionPolicy updatedPolicy(true, holder, MakeConfig());
        UNIT_ASSERT_VALUES_EQUAL(updatedPolicy.GetCleanupBoundary(TRowVersion(2000000, 0)), TRowVersion(1965000, 0));
    }

    Y_UNIT_TEST(FreshnessSubtractionSaturatesAtZero) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        for (const ui64 collectionTimeMs : {0, 10000, 35000}) {
            PublishRegistry(holder, TInstant::MilliSeconds(collectionTimeMs));
            const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());
            UNIT_ASSERT(!policy.IsWaitingForRegistry());
            UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion::Min());
            UNIT_ASSERT(policy.ShouldRetain(TABLE_A, TRowVersion::Min()));
        }
    }

    Y_UNIT_TEST(LargeMarginsDoNotWrapAround) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder);
        auto config = MakeConfig();
        config.SetLocalSnapshotPromotionTimeSeconds(Max<ui64>());
        config.SetMaxClockSkewMs(Max<ui64>());
        const TSnapshotRetentionPolicy policy(true, holder, config);

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion::Min());
    }

    Y_UNIT_TEST(RetainedPointsAreScopedToTable) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion snapshot(100, 5);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion::Max(), {
            {{TABLE_A}, snapshot},
        });
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, snapshot.Prev()));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, snapshot.Next()));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_B, snapshot));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A, snapshot, snapshot).empty());
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A, snapshot.Next()).empty());
        UNIT_ASSERT_VALUES_EQUAL(CollectRetainedSnapshots(policy, TABLE_A).size(), 1u);
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).contains(snapshot));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_B).empty());
    }

    Y_UNIT_TEST(AllTablesAndTableSpecificRegistrationsAreCombined) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion globalSnapshot(100, 5);
        const TRowVersion tableSnapshot(200, 10);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion::Max(), {
            {{}, globalSnapshot},
            {{TABLE_A}, tableSnapshot},
        });
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, globalSnapshot));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_B, globalSnapshot));
        UNIT_ASSERT_VALUES_EQUAL(CollectRetainedSnapshots(policy, TABLE_A).size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(CollectRetainedSnapshots(policy, TABLE_B).size(), 1u);
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).contains(tableSnapshot));
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_B, tableSnapshot));
    }

    Y_UNIT_TEST(TableSchemaVersionDoesNotChangeRetention) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion snapshot(100, 5);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion::Max(), {
            {{TTableId(100, 200, 3)}, snapshot},
        });
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT(policy.ShouldRetain(TTableId(100, 200, 4), snapshot));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).contains(snapshot));
    }

    Y_UNIT_TEST(BorderIsInclusiveAndPreservesTxId) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion border(500, 10);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), border);
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), border);
        UNIT_ASSERT(!policy.ShouldRetain(TABLE_A, border.Prev()));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, border));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_B, border.Next()));
        UNIT_ASSERT(CollectRetainedSnapshots(policy, TABLE_A).empty());
    }

    Y_UNIT_TEST(FreshnessWindowMayBeOlderThanBorder) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion(980000, 10));
        const TSnapshotRetentionPolicy policy(true, holder, MakeConfig());

        UNIT_ASSERT_VALUES_EQUAL(policy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion(965000, 0));
        UNIT_ASSERT(policy.ShouldRetain(TABLE_A, TRowVersion(970000, 1)));
    }

    Y_UNIT_TEST(RegistryViewIsStableAcrossReplacementAndReset) {
        const auto holder = CreateImmutableSnapshotRegistryHolder();
        const TRowVersion snapshot(100, 5);
        PublishRegistry(holder, TInstant::MilliSeconds(1000000), TRowVersion::Max(), {
            {{TABLE_A}, snapshot},
        });
        const TSnapshotRetentionPolicy originalPolicy(true, holder, MakeConfig());

        PublishRegistry(holder, TInstant::MilliSeconds(2000000));
        const TSnapshotRetentionPolicy updatedPolicy(true, holder, MakeConfig());
        UNIT_ASSERT(originalPolicy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT(!updatedPolicy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(originalPolicy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion(965000, 0));

        holder->Set(nullptr);
        const TSnapshotRetentionPolicy warmingUpPolicy(true, holder, MakeConfig());
        UNIT_ASSERT(warmingUpPolicy.IsWaitingForRegistry());
        UNIT_ASSERT(!originalPolicy.IsWaitingForRegistry());
        UNIT_ASSERT(!updatedPolicy.IsWaitingForRegistry());
        UNIT_ASSERT(originalPolicy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT(!updatedPolicy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT(warmingUpPolicy.ShouldRetain(TABLE_A, snapshot));
        UNIT_ASSERT_VALUES_EQUAL(warmingUpPolicy.GetCleanupBoundary(TRowVersion::Max()), TRowVersion::Min());
        UNIT_ASSERT(CollectRetainedSnapshots(originalPolicy, TABLE_A).contains(snapshot));
        UNIT_ASSERT(CollectRetainedSnapshots(updatedPolicy, TABLE_A).empty());
        UNIT_ASSERT(CollectRetainedSnapshots(warmingUpPolicy, TABLE_A).empty());
    }
}

} // namespace NKikimr::NDataShard
