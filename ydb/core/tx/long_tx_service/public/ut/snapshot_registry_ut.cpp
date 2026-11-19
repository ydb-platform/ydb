#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TImmutableSnapshotRegistryHolderTest) {
    std::unique_ptr<IImmutableSnapshotRegistry> BuildRegistry() {
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        builder->SetSnapshotBorder(TRowVersion(100, 0));
        builder->SetOldestCollectionTime(TInstant::MilliSeconds(50));
        return std::move(*builder).Build();
    }

    Y_UNIT_TEST(EmptyByDefault) {
        auto holder = CreateImmutableSnapshotRegistryHolder();
        const auto& registry = holder->Get();
        UNIT_ASSERT(!registry);
        UNIT_ASSERT(registry.get() == nullptr);
    }

    Y_UNIT_TEST(SetAndGet) {
        auto holder = CreateImmutableSnapshotRegistryHolder();
        holder->Set(BuildRegistry());
        const auto& registry = holder->Get();
        UNIT_ASSERT(registry);
        UNIT_ASSERT(registry.get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(registry->GetBorder(), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(SetNullClearsRegistry) {
        auto holder = CreateImmutableSnapshotRegistryHolder();
        holder->Set(BuildRegistry());
        UNIT_ASSERT(holder->Get());

        holder->Set(nullptr);
        const auto& registry = holder->Get();
        UNIT_ASSERT(!registry);
        UNIT_ASSERT(registry.get() == nullptr);
    }

    Y_UNIT_TEST(VisitSnapshotsInOrderWithinRange) {
        const TTableId tableA(100, 200);
        const TTableId tableB(100, 201);
        auto builder = CreateImmutableSnapshotRegistryBuilder();
        for (ui64 step : {30, 10, 20, 20}) {
            builder->AddSnapshot({tableA}, TRowVersion(step, 0));
        }
        for (ui64 step : {40, 20, 5, 20}) {
            builder->AddSnapshot({}, TRowVersion(step, 0));
        }
        builder->AddSnapshot({tableB}, TRowVersion(15, 0));
        const auto registry = std::move(*builder).Build();

        auto collect = [&](const TTableId& tableId, const TRowVersion& from, const TRowVersion& to) {
            TVector<TRowVersion> snapshots;
            registry->ForEachActiveSnapshot(tableId, from, to, [&](const TRowVersion& version) {
                snapshots.push_back(version);
            });
            return snapshots;
        };
        const auto snapshots = collect(TTableId(100, 200, 7), TRowVersion(10, 0), TRowVersion(40, 0));
        UNIT_ASSERT_VALUES_EQUAL(snapshots.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(snapshots[0], TRowVersion(10, 0));
        UNIT_ASSERT_VALUES_EQUAL(snapshots[1], TRowVersion(20, 0));
        UNIT_ASSERT_VALUES_EQUAL(snapshots[2], TRowVersion(30, 0));

        const auto otherTable = collect(tableB, TRowVersion(10, 0), TRowVersion(40, 0));
        UNIT_ASSERT_VALUES_EQUAL(otherTable.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(otherTable[0], TRowVersion(15, 0));
        UNIT_ASSERT_VALUES_EQUAL(otherTable[1], TRowVersion(20, 0));
        const auto globalOnly = collect(TTableId(100, 999), TRowVersion(10, 0), TRowVersion(40, 0));
        UNIT_ASSERT_VALUES_EQUAL(globalOnly.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(globalOnly[0], TRowVersion(20, 0));
        UNIT_ASSERT(collect(tableA, TRowVersion(20, 0), TRowVersion(20, 0)).empty());
        UNIT_ASSERT(collect(tableA, TRowVersion(40, 0), TRowVersion(10, 0)).empty());

        const auto all = collect(tableA, TRowVersion::Min(), TRowVersion::Max());
        const auto legacy = registry->GetActiveSnapshots(tableA);
        UNIT_ASSERT_VALUES_EQUAL(all.size(), legacy.size());
        UNIT_ASSERT(std::equal(all.begin(), all.end(), legacy.begin()));
    }
}

} // namespace NKikimr
