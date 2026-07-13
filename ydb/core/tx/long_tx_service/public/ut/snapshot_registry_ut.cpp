#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

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
}

} // namespace NKikimr
