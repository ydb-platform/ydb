#include <ydb/core/tx/schemeshard/schemeshard_self_ref_map.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n) ? TString(1, char(i + 1)) : TString{};
        v.emplace_back(TShardIdx(ownerId, i), range);
    }
    return v;
}

} // namespace

Y_UNIT_TEST_SUITE(TSelfRefMapTest) {

    // at() must hand out a read-only view of whatever smart pointer the map holds:
    // TIntrusivePtr -> TIntrusiveConstPtr, std::shared_ptr -> shared_ptr<const>.
    Y_UNIT_TEST(ConstViewTypeMapping) {
        static_assert(std::is_same_v<
            NSelfRefDetail::TConstView<TIntrusivePtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NSelfRefDetail::TConstView<TIntrusiveConstPtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NSelfRefDetail::TConstView<std::shared_ptr<TOlapStoreInfo>>::type,
            std::shared_ptr<const TOlapStoreInfo>>);
    }

    // The Update() undo snapshot of a TTableInfo must be a DEEP copy: its
    // Partitions are raw pointers into PartitionStore, so a shallow copy would
    // leave them aliasing the original's store and a restored snapshot would
    // dangle (the TCdcStreamTests::ReplicationAttribute crash).
    Y_UNIT_TEST(UndoCloneTableIsDeepCopy) {
        TTableInfo::TPtr orig(new TTableInfo());
        orig->SetPartitioning(MakeShards(3));

        TTableInfo::TPtr clone = SelfRefUndoClone(orig);

        UNIT_ASSERT(clone);
        UNIT_ASSERT_UNEQUAL(clone.Get(), orig.Get());
        UNIT_ASSERT_VALUES_EQUAL(clone->GetPartitions().size(), 3u);

        // Every raw partition pointer resolves inside the clone's OWN store...
        for (const auto* p : clone->GetPartitions()) {
            UNIT_ASSERT_EQUAL(p, clone->GetPartitionStore().FindPtr(p->ShardIdx));
        }
        // ...and is a distinct object from the original's (a shallow copy aliases).
        UNIT_ASSERT_UNEQUAL(clone->GetPartitions()[0], orig->GetPartitions()[0]);

        clone->VerifyConsistency();
    }

    // The clone owns a separate PartitionStore, so the two tables share no
    // partition state (a shallow copy would alias one store between them).
    Y_UNIT_TEST(UndoCloneTableHasSeparateStore) {
        TTableInfo::TPtr orig(new TTableInfo());
        orig->SetPartitioning(MakeShards(2));

        TTableInfo::TPtr clone = SelfRefUndoClone(orig);

        UNIT_ASSERT_UNEQUAL(&clone->GetPartitionStore(), &orig->GetPartitionStore());
        for (const auto* p : orig->GetPartitions()) {
            UNIT_ASSERT_UNEQUAL(p, clone->GetPartitionStore().FindPtr(p->ShardIdx));
        }
    }

    Y_UNIT_TEST(UndoCloneNullIsNull) {
        TTableInfo::TPtr nul;
        UNIT_ASSERT(!SelfRefUndoClone(nul));
    }
}
