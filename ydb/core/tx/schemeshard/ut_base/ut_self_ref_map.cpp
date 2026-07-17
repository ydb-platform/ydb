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

Y_UNIT_TEST_SUITE(TDbRefMapTest) {

    // at() must hand out a read-only view of whatever smart pointer the map holds:
    // TIntrusivePtr -> TIntrusiveConstPtr, std::shared_ptr -> shared_ptr<const>.
    Y_UNIT_TEST(ConstViewTypeMapping) {
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<TIntrusivePtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<TIntrusiveConstPtr<TTableInfo>>::type,
            TIntrusiveConstPtr<TTableInfo>>);
        static_assert(std::is_same_v<
            NDbRefDetail::TConstView<std::shared_ptr<TOlapStoreInfo>>::type,
            std::shared_ptr<const TOlapStoreInfo>>);
    }

    // The undo clone shares the immutable partitioning copy-on-write (O(1), no
    // fixup); a shallow copy would dangle Order's raw ptrs (the ReplicationAttribute crash).
    Y_UNIT_TEST(UndoCloneSharesPartitioning) {
        TTableInfo::TPtr orig(new TTableInfo());
        orig->SetPartitioning(MakeShards(3));

        TTableInfo::TPtr clone = DbRefUndoClone(orig);

        UNIT_ASSERT(clone);
        UNIT_ASSERT_UNEQUAL(clone.Get(), orig.Get());
        UNIT_ASSERT_VALUES_EQUAL(clone->GetPartitions().size(), 3u);

        // Shared: same store and the very same partition objects (no deep copy).
        UNIT_ASSERT_EQUAL(&clone->GetPartitionStore(), &orig->GetPartitionStore());
        UNIT_ASSERT_EQUAL(clone->GetPartitions()[0], orig->GetPartitions()[0]);

        clone->VerifyConsistency();
    }

    // An in-place mutation on one side detaches it (copy-on-write): the two tables
    // then own separate stores and neither sees the other's change.
    Y_UNIT_TEST(UndoCloneCopiesOnWrite) {
        TTableInfo::TPtr orig(new TTableInfo());
        orig->SetPartitioning(MakeShards(2));

        TTableInfo::TPtr clone = DbRefUndoClone(orig);
        UNIT_ASSERT_EQUAL(&clone->GetPartitionStore(), &orig->GetPartitionStore()); // shared

        // Mutate orig's cond-erase in place — must copy-on-write away from the clone.
        const TShardIdx shardIdx = orig->GetPartitions()[0]->ShardIdx;
        orig->UpdateNextCondErase(shardIdx, TInstant::Seconds(100), TDuration::Seconds(10));

        UNIT_ASSERT_UNEQUAL(&clone->GetPartitionStore(), &orig->GetPartitionStore());
        UNIT_ASSERT_VALUES_EQUAL(clone->GetPartitions().size(), 2u);
        clone->VerifyConsistency();
        orig->VerifyConsistency();
    }

    Y_UNIT_TEST(UndoCloneNullIsNull) {
        TTableInfo::TPtr nul;
        UNIT_ASSERT(!DbRefUndoClone(nul));
    }
}
