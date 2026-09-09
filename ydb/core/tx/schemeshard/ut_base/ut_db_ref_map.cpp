#include <ydb/core/tx/schemeshard/schemeshard_db_ref_map.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

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

template <class TTest>
void WithSchemeShard(TTest test) {
    TSchemeShard* ss = nullptr;
    auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
        ss = new TSchemeShard(tablet, info);
        return ss;
    };
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions(), factory);
    runtime.RunCall([&]() {
        const TPathId pathId = TPath::Resolve("/MyRoot", ss).Base()->PathId;
        // Use a real registered map and path for reference reconciliation. The
        // temporary table entry is removed before returning to the event loop.
        UNIT_ASSERT(!ss->Tables.contains(pathId));
        test(*ss, pathId);
        UNIT_ASSERT(!ss->Tables.contains(pathId));
        ss->DebugCheckDbRefIntegrity();
        return true;
    });
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

    Y_UNIT_TEST(MembershipOwnsExactlyOnePathReference) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();

            ss.Tables.Set(pathId, first);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);
            ss.Tables.Set(pathId, second);
            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), second.Get());
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.erase(pathId), 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.erase(pathId), 0);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
        });
    }

    Y_UNIT_TEST(InsertAndReplaceUndoAfterPathCounterRestoration) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();
            first->AlterVersion = 10;
            second->AlterVersion = 20;

            TMemoryChanges changes;

            changes.GrabPath(&ss, pathId);
            changes.GrabNewTable(&ss, pathId);
            ss.Tables.Set(pathId, first);
            changes.GrabTable(&ss, pathId);
            first->AlterVersion = 11;
            changes.GrabTable(&ss, pathId);
            ss.Tables.Set(pathId, second);
            changes.GrabTable(&ss, pathId);
            second->AlterVersion = 21;
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs + 1);

            changes.UnDo(&ss);

            // Paths restore the count first. Undoing the insertion must not
            // decrement it again.
            UNIT_ASSERT(!ss.Tables.contains(pathId));
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
        });
    }

    Y_UNIT_TEST(ReplacementAndSnapshotsShareReverseOrder) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            auto first = MakeIntrusive<TTableInfo>();
            auto second = MakeIntrusive<TTableInfo>();
            first->AlterVersion = 10;
            second->AlterVersion = 20;
            ss.Tables.Set(pathId, first);
            const auto initialRefs = ss.PathsById.at(pathId)->DbRefCount;

            TMemoryChanges changes;

            changes.GrabPath(&ss, pathId);
            changes.GrabTable(&ss, pathId);
            ss.Tables.Update(pathId)->AlterVersion = 11;
            changes.GrabTable(&ss, pathId);
            ss.Tables.Set(pathId, second);
            changes.GrabTable(&ss, pathId);
            ss.Tables.Update(pathId)->AlterVersion = 21;

            changes.UnDo(&ss);

            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.at(pathId)->AlterVersion, 10);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            ss.Tables.erase(pathId);
        });
    }

    Y_UNIT_TEST(UpdateDoesNotSnapshotTwoHundredThousandShards) {
        WithSchemeShard([](TSchemeShard& ss, const TPathId& pathId) {
            constexpr ui32 shardCount = 200000;
            auto table = MakeIntrusive<TTableInfo>();
            table->SetPartitioning(MakeShards(shardCount));
            table->AlterData = MakeIntrusive<TTableInfo::TAlterTableInfo>();
            ss.Tables.Set(pathId, table);
            UNIT_ASSERT_VALUES_EQUAL(table->GetStats().PartitionStats.size(), shardCount);
            const auto* partition = table->GetPartitions().front();
            const auto* stats = &table->GetStats().PartitionStats.at(partition->ShardIdx);
            const auto alterOwners = table->AlterData.RefCount();

            TMemoryChanges changes;

            for (ui32 i = 0; i < 256; ++i) {
                const auto& writable = ss.Tables.Update(pathId);
                UNIT_ASSERT_EQUAL(writable.Get(), table.Get());
                // A retained whole-table snapshot would copy the AlterData
                // smart pointer too, increasing its owner count even though
                // the live table's partition/statistics addresses stay unchanged.
                UNIT_ASSERT_VALUES_EQUAL(writable->AlterData.RefCount(), alterOwners);
            }
            changes.UnDo(&ss);

            UNIT_ASSERT_EQUAL(ss.Tables.at(pathId).Get(), table.Get());
            UNIT_ASSERT_EQUAL(table->GetPartitions().front(), partition);
            UNIT_ASSERT_EQUAL(&table->GetStats().PartitionStats.at(partition->ShardIdx), stats);
            UNIT_ASSERT_VALUES_EQUAL(table->AlterData.RefCount(), alterOwners);
            table->VerifyConsistency();
            ss.Tables.erase(pathId);
        });
    }
}
