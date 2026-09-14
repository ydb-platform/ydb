#include <ydb/core/tx/schemeshard/schemeshard_db_ref_map.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

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
            ss.Tables.at(pathId)->AlterVersion = 11;
            changes.GrabTable(&ss, pathId);
            ss.Tables.Set(pathId, second);
            changes.GrabTable(&ss, pathId);
            ss.Tables.at(pathId)->AlterVersion = 21;

            changes.UnDo(&ss);

            UNIT_ASSERT_VALUES_EQUAL(ss.Tables.at(pathId)->AlterVersion, 10);
            UNIT_ASSERT_VALUES_EQUAL(ss.PathsById.at(pathId)->DbRefCount, initialRefs);
            ss.Tables.erase(pathId);
        });
    }

}
