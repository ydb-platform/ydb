#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(SelectGroups) {

    // Regression test for a hanging blocking SelectGroups (fixed in d437868).
    // A freshly created group has no VDisk metrics yet, so a SelectGroups request with
    // BlockUntilAllResourcesAreComplete parks until the group obtains full metrics. The bug was that
    // arriving VDisk metrics (normalized occupancy) did not re-check the parked request unless the
    // VDisk status flags changed too -- which they don't for a fresh empty disk -- so the request hung
    // forever.
    Y_UNIT_TEST(BlockUntilAllResourcesAreComplete) {
        TEnvironmentSetup env(false);

        // create a fresh group; at this point it has no VDisk metrics yet
        env.CreateBoxAndPool();

        // issue a blocking SelectGroups immediately -- the group is not complete yet, so it parks
        const TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
        auto& r = ev->Record;
        r.SetReturnAllMatchingGroups(true);
        r.SetBlockUntilAllResourcesAreComplete(true);
        r.AddGroupParameters()->MutableStoragePoolSpecifier()->SetName(env.StoragePoolName);
        env.Runtime->SendToPipe(env.TabletId, edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());

        // wait for the response with a deadline; on buggy code no response ever arrives and this returns nullptr
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerSelectGroupsResult>(
            edge, true, env.Runtime->GetClock() + TDuration::Minutes(2));
        UNIT_ASSERT_C(response, "BSC hung on blocking SelectGroups for a freshly created group");

        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(record.MatchingGroupsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(record.GetMatchingGroups(0).GroupsSize(), 1);
    }

}
