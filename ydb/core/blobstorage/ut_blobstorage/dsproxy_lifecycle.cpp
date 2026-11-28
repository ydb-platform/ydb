#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(DSProxyLifecycle) {

    Y_UNIT_TEST(PoisonedProxyLeavesNoSessionsTimeout) {
        TEnvironmentSetup env{{.Erasure = TBlobStorageGroupType::Erasure4Plus2Block}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);

        // a put makes sure the proxy of the group works on node 1
        const TString data = "hello, world";
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 0, data.size(), 0), data,
                TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);

        const auto gauge = GetServiceCounters(env.Runtime->GetNode(1)->AppData->Counters, "dsproxynode")
            ->GetSubgroup("subsystem", "malfunction")->GetCounter("EstablishingSessionsTimeout", false);
        const i64 before = gauge->Val();

        env.Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, MakeBlobStorageProxyID(groupId), edge, nullptr,
            0), 1);
        env.Sim(TDuration::Seconds(1));

        // the stopped proxy is not in the sessions timeout state, so it must not be counted there
        UNIT_ASSERT_VALUES_EQUAL(gauge->Val(), before);
    }
}
