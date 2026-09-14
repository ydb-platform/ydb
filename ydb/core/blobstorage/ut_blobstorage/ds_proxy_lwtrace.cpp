#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>


Y_UNIT_TEST_SUITE(DsProxyLwTrace) {
    void RunTestGetDSProxyVDiskRequestDuration(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env({
            .NodeCount = TBlobStorageGroupType(erasure).BlobSubgroupSize(),
            .VDiskReplPausedAtStart = false,
            .Erasure = erasure,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        auto groupInfo = env.GetGroupInfo(groups.front());

        TActorId vDiskActorId = groupInfo->GetActorId(0);


        env.Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGet::EventType) {                
                UNIT_ASSERT_VALUES_UNEQUAL(ev->Get<TEvBlobStorage::TEvVGet>()->Record.GetTimestamps().GetSentByDSProxyUs(), 0);
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {                
                UNIT_ASSERT_VALUES_UNEQUAL(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetTimestamps().GetSentByDSProxyUs(), 0);
            }
            return true;
        };


        TString data("qweryy");
        const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
        const TActorId sender = env.Runtime->AllocateEdgeActor(vDiskActorId.NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blobId, 0, data.size(), TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead);
        env.Runtime->WrapInActorContext(sender, [&] () {
            SendToBSProxy(sender, groupInfo->GroupID, ev.release());
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, /* termOnCapture */ false);
    }

    Y_UNIT_TEST(TestGetDSProxyVDiskRequestDuration) { RunTestGetDSProxyVDiskRequestDuration(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(TestGetDSProxyVDiskRequestDurationBlock82) { RunTestGetDSProxyVDiskRequestDuration(TBlobStorageGroupType::Erasure8Plus2Block); }
}
