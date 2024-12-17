#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>


Y_UNIT_TEST_SUITE(VDiskRestart) {

    Y_UNIT_TEST(Simple) {
        return;

        TEnvironmentSetup env({
            .NodeCount = 8,
            .VDiskReplPausedAtStart = false,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);

        auto groupInfo = env.GetGroupInfo(groups.front());


        ui32 pos = 0;
        auto vDiskActorId = groupInfo->GetActorId(pos);
        auto baseConfig = env.FetchBaseConfig();
        const TVDiskID& someVDisk = groupInfo->GetVDiskId(pos);
        const auto& somePDisk = baseConfig.GetPDisk(pos);
        const auto& someVSlot = baseConfig.GetVSlot(pos);
        auto getOwnerRound = [&]() {
            return env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->GetOwnerRound(someVDisk);
        };
        auto currentRound = getOwnerRound();

        const TActorId sender = env.Runtime->AllocateEdgeActor(vDiskActorId.NodeId(), __FILE__, __LINE__);

        THttpRequestMock httpRequest;
        httpRequest.CgiParameters.InsertUnescaped("type", "restart");
        httpRequest.Path = Sprintf("/actors/vdisks/vdisk%09" PRIu32 "_%09" PRIu32, somePDisk.GetPDiskId(), someVSlot.GetVSlotId().GetVSlotId());
        NMonitoring::TMonService2HttpRequest monService2HttpRequest(nullptr, &httpRequest, nullptr, nullptr, "", nullptr);
        auto ev = std::make_unique<NMon::TEvHttpInfo>(monService2HttpRequest);

        auto sendPDiskEvent = [&](EPDiskMockEvents ev) {
            for (const auto& pDisk: env.PDiskActors) {
                env.Runtime->WrapInActorContext(sender, [&] () {
                    env.Runtime->Send(new IEventHandle(ev, 0, pDisk, sender, nullptr, 0));
                });
            }
        };
        sendPDiskEvent(EvBecomeError);
        env.Sim(TDuration::Seconds(1));

        env.Runtime->WrapInActorContext(sender, [&] () {
            env.Runtime->Send(new IEventHandle(vDiskActorId, sender, ev.release()));
        });
        auto res = env.WaitForEdgeActorEvent<NMon::TEvHttpInfoRes>(sender, false);
        UNIT_ASSERT_VALUES_UNEQUAL(res->Get()->Answer.find("VDisk restart request has been sent"), std::string::npos);

        env.Sim(TDuration::Seconds(10));
        sendPDiskEvent(EvBecomeNormal);
        env.Sim(TDuration::Seconds(10));

        UNIT_ASSERT_VALUES_UNEQUAL(currentRound, getOwnerRound());
    }
}
