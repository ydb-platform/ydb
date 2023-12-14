#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/iterator/enumerate.h>


struct TTetsEnv {
    TTetsEnv()
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());
    }

    static TString PrepareData(const ui32 dataLen, const ui32 start) {
        TString data(Reserve(dataLen));
        for (ui32 i = 0; i < dataLen; ++i) {
            data.push_back('a' + (start + i) % 26);
        }
        return data;
    };

    void SendPut(ui32 step, const TString& data, NKikimrProto::EReplyStatus expectedStatus, ui32 nodeIndex=0) {
        const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
        Cerr << "SEND TEvPut with key " << id.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(nodeIndex).NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, expectedStatus);
        Cerr << "TEvPutResult: " << res->Get()->ToString() << Endl;
    };

    auto SendGet(ui32 step, ui32 dataSize, bool mustRestoreFirst=false, ui32 nodeIndex=0) {
        const TLogoBlobID blobId(1, 1, step, 0, dataSize, 0);
        Cerr << "SEND TEvGet with key " << blobId.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(nodeIndex).NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blobId,
            /* shift */ 0,
            /* size */ dataSize,
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst
        );
        Env.Runtime->WrapInActorContext(sender, [&] () {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        TInstant getDeadline = Env.Now() + TDuration::Seconds(30);
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, /* termOnCapture */ false, getDeadline);
        Cerr << "TEvGetResult: " << res->Get()->ToString() << Endl;
        return res;
    };

    void SetVDiskReadOnly(ui32 position, bool value) {
        const TVDiskID& someVDisk = GroupInfo->GetVDiskId(position);
        auto baseConfig = Env.FetchBaseConfig();

        const auto& somePDisk = baseConfig.GetPDisk(position);
        const auto& someVSlot = baseConfig.GetVSlot(position);
        Cerr << "Setting VDisk read-only to " << value << " for position " << position << Endl;
        if (!value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
        Env.SetVDiskReadOnly(somePDisk.GetNodeId(), somePDisk.GetPDiskId(), someVSlot.GetVSlotId().GetVSlotId(), someVDisk, value);
        Env.Sim(TDuration::Seconds(3));
        if (value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
    }

    auto GetParts(ui32 position, const TLogoBlobID& blobId) {
        auto vDiskActorId = GroupInfo->GetActorId(position);
        auto vDiskId = GroupInfo->GetVDiskId(position);
        auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
            vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
            TEvBlobStorage::TEvVGet::EFlags::None, 0,
            {{blobId, 0, 0}}
        );
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(0).NodeId(), __FILE__, __LINE__);
        Env.Runtime->WrapInActorContext(sender, [&] {
            Env.Runtime->Send(new IEventHandle(vDiskActorId, sender, ev.release()));
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, false);
        return res->Get()->Record.GetResult().at(0).GetParts();
    }

    bool CheckBlobsPositions(const TLogoBlobID& blobId, bool assert=true) {
        TBlobStorageGroupInfo::TOrderNums orderNums;
        GroupInfo->GetTopology().PickSubgroup(blobId.Hash(), orderNums);

        for (const auto& [partIdx, orderNum]: Enumerate(orderNums)) {
            const auto& parts = GetParts(orderNum, blobId);
            std::function<bool(ui32, ui32)> checkFun;
            if (assert) {
                checkFun = [](ui32 a, ui32 b) {
                    UNIT_ASSERT_VALUES_EQUAL(a, b);
                    return true;
                };
            } else {
                checkFun = [](ui32 a, ui32 b) {
                    return a == b;
                };
            }
            if (assert) {
                if (partIdx < GroupInfo->GetTopology().GType.TotalPartCount()) {  // it should be on mains
                    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(parts.at(0), partIdx + 1);
                } else { // and should not be on handoffs
                    UNIT_ASSERT_VALUES_EQUAL(parts.size(), 0);
                }
            } else {  // TODO: remove copypaste
                if (partIdx < GroupInfo->GetTopology().GType.TotalPartCount()) {  // it should be on mains
                    if (parts.size() != 1 || parts.at(0) != partIdx + 1) {
                        return false;
                    }
                } else { // and should not be on handoffs
                    if (parts.size() != 0) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
};

TLogoBlobID MakeLogoBlobId(ui32 step, ui32 dataSize) {
    return TLogoBlobID(1, 1, step, 0, dataSize, 0);
}


Y_UNIT_TEST_SUITE(VDiskBalancing) {

    Y_UNIT_TEST(SimpleTest) {
        TTetsEnv Env;

        TString data = "qweasdfrty";

        ui32 step = 0;
        {
            Env.SendPut(++step, data, NKikimrProto::OK);
            Cerr << "0$ 2$ First blob sent" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
            Env.CheckBlobsPositions(MakeLogoBlobId(step, data.size()));
        }


        {
            auto blobId = MakeLogoBlobId(++step, data.size());
            TBlobStorageGroupInfo::TOrderNums orderNums;
            Env.GroupInfo->GetTopology().PickSubgroup(blobId.Hash(), orderNums);

            // Env.SetVDiskReadOnly(orderNums[0], true);
            Env.Env.StopNode(Env.GroupInfo->GetActorId(1).NodeId());

            Env.SendPut(step, data, NKikimrProto::OK);
            Cerr << "0$ 1$ 2$ 3$ Second blob sent" << Endl;
            Env.Env.Sim(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
            // UNIT_ASSERT(!Env.CheckBlobsPositions(MakeLogoBlobId(step, data.size()), false));

            // Env.SetVDiskReadOnly(orderNums[0], false);
            Env.Env.StartNode(Env.GroupInfo->GetActorId(1).NodeId());
            Cerr << "0$ 1$ 2$ 3$ Readonly false" << Endl;
            Env.Env.Sim(TDuration::Seconds(10));
            Env.CheckBlobsPositions(MakeLogoBlobId(step, data.size()));

            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
        }

    }

}
