#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(ReadOnlyVDisk) {

    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .VDiskReplPausedAtStart = false,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const TActorId vdiskActorId = info->GetActorId(0);

        auto prepData = [&] (const ui32 dataLen, const ui32 start) {
            TString data(Reserve(dataLen));
            for (ui32 i = 0; i < dataLen; ++i) {
                data.push_back('a' + (start + i) % 26);
            }
            return data;
        };

        TVector<TString> dataArr = {
            prepData(128 * 1024, 0),
            prepData(32 * 1024, 3),
        };

        auto sendPut = [&] (ui32 step, NKikimrProto::EReplyStatus expectedStatus) {
            const TString& data = dataArr[step % 2];
            const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
            Cerr << "SEND TEvPut with key " << id.ToString() << Endl;
            const TActorId sender = env.Runtime->AllocateEdgeActor(vdiskActorId.NodeId());
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, ev.release());
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, expectedStatus);
            Cerr << "TEvPutResult: " << res->Get()->ToString() << Endl;
        };

        auto sendGet = [&] (ui32 step) {
            const TString& data = dataArr[step % 2];
            const TLogoBlobID blobId(1, 1, step, 0, data.size(), 0);
            Cerr << "SEND TEvGet with key " << blobId.ToString() << Endl;
            const TActorId sender = env.Runtime->AllocateEdgeActor(vdiskActorId.NodeId());
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
                blobId,
                /* shift */ 0,
                /* size */ data.size(),
                TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead
            );
            env.Runtime->WrapInActorContext(sender, [&] () {
                SendToBSProxy(sender, info->GroupID, ev.release());
            });
            TInstant getDeadline = env.Now() + TDuration::Seconds(30);
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, /* termOnCapture */ false, getDeadline);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            Cerr << "TEvGetResult: " << res->Get()->ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
        };

        Cerr << "=== Trying to put and get a blob ===" << Endl;
        ui32 step = 0;
        sendPut(step, NKikimrProto::OK);
        sendGet(step);
        ++step;

        auto putVDiskToRo = [&] (ui32 position) {
            const TVDiskID& someVDisk = info->GetVDiskId(position);

            auto baseConfig = env.FetchBaseConfig();

            const auto& somePDisk = baseConfig.GetPDisk(position);
            const auto& someVSlot = baseConfig.GetVSlot(position);
            Cerr << "Issuing PutVDiskToReadOnly for position " << position << Endl;
            env.PutVDiskToReadOnly(somePDisk.GetNodeId(), somePDisk.GetPDiskId(), someVSlot.GetVSlotId().GetVSlotId(), someVDisk);
            env.Sim(TDuration::Seconds(30));
        };

        Cerr << "=== Putting VDisk #0 to read-only ===" << Endl;
        putVDiskToRo(0);

        Cerr << "=== Write 10 blobs ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            sendPut(step, NKikimrProto::OK);
            ++step;
        }

        Cerr << "=== Read all blobs ===" << Endl;
        for (ui32 i = 0; i < step; ++i) {
            sendGet(i);
        }

        Cerr << "=== Put 2 more VDisks to read-only ===" << Endl;
        putVDiskToRo(1);
        putVDiskToRo(2);

        Cerr << "=== Write 10 more blobs, expect errors ===" << Endl;
        for (ui32 i = 0; i < 10; ++i) {
            sendPut(step, NKikimrProto::ERROR);
            ++step;
        }

        Cerr << "=== Read all blobs again, expect it to work ===" << Endl;
        for (ui32 i = 0; i < step; ++i) {
            sendGet(i);
        }
    }
}
