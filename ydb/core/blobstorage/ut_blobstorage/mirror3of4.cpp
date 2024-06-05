#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(Mirror3of4) {

    Y_UNIT_TEST(Compaction) {
        SetRandomSeed(1);
        TEnvironmentSetup env(false, TBlobStorageGroupType::ErasureMirror3of4);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
//        runtime->SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
//        runtime->SetLogPriority(NKikimrServices::BS_VDISK_PUT, NLog::PRI_DEBUG);

        const auto& groups = env.GetGroups();
        const ui32 groupId = *groups.begin();
        Cerr << "operating on group " << groupId << Endl;

        TString blob = "hello, world";
        const ui64 tabletId = 1;
        const ui32 gen = 1;
        ui32 step = 1;
        const ui8 channel = 0;
        const TLogoBlobID id(tabletId, gen, step++, channel, blob.size(), 0);

        const TActorId& edge = runtime->AllocateEdgeActor(1);
        runtime->WrapInActorContext(edge, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, blob, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, TEvBlobStorage::TEvPut::TacticMaxThroughput);
            SendToBSProxy(edge, groupId, ev.release());
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        auto info = env.GetGroupInfo(groupId);

        for (ui32 i = 0; i < 10000; ++i) {
            Cerr << "iteration " << i << Endl;
            const ui32 size = 1048576;
            TString data = TString::Uninitialized(size);
            memset(data.Detach(), 1, size);
            const TLogoBlobID xid(tabletId, gen, step++, channel, size, 0);
            runtime->WrapInActorContext(edge, [&] {
                auto ev = std::make_unique<TEvBlobStorage::TEvPut>(xid, data, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, TEvBlobStorage::TEvPut::TacticMaxThroughput);
                SendToBSProxy(edge, groupId, ev.release());
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            if (i % 100 == 99) {
                Cerr << "compaction" << Endl;
                for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                    env.CompactVDisk(info->GetActorId(i));
                }
            }
            if (i % 200 == 199) {
                Cerr << "garbage collect" << Endl;
                TVector<TLogoBlobID> keep;
                if (i < 200) {
                    keep.push_back(id);
                }
                auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(tabletId, gen, i, channel, true, gen,
                    step - 1, keep ? new TVector<TLogoBlobID>(keep) : nullptr, nullptr, TInstant::Max(), true);
                runtime->WrapInActorContext(edge, [&] { SendToBSProxy(edge, groupId, ev.release()); });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }
            if (i == 500) {
                env.Wipe(2, 1000, 1000, TVDiskID(groupId, 1, 0, 1, 0));
            }
            if (i == 600) {
                env.Wipe(3, 1000, 1000, TVDiskID(groupId, 1, 0, 2, 0));
            }
        }

        env.Sim(TDuration::Minutes(30));

        std::vector<TActorId> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            queues.push_back(env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000));
        }

        ui32 numData = 0, numMetadata = 0;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(i), TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead);
            query->AddExtremeQuery(id, 0, 0);
            runtime->Send(new IEventHandle(queues[i], edge, query.release()), queues[i].NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
            Cerr << res->Get()->ToString() << Endl;
            auto& record = res->Get()->Record;
            UNIT_ASSERT_EQUAL(record.ResultSize(), 1);
            auto& r = record.GetResult(0);
            UNIT_ASSERT(r.GetStatus() == NKikimrProto::OK || r.GetStatus() == NKikimrProto::NODATA || r.GetStatus() == NKikimrProto::NOT_YET);
            if (r.GetStatus() == NKikimrProto::OK) {
                const auto& id = LogoBlobIDFromLogoBlobID(r.GetBlobID());
                UNIT_ASSERT(id.PartId() >= 1 && id.PartId() <= 3);
                if (id.PartId() == 3) {
                    ++numMetadata;
                } else {
                    ++numData;
                }
            }
        }
        UNIT_ASSERT(numData >= 3 && numData + numMetadata >= 5);
    }

}
