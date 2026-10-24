#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

Y_UNIT_TEST_SUITE(VDiskSectorPreparation) {

    // With VDiskControls.EnableVDiskSectorPreparation the VDisks seal the sectors of their chunk writes and open raw
    // sectors themselves. Data written either way, before and after compaction, with the control switched off and on
    // again and after every node restarted, must read back the same.
    Y_UNIT_TEST(WriteCompactRestartRead) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .MinHugeBlobInBytes = 16 << 10, // the large blobs below are surely huge, the small ones surely not
            .EnableVDiskSectorPreparation = true,
        });

        env.CreateBoxAndPool(1, 1, 0, NKikimrBlobStorage::EPDiskType::NVME);
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        env.Sim(TDuration::Minutes(5));

        TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::vector<TLogoBlobID> blobs;
        ui32 step = 1;

        auto write = [&](ui32 count) {
            for (ui32 i = 0; i < count; ++i, ++step) {
                // small blobs end up in SSTs, the large ones are huge blobs
                const size_t size = step % 3 ? 100 + RandomNumber(10000u) : 4 * 65536 + RandomNumber(32768u);
                const TString data = FastGenDataForLZ4(size, step);
                const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(id, TRope(data), TInstant::Max()));
                });
                const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                blobs.push_back(id);
            }
        };

        auto compactAll = [&] {
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TActorId actorId = info->GetActorId(i);
                const TActorId sender = env.Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
                auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
                ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
                env.Runtime->Send(ev.release(), actorId.NodeId());
                env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
            }
        };

        auto readAll = [&] {
            for (const TLogoBlobID& id : blobs) {
                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead));
                });
                const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
                const auto& response = res->Get()->Responses[0];
                UNIT_ASSERT_VALUES_EQUAL_C(response.Status, NKikimrProto::OK, id);
                UNIT_ASSERT_C(response.Buffer.ConvertToString() == FastGenDataForLZ4(id.BlobSize(), id.Step()), id);
            }
        };

        // PreformattedSectors writes and RawSectors reads served by the mock PDisks so far
        auto served = [&] {
            std::pair<ui64, ui64> res;
            for (const auto& [key, state] : env.PDiskMockStates) {
                res.first += state->GetPreformattedSectorWrites();
                res.second += state->GetRawSectorReads();
            }
            return res;
        };

        auto setControl = [&](bool enabled) {
            for (ui32 nodeId : env.Runtime->GetNodes()) {
                env.SetIcbControl(nodeId, "VDiskControls.EnableVDiskSectorPreparation", enabled);
            }
        };

        write(3000);
        compactAll();
        readAll();
        {
            const auto [writes, reads] = served();
            UNIT_ASSERT_C(writes, "no write went through the VDisk sealing path");
            UNIT_ASSERT_C(reads, "no read went through the raw sectors path");
        }

        // switched off: new data is written and read the ordinary way, next to the sealed data
        setControl(false);
        write(1000);
        compactAll();
        readAll();

        // on again, and every node restarted: the VDisks learn their chunk nonces from YardInit
        setControl(true);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            env.RestartNode(nodeId);
        }
        env.Sim(TDuration::Minutes(1));
        edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const auto before = served();
        write(1000);
        compactAll();
        readAll();
        const auto after = served();
        UNIT_ASSERT_C(after.first > before.first, "no sealed write after the restart");
        UNIT_ASSERT_C(after.second > before.second, "no raw sector read after the restart");
    }
}
