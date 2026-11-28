#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/chunk_keeper/chunk_keeper_events.h>
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
        std::vector<ui32> groups = env.GetGroups();
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
                const TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> res =
                    env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                blobs.push_back(id);
            }
        };

        auto compactAll = [&] {
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TActorId actorId = info->GetActorId(i);
                const TActorId sender = env.Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
                std::unique_ptr<IEventHandle> ev =
                    std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
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
                const TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> res =
                    env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
                const TEvBlobStorage::TEvGetResult::TResponse& response = res->Get()->Responses[0];
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
        const std::pair<ui64, ui64> before = served();
        write(1000);
        compactAll();
        readAll();
        const std::pair<ui64, ui64> after = served();
        UNIT_ASSERT_C(after.first > before.first, "no sealed write after the restart");
        UNIT_ASSERT_C(after.second > before.second, "no raw sector read after the restart");
    }

    Y_UNIT_TEST(ReuseChunkWhileSectorPreparationIsOff) {
        TEnvironmentSetup env({.NodeCount = 1, .MinHugeBlobInBytes = 512_KB, .PDiskChunkSize = 32_MB,
            .EnableVDiskSectorPreparation = true});
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(env.GetGroups().front());
        const TVDiskID vdisk = info->GetVDiskId(0);
        const TActorId queue = env.CreateQueueActor(vdisk, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, 1000);
        const TString control = "VDiskControls.EnableVDiskSectorPreparation";
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(1);

        const auto sendPut = [&](ui32 step, char fill) {
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            const TString data(1_MB, fill);
            const TLogoBlobID id(1000, 1, step, 0, data.size(), 0, 1);
            env.Runtime->Send(new IEventHandle(queue, sender, new TEvBlobStorage::TEvVPut(id, TRope(data),
                vdisk, false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), 1);
            return sender;
        };
        const auto writtenChunk = [&](const TActorId& sender) {
            const TAutoPtr<TEventHandle<TEvBlobStorage::TEvVPutResult>> result =
                env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(sender, true, deadline);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            return result->Get()->WrittenLocation.ChunkIdx;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TActorId front = info->GetActorId(0);
        std::unique_ptr<IEventHandle> request = std::make_unique<IEventHandle>(front, edge, new TEvGetSkeletonState);
        request->Rewrite(TEvBlobStorage::EvForwardToSkeleton, front);
        env.Runtime->Send(request.release(), 1);
        const TAutoPtr<TEventHandle<TEvGetSkeletonStateResult>> state =
            env.WaitForEdgeActorEvent<TEvGetSkeletonStateResult>(edge, false, deadline);
        UNIT_ASSERT(state);
        const TActorId keeper = state->Get()->ChunkKeeperActorId;
        UNIT_ASSERT(keeper);

        // Hold the huge writer's first reservation while the chunk keeper allocates and releases the same chunk.
        TActorId hugeKeeper;
        std::unique_ptr<IEventHandle> heldReserve;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvHullWriteHugeBlob) {
                hugeKeeper = ev->Recipient;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvChunkReserve && hugeKeeper
                    && ev->Sender.Hint() == hugeKeeper.Hint()) {
                UNIT_ASSERT(!heldReserve);
                heldReserve = std::move(ev);
                return false;
            }
            return true;
        };
        const TActorId firstPut = sendPut(1, 'x');
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(heldReserve);
        const TActorId hugeAllocator = heldReserve->Sender;

        TChunkIdx chunk = 0;
        ui64 oldNonce = 0;
        ui64 newNonce = 0;
        std::vector<bool> preparedWrites;
        bool sawRawRead = false;
        // Observe both reservations and verify the new nonce on subsequent I/O.
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvChunkReserveResult:
                    if (ev->Recipient == keeper || ev->Recipient == hugeAllocator) {
                        const NPDisk::TEvChunkReserveResult* msg = ev->Get<NPDisk::TEvChunkReserveResult>();
                        UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(msg->ChunkIds.size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(msg->ChunkNonces.size(), 1);
                        if (ev->Recipient == keeper) {
                            chunk = msg->ChunkIds.front();
                            oldNonce = msg->ChunkNonces.front();
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(msg->ChunkIds.front(), chunk);
                            newNonce = msg->ChunkNonces.front();
                        }
                    }
                    break;
                case TEvBlobStorage::EvChunkWrite: {
                    const NPDisk::TEvChunkWrite* msg = ev->Get<NPDisk::TEvChunkWrite>();
                    if (chunk && msg->ChunkIdx == chunk) {
                        preparedWrites.push_back(msg->PreformattedSectors);
                    }
                    break;
                }
                case TEvBlobStorage::EvChunkReadResult: {
                    const NPDisk::TEvChunkReadResult* msg = ev->Get<NPDisk::TEvChunkReadResult>();
                    if (chunk && msg->ChunkIdx == chunk && msg->RawSectors) {
                        UNIT_ASSERT_VALUES_EQUAL(msg->ChunkNonce, newNonce);
                        sawRawRead = true;
                    }
                    break;
                }
            }
            return true;
        };

        constexpr TEvChunkKeeperAllocate::TSubsystem subsystem = NKikimrVDiskData::TChunkKeeperEntryPoint::Test1;
        env.Runtime->Send(new IEventHandle(keeper, edge, new TEvChunkKeeperAllocate(subsystem)), 1);
        const TAutoPtr<TEventHandle<TEvChunkKeeperAllocateResult>> allocation =
            env.WaitForEdgeActorEvent<TEvChunkKeeperAllocateResult>(edge, false, deadline);
        UNIT_ASSERT(allocation);
        UNIT_ASSERT_VALUES_EQUAL(allocation->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT(allocation->Get()->ChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(*allocation->Get()->ChunkIdx, chunk);
        UNIT_ASSERT(oldNonce);

        env.SetIcbControl(1, control, 0);
        env.Runtime->Send(new IEventHandle(keeper, edge, new TEvChunkKeeperFree(chunk, subsystem)), 1);
        const TAutoPtr<TEventHandle<TEvChunkKeeperFreeResult>> freed =
            env.WaitForEdgeActorEvent<TEvChunkKeeperFreeResult>(edge, false, deadline);
        UNIT_ASSERT(freed);
        UNIT_ASSERT_VALUES_EQUAL(freed->Get()->Status, NKikimrProto::OK);
        env.Runtime->Send(heldReserve.release(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writtenChunk(firstPut), chunk);
        UNIT_ASSERT(newNonce && newNonce != oldNonce);
        UNIT_ASSERT_VALUES_EQUAL(preparedWrites.size(), 1);
        UNIT_ASSERT(!preparedWrites.front());

        env.SetIcbControl(1, control, 1);
        const TActorId secondPut = sendPut(2, 'y');
        UNIT_ASSERT_VALUES_EQUAL(writtenChunk(secondPut), chunk);
        UNIT_ASSERT_VALUES_EQUAL(preparedWrites.size(), 2);
        UNIT_ASSERT(preparedWrites.back());

        env.WithQueueId(vdisk, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId readQueue) {
            for (ui32 step : {1u, 2u}) {
                const TLogoBlobID id(1000, 1, step, 0, 1_MB, 0, 1);
                sawRawRead = false;
                env.Runtime->Send(new IEventHandle(readQueue, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                    vdisk, TInstant::Max(), NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                    Nothing(), {{id, 0u, 1_MB}}).release()), 1);
                const TAutoPtr<TEventHandle<TEvBlobStorage::TEvVGetResult>> read =
                    env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false, deadline);
                UNIT_ASSERT(read);
                UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.ResultSize(), 1);
                const NKikimrBlobStorage::TQueryResult& result = read->Get()->Record.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT(read->Get()->GetBlobData(result).ConvertToString() == TString(1_MB, step == 1 ? 'x' : 'y'));
                UNIT_ASSERT(sawRawRead);
            }
        });
        env.Runtime->FilterFunction = {};
    }
}
