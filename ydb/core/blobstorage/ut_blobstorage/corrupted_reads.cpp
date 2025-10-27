#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

Y_UNIT_TEST_SUITE(CorruptedReads) {
    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });

        env.CreateBoxAndPool(1, 1, 0, NKikimrBlobStorage::EPDiskType::NVME);
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        env.Sim(TDuration::Minutes(5));

        const TActorId writer = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        ui32 index = 1;

        for (ui32 majorIter = 0; majorIter < 3; ++majorIter) {
            for (ui32 i = 0; i < 5000; ++i) {
                const size_t size = index % 2 ? 100 + RandomNumber(200u) : 4 * 65536 + RandomNumber(32768u);
                TString data = FastGenDataForLZ4(size, index);
                const TLogoBlobID id(1, 1, index, 0, data.size(), 0);
                ++index;

                env.Runtime->WrapInActorContext(writer, [&] {
                    SendToBSProxy(writer, info->GroupID, new TEvBlobStorage::TEvPut(id, TRcBuf(data), TInstant::Max()));
                });
                const auto& res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(writer, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }

            env.Sim(TDuration::Seconds(10));

            const ui32 orderNum = 0;
            const TVDiskID& vdiskId = info->GetVDiskId(orderNum);
            const TActorId& actorId = info->GetActorId(orderNum);
            UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), writer.NodeId());

            auto ev = std::make_unique<IEventHandle>(actorId, writer, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
            env.Runtime->Send(ev.release(), writer.NodeId());
            env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(writer, false);

            env.Sim(TDuration::Seconds(10));

            TIntrusivePtr<TPDiskMockState> state;
            for (auto& [pdiskId, ptr] : env.PDiskMockStates) {
                const auto& [nodeId, _] = pdiskId;
                if (nodeId == writer.NodeId()) {
                    UNIT_ASSERT(!state);
                    state = ptr;
                }
            }

            std::vector<TLogoBlobID> blobs;
            auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : res->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database != T::EDatabase::LogoBlobs) {
                    continue;
                }
                if (!item.BlobId) {
                    continue;
                }
                blobs.push_back(item.BlobId);
                const auto& location = item.Location;
                switch (RandomNumber(3u)) {
                    case 0:
                        break;

                    case 1:
                        state->SetCorruptedArea(location.ChunkIdx, location.Offset, location.Offset + location.Size, true);
                        break;

                    case 2:
                        state->SetCorruptedArea(location.ChunkIdx, location.Offset, location.Offset + location.Size / 2, true);
                        break;
                }
                // Cerr << item.BlobId << ' ' << item.Location.ToString() << Endl;
            }

            auto queueActorId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::GetFastRead, 0, actorId.NodeId());

            for (ui32 iter = 0; iter < 1000; ++iter) {
                ui32 numBlobs = 1 + RandomNumber(100u);
                THashMap<TLogoBlobID, TIntervalMap<ui32>> availableOffsets;
                auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(), NKikimrBlobStorage::FastRead);
                ui32 replySize = 0;
                for (ui32 k = 0; k < numBlobs; ++k) {
                    const size_t blobIndex = RandomNumber(blobs.size());
                    const TLogoBlobID& id = blobs[blobIndex];
                    // Cerr << id << Endl;
                    const size_t partSize = info->Type.MaxPartSize(id);
                    UNIT_ASSERT(partSize != 0);

                    const auto [it, inserted] = availableOffsets.try_emplace(id);
                    if (inserted) {
                        it->second.Assign(0, partSize);
                    }
                    auto& map = it->second;
                    if (map.IsEmpty()) {
                        --k;
                        continue;
                    }

                    auto jt = map.EndForBegin.begin();
                    std::advance(jt, RandomNumber(map.EndForBegin.size()));
                    const auto& [begin, end] = *jt;
                    const ui32 shift = begin + RandomNumber(end - begin);
                    const ui32 size = 1 + RandomNumber(end - shift);
                    // Cerr << partSize << '/' << begin << '/' << end << '/' << shift << '/' << size << Endl;
                    UNIT_ASSERT(shift < partSize);
                    UNIT_ASSERT(size <= partSize - shift);
                    map.Subtract(shift, shift + size);
                    query->AddExtremeQuery(id, shift, size);

                    replySize += size;
                    if (replySize >= 50'000'000) {
                        break;
                    }
                }

                env.Runtime->Send(new IEventHandle(queueActorId, writer, query.release()), queueActorId.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(writer, false);
                const auto& ev = res->Get();
                auto& record = ev->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), numBlobs);
                for (auto& item : record.GetResult()) {
                    UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
                    const TLogoBlobID id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                    const TString data = FastGenDataForLZ4(id.BlobSize(), id.Step());
                    std::vector<TRope> parts(info->Type.TotalPartCount());
                    ErasureSplit(static_cast<TBlobStorageGroupType::ECrcMode>(id.CrcMode()), info->Type, TRope(data), parts);
                    UNIT_ASSERT(ev->HasBlob(item));
                    const TRope& readPartData = ev->GetBlobData(item);
                    const TRope& expectedPart = parts[id.PartId() - 1];
                    const auto begin = expectedPart.Begin();
                    const TRope expectedPartSubrange(begin + item.GetShift(), begin + item.GetShift() + item.GetSize());
                    UNIT_ASSERT_VALUES_EQUAL(readPartData.size(), expectedPartSubrange.size());
                    UNIT_ASSERT_EQUAL(readPartData, expectedPartSubrange);
                }
            }
        }
    }

    Y_UNIT_TEST(Compaction) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });

        env.CreateBoxAndPool(1, 1, 0, NKikimrBlobStorage::EPDiskType::NVME);
        env.Sim(TDuration::Minutes(1));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        env.Sim(TDuration::Minutes(5));

        const TActorId writer = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        const ui32 orderNum = 0;
        const TVDiskID& vdiskId = info->GetVDiskId(orderNum);
        const TActorId& actorId = info->GetActorId(orderNum);
        UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), writer.NodeId());

        std::vector<TLogoBlobID> blobs;
        THashMap<TLogoBlobID, TDiskPart> prevLocation;

        for (ui32 iter = 0; iter < 2; ++iter) {
            ui32 index = iter + 1;

            for (ui32 i = 0; i < 5000; ++i) {
                const size_t size = 100 + RandomNumber(10000u);
                TString data = FastGenDataForLZ4(size, index);
                const TLogoBlobID id(1, 1, index, 0, data.size(), 0);
                index += 2;

                env.Runtime->WrapInActorContext(writer, [&] {
                    SendToBSProxy(writer, info->GroupID, new TEvBlobStorage::TEvPut(id, TRcBuf(data), TInstant::Max()));
                });
                const auto& res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(writer, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }

            auto ev = std::make_unique<IEventHandle>(actorId, writer, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
            env.Runtime->Send(ev.release(), writer.NodeId());
            env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(writer, false);

            TIntrusivePtr<TPDiskMockState> state;
            for (auto& [pdiskId, ptr] : env.PDiskMockStates) {
                const auto& [nodeId, _] = pdiskId;
                if (nodeId == writer.NodeId()) {
                    UNIT_ASSERT(!state);
                    state = ptr;
                }
            }

            auto res = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : res->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database != T::EDatabase::LogoBlobs) {
                    continue;
                }
                if (!item.BlobId) {
                    continue;
                }
                const auto& location = item.Location;
                if (iter == 0) {
                    blobs.push_back(item.BlobId);
                    prevLocation[item.BlobId] = location;
                    state->SetCorruptedArea(location.ChunkIdx, location.Offset, location.Offset + location.Size / 2, true);
                } else if (prevLocation.contains(item.BlobId)) {
                    UNIT_ASSERT(prevLocation[item.BlobId] != location);
                }
            }
        }

        auto queueActorId = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::GetFastRead, 0, actorId.NodeId());

        for (ui32 nodeId : env.Runtime->GetNodes()) {
            if (nodeId != queueActorId.NodeId()) {
                env.StopNode(nodeId);
            }
        }

        for (const TLogoBlobID& id : blobs) {
            auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {id});
            env.Runtime->Send(new IEventHandle(queueActorId, writer, query.release()), queueActorId.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(writer, false);
            const auto& ev = res->Get();
            auto& record = ev->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
            for (auto& item : record.GetResult()) {
                UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
                const TLogoBlobID id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                const TString data = FastGenDataForLZ4(id.BlobSize(), id.Step());
                std::vector<TRope> parts(info->Type.TotalPartCount());
                ErasureSplit(static_cast<TBlobStorageGroupType::ECrcMode>(id.CrcMode()), info->Type, TRope(data), parts);
                UNIT_ASSERT(ev->HasBlob(item));
                const TRope& readPartData = ev->GetBlobData(item);
                const TRope& expectedPart = parts[id.PartId() - 1];
                UNIT_ASSERT_VALUES_EQUAL(readPartData.size(), expectedPart.size());
                UNIT_ASSERT_EQUAL(readPartData, expectedPart);
            }
        }
    }
}
