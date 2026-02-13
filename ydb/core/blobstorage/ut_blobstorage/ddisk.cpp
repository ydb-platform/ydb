#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor.h>

Y_UNIT_TEST_SUITE(DDisk) {

    struct TDDiskTestContext {
        TEnvironmentSetup Env{{
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            }};

        const ui32 BlockSize = 4096;
        ui32 SurfaceSize;
        ui32 SurfaceBlocks;
        std::map<ui64, std::tuple<ui32, ui32, TString>> PersistentBuffers;
        TString Surface;
        TActorId ServiceId;
        TActorId PBServiceId;
        NKikimrBlobStorage::NDDisk::TDDiskId PersId;

        NDDisk::TQueryCredentials Creds;
        NDDisk::TQueryCredentials PBCreds;
        TActorId Edge;

        ui64 VChunkIndex = 0;
        ui64 NextLsn = 1;
        int LetterIndex = 0;
        const TString Letters = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        TDDiskTestContext(ui32 surfaceSize = 64_KB) {
            SurfaceSize = surfaceSize;
            SurfaceBlocks = SurfaceSize / BlockSize;
            Env.CreateBoxAndPool();
            Env.Sim(TDuration::Seconds(30));

            {
                NKikimrBlobStorage::TConfigRequest request;
                auto *cmd = request.AddCommand()->MutableDefineDDiskPool();
                cmd->SetBoxId(1);
                cmd->SetName("ddisk_pool");
                auto *g = cmd->MutableGeometry();
                g->SetRealmLevelBegin(10);
                g->SetRealmLevelEnd(20);
                g->SetDomainLevelBegin(10);
                g->SetDomainLevelEnd(40);
                g->SetNumFailRealms(1);
                g->SetNumFailDomainsPerFailRealm(5);
                g->SetNumVDisksPerFailDomain(1);
                cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
                cmd->SetNumDDiskGroups(3);
                auto res = Env.Invoke(request);
                UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
            }
            Edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
        }

        auto AllocateDDiskBlockGroup() {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
            auto& r = ev->Record;
            r.SetDDiskPoolName("ddisk_pool");
            r.SetPersistentBufferDDiskPoolName("ddisk_pool");
            r.SetTabletId(1);
            for (ui32 i = 0; i < 8; ++i) {
                auto *q = r.AddQueries();
                q->SetDirectBlockGroupId(i + 1);
                q->SetTargetNumVChunks(1);
            }
            Env.Runtime->SendToPipe(MakeBSControllerID(), Edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(Edge);
            auto& rr = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(rr.ResponsesSize(), 8);
            return rr.GetResponses();
        }

        void GreetDDisks() {
            Creds.TabletId = 1;
            Creds.Generation = 1;
            {
                Env.Runtime->Send(new IEventHandle(ServiceId, Edge, new NDDisk::TEvConnect(Creds)), Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                Creds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
            }

            PBCreds = Creds;
            {
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvConnect(PBCreds)), Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                PBCreds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
            }
        }

        void ChangeTestingNode(auto node) {
            UNIT_ASSERT(node.HasDDiskId());
            UNIT_ASSERT(node.HasPersistentBufferDDiskId());
            auto& ddiskId = node.GetDDiskId();
            ServiceId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());

            PersId = node.GetPersistentBufferDDiskId();
            PBServiceId = MakeBlobStorageDDiskId(PersId.GetNodeId(), PersId.GetPDiskId(), PersId.GetDDiskSlotId());

            Edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);

            GreetDDisks();
            Surface = TString::Uninitialized(SurfaceSize);
            memset(Surface.Detach(), 0, Surface.size());
            PersistentBuffers.clear();
        }

        void RandomWrite() {
            const ui32 offset = RandomNumber(SurfaceBlocks) * BlockSize;
            const ui32 numBlocks = 1 + RandomNumber(Min<ui32>(3, SurfaceBlocks - offset / BlockSize));
            const ui32 size = numBlocks * BlockSize;
            UNIT_ASSERT(offset < Surface.size() && offset + size <= Surface.size());

            TString update = TString::Uninitialized(size);
            char letter = Letters[LetterIndex++ % Letters.size()];
            memset(update.Detach(), letter, update.size());

            memcpy(Surface.Detach() + offset, update.data(), update.size());

            Cerr << "write offset# " << offset << " size# " << size << " letter# " << letter << "\n";

            std::unique_ptr<NDDisk::TEvWrite> ev(new NDDisk::TEvWrite(Creds,
                {VChunkIndex, offset, size}, {0}));
            ev->AddPayload(TRope(std::move(update)));
            Env.Runtime->Send(new IEventHandle(ServiceId, Edge, ev.release()), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvWriteResult>(Edge, false);

            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        }

        void RandomRead() {
            const ui32 offset = RandomNumber(SurfaceBlocks) * BlockSize;
            const ui32 numBlocks = 1 + RandomNumber(Min<ui32>(3, SurfaceBlocks - offset / BlockSize));
            const ui32 size = numBlocks * BlockSize;
            UNIT_ASSERT(offset < Surface.size() && offset + size <= Surface.size());

            Cerr << "read offset# " << offset << " size# " << size << "\n";

            Env.Runtime->Send(new IEventHandle(ServiceId, Edge, new NDDisk::TEvRead(Creds,
                {VChunkIndex, offset, size}, {true})), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvReadResult>(Edge, false);

            const auto& rr = res->Get()->Record;
            UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(rr.HasReadResult());
            const auto& rr2 = rr.GetReadResult();
            UNIT_ASSERT(rr2.HasPayloadId());
            UNIT_ASSERT_VALUES_EQUAL(rr2.GetPayloadId(), 0);
            TRope rope = res->Get()->GetPayload(0);
            UNIT_ASSERT_VALUES_EQUAL(rope.size(), size);
            UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), Surface.substr(offset, size));
        }

        void ListPB() {
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvListPersistentBuffer(
                PBCreds)), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvListPersistentBufferResult>(Edge, false);
            const auto& rr = res->Get()->Record;
            UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

            Cerr << "list\n";

            THashSet<std::tuple<ui64, ui32, ui32>> returnedLsns;
            for (const auto& item : rr.GetRecords()) {
                const auto& sel = item.GetSelector();
                if (sel.GetVChunkIndex() != VChunkIndex) {
                    continue;
                }
                returnedLsns.emplace(item.GetLsn(), sel.GetOffsetInBytes(), sel.GetSize());
            }

            THashSet<std::tuple<ui64, ui32, ui32>> ourLsns;
            for (const auto& [lsn, item] : PersistentBuffers) {
                const auto& [offsetInBytes, size, buffer] = item;
                ourLsns.emplace(lsn, offsetInBytes, size);
            }

            UNIT_ASSERT_EQUAL(returnedLsns, ourLsns);
        }

        double WritePB(ui32 offset = 0, ui32 numBlocks = 0) {
            if (numBlocks == 0) {
                offset = RandomNumber(SurfaceBlocks) * BlockSize;
                numBlocks = 1 + RandomNumber(Min<ui32>(3, SurfaceBlocks - offset / BlockSize));
            }
            const ui32 size = numBlocks * BlockSize;
            UNIT_ASSERT(offset < Surface.size() && offset + size <= Surface.size());

            const ui64 lsn = NextLsn++;
            TString update = TString::Uninitialized(size);
            char letter = Letters[LetterIndex++ % Letters.size()];
            memset(update.Detach(), letter, update.size());
            PersistentBuffers.emplace(lsn, std::make_tuple(offset, size, update));

            Cerr << "write persistent buffer offset# " << offset << " size# " << size << " lsn# " << lsn
                << " letter# " << letter << "\n";

            std::unique_ptr<NDDisk::TEvWritePersistentBuffer> ev(new NDDisk::TEvWritePersistentBuffer(
                PBCreds, {VChunkIndex, offset, size}, lsn, {0}));
            ev->AddPayload(TRope(std::move(update)));
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, ev.release()), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBufferResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            return res->Get()->Record.GetFreeSpace();
        }

        void ReadPB() {
            std::vector<ui64> lsns;
            lsns.reserve(PersistentBuffers.size());
            for (ui64 lsn : PersistentBuffers | std::views::keys) {
                lsns.push_back(lsn);
            }
            if (lsns.empty()) {
                return;
            }
            const size_t index = RandomNumber(lsns.size());
            const ui64 lsn = lsns[index];
            const auto& [offsetInBytes, size, buffer] = PersistentBuffers.at(lsn);

            Cerr << "read persistent buffer offset# " << offsetInBytes << " size# " << size
                << " lsn# " << lsn << "\n";

            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvReadPersistentBuffer(
                PBCreds, {VChunkIndex, offsetInBytes, size}, lsn, {true})), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvReadPersistentBufferResult>(Edge, false);
            const auto& rr = res->Get()->Record;
            UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(rr.HasReadResult());
            const auto& rr2 = rr.GetReadResult();
            UNIT_ASSERT(rr2.HasPayloadId());
            UNIT_ASSERT_VALUES_EQUAL(rr2.GetPayloadId(), 0);
            TRope rope = res->Get()->GetPayload(0);
            UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), buffer);
        }

        double ErasePB(ui64 lsn = 0) {
            if (lsn == 0) {
                std::vector<ui64> lsns;
                lsns.reserve(PersistentBuffers.size());
                for (ui64 lsn : PersistentBuffers | std::views::keys) {
                    lsns.push_back(lsn);
                }
                if (lsns.empty()) {
                    return -1;
                }
                const size_t index = RandomNumber(lsns.size());
                lsn = lsns[index];
            }
            const auto& [offsetInBytes, size, buffer] = PersistentBuffers.at(lsn);

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "erase persistent buffer offset# " << offsetInBytes << " size# " << size
                    << " lsn# " << lsn << "\n";

                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvErasePersistentBuffer(
                    PBCreds, {VChunkIndex, offsetInBytes, size}, lsn)),
                    Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == status);
                return res->Get()->Record.GetFreeSpace();
            };

            auto res = testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD) == -1);
            PersistentBuffers.erase(lsn);
            return res;
        }

        double BatchErasePB() {
            std::vector<std::tuple<NDDisk::TBlockSelector, ui64>> erases;
            std::set<ui64> lsns;
            for (ui64 lsn : PersistentBuffers | std::views::keys) {
                lsns.emplace(lsn);
            }
            if (lsns.empty()) {
                return -1;
            }
            for (ui32 _ : xrange(RandomNumber(4u) + 1)) {
                if (lsns.empty()) {
                    break;
                }
                auto lsn = lsns.begin();
                std::advance(lsn, RandomNumber(lsns.size()));
                const auto& [offsetInBytes, size, buffer] = PersistentBuffers.at(*lsn);
                erases.push_back({{VChunkIndex, offsetInBytes, size}, *lsn});
                lsns.erase(lsn);
            }

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "batch erase persistent buffer count# " << erases.size() << Endl;

                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvBatchErasePersistentBuffer(
                    PBCreds, erases)), Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == status);
                return res->Get()->Record.GetFreeSpace();
            };

            auto res = testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD) == -1);
            for (auto [_, lsn] : erases) {
                PersistentBuffers.erase(lsn);
            }
            return res;
        }

        void SyncPB() {
            std::vector<ui64> lsns;
            lsns.reserve(PersistentBuffers.size());
            for (ui64 lsn : PersistentBuffers | std::views::keys) {
                lsns.push_back(lsn);
            }
            if (lsns.empty()) {
                return;
            }

            std::vector<ui64> selectedLsns;
            selectedLsns.reserve(3);

            while (!lsns.empty() && selectedLsns.size() < 3) {
                const size_t index = RandomNumber(lsns.size());
                const ui64 lsn = lsns[index];
                lsns[index] = lsns.back();
                lsns.pop_back();

                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                const ui32 end = offsetInBytes + size;

                bool overlap = false;
                for (ui64 selectedLsn : selectedLsns) {
                    const auto& selectedRecord = PersistentBuffers.at(selectedLsn);
                    const ui32 selectedOffsetInBytes = std::get<0>(selectedRecord);
                    const ui32 selectedSize = std::get<1>(selectedRecord);
                    const ui32 selectedEnd = selectedOffsetInBytes + selectedSize;
                    if (!(end <= selectedOffsetInBytes || selectedEnd <= offsetInBytes)) {
                        overlap = true;
                        break;
                    }
                }
                if (!overlap) {
                    selectedLsns.push_back(lsn);
                }
            }

            if (selectedLsns.empty()) {
                return;
            }

            std::tuple<ui32, ui32, ui32> sourceDDiskId(PersId.GetNodeId(), PersId.GetPDiskId(), PersId.GetDDiskSlotId());
            ui64 sourceDDiskInstanceGuid = *PBCreds.DDiskInstanceGuid;
            auto sync = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(Creds, sourceDDiskId, sourceDDiskInstanceGuid);

            for (ui64 lsn : selectedLsns) {
                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                Cerr << "sync persistent buffer offset# " << offsetInBytes << " size# " << size
                    << " lsn# " << lsn << "\n";
                sync->AddSegment({VChunkIndex, offsetInBytes, size}, lsn);
            }

            Env.Runtime->Send(new IEventHandle(ServiceId, Edge, sync.release()), Edge.NodeId());
            auto syncRes = Env.WaitForEdgeActorEvent<NDDisk::TEvSyncWithPersistentBufferResult>(Edge, false);
            const auto& syncRecord = syncRes->Get()->Record;
            UNIT_ASSERT(syncRecord.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(syncRecord.SegmentResultsSize(), selectedLsns.size());
            for (const auto& segmentResult : syncRecord.GetSegmentResults()) {
                UNIT_ASSERT(segmentResult.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            }

            for (ui64 lsn : selectedLsns) {
                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                const TString& buffer = std::get<2>(record);
                Env.Runtime->Send(new IEventHandle(ServiceId, Edge, new NDDisk::TEvRead(Creds,
                    {VChunkIndex, offsetInBytes, size}, {true})), Edge.NodeId());
                auto readRes = Env.WaitForEdgeActorEvent<NDDisk::TEvReadResult>(Edge, false);
                const auto& readRecord = readRes->Get()->Record;
                UNIT_ASSERT(readRecord.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                UNIT_ASSERT(readRecord.HasReadResult());
                const auto& readResult = readRecord.GetReadResult();
                UNIT_ASSERT(readResult.HasPayloadId());
                UNIT_ASSERT_VALUES_EQUAL(readResult.GetPayloadId(), 0);
                TRope rope = readRes->Get()->GetPayload(0);
                UNIT_ASSERT_VALUES_EQUAL(rope.size(), size);
                UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), buffer);

                memcpy(Surface.Detach() + offsetInBytes, buffer.data(), size);
            }

            for (ui64 lsn : selectedLsns) {
                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvErasePersistentBuffer(
                    PBCreds, {VChunkIndex, offsetInBytes, size}, lsn)),
                    Edge.NodeId());
                auto eraseRes = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
                UNIT_ASSERT(eraseRes->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                PersistentBuffers.erase(lsn);
            }
        }
        void RestartNode() {
            Cerr << "Restarting node " << PersId.GetNodeId() << "\n";
            Env.RestartNode(PersId.GetNodeId());
            Env.Sim(TDuration::Seconds(60));

            Edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);

            // Reconnect to DDisk after restart
            Env.Runtime->Send(new IEventHandle(ServiceId, Edge, new NDDisk::TEvConnect(Creds)), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            Creds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();

            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvConnect(PBCreds)), Edge.NodeId());
            res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            PBCreds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
        }
    };

    Y_UNIT_TEST(Basic) {
        TDDiskTestContext f;
        for (auto& item : f.AllocateDDiskBlockGroup()) {
            for (auto& node : item.GetNodes()) {
                f.ChangeTestingNode(node);
                Cerr << "next iteration\n";

                for (ui32 iter = 0; iter < 1000; ++iter) {
                    switch (RandomNumber(7u)) {
                        case 0: {
                            f.RandomWrite();
                            break;
                        }
                        case 1: {
                            f.RandomRead();
                            break;
                        }
                        case 2: {
                            f.ListPB();
                            break;
                        }
                        case 3: {
                            f.WritePB();
                            break;
                        }
                        case 4: {
                            f.ReadPB();
                            break;
                        }
                        case 5: {
                            f.ErasePB();
                            break;
                        }
                        case 6: {
                            f.SyncPB();
                            break;
                        }
                    }
                }
                ++f.VChunkIndex;
            }
        }
    }

    Y_UNIT_TEST(PersistentBufferWithRestarts) {
        TDDiskTestContext f;
        for (auto& item : f.AllocateDDiskBlockGroup()) {
            for (auto& node : item.GetNodes()) {
                f.ChangeTestingNode(node);
                Cerr << "next iteration\n";
                for (ui32 iter = 0; iter < 1000; ++iter) {
                    if (iter % 400 == 399) {
                        f.RestartNode();
                    }
                    switch (RandomNumber(5u)) {
                        case 0: {
                            f.ListPB();
                            break;
                        }
                        case 1: {
                            f.WritePB();
                            break;
                        }
                        case 2: {
                            f.ReadPB();
                            break;
                        }
                        case 3: {
                            f.ErasePB();
                            break;
                        }
                        case 4: {
                            f.BatchErasePB();
                            break;
                        }
                    }
                }
                ++f.VChunkIndex;
            }
        }
    }

    Y_UNIT_TEST(PersistentBufferFreeSpace) {
        const ui32 maxChunks = 128;
        const ui32 sectorInChunk = 32768;
        TDDiskTestContext f(1_MB);
        auto& node = f.AllocateDDiskBlockGroup().begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 i = 0; i < 10; ++i) {
            f.WritePB(0, RandomNumber(127u) + 1);
            auto fs = f.ErasePB();
            UNIT_ASSERT(fs == 1);
        }

        double occupiedSpace = 0;

        for (ui32 i = 1; i < 1000; ++i) {
            if (i % 200 == 99) {
                f.RestartNode();
            }
            ui32 sectorsCnt = RandomNumber(127u) + 1;
            auto freeSpace = f.WritePB(0, sectorsCnt);
            occupiedSpace += sectorsCnt + 1;
            double total = maxChunks * sectorInChunk;
            double calcFreeSpace = (total - occupiedSpace) / total;
            UNIT_ASSERT(std::abs(freeSpace - calcFreeSpace) < 0.00001);
        }

        double freeSpace = 0;

        for (ui32 i = 1000; i > 1; --i) {
            if (i % 200 == 99) {
                f.RestartNode();
            }
            auto fs = i % 2 ? f.ErasePB() : f.BatchErasePB();
            freeSpace = fs >= 0 ? fs : freeSpace;
            Cerr << "freeSpace: " << freeSpace << Endl;
        }
        UNIT_ASSERT(freeSpace == 1);
    }

}
