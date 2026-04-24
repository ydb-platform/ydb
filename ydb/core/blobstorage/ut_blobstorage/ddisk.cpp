#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/protos/config.pb.h>

Y_UNIT_TEST_SUITE(DDisk) {

    struct TDDiskTestContext {
        TEnvironmentSetup Env;

        const ui32 BlockSize = 4096;
        ui32 SurfaceSize;
        ui32 SurfaceBlocks;
        std::map<ui64, std::tuple<ui32, ui32, TString>> PersistentBuffers;
        TString Surface;
        TActorId ServiceId;
        TActorId PBServiceId;
        NKikimrBlobStorage::NDDisk::TDDiskId PersId;

        NDDisk::TQueryCredentials Creds;
        std::vector<NDDisk::TQueryCredentials> PBCreds;
        TActorId Edge;

        ui64 VChunkIndex = 0;
        ui64 NextLsn = 1;
        int LetterIndex = 0;
        const TString Letters = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        TDDiskTestContext(ui32 surfaceSize = 64_KB, ui64 inMemCache = 128_MB)
            : Env({
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                .ConfigPreprocessor = [inMemCache](ui32, TNodeWardenConfig& cfg){
                    NYdb::NBS::NProto::TPBufferConfig pbCfg;
                    pbCfg.SetMaxChunks(10);
                    pbCfg.SetMaxInMemoryCache(inMemCache);
                    cfg.PBufferConfig = pbCfg;
                }
            }) {
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

        auto AllocateDDiskBlockGroup(ui32 count = 8) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
            auto& r = ev->Record;
            r.SetDDiskPoolName("ddisk_pool");
            r.SetPersistentBufferDDiskPoolName("ddisk_pool");
            r.SetTabletId(1);
            for (ui32 i = 0; i < count; ++i) {
                auto *q = r.AddQueries();
                q->SetDirectBlockGroupId(i + 1);
                q->SetTargetNumVChunks(1);
            }
            Env.Runtime->SendToPipe(MakeBSControllerID(), Edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(Edge);
            auto& rr = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(rr.ResponsesSize(), count);
            auto responses = rr.GetResponses();
            return responses;
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

            PBCreds.clear();
            PBCreds.resize(10);
            for (ui32 i : xrange(10)) {
                PBCreds[i].TabletId = i + 1;
                PBCreds[i].Generation = 1;
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvConnect(PBCreds[i])), Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                PBCreds[i].DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
            }
        }

        void ChangeTestingNode(auto node) {
            UNIT_ASSERT(node.HasDDiskId());
            UNIT_ASSERT(node.HasPersistentBufferDDiskId());
            auto& ddiskId = node.GetDDiskId();
            ServiceId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());

            PersId = node.GetPersistentBufferDDiskId();
            PBServiceId = MakeBlobStoragePersistentBufferId(PersId.GetNodeId(), PersId.GetPDiskId(), PersId.GetDDiskSlotId());

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

            auto buf = TRcBuf::UninitializedPageAligned(size);
            char letter = Letters[LetterIndex++ % Letters.size()];
            memset(buf.GetDataMut(), letter, buf.size());

            memcpy(Surface.Detach() + offset, buf.data(), buf.size());

            Cerr << "write offset# " << offset << " size# " << size << " letter# " << letter << "\n";

            std::unique_ptr<NDDisk::TEvWrite> ev(new NDDisk::TEvWrite(Creds,
                {VChunkIndex, offset, size}, {0}));
            ev->AddPayload(TRope(std::move(buf)));
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
                PBCreds[0])), Edge.NodeId());
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

        double WritePB(ui32 offset = 0, ui32 numBlocks = 0, ui32 tabletIdx = 0) {
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
                PBCreds[tabletIdx], {VChunkIndex, offset, size}, lsn, {0}));
            ev->AddPayload(TRope(std::move(update)));
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, ev.release()), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBufferResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            return res->Get()->Record.GetFreeSpace();
        }

        ui32 GetPBInMemoryCacheSize() {
            Cerr << "get persistent buffer info \n";
            return GetPBInfo()->Get()->InMemoryCacheSize;
        }

        TAutoPtr<TEventHandle<NDDisk::TEvPersistentBufferInfo>> GetPBInfo(bool describeFreeSpace = false, bool describeTablets = false) {
            Cerr << "get persistent buffer info \n";

            std::unique_ptr<NDDisk::TEvGetPersistentBufferInfo> ev(new NDDisk::TEvGetPersistentBufferInfo(describeFreeSpace, describeTablets));
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, ev.release()), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvPersistentBufferInfo>(Edge, false);
            return res;
        }

        void ReadPB(ui32 repeat = 1) {
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
            for (ui32 _ : xrange(repeat)) {
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvReadPersistentBuffer(
                    PBCreds[0], {VChunkIndex, offsetInBytes, size}, lsn, PBCreds[0].Generation, {true})), Edge.NodeId());
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
        }

        double ErasePB(ui64 lsn = 0, ui32 tabletIdx = 0) {
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

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "erase persistent buffer lsn# " << lsn << "\n";

                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvErasePersistentBuffer(
                    PBCreds[tabletIdx], lsn, PBCreds[tabletIdx].Generation)),
                    Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == status);
                return res->Get()->Record.GetFreeSpace();
            };

            auto res = testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK) == res);
            for (auto it = PersistentBuffers.begin(); it != PersistentBuffers.end() && it->first <= lsn; ) {
                auto current = it++;
                PersistentBuffers.erase(current);
            }
            return res;
        }

        double BatchErasePB() {
            std::vector<std::tuple<ui64, ui32>> erases;
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
                erases.push_back({*lsn, PBCreds[0].Generation});
                lsns.erase(lsn);
            }

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "batch erase persistent buffer count# " << erases.size() << Endl;

                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvBatchErasePersistentBuffer(
                    PBCreds[0], erases)), Edge.NodeId());
                auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == status);
                return res->Get()->Record.GetFreeSpace();
            };

            auto res = testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            UNIT_ASSERT(testErase(NKikimrBlobStorage::NDDisk::TReplyStatus::OK) == res);
            for (auto [lsn, _] : erases) {
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
            ui64 sourceDDiskInstanceGuid = *PBCreds[0].DDiskInstanceGuid;
            auto sync = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(Creds, sourceDDiskId, sourceDDiskInstanceGuid);

            for (ui64 lsn : selectedLsns) {
                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                Cerr << "sync persistent buffer offset# " << offsetInBytes << " size# " << size
                    << " lsn# " << lsn << "\n";
                sync->AddSegment({VChunkIndex, offsetInBytes, size}, lsn, PBCreds[0].Generation);
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
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvBatchErasePersistentBuffer(
                    PBCreds[0], {{lsn, PBCreds[0].Generation}})),
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

            for (auto& cred : PBCreds) {
                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvConnect(cred)), Edge.NodeId());
                res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                cred.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
            }
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
        const ui32 maxChunks = 256;
        const ui32 sectorInChunk = 32768;
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
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

    Y_UNIT_TEST(PersistentBufferFillAndRead) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 i = 1; i < 2000; ++i) {
            f.WritePB(0, 128); // Max size records to overfill in memory buffer
            ui32 size = f.GetPBInMemoryCacheSize();
            UNIT_ASSERT(size > 0 && size <= 128_MB);
        }
        for (ui32 i = 1; i < 1000; ++i) {
            f.ReadPB(2);
            ui32 size = f.GetPBInMemoryCacheSize();
            UNIT_ASSERT(size > 0 && size <= 128_MB);
        }
        f.RestartNode();
        f.ListPB();
        for (ui32 i = 1; i < 1000; ++i) {
            f.ReadPB(2);
            ui32 size = f.GetPBInMemoryCacheSize();
            UNIT_ASSERT(size > 0 && size <= 128_MB);
        }
    }

    Y_UNIT_TEST(PersistentBufferCustomConfig) {
        TDDiskTestContext f(1_MB, 1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 _ : xrange(100)) {
            f.WritePB(0, 128);
        }
        ui32 cacheSize = f.GetPBInMemoryCacheSize();
        UNIT_ASSERT(cacheSize == 1_MB);
    }

    Y_UNIT_TEST(PBAllocatedOnSameNodeButDifferentPDisk) {
        TDDiskTestContext f;
        auto r = f.AllocateDDiskBlockGroup(32);
        std::unordered_map<TNodeId, ui32> ddisksCnt;
        std::unordered_map<TNodeId, ui32> pbuffsCnt;
        for (auto& item : r) {
            Cerr << "DBG: " << item.GetDirectBlockGroupId() << Endl;
            std::unordered_map<TNodeId, TPDiskId> ddisks;
            std::unordered_map<TNodeId, TPDiskId> pbuffs;
            UNIT_ASSERT(item.NodesSize() == 5);
            for (auto& node : item.GetNodes()) {
                TDDiskId ddid = node.GetDDiskId();
                auto [_, inserted1] = ddisks.emplace(ddid.NodeId, ddid.ComprisingPDiskId());
                UNIT_ASSERT(inserted1);
                ddisksCnt[ddid.NodeId]++;
                TDDiskId pbid = node.GetPersistentBufferDDiskId();
                auto [__, inserted2] = pbuffs.emplace(pbid.NodeId, pbid.ComprisingPDiskId());
                UNIT_ASSERT(inserted2);
                pbuffsCnt[pbid.NodeId]++;
                UNIT_ASSERT(ddid.NodeId == pbid.NodeId);
                Cerr << "   DDisk: " << node.GetDDiskId() << Endl;
                Cerr << "   PBuff: " << node.GetPersistentBufferDDiskId() << Endl;
            }
            for (auto& [nId, dd] : ddisks) {
                auto& pb = pbuffs[nId];
                UNIT_ASSERT(nId == 8 ? pb == dd : pb != dd);
            }
        }
        UNIT_ASSERT(pbuffsCnt.size() == 8);
        UNIT_ASSERT(ddisksCnt.size() == 8);
        ui32 cnt = ddisksCnt.begin()->second;
        for (auto [k, v] : ddisksCnt) {
            UNIT_ASSERT(cnt == v);
        }
        for (auto [kd, v] : pbuffsCnt) {
            UNIT_ASSERT(cnt == v);
        }
    }

    Y_UNIT_TEST(PersistentBufferEraseBarrier) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 i = 1; i < 20; ++i) {
            f.WritePB();
        }
        f.ErasePB();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 1);
            UNIT_ASSERT(b.begin()->first == f.PBCreds[0].TabletId);
        }
        f.ListPB();
        f.RestartNode();
        f.ListPB();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 1);
            UNIT_ASSERT(b.begin()->first == f.PBCreds[0].TabletId);
        }
    }

    Y_UNIT_TEST(PersistentBufferEraseSingleLsnNoBarrier) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.WritePB();
        f.ErasePB();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 0);
        }
    }

    Y_UNIT_TEST(PersistentBufferEraseBarrierDeleted) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.WritePB();
        f.WritePB();
        f.ErasePB(10);
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 1);
            UNIT_ASSERT(b.begin()->first == f.PBCreds[0].TabletId);
            UNIT_ASSERT(b.begin()->second == 10);
        }
        f.ListPB();
        f.RestartNode();
        f.ListPB();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 0);
        }
    }

    Y_UNIT_TEST(PersistentBufferEraseBarrierManyTablets) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 i : xrange(5)) {
            f.WritePB(0, 128, i);
            f.WritePB(0, 128, i);
            f.WritePB(0, 128, i);
        }
        f.ErasePB(5, 1); // one record left
        f.ErasePB(100, 4); // all records deleted
        f.ErasePB(1, 0); // 1 record deleted, no barrier
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 2);
            UNIT_ASSERT(b[f.PBCreds[4].TabletId] == 100);
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.RestartNode();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 2);
            UNIT_ASSERT(b[f.PBCreds[4].TabletId] == 100); // Barrier was not deleted, because record was not overwritten
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }

        f.WritePB(0, 128, 6);
        f.WritePB(0, 128, 6);
        f.WritePB(0, 128, 6);
        f.ErasePB(300, 6); // all records deleted
        for (ui32 _ : xrange(2530)) {
            f.WritePB(0, 128, 7); // write full PB to overwrite deleted records space
        }
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[6].TabletId] == 300);
            UNIT_ASSERT(b[f.PBCreds[4].TabletId] == 100);
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.RestartNode();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[6].TabletId] == 300); // Hole
            UNIT_ASSERT(b[f.PBCreds[4].TabletId] == 100); // Hole
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.ErasePB(2000, 7); // clear PB space to write more
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000); // Hole replaced
            UNIT_ASSERT(b[f.PBCreds[4].TabletId] == 100); // Hole
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.WritePB(0, 128, 8);
        f.WritePB(0, 128, 8);
        f.WritePB(0, 128, 8);
        f.ErasePB(3000, 8); // all records deleted
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000);
            UNIT_ASSERT(b[f.PBCreds[8].TabletId] == 3000); // Hole replaced
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.WritePB(0, 128, 8);
        f.WritePB(0, 128, 8);
        f.ErasePB(3500, 8); // all records deleted
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000);
            UNIT_ASSERT(b[f.PBCreds[8].TabletId] == 3500); // Same place used
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.WritePB(0, 128, 9);
        f.WritePB(0, 128, 9);
        f.ErasePB(6000, 9); // all records deleted
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 4);
            UNIT_ASSERT(b[f.PBCreds[9].TabletId] == 6000); // One new
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000);
            UNIT_ASSERT(b[f.PBCreds[8].TabletId] == 3500);
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
        f.RestartNode();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 4);
            UNIT_ASSERT(b[f.PBCreds[9].TabletId] == 6000);
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000);
            UNIT_ASSERT(b[f.PBCreds[8].TabletId] == 3500);
            UNIT_ASSERT(b[f.PBCreds[1].TabletId] == 5);
        }
    }
}
