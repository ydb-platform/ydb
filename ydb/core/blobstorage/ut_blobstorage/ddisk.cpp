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

        TDDiskTestContext(ui32 surfaceSize = 64_KB, ui64 inMemCache = 128_MB,
                std::optional<ui32> minFreeSectorsReserve = std::nullopt,
                std::optional<ui32> preallocateFreeSpaceThresholdPercent = std::nullopt,
                std::optional<ui32> deallocateFreeSpaceThresholdPercent = std::nullopt,
                std::optional<ui32> deallocateThresholdSeconds = std::nullopt)
            : Env({
                .NodeCount = 8,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                .ConfigPreprocessor = [inMemCache, minFreeSectorsReserve, preallocateFreeSpaceThresholdPercent,
                        deallocateFreeSpaceThresholdPercent, deallocateThresholdSeconds](ui32, TNodeWardenConfig& cfg){
                    NYdb::NBS::NProto::TPBufferConfig pbCfg;
                    pbCfg.SetMaxChunks(10);
                    pbCfg.SetMaxInMemoryCache(inMemCache);
                    if (minFreeSectorsReserve.has_value()) {
                        pbCfg.SetMinFreeSectorsReserve(*minFreeSectorsReserve);
                    }
                    if (preallocateFreeSpaceThresholdPercent.has_value()) {
                        pbCfg.SetPreallocateFreeSpaceThresholdPercent(*preallocateFreeSpaceThresholdPercent);
                    }
                    if (deallocateFreeSpaceThresholdPercent.has_value()) {
                        pbCfg.SetDeallocateFreeSpaceThresholdPercent(*deallocateFreeSpaceThresholdPercent);
                    }
                    if (deallocateThresholdSeconds.has_value()) {
                        pbCfg.SetDeallocateThresholdSeconds(*deallocateThresholdSeconds);
                    }
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

        // Allocate a single direct block group via the new-style DirectBlockGroupOperations
        // (as opposed to the compat Queries path) so that DDisks/PersistentBuffers can later
        // be individually manipulated (deleted, reassigned, etc.) by DirectBlockGroupId.
        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TDirectBlockGroup
        DefineDirectBlockGroup(ui64 directBlockGroupId, ui32 numDDisks, ui32 numChunksPerDDisk, ui32 numPersistentBuffers) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
            auto& r = ev->Record;
            r.SetDDiskPoolName("ddisk_pool");
            r.SetPersistentBufferDDiskPoolName("ddisk_pool");
            r.SetTabletId(1);
            auto *op = r.AddDirectBlockGroupOperations();
            op->SetDirectBlockGroupId(directBlockGroupId);
            auto *def = op->MutableDefineDirectBlockGroup();
            def->SetNumDDisks(numDDisks);
            def->SetNumChunksPerDDisk(numChunksPerDDisk);
            def->SetNumPersistentBuffers(numPersistentBuffers);
            // Use a fresh edge actor for every request: WaitForEdgeActorEvent()
            // consumes/terminates the edge actor on capture by default (termOnCapture=true),
            // so it can't be safely reused across multiple requests.
            TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
            Env.Runtime->SendToPipe(MakeBSControllerID(), edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(edge);
            auto& rr = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(rr.GetStatus(), NKikimrProto::OK, rr.GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(rr.DirectBlockGroupsSize(), 1);
            return rr.GetDirectBlockGroups(0);
        }

        // Send an arbitrary set of DirectBlockGroupOperation commands for a given
        // direct block group id and return the whole result record (including status).
        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult SendDirectBlockGroupOperation(
                ui64 directBlockGroupId,
                std::function<void(NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup::TDirectBlockGroupOperation*)> fill) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
            auto& r = ev->Record;
            r.SetDDiskPoolName("ddisk_pool");
            r.SetPersistentBufferDDiskPoolName("ddisk_pool");
            r.SetTabletId(1);
            auto *op = r.AddDirectBlockGroupOperations();
            op->SetDirectBlockGroupId(directBlockGroupId);
            fill(op);
            // Fresh edge actor, see comment in DefineDirectBlockGroup() above.
            TActorId edge = Env.Runtime->AllocateEdgeActor(Env.Settings.ControllerNodeId, __FILE__, __LINE__);
            Env.Runtime->SendToPipe(MakeBSControllerID(), edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(edge);
            return response->Get()->Record;
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
            Cerr << "Changed node: " << node <<" pb: " << PersId << " pbs: " << PBServiceId << Endl;
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
            if (returnedLsns != ourLsns) {
                Cerr << "returnedLsns: ";
                for (auto lsn : returnedLsns) {
                    Cerr << lsn << ", ";
                }
                Cerr << Endl;
                Cerr << "     ourLsns: ";
                for (auto lsn : ourLsns) {
                    Cerr << lsn << ", ";
                }
                Cerr << Endl;
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

        void MoveBarrier(ui64 lsn = 0, ui32 tabletIdx = 0) {
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvErasePersistentBuffer(
                PBCreds[tabletIdx], lsn)),
                Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        }

        // Reconnect a tablet with a new (higher) generation.
        // After this call PBCreds[tabletIdx].Generation == newGeneration.
        void BumpGeneration(ui32 tabletIdx, ui32 newGeneration) {
            PBCreds[tabletIdx].Generation = newGeneration;
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvConnect(PBCreds[tabletIdx])), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            PBCreds[tabletIdx].DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
        }

        // Write a persistent buffer record with an explicit generation (does NOT update PersistentBuffers map).
        // Returns the lsn used.
        ui64 WritePBWithGeneration(ui32 tabletIdx, ui32 generation, ui32 offset = 0, ui32 numBlocks = 4) {
            const ui32 size = numBlocks * BlockSize;
            UNIT_ASSERT(offset + size <= Surface.size());

            const ui64 lsn = NextLsn++;
            TString update = TString::Uninitialized(size);
            char letter = Letters[LetterIndex++ % Letters.size()];
            memset(update.Detach(), letter, update.size());

            NDDisk::TQueryCredentials creds = PBCreds[tabletIdx];
            creds.Generation = generation;

            Cerr << "write PB gen# " << generation << " offset# " << offset << " size# " << size
                << " lsn# " << lsn << " letter# " << letter << "\n";

            std::unique_ptr<NDDisk::TEvWritePersistentBuffer> ev(new NDDisk::TEvWritePersistentBuffer(
                creds, {VChunkIndex, offset, size}, lsn, {0}));
            ev->AddPayload(TRope(std::move(update)));
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, ev.release()), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBufferResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            return lsn;
        }

        // Batch-erase records from multiple generations in a single call.
        // erases is a list of (lsn, generation) pairs.
        double BatchEraseMultiGen(ui32 tabletIdx, const std::vector<std::tuple<ui64, ui32>>& erases) {
            Cerr << "batch erase multi-gen count# " << erases.size() << Endl;
            Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvBatchErasePersistentBuffer(
                PBCreds[tabletIdx], erases)), Edge.NodeId());
            auto res = Env.WaitForEdgeActorEvent<NDDisk::TEvErasePersistentBufferResult>(Edge, false);
            UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            return res->Get()->Record.GetFreeSpace();
        }

        double ErasePB(ui64 lsn = 0, ui32 tabletIdx = 0) {
            if (lsn == 0) {
                std::set<ui64> lsns;
                for (ui64 lsn : PersistentBuffers | std::views::keys) {
                    lsns.insert(lsn);
                }
                if (lsns.empty()) {
                    return -1;
                }
                auto it = lsns.begin();
                lsn = *it;
                it++;
                if (it != lsns.end()) {
                    lsn = *it;
                }
                for (; it != lsns.end() && RandomNumber(2u); it++) {
                    lsn = *it;
                }
            }

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "erase persistent buffer lsn# " << lsn << "\n";

                Env.Runtime->Send(new IEventHandle(PBServiceId, Edge, new NDDisk::TEvErasePersistentBuffer(
                    PBCreds[tabletIdx], lsn)),
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

        double BatchErasePB(ui32 cnt = 0, bool continuous = false) {
            std::vector<std::tuple<ui64, ui32>> erases;
            std::set<ui64> lsns;
            for (ui64 lsn : PersistentBuffers | std::views::keys) {
                lsns.emplace(lsn);
            }
            if (lsns.empty()) {
                return -1;
            }
            for (ui32 _ : xrange(cnt > 0 ? cnt : (RandomNumber(4u) + 1))) {
                if (lsns.empty()) {
                    break;
                }
                auto lsn = lsns.begin();
                if (!continuous) {
                    std::advance(lsn, RandomNumber(lsns.size()));
                }
                erases.push_back({*lsn, PBCreds[0].Generation});
                lsns.erase(lsn);
            }

            auto testErase = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status) {
                Cerr << "batch erase persistent buffer count# " << erases.size() << Endl;
                for (auto lsn : erases) {
                    Cerr << lsn << ", ";
                }
                Cerr << Endl;
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
            auto sync = std::make_unique<NDDisk::TEvSync>(Creds);

            for (ui64 lsn : selectedLsns) {
                const auto& record = PersistentBuffers.at(lsn);
                const ui32 offsetInBytes = std::get<0>(record);
                const ui32 size = std::get<1>(record);
                Cerr << "sync persistent buffer offset# " << offsetInBytes << " size# " << size
                    << " lsn# " << lsn << "\n";
                sync->AddSegmentFromPB(
                    sourceDDiskId,
                    sourceDDiskInstanceGuid,
                    {VChunkIndex, offsetInBytes, size},
                    lsn,
                    PBCreds[0].Generation);
            }

            Env.Runtime->Send(new IEventHandle(ServiceId, Edge, sync.release()), Edge.NodeId());
            auto syncRes = Env.WaitForEdgeActorEvent<NDDisk::TEvSyncResult>(Edge, false);
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
        auto group = f.AllocateDDiskBlockGroup();
        auto& item = group[0];
        for (auto& node : item.GetNodes()) {
            f.ChangeTestingNode(node);
            f.MoveBarrier(0, 0);

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

    Y_UNIT_TEST(PersistentBufferWithRestarts) {
        TDDiskTestContext f;
        auto group = f.AllocateDDiskBlockGroup();
        auto& item = group[0];
        for (auto& node : item.GetNodes()) {
            f.ChangeTestingNode(node);
            f.MoveBarrier(0, 0);

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

    Y_UNIT_TEST(PersistentBufferFreeSpace) {
        const ui32 maxChunks = 10;
        const ui32 sectorInChunk = 32768;
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        for (ui32 i = 0; i < 10; ++i) {
            f.WritePB(0, RandomNumber(127u) + 1);
            auto fs = f.BatchErasePB();
            UNIT_ASSERT(fs == 1);
        }
        f.MoveBarrier(0, 0);

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
            auto fs = f.BatchErasePB();
            freeSpace = fs >= 0 ? fs : freeSpace;
            Cerr << "freeSpace: " << freeSpace << Endl;
        }
        UNIT_ASSERT((1 - freeSpace) < 0.00001); // fast erase
    }

    Y_UNIT_TEST(PersistentBufferFastEraseFreeSpace) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.MoveBarrier(0, 0);

        auto info = f.GetPBInfo(false, true);
        auto freeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(freeSectors, 131072 - 1);
        for (ui32 i = 1; i < 1001; ++i) {
            if (i % 200 == 99) {
                f.RestartNode();
            }
            ui32 sectorsCnt = 4;
            f.WritePB(0, sectorsCnt);
            info = f.GetPBInfo(false, true);
            auto newFreeSectors = info->Get()->FreeSectors;
            UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors - i * 5);
            UNIT_ASSERT_EQUAL(0, info->Get()->TabletInfos[0].FastErasesCount);
        }
        info = f.GetPBInfo(false, true);
        auto newFreeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors - 5000);
        freeSectors = newFreeSectors;

        for (ui32 i = 1; i < 101; ++i) {
            if (i % 20 == 10) {
                f.RestartNode();
            }
            f.BatchErasePB(10);
            info = f.GetPBInfo(false, true);
            newFreeSectors = info->Get()->FreeSectors;
            UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors + i * 50 - 1);
            if (i < 100) {
                UNIT_ASSERT_EQUAL(i * 10, info->Get()->TabletInfos[0].FastErasesCount);
            } else {
                UNIT_ASSERT(info->Get()->TabletInfos.size() == 0);
            }
        }
        info = f.GetPBInfo(false, true);
        newFreeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(newFreeSectors, 131072 - 2);
    }

    Y_UNIT_TEST(PersistentBufferFastEraseFallback) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.MoveBarrier(0, 0);

        auto info = f.GetPBInfo(false, true);
        auto freeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(freeSectors, 131072 - 1);
        for (ui32 i = 1; i < 5001; ++i) {
            ui32 sectorsCnt = 4;
            f.WritePB(0, sectorsCnt);
        }
        info = f.GetPBInfo(false, true);
        auto newFreeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors - 25000);
        freeSectors = newFreeSectors;

        for (ui32 i = 1; i < 11; ++i) {
            f.BatchErasePB(1);
            info = f.GetPBInfo(false, true);
            newFreeSectors = info->Get()->FreeSectors;
            UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors + i * 5);
            UNIT_ASSERT_EQUAL(0, info->Get()->TabletInfos[0].FastErasesCount);
        }
    }

    Y_UNIT_TEST(PersistentBufferFastEraseSimpleCases) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.MoveBarrier(0, 0);

        auto info = f.GetPBInfo(false, true);
        auto freeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(freeSectors, 131072 - 1);
        for (ui32 i = 1; i < 5001; ++i) {
            ui32 sectorsCnt = 4;
            f.WritePB(0, sectorsCnt);
        }
        info = f.GetPBInfo(false, true);
        auto newFreeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors - 25000);
        freeSectors = newFreeSectors;

        for (ui32 i = 1; i < 39; ++i) {
            f.BatchErasePB(100, true);
            info = f.GetPBInfo(false, true);
            newFreeSectors = info->Get()->FreeSectors;
            UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors + i * 500 - 1);
            UNIT_ASSERT_EQUAL(i * 100, info->Get()->TabletInfos[0].FastErasesCount);
        }

        // overfill erase sector - fallback to zeroing headers
        f.BatchErasePB(100, true);
        info = f.GetPBInfo(false, true);
        newFreeSectors = info->Get()->FreeSectors;
        UNIT_ASSERT_EQUAL(newFreeSectors, freeSectors + 39 * 500 - 1);
        UNIT_ASSERT_EQUAL(3900, info->Get()->TabletInfos[0].FastErasesCount);

        //move barrier
        f.MoveBarrier(2000);
        f.BatchErasePB(100, true);
        info = f.GetPBInfo(false, true);
        UNIT_ASSERT_EQUAL(2000, info->Get()->TabletInfos[0].FastErasesCount);
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

    Y_UNIT_TEST(PersistentBufferEraseSingleLsnBarrier) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.WritePB();
        f.ErasePB();
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 1);
            UNIT_ASSERT(b[f.PBCreds[0].TabletId] == 1);
        }
    }

    Y_UNIT_TEST(PersistentBufferEraseBarrierManyTablets) {
        // Use MinFreeSectorsReserve=0 so that the guard never fires when the
        // disk fills up during the large write loop in this test.
        TDDiskTestContext f(1_MB, 128_MB, 0);
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
        f.ErasePB(2551  , 8); // all records deleted
        {
            auto info = f.GetPBInfo(false, true);
            auto& b = info->Get()->EraseBarriers;
            UNIT_ASSERT(b.size() == 3);
            UNIT_ASSERT(b[f.PBCreds[7].TabletId] == 2000);
            UNIT_ASSERT(b[f.PBCreds[8].TabletId] == 2551); // Hole replaced
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

    // Test that moving the barrier with a new (higher) generation removes all
    // records that belong to older generations, even if their LSNs are above
    // the new barrier LSN.
    //
    // Scenario:
    //   1. Tablet writes several records with generation 1.
    //   2. Tablet bumps to generation 2 and writes a few more records.
    //   3. Tablet calls TEvErasePersistentBuffer with generation 2 and lsn=0
    //      (barrier at the very beginning of gen-2).
    //   4. All gen-1 records must be gone; gen-2 records must survive.
    //   5. Verify via ListPB and GetPBInfo.
    Y_UNIT_TEST(PersistentBufferGenerationChangeBarrier) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);

        // Write 5 records with generation 1 (tabletIdx=0, default gen=1).
        const ui32 tabletIdx = 0;
        std::vector<ui64> gen1Lsns;
        for (ui32 i = 0; i < 5; ++i) {
            gen1Lsns.push_back(f.WritePBWithGeneration(tabletIdx, /*generation=*/1, /*offset=*/0, /*numBlocks=*/4));
        }

        // Verify all 5 gen-1 records are present.
        {
            auto info = f.GetPBInfo(false, true);
            bool found = false;
            for (auto& ti : info->Get()->TabletInfos) {
                if (ti.TabletId == f.PBCreds[tabletIdx].TabletId && ti.Generation == 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(ti.LsnsCount, 5u,
                        "Expected 5 gen-1 records before generation bump");
                    found = true;
                }
            }
            UNIT_ASSERT_C(found, "Gen-1 tablet info not found before generation bump");
        }

        // Bump to generation 2 and write 3 more records.
        f.BumpGeneration(tabletIdx, /*newGeneration=*/2);
        std::vector<ui64> gen2Lsns;
        for (ui32 i = 0; i < 3; ++i) {
            gen2Lsns.push_back(f.WritePBWithGeneration(tabletIdx, /*generation=*/2, /*offset=*/0, /*numBlocks=*/4));
        }

        // Verify both generations are present.
        {
            auto info = f.GetPBInfo(false, true);
            ui32 gen1Count = 0, gen2Count = 0;
            for (auto& ti : info->Get()->TabletInfos) {
                if (ti.TabletId == f.PBCreds[tabletIdx].TabletId) {
                    if (ti.Generation == 1) gen1Count = ti.LsnsCount;
                    if (ti.Generation == 2) gen2Count = ti.LsnsCount;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(gen1Count, 5u, "Expected 5 gen-1 records before barrier move");
            UNIT_ASSERT_VALUES_EQUAL_C(gen2Count, 3u, "Expected 3 gen-2 records before barrier move");
        }

        // Move barrier with generation 2, lsn=0.
        // This must delete ALL gen-1 records (older generation) and keep gen-2 records.
        f.MoveBarrier(/*lsn=*/0, tabletIdx);

        // Verify: gen-1 records are gone, gen-2 records survive.
        {
            auto info = f.GetPBInfo(false, true);
            ui32 gen1Count = 0, gen2Count = 0;
            for (auto& ti : info->Get()->TabletInfos) {
                if (ti.TabletId == f.PBCreds[tabletIdx].TabletId) {
                    if (ti.Generation == 1) gen1Count = ti.LsnsCount;
                    if (ti.Generation == 2) gen2Count = ti.LsnsCount;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(gen1Count, 0u,
                "All gen-1 records must be deleted after barrier move with gen-2");
            UNIT_ASSERT_VALUES_EQUAL_C(gen2Count, 3u,
                "Gen-2 records must survive barrier move with lsn=0");
        }

        // Verify via ListPB: only gen-2 LSNs should be listed.
        {
            f.Env.Runtime->Send(new IEventHandle(f.PBServiceId, f.Edge,
                new NDDisk::TEvListPersistentBuffer(f.PBCreds[tabletIdx])), f.Edge.NodeId());
            auto res = f.Env.WaitForEdgeActorEvent<NDDisk::TEvListPersistentBufferResult>(f.Edge, false);
            const auto& rr = res->Get()->Record;
            UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

            THashSet<ui64> listedLsns;
            for (const auto& item : rr.GetRecords()) {
                listedLsns.insert(item.GetLsn());
            }
            // None of the gen-1 LSNs should appear.
            for (ui64 lsn : gen1Lsns) {
                UNIT_ASSERT_C(!listedLsns.contains(lsn),
                    TStringBuilder() << "Gen-1 lsn# " << lsn << " must not appear after barrier move");
            }
            // All gen-2 LSNs must appear.
            for (ui64 lsn : gen2Lsns) {
                UNIT_ASSERT_C(listedLsns.contains(lsn),
                    TStringBuilder() << "Gen-2 lsn# " << lsn << " must appear after barrier move");
            }
        }

        // Restart and verify the same invariant holds after recovery.
        f.RestartNode();

        {
            auto info = f.GetPBInfo(false, true);
            ui32 gen1Count = 0, gen2Count = 0;
            for (auto& ti : info->Get()->TabletInfos) {
                if (ti.TabletId == f.PBCreds[tabletIdx].TabletId) {
                    if (ti.Generation == 1) gen1Count = ti.LsnsCount;
                    if (ti.Generation == 2) gen2Count = ti.LsnsCount;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(gen1Count, 0u,
                "Gen-1 records must still be absent after restart");
            UNIT_ASSERT_VALUES_EQUAL_C(gen2Count, 3u,
                "Gen-2 records must survive restart");
        }
    }

    // Test that a BatchErase request containing records from multiple generations
    // does NOT use the fast-erase path.
    //
    // Fast erase writes a single sector that lists all erased LSNs.  Regular
    // erase zeroes the header sector of every erased record individually.
    // Therefore:
    //   - Fast erase of N records: frees N*recordSectors - 1 sector
    //     (new fast-erase sector costs 1, records freed = N*recordSectors).
    //   - Regular erase of N records: frees exactly N*recordSectors sectors
    //     (no extra sector allocated or freed).
    //
    // We verify the sector accounting to distinguish the two paths.
    //
    // Setup:
    //   1. Barrier at gen-1 (initial MoveBarrier).
    //   2. Write 2 records with gen-1.
    //   3. Bump to gen-2, write 2 more records with gen-2.
    //   4. Batch-erase all 4 records in one call (creds gen-2, erases mix gen-1
    //      and gen-2).  Because some erases have generation != creds.Generation,
    //      canFastErase is forced to false → regular erase path.
    //   5. Verify freed sectors == 4 * sectorsCntPerRecord (no fast-erase overhead).
    //
    //   Then move barrier to gen-2 and verify that a same-generation batch erase
    //   DOES use fast erase (freed == 2*sectorsCntPerRecord - 1).
    Y_UNIT_TEST(PersistentBufferBatchEraseMultiGenerationNoFastErase) {
        TDDiskTestContext f(1_MB);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);

        // Establish barrier at gen-1 lsn=0.
        f.MoveBarrier(/*lsn=*/0, /*tabletIdx=*/0);

        const ui32 tabletIdx = 0;
        // Each record: 4 data blocks (4096 B each) + 1 header sector = 5 sectors.
        const ui32 sectorsCntPerRecord = 5;

        // Write 2 records with generation 1.
        ui64 lsn1 = f.WritePBWithGeneration(tabletIdx, /*generation=*/1, /*offset=*/0, /*numBlocks=*/4);
        ui64 lsn2 = f.WritePBWithGeneration(tabletIdx, /*generation=*/1, /*offset=*/0, /*numBlocks=*/4);

        // Bump to generation 2 and write 2 more records.
        // NOTE: we do NOT call MoveBarrier here – the barrier stays at gen-1.
        // This keeps the gen-1 records alive so we can include them in the
        // mixed-generation batch erase below.
        f.BumpGeneration(tabletIdx, /*newGeneration=*/2);
        ui64 lsn3 = f.WritePBWithGeneration(tabletIdx, /*generation=*/2, /*offset=*/0, /*numBlocks=*/4);
        ui64 lsn4 = f.WritePBWithGeneration(tabletIdx, /*generation=*/2, /*offset=*/0, /*numBlocks=*/4);

        // Snapshot free sectors before the mixed-generation batch erase.
        auto infoBeforeErase = f.GetPBInfo(false, true);
        ui64 freeSectorsBefore = infoBeforeErase->Get()->FreeSectors;

        // Batch-erase records from BOTH generations in a single call.
        // creds.Generation == 2, but erases include gen-1 entries.
        // The handler sets canFastErase = false when any erase.generation !=
        // creds.Generation, so the regular (slow) erase path is taken.
        std::vector<std::tuple<ui64, ui32>> erases = {
            {lsn1, 1u},
            {lsn2, 1u},
            {lsn3, 2u},
            {lsn4, 2u},
        };
        f.BatchEraseMultiGen(tabletIdx, erases);

        auto infoAfterErase = f.GetPBInfo(false, true);
        ui64 freeSectorsAfter = infoAfterErase->Get()->FreeSectors;

        // Regular erase: each record's header sector is zeroed and all of the
        // record's sectors are freed.  No extra "fast-erase" sector is allocated.
        // Expected freed = 4 records * 5 sectors/record = 20 sectors.
        ui64 expectedFreed = 4 * sectorsCntPerRecord;
        UNIT_ASSERT_VALUES_EQUAL_C(freeSectorsAfter - freeSectorsBefore, expectedFreed,
            TStringBuilder() << "Regular erase must free exactly " << expectedFreed
                << " sectors; freeBefore=" << freeSectorsBefore
                << " freeAfter=" << freeSectorsAfter);

        // Fast-erase count must be 0 for this tablet (no fast erases happened).
        for (auto& ti : infoAfterErase->Get()->TabletInfos) {
            if (ti.TabletId == f.PBCreds[tabletIdx].TabletId) {
                UNIT_ASSERT_VALUES_EQUAL_C(ti.FastErasesCount, 0u,
                    "No fast erases must have occurred for a mixed-generation batch erase");
            }
        }

        // ---------------------------------------------------------------
        // Contrast: same-generation batch erase DOES use fast erase.
        // Move barrier to gen-2 (no gen-1 records remain, so nothing is
        // deleted by the barrier move itself).
        f.MoveBarrier(/*lsn=*/0, tabletIdx);

        // Write 2 new gen-2 records.
        ui64 lsn5 = f.WritePBWithGeneration(tabletIdx, /*generation=*/2, /*offset=*/0, /*numBlocks=*/4);
        ui64 lsn6 = f.WritePBWithGeneration(tabletIdx, /*generation=*/2, /*offset=*/0, /*numBlocks=*/4);

        auto infoBeforeFastErase = f.GetPBInfo(false, true);
        ui64 freeSectorsBeforeFastErase = infoBeforeFastErase->Get()->FreeSectors;

        // Erase both gen-2 records in one call – all erases have the same
        // generation as creds, so fast erase is used.
        std::vector<std::tuple<ui64, ui32>> sameGenErases = {
            {lsn5, 2u},
            {lsn6, 2u},
        };
        f.BatchEraseMultiGen(tabletIdx, sameGenErases);

        auto infoAfterFastErase = f.GetPBInfo(false, true);
        ui64 freeSectorsAfterFastErase = infoAfterFastErase->Get()->FreeSectors;

        // Fast erase: 2 records freed (2 * 5 = 10 sectors) minus 1 sector
        // consumed by the new fast-erase sector (no old fast-erase sector to
        // reclaim on the first fast erase).  Net freed = 10 - 1 = 9.
        ui64 expectedFastFreed = 2 * sectorsCntPerRecord - 1;
        UNIT_ASSERT_VALUES_EQUAL_C(freeSectorsAfterFastErase - freeSectorsBeforeFastErase, expectedFastFreed,
            TStringBuilder() << "Fast erase must free exactly " << expectedFastFreed
                << " sectors; freeBefore=" << freeSectorsBeforeFastErase
                << " freeAfter=" << freeSectorsAfterFastErase);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Integration test for TDDiskActor::Handle(TEvPrivate::TEvDeallocatePersistentBufferChunk)
    //
    // This test drives the persistent buffer through a real actor system (with a
    // mock PDisk) to verify that a chunk which is proactively allocated and later
    // becomes fully free is actually deallocated: the chunk-map log record commits
    // with the chunk index in DeleteChunks, and PDisk (the mock) releases the
    // physical chunk back to its free pool, making it available for reuse.
    //
    // Strategy:
    //  1. Configure aggressive proactive allocation (PreallocateFreeSpaceThresholdPercent=99)
    //     so writing enough records forces growth beyond the initial owned-chunk count.
    //  2. Configure aggressive proactive deallocation (DeallocateFreeSpaceThresholdPercent=90,
    //     DeallocateThresholdSeconds=1) so that once all data is erased, the extra chunk(s)
    //     are quickly detected as fully free and deallocated.
    //  3. Verify the owned-chunk count (via TEvGetPersistentBufferInfo's FreeSpace
    //     description, whose size equals TPersistentBufferSpaceAllocator::OwnedChunks.size())
    //     grows after the writes and shrinks back down after the erases + wait.
    //  4. Independently verify at the (mock) PDisk level that the physical chunk which
    //     was written to disappears from the PDisk's set of chunks with data, proving
    //     the chunk was genuinely returned to PDisk and not just dropped from DDisk's
    //     own bookkeeping.
    // ─────────────────────────────────────────────────────────────────────────
    Y_UNIT_TEST(PersistentBufferChunkDeallocationReturnsChunkToPDisk) {
        TDDiskTestContext f(1_MB, 128_MB, /*minFreeSectorsReserve=*/std::nullopt,
            /*preallocateFreeSpaceThresholdPercent=*/99,
            /*deallocateFreeSpaceThresholdPercent=*/90,
            /*deallocateThresholdSeconds=*/1);
        auto groups = f.AllocateDDiskBlockGroup();
        auto& node = groups.begin()->GetNodes(0);
        f.ChangeTestingNode(node);
        f.MoveBarrier(0, 0);

        auto* pdiskState = f.Env.PDiskMockStates[{f.PersId.GetNodeId(), f.PersId.GetPDiskId()}].Get();
        UNIT_ASSERT_C(pdiskState, "PDisk mock state must exist for the persistent buffer's PDisk");

        // Baseline: number of chunks owned by the PB space allocator (should be
        // exactly PersistentBufferInitChunks == 4, matching MaxChunks=10 config).
        auto describeFreeSpace = [&] {
            return f.GetPBInfo(true, false)->Get()->FreeSpace.size();
        };
        const size_t initialOwnedChunks = describeFreeSpace();
        UNIT_ASSERT_VALUES_EQUAL(initialOwnedChunks, 4u);

        // Write enough 128-block records to push free space below the 99% threshold
        // of the currently-owned capacity, forcing proactive allocation of an extra
        // (5th) chunk. Each write occupies 129 sectors (128 data + 1 header); with
        // 4 chunks * 32768 sectors = 131072 total, threshold ~= 129761 sectors, so
        // ~12 writes (1548 sectors) push free space below the threshold.
        std::vector<ui64> lsns;
        for (ui32 i = 0; i < 20; ++i) {
            const ui64 lsn = f.NextLsn;
            f.WritePB(0, 128);
            lsns.push_back(lsn);
        }
        f.Env.Sim(TDuration::Seconds(5));

        const size_t ownedChunksAfterWrites = describeFreeSpace();
        UNIT_ASSERT_C(ownedChunksAfterWrites > initialOwnedChunks,
            TStringBuilder() << "Expected proactive allocation to grow owned chunks, got "
                << ownedChunksAfterWrites << " (was " << initialOwnedChunks << ")");

        // Capture the set of chunks with actual data at the mock PDisk level: this
        // must include the extra, proactively-allocated chunk(s) since we wrote
        // enough data to spill onto them.
        const std::set<ui32> chunksAfterWrites = pdiskState->GetChunks();

        // Erase every record we wrote: this frees all occupied sectors and, via
        // ClearPersistentBufferRecords -> ProcessDeallocatePersistentBufferChunk,
        // triggers the round-robin lock/deallocate cycle implemented in
        // TDDiskActor::Handle(TEvPrivate::TEvDeallocatePersistentBufferChunk).
        f.ErasePB(lsns.back(), 0);

        // Give the round-robin deallocation (1 chunk per DeallocateThresholdSeconds)
        // enough simulated time to walk through every owned chunk and release the
        // ones that end up fully free.
        f.Env.Sim(TDuration::Seconds(30));

        const size_t ownedChunksAfterDeallocation = describeFreeSpace();
        UNIT_ASSERT_VALUES_EQUAL_C(ownedChunksAfterDeallocation, initialOwnedChunks,
            TStringBuilder() << "Expected owned chunks to shrink back to the initial count "
                << initialOwnedChunks << " after erasing all data and waiting for deallocation, got "
                << ownedChunksAfterDeallocation);

        // Verify at the PDisk mock level that at least one chunk which had data
        // written to it during the growth phase is now gone (i.e. actually
        // released back to PDisk's free pool), not merely dropped from DDisk's
        // internal PersistentBufferChunks bookkeeping.
        const std::set<ui32> chunksAfterDeallocation = pdiskState->GetChunks();
        UNIT_ASSERT_C(chunksAfterDeallocation.size() < chunksAfterWrites.size(),
            TStringBuilder() << "Expected fewer chunks with data at the PDisk level after deallocation: before="
                << chunksAfterWrites.size() << " after=" << chunksAfterDeallocation.size());

        bool foundReleasedChunk = false;
        for (ui32 chunkIdx : chunksAfterWrites) {
            if (!chunksAfterDeallocation.contains(chunkIdx)) {
                foundReleasedChunk = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundReleasedChunk,
            "At least one chunk that held persistent-buffer data must have been released back to PDisk");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests for TDirectBlockGroupOperation::DeletePersistentBuffers and
    // TDirectBlockGroupOperation::DeleteDDisks (ddisk.cpp handling of
    // op.GetDeletePersistentBuffers() / op.GetDeleteDDisks()).
    // ─────────────────────────────────────────────────────────────────────────

    Y_UNIT_TEST(DeletePersistentBufferRemovesEntry) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/2);
        UNIT_ASSERT_VALUES_EQUAL(group.DDiskIdSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(group.PersistentBufferDDiskIdSize(), 2);

        const auto pbToDelete = group.GetPersistentBufferDDiskId(0);

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            auto *cmd = op->AddDeletePersistentBuffers();
            cmd->MutablePersistentBufferId()->CopyFrom(pbToDelete);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr.GetStatus(), NKikimrProto::OK, rr.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(rr.DirectBlockGroupsSize(), 1);

        const auto& updated = rr.GetDirectBlockGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(updated.PersistentBufferDDiskIdSize(), 1);
        // the remaining persistent buffer must be the one we did NOT delete
        UNIT_ASSERT(TDDiskId(updated.GetPersistentBufferDDiskId(0)) == TDDiskId(group.GetPersistentBufferDDiskId(1)));
        // DDisks themselves are untouched by persistent buffer deletion
        UNIT_ASSERT_VALUES_EQUAL(updated.DDiskIdSize(), 2);
    }

    Y_UNIT_TEST(DeletePersistentBufferNotFoundReturnsNotFound) {
        TDDiskTestContext f;
        f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            auto *cmd = op->AddDeletePersistentBuffers();
            // bogus, never-allocated identifier
            cmd->MutablePersistentBufferId()->SetNodeId(999);
            cmd->MutablePersistentBufferId()->SetPDiskId(999);
            cmd->MutablePersistentBufferId()->SetDDiskSlotId(999);
        });
        // A missing PersistentBuffer must be reported as NOT_FOUND, not a generic ERROR,
        // so that callers can distinguish "target doesn't exist" from other failures.
        UNIT_ASSERT_VALUES_EQUAL(rr.GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(rr.GetErrorReason(), "PersistentBuffer not found");
    }

    Y_UNIT_TEST(DeleteDDiskRemovesEntry) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/3,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);
        UNIT_ASSERT_VALUES_EQUAL(group.DDiskIdSize(), 3);

        const auto ddiskToDelete = group.GetDDiskId(2); // delete the last one so trimming kicks in

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            auto *cmd = op->AddDeleteDDisks();
            cmd->MutableDDiskId()->CopyFrom(ddiskToDelete);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr.GetStatus(), NKikimrProto::OK, rr.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(rr.DirectBlockGroupsSize(), 1);

        const auto& updated = rr.GetDirectBlockGroups(0);
        // the trailing empty item gets trimmed away entirely
        UNIT_ASSERT_VALUES_EQUAL(updated.DDiskIdSize(), 2);
        for (ui32 i = 0; i < 2; ++i) {
            UNIT_ASSERT(TDDiskId(updated.GetDDiskId(i)) == TDDiskId(group.GetDDiskId(i)));
        }
        // persistent buffers are untouched by DDisk deletion
        UNIT_ASSERT_VALUES_EQUAL(updated.PersistentBufferDDiskIdSize(), 1);
    }

    Y_UNIT_TEST(DeleteDDiskNotFoundReturnsNotFound) {
        TDDiskTestContext f;
        f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            auto *cmd = op->AddDeleteDDisks();
            cmd->MutableDDiskId()->SetNodeId(999);
            cmd->MutableDDiskId()->SetPDiskId(999);
            cmd->MutableDDiskId()->SetDDiskSlotId(999);
        });
        // A missing DDisk must be reported as NOT_FOUND, not a generic ERROR,
        // so that callers can distinguish "target doesn't exist" from other failures.
        UNIT_ASSERT_VALUES_EQUAL(rr.GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(rr.GetErrorReason(), "DDisk not found");
    }

    Y_UNIT_TEST(DeleteMiddleDDiskRemovesEntry) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/3,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);
        UNIT_ASSERT_VALUES_EQUAL(group.DDiskIdSize(), 3);

        const auto ddiskToDelete = group.GetDDiskId(1); // middle one

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            auto *cmd = op->AddDeleteDDisks();
            cmd->MutableDDiskId()->CopyFrom(ddiskToDelete);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr.GetStatus(), NKikimrProto::OK, rr.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(rr.DirectBlockGroupsSize(), 1);

        const auto& updated = rr.GetDirectBlockGroups(0);
        // the deleted middle entry must be removed from the list entirely --
        // no hole is left behind, and the remaining entries keep their relative order
        UNIT_ASSERT_VALUES_EQUAL(updated.DDiskIdSize(), 2);
        UNIT_ASSERT(TDDiskId(updated.GetDDiskId(0)) == TDDiskId(group.GetDDiskId(0)));
        UNIT_ASSERT(TDDiskId(updated.GetDDiskId(1)) == TDDiskId(group.GetDDiskId(2)));
        // persistent buffers are untouched by DDisk deletion
        UNIT_ASSERT_VALUES_EQUAL(updated.PersistentBufferDDiskIdSize(), 1);
    }

    Y_UNIT_TEST(DeleteAllPersistentBuffersAndDDisks) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/2);

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            for (const auto& pb : group.GetPersistentBufferDDiskId()) {
                op->AddDeletePersistentBuffers()->MutablePersistentBufferId()->CopyFrom(pb);
            }
            for (const auto& dd : group.GetDDiskId()) {
                op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(dd);
            }
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr.GetStatus(), NKikimrProto::OK, rr.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(rr.DirectBlockGroupsSize(), 1);

        const auto& updated = rr.GetDirectBlockGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(updated.PersistentBufferDDiskIdSize(), 0);
        // both DDisks removed and the last one had zero claim -> full trim
        UNIT_ASSERT_VALUES_EQUAL(updated.DDiskIdSize(), 0);
    }

    // Re-deleting a PersistentBuffer that was already removed by a prior operation
    // must also be reported as NOT_FOUND (not a stale/cached OK, and not a generic
    // ERROR), confirming the status is derived from actual current state each time.
    Y_UNIT_TEST(DeletePersistentBufferTwiceReturnsNotFoundOnSecondAttempt) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/1,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);
        const auto pb = group.GetPersistentBufferDDiskId(0);

        auto rr1 = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            op->AddDeletePersistentBuffers()->MutablePersistentBufferId()->CopyFrom(pb);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr1.GetStatus(), NKikimrProto::OK, rr1.GetErrorReason());

        auto rr2 = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            op->AddDeletePersistentBuffers()->MutablePersistentBufferId()->CopyFrom(pb);
        });
        UNIT_ASSERT_VALUES_EQUAL(rr2.GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(rr2.GetErrorReason(), "PersistentBuffer not found");
    }

    // Same as above, but for DeleteDDisks: deleting an already-deleted DDisk a
    // second time must be reported as NOT_FOUND.
    Y_UNIT_TEST(DeleteDDiskTwiceReturnsNotFoundOnSecondAttempt) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);
        const auto ddisk = group.GetDDiskId(1);

        auto rr1 = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(ddisk);
        });
        UNIT_ASSERT_VALUES_EQUAL_C(rr1.GetStatus(), NKikimrProto::OK, rr1.GetErrorReason());

        auto rr2 = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(ddisk);
        });
        UNIT_ASSERT_VALUES_EQUAL(rr2.GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(rr2.GetErrorReason(), "DDisk not found");
    }

    // A single request mixing a not-found PersistentBuffer delete together with a
    // valid DDisk delete must still surface NOT_FOUND for the whole operation
    // (the exception aborts the entire DirectBlockGroupOperations loop for this
    // transaction, so no partial changes are committed).
    Y_UNIT_TEST(DeleteWithMixedNotFoundAndValidTargetsReturnsNotFound) {
        TDDiskTestContext f;
        auto group = f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/2,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);
        const auto validDDisk = group.GetDDiskId(0);

        auto rr = f.SendDirectBlockGroupOperation(1, [&](auto *op) {
            // valid delete, would succeed on its own
            op->AddDeleteDDisks()->MutableDDiskId()->CopyFrom(validDDisk);
            // bogus, never-allocated PersistentBuffer identifier
            auto *cmd = op->AddDeletePersistentBuffers();
            cmd->MutablePersistentBufferId()->SetNodeId(999);
            cmd->MutablePersistentBufferId()->SetPDiskId(999);
            cmd->MutablePersistentBufferId()->SetDDiskSlotId(999);
        });
        UNIT_ASSERT_VALUES_EQUAL(rr.GetStatus(), NKikimrProto::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(rr.GetErrorReason(), "PersistentBuffer not found");

        // Verify nothing was actually committed: querying the group again must
        // show both DDisks and the PersistentBuffer still present.
        auto rr2 = f.SendDirectBlockGroupOperation(1, [&](auto*) {});
        UNIT_ASSERT_VALUES_EQUAL_C(rr2.GetStatus(), NKikimrProto::OK, rr2.GetErrorReason());
        const auto& unchanged = rr2.GetDirectBlockGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.DDiskIdSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.PersistentBufferDDiskIdSize(), 1);
    }

    // Errors that are NOT "not found" (e.g. combining incompatible Queries and
    // DirectBlockGroupOperations in the same request) must remain plain ERROR,
    // not be misreported as NOT_FOUND.
    Y_UNIT_TEST(NonNotFoundErrorStillReturnsError) {
        TDDiskTestContext f;
        f.DefineDirectBlockGroup(/*directBlockGroupId=*/1, /*numDDisks=*/1,
            /*numChunksPerDDisk=*/1, /*numPersistentBuffers=*/1);

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
        auto& r = ev->Record;
        r.SetDDiskPoolName("ddisk_pool");
        r.SetPersistentBufferDDiskPoolName("ddisk_pool");
        r.SetTabletId(1);
        // Queries and DirectBlockGroupOperations together are rejected up-front
        // with a plain ERROR (see Execute() in ddisk.cpp), well before any
        // not-found checks are reached.
        auto *q = r.AddQueries();
        q->SetDirectBlockGroupId(2);
        q->SetTargetNumVChunks(1);
        auto *op = r.AddDirectBlockGroupOperations();
        op->SetDirectBlockGroupId(1);
        op->AddDeleteDDisks(); // deliberately empty/bogus, shouldn't even be reached

        TActorId edge = f.Env.Runtime->AllocateEdgeActor(f.Env.Settings.ControllerNodeId, __FILE__, __LINE__);
        f.Env.Runtime->SendToPipe(MakeBSControllerID(), edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = f.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(edge);
        auto& rr = response->Get()->Record;

        UNIT_ASSERT_VALUES_EQUAL(rr.GetStatus(), NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(rr.GetErrorReason(), "can't be provided at the same time");
    }
}
