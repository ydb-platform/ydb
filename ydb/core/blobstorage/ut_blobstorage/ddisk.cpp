#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>

Y_UNIT_TEST_SUITE(DDisk) {

    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        env.Sim(TDuration::Seconds(30));

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
            auto res = env.Invoke(request);
            UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
        }

        {
            const TActorId& edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
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
            runtime->SendToPipe(MakeBSControllerID(), edge, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
            auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult>(edge);
            auto& rr = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(rr.ResponsesSize(), 8);

            ui64 vChunkIndex = 0;
            ui64 nextLsn = 1;
            int letterIndex = 0;
            const TString letters = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

            for (auto& item : rr.GetResponses()) {
                for (auto& node : item.GetNodes()) {
                    UNIT_ASSERT(node.HasDDiskId());
                    UNIT_ASSERT(node.HasPersistentBufferDDiskId());

                    const auto& ddiskId = node.GetDDiskId();
                    auto serviceId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());

                    const auto& persId = node.GetPersistentBufferDDiskId();
                    auto pbServiceId = MakeBlobStorageDDiskId(persId.GetNodeId(), persId.GetPDiskId(), persId.GetDDiskSlotId());

                    auto edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);

                    // greet ddisk
                    NDDisk::TQueryCredentials creds;
                    creds.TabletId = 1;
                    creds.Generation = 1;
                    {
                        runtime->Send(new IEventHandle(serviceId, edge, new NDDisk::TEvConnect(creds)), edge.NodeId());
                        auto res = env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(edge, false);
                        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                        creds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
                    }

                    NDDisk::TQueryCredentials pbCreds = creds;
                    {
                        runtime->Send(new IEventHandle(pbServiceId, edge, new NDDisk::TEvConnect(pbCreds)), edge.NodeId());
                        auto res = env.WaitForEdgeActorEvent<NDDisk::TEvConnectResult>(edge, false);
                        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                        pbCreds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
                    }

                    const ui32 blockSize = 4096;
                    TString surface = TString::Uninitialized(64_KB);
                    memset(surface.Detach(), 0, surface.size());
                    const ui32 surfaceBlocks = surface.size() / blockSize;

                    std::map<ui64, std::tuple<ui32, ui32, TString>> persistentBuffers;

                    Cerr << "next iteration\n";

                    for (ui32 iter = 0; iter < 1000; ++iter) {
                        switch (RandomNumber(6u)) {
                            case 0: {
                                const ui32 offset = RandomNumber(surfaceBlocks) * blockSize;
                                const ui32 numBlocks = 1 + RandomNumber(Min<ui32>(3, surfaceBlocks - offset / blockSize));
                                const ui32 size = numBlocks * blockSize;
                                UNIT_ASSERT(offset < surface.size() && offset + size <= surface.size());

                                TString update = TString::Uninitialized(size);
                                char letter = letters[letterIndex++ % letters.size()];
                                memset(update.Detach(), letter, update.size());

                                memcpy(surface.Detach() + offset, update.data(), update.size());

                                Cerr << "write offset# " << offset << " size# " << size << " letter# " << letter << "\n";

                                std::unique_ptr<NDDisk::TEvWrite> ev(new NDDisk::TEvWrite(creds,
                                    {vChunkIndex, offset, size}, {0}));
                                ev->AddPayload(TRope(std::move(update)));
                                runtime->Send(new IEventHandle(serviceId, edge, ev.release()), edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvWriteResult>(edge, false);

                                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                                break;
                            }

                            case 1: {
                                const ui32 offset = RandomNumber(surfaceBlocks) * blockSize;
                                const ui32 numBlocks = 1 + RandomNumber(Min<ui32>(3, surfaceBlocks - offset / blockSize));
                                const ui32 size = numBlocks * blockSize;
                                UNIT_ASSERT(offset < surface.size() && offset + size <= surface.size());

                                Cerr << "read offset# " << offset << " size# " << size << "\n";

                                runtime->Send(new IEventHandle(serviceId, edge, new NDDisk::TEvRead(creds,
                                    {vChunkIndex, offset, size}, {true})), edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvReadResult>(edge, false);

                                const auto& rr = res->Get()->Record;
                                UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                                UNIT_ASSERT(rr.HasReadResult());
                                const auto& rr2 = rr.GetReadResult();
                                UNIT_ASSERT(rr2.HasPayloadId());
                                UNIT_ASSERT_VALUES_EQUAL(rr2.GetPayloadId(), 0);
                                TRope rope = res->Get()->GetPayload(0);
                                UNIT_ASSERT_VALUES_EQUAL(rope.size(), size);
                                UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), surface.substr(offset, size));
                                break;
                            }

                            case 2: {
                                runtime->Send(new IEventHandle(pbServiceId, edge, new NDDisk::TEvListPersistentBuffer(
                                    pbCreds)), edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvListPersistentBufferResult>(edge, false);
                                const auto& rr = res->Get()->Record;
                                UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

                                Cerr << "list\n";

                                THashSet<std::tuple<ui64, ui32, ui32>> returnedLsns;
                                for (const auto& item : rr.GetRecords()) {
                                    const auto& sel = item.GetSelector();
                                    if (sel.GetVChunkIndex() != vChunkIndex) {
                                        continue;
                                    }
                                    returnedLsns.emplace(item.GetLsn(), sel.GetOffsetInBytes(), sel.GetSize());
                                }

                                THashSet<std::tuple<ui64, ui32, ui32>> ourLsns;
                                for (const auto& [lsn, item] : persistentBuffers) {
                                    const auto& [offsetInBytes, size, buffer] = item;
                                    ourLsns.emplace(lsn, offsetInBytes, size);
                                }

                                UNIT_ASSERT_EQUAL(returnedLsns, ourLsns);

                                break;
                            }

                            case 3: {
                                const ui32 offset = RandomNumber(surfaceBlocks) * blockSize;
                                const ui32 numBlocks = 1 + RandomNumber(Min<ui32>(3, surfaceBlocks - offset / blockSize));
                                const ui32 size = numBlocks * blockSize;
                                UNIT_ASSERT(offset < surface.size() && offset + size <= surface.size());

                                const ui64 lsn = nextLsn++;
                                TString update = TString::Uninitialized(size);
                                char letter = letters[letterIndex++ % letters.size()];
                                memset(update.Detach(), letter, update.size());
                                persistentBuffers.emplace(lsn, std::make_tuple(offset, size, update));

                                Cerr << "write persistent buffer offset# " << offset << " size# " << size << " lsn# " << lsn
                                    << " letter# " << letter << "\n";

                                std::unique_ptr<NDDisk::TEvWritePersistentBuffer> ev(new NDDisk::TEvWritePersistentBuffer(
                                    pbCreds, {vChunkIndex, offset, size}, lsn, {0}));
                                ev->AddPayload(TRope(std::move(update)));
                                runtime->Send(new IEventHandle(pbServiceId, edge, ev.release()), edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvWritePersistentBufferResult>(edge, false);
                                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                                break;
                            }

                            case 4: {
                                std::vector<ui64> lsns;
                                lsns.reserve(persistentBuffers.size());
                                for (ui64 lsn : persistentBuffers | std::views::keys) {
                                    lsns.push_back(lsn);
                                }
                                if (lsns.empty()) {
                                    break;
                                }
                                const size_t index = RandomNumber(lsns.size());
                                const ui64 lsn = lsns[index];
                                const auto& [offsetInBytes, size, buffer] = persistentBuffers.at(lsn);

                                Cerr << "read persistent buffer offset# " << offsetInBytes << " size# " << size
                                    << " lsn# " << lsn << "\n";

                                runtime->Send(new IEventHandle(pbServiceId, edge, new NDDisk::TEvReadPersistentBuffer(
                                    pbCreds, {vChunkIndex, offsetInBytes, size}, lsn, {true})), edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvReadPersistentBufferResult>(edge, false);
                                const auto& rr = res->Get()->Record;
                                UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                                UNIT_ASSERT(rr.HasReadResult());
                                const auto& rr2 = rr.GetReadResult();
                                UNIT_ASSERT(rr2.HasPayloadId());
                                UNIT_ASSERT_VALUES_EQUAL(rr2.GetPayloadId(), 0);
                                TRope rope = res->Get()->GetPayload(0);
                                UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), buffer);
                                break;
                            }

                            case 5: {
                                std::vector<ui64> lsns;
                                lsns.reserve(persistentBuffers.size());
                                for (ui64 lsn : persistentBuffers | std::views::keys) {
                                    lsns.push_back(lsn);
                                }
                                if (lsns.empty()) {
                                    break;
                                }
                                const size_t index = RandomNumber(lsns.size());
                                const ui64 lsn = lsns[index];
                                const auto& [offsetInBytes, size, buffer] = persistentBuffers.at(lsn);
                                const bool doCommit = RandomNumber(2u);

                                std::optional<std::tuple<ui32, ui32, ui32>> targetDDiskId;
                                std::optional<ui64> ddiskInstanceGuid;
                                if (doCommit) {
                                    memcpy(surface.Detach() + offsetInBytes, buffer.data(), size);
                                    targetDDiskId.emplace(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
                                    ddiskInstanceGuid.emplace(*creds.DDiskInstanceGuid);
                                }

                                Cerr << "flush persistent buffer offset# " << offsetInBytes << " size# " << size
                                    << " lsn# " << lsn << " doCommit# " << doCommit << "\n";

                                runtime->Send(new IEventHandle(pbServiceId, edge, new NDDisk::TEvFlushPersistentBuffer(
                                    pbCreds, {vChunkIndex, offsetInBytes, size}, lsn, targetDDiskId, ddiskInstanceGuid)),
                                    edge.NodeId());
                                auto res = env.WaitForEdgeActorEvent<NDDisk::TEvFlushPersistentBufferResult>(edge, false);
                                UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

                                persistentBuffers.erase(lsn);

                                break;
                            }
                        }
                    }

                    ++vChunkIndex;
                }
            }
        }
    }

}
