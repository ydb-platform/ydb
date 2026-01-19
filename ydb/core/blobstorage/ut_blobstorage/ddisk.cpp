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
            for (auto& item : rr.GetResponses()) {
                for (auto& node : item.GetNodes()) {
                    UNIT_ASSERT(node.HasDDiskId());
                    const auto& ddiskId = node.GetDDiskId();
                    auto serviceId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
                    auto edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);

                    // greet ddisk
                    NDDisk::TQueryCredentials creds;
                    creds.TabletId = 1;
                    creds.Generation = 1;
                    {
                        runtime->Send(new IEventHandle(serviceId, edge, new NDDisk::TEvDDiskConnect(creds)), edge.NodeId());
                        auto res = env.WaitForEdgeActorEvent<NDDisk::TEvDDiskConnectResult>(edge, false);
                        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK);
                        creds.DDiskInstanceGuid = res->Get()->Record.GetDDiskInstanceGuid();
                    }

                    // write some data
                    const ui32 blockSize = 4096;
                    TString buffer;
                    {
                        buffer = TString::Uninitialized(blockSize * 4);
                        std::unique_ptr<NDDisk::TEvDDiskWrite> ev(new NDDisk::TEvDDiskWrite(creds, {0, 0,
                            static_cast<ui32>(buffer.size())}, {0}));
                        memset(buffer.Detach(), 'X', buffer.size());
                        memset(buffer.Detach() + blockSize, 'Y', buffer.size());
                        memset(buffer.Detach() + blockSize * 2, 'Z', buffer.size());
                        memset(buffer.Detach() + blockSize * 3, 'A', buffer.size());
                        ev->AddPayload(TRope(std::move(buffer)));
                        runtime->Send(new IEventHandle(serviceId, edge, ev.release()), edge.NodeId());
                        auto res = env.WaitForEdgeActorEvent<NDDisk::TEvDDiskWriteResult>(edge, false);
                        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK);
                    }

                    // read some data back
                    for (ui32 i = 0; i + 2 <= buffer.size() / blockSize; ++i) {
                        const ui32 offset = i * blockSize;
                        const ui32 size = 2 * blockSize;
                        runtime->Send(new IEventHandle(serviceId, edge, new NDDisk::TEvDDiskRead(creds,
                            {0, offset, size}, {true})), edge.NodeId());
                        auto res = env.WaitForEdgeActorEvent<NDDisk::TEvDDiskReadResult>(edge, false);
                        const auto& rr = res->Get()->Record;
                        UNIT_ASSERT(rr.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK);
                        UNIT_ASSERT(rr.HasReadResult());
                        const auto& rr2 = rr.GetReadResult();
                        UNIT_ASSERT(rr2.HasPayloadId());
                        UNIT_ASSERT_VALUES_EQUAL(rr2.GetPayloadId(), 0);
                        TRope rope = res->Get()->GetPayload(0);
                        UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString(), buffer.substr(offset, size));
                    }

                    UNIT_ASSERT(node.HasPersistentBufferDDiskId());
                }
            }
        }
    }

}
