#include <ydb/core/blobstorage/ut_blobstorage/ut_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <util/random/random.h>

Y_UNIT_TEST_SUITE(BlobStorageSync) {

    void TestCutting(TBlobStorageGroupType groupType) {
        const ui32 groupSize = groupType.BlobSubgroupSize();

        // for (ui32 mask = 0; mask < (1 << groupSize); ++mask) {  // TIMEOUT
        {
            ui32 mask = RandomNumber(1ull << groupSize);
            for (bool compressChunks : { true, false }) {
                TEnvironmentSetup env{{
                    .NodeCount = groupSize,
                    .Erasure = groupType,
                }};

                env.CreateBoxAndPool(1, 1);
                std::vector<ui32> groups = env.GetGroups();
                UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
                ui32 groupId = groups[0];

                const ui64 tabletId = 5000;
                const ui32 channel = 10;
                ui32 gen = 1;
                ui32 step = 1;
                ui64 cookie = 1;

                ui64 totalSize = 0;

                std::vector<TControlWrapper> cutLocalSyncLogControls;
                std::vector<TControlWrapper> compressChunksControls;
                std::vector<TActorId> edges;

                for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
                    cutLocalSyncLogControls.emplace_back(0, 0, 1);
                    compressChunksControls.emplace_back(1, 0, 1);
                    TAppData* appData = env.Runtime->GetNode(nodeId)->AppData.get();
                    appData->Icb->RegisterSharedControl(cutLocalSyncLogControls.back(), "VDiskControls.EnableLocalSyncLogDataCutting");
                    appData->Icb->RegisterSharedControl(compressChunksControls.back(), "VDiskControls.EnableSyncLogChunkCompressionHDD");
                    edges.push_back(env.Runtime->AllocateEdgeActor(nodeId));
                }

                for (ui32 i = 0; i < groupSize; ++i) {
                    env.Runtime->WrapInActorContext(edges[i], [&] {
                        SendToBSProxy(edges[i], groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edges[i], false);
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                }

                auto writeBlob = [&](ui32 nodeId, ui32 blobSize) {
                    TLogoBlobID blobId(tabletId, gen, step, channel, blobSize, ++cookie);
                    totalSize += blobSize;
                    TString data = MakeData(blobSize);
                    
                    const TActorId& sender = edges[nodeId - 1];
                    env.Runtime->WrapInActorContext(sender, [&] () {
                        SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(blobId, std::move(data), TInstant::Max()));
                    });
                };

                env.Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                    switch(ev->Type) {
                        case TEvBlobStorage::TEvPutResult::EventType:
                            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvBlobStorage::TEvPutResult>()->Status, NKikimrProto::OK);
                            return false;
                        default:
                            return true;
                    }
                };
                
                while (totalSize < 16_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }
                env.Sim(TDuration::Minutes(5));

                for (ui32 i = 0; i < groupSize; ++i) {
                    cutLocalSyncLogControls[i] = !!(mask & (1 << i));
                    compressChunksControls[i] = compressChunks;
                }

                while (totalSize < 32_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }

                env.Sim(TDuration::Minutes(5));
            }
        }
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3dc) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3of4) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(TestSyncLogCuttingBlock4Plus2) {
        TestCutting(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(SyncWhenDiskGetsDown) {
        return; // re-enable when protocol issue is resolved

        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString buffer = "hello, world!";
        TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        std::unordered_map<TVDiskID, TActorId, THash<TVDiskID>> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TVDiskID vdiskId = info->GetVDiskId(i);
            queues[vdiskId] = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000);
        }

        struct TBlobInfo {
            TVDiskID VDiskId;
            TLogoBlobID BlobId;
            NKikimrProto::EReplyStatus Status;
            std::optional<TIngress> Ingress;
        };
        auto collectBlobInfo = [&] {
            std::vector<TBlobInfo> blobs;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID vdiskId = info->GetVDiskId(i);
                const TActorId queueId = queues.at(vdiskId);
                const TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                    vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {{id}}).release()), queueId.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                auto& record = res->Get()->Record;
                UNIT_ASSERT(record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::NOTREADY);
                for (auto& result : record.GetResult()) {
                    blobs.push_back({.VDiskId = vdiskId, .BlobId = LogoBlobIDFromLogoBlobID(result.GetBlobID()),
                        .Status = result.GetStatus(), .Ingress = result.HasIngress() ? std::make_optional(
                        TIngress(result.GetIngress())) : std::nullopt});
                }
            }
            return blobs;
        };
        auto dumpBlobs = [&](const char *name, const std::vector<TBlobInfo>& blobs) {
            Cerr << "Blobs(" << name <<"):" << Endl;
            for (const auto& item : blobs) {
                Cerr << item.VDiskId << " " << item.BlobId << " " << NKikimrProto::EReplyStatus_Name(item.Status)
                    << " " << (item.Ingress ? item.Ingress->ToString(&info->GetTopology(), item.VDiskId, item.BlobId) :
                    "none") << Endl;
            }
        };

        env.Sim(TDuration::Seconds(10)); // wait for blob to get synced across the group

        auto blobsInitial = collectBlobInfo();

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                0, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>(1, id), nullptr, TInstant::Max(),
                false));
        });
        auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        const ui32 suspendedNodeId = 2;
        env.StopNode(suspendedNodeId);

        runtime->WrapInActorContext(edge, [&] {
            // send the sole do not keep flag
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 0, 0, 0, false, 0,
                0, nullptr, new TVector<TLogoBlobID>(1, id), TInstant::Max(), false));
        });
        res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        // sync barriers and then compact them all
        env.Sim(TDuration::Seconds(10));
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TActorId actorId = info->GetActorId(i);
            if (actorId.NodeId() != suspendedNodeId) {
                const auto& sender = env.Runtime->AllocateEdgeActor(actorId.NodeId());
                auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
                ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
                runtime->Send(ev.release(), sender.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
            }
        }
        env.Sim(TDuration::Minutes(1));
        auto blobsIntermediate = collectBlobInfo();

        env.StartNode(suspendedNodeId);
        env.Sim(TDuration::Minutes(1));

        auto blobsFinal = collectBlobInfo();

        dumpBlobs("initial", blobsInitial);
        dumpBlobs("intermediate", blobsIntermediate);
        dumpBlobs("final", blobsFinal);

        for (auto& item : blobsIntermediate) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }

        for (auto& item : blobsFinal) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }
    }
}
