#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <random>

Y_UNIT_TEST_SUITE(Mirror3dc) {

    Y_UNIT_TEST(GcQuorum) {
        // generate collect garbage disk ordering options
        std::vector<std::vector<ui32>> orders;
        {
            std::vector<ui32> order;
            for (ui32 i = 0; i < 9; ++i) {
                order.push_back(i);
            }

            for (ui32 i = 0; i < 18144; ++i) {
                orders.push_back(order);
                std::next_permutation(order.begin(), order.end());
            }
        }

        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        auto& topology = info->GetTopology();
        auto& checker = topology.GetQuorumChecker();

        std::unordered_map<ui32, TActorId> edgeActors;
        auto getEdgeActor = [&](ui32 nodeId) {
            TActorId& res = edgeActors[nodeId];
            if (!res) {
                res = runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            }
            return res;
        };

        std::vector<TActorId> queueIds;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            queueIds.push_back(env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000));
        }

        // generate blob ids
        const TString buffer("x");
        std::vector<TLogoBlobID> ids;
        for (ui32 i = 1; i <= orders.size(); ++i) {
            const TLogoBlobID id(100500 + i, 1, 1, 0, buffer.size(), 0);
            ids.push_back(id);
        }

        // write out blobs
        {
            const TActorId edge = getEdgeActor(1);
            for (const TLogoBlobID& id : ids) {
                runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }
        }

        Cerr << "BlobsWritten# " << ids.size() << Endl;

        // obtain part maps
        auto getPartMap = [&] {
            std::unordered_map<TLogoBlobID, std::map<ui32, ui32>, THash<TLogoBlobID>> partMap;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID& vdiskId = info->GetVDiskId(i);
                TLogoBlobID from(Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                    TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                for (;;) {
                    auto query = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vdiskId, TInstant::Max(),
                        NKikimrBlobStorage::EGetHandleClass::FastRead, {}, {}, from, TLogoBlobID());

                    const TActorId queueId = queueIds[i];
                    const TActorId edge = getEdgeActor(queueId.NodeId());
                    runtime->Send(new IEventHandle(queueId, edge, query.release()), edge.NodeId());
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);

                    const auto& record = res->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    for (const auto& result : record.GetResult()) {
                        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                        const auto& id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                        if (result.PartsSize()) {
                            auto& mask = partMap[id][i];
                            for (const ui32 partId : result.GetParts()) {
                                mask |= 1 << partId - 1;
                            }
                        }
                        from = TLogoBlobID::PrevFull(id, TLogoBlobID::MaxBlobSize);
                    }

                    if (record.ResultSize() == 0) {
                        break;
                    }
                }
            }
            return partMap;
        };

        auto partMap = getPartMap();
        UNIT_ASSERT_VALUES_EQUAL(partMap.size(), ids.size());

        std::vector<TBlobStorageGroupInfo::TGroupVDisks> vdisks(ids.size(), {&topology});

        for (ui32 i = 0; i < 9; ++i) {
            Cerr << "step " << i << Endl;

            std::map<TActorId, ui32> waitQueue;

            for (ui32 j = 0; j < orders.size(); ++j) {
                const ui32 diskIdx = orders[j][i];
                const TVDiskID& vdiskId = info->GetVDiskId(diskIdx);
                const TActorId queueId = queueIds[diskIdx];
                const TActorId edge = getEdgeActor(queueId.NodeId());
                const TLogoBlobID& id = ids[j];
                runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVCollectGarbage(id.TabletID(),
                    id.Generation(), 0, id.Channel(), true, id.Generation(), id.Step(), false, nullptr, nullptr, vdiskId,
                    TInstant::Max())), edge.NodeId());
                ++waitQueue[edge];
            }

            Cerr << "waiting for replies" << Endl;

            while (!waitQueue.empty()) {
                std::set<TActorId> edges;
                for (const auto& [key, value] : waitQueue) {
                    edges.insert(key);
                }
                auto res = runtime->WaitForEdgeActorEvent(edges);
                if (!--waitQueue[res->Recipient]) {
                    waitQueue.erase(res->Recipient);
                }
                const auto *msg = res->CastAsLocal<TEvBlobStorage::TEvVCollectGarbageResult>();
                UNIT_ASSERT(msg);
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrProto::OK);
            }

            env.Sim(TDuration::Seconds(10)); // wait for sync

            Cerr << "scanning parts" << Endl;

            auto currentMap = getPartMap();

            for (ui32 j = 0; j < orders.size(); ++j) {
                const TLogoBlobID& id = ids[j];
                if (!partMap.count(id)) {
                    continue; // this blob is already collected
                }

                // update collected state for current blob
                auto& collected = vdisks[j];
                auto prev = collected;
                collected |= {&topology, info->GetVDiskId(orders[j][i])};

                if (currentMap.count(id)) {
                    UNIT_ASSERT_EQUAL(currentMap[id], partMap[id]); // blob is still inplace
                } else {
                    UNIT_ASSERT(!checker.CheckQuorumForGroup(prev));
                    UNIT_ASSERT(checker.CheckQuorumForGroup(collected));
                    Cerr << "empty@ " << i <<  " " << j << Endl;
                }
            }

            partMap = std::move(currentMap);

            if (partMap.empty()) {
                break;
            }
        }

        UNIT_ASSERT(partMap.empty());
    }

}
