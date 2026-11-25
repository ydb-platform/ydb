#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

#include <random>

Y_UNIT_TEST_SUITE(Mirror3dc) {

    Y_UNIT_TEST(SoloSoftBarrierWaitsForKeepMetadata) {
        using TGroupState = NKikimrBlobStorage::TGroupState;
        using TDataCenterState = NKikimrBlobStorage::TDataCenterState;
        using TEv = TEvBlobStorage;
        constexpr ui32 survivor = 0;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto normal = env.GetGroupInfo(groupId);
        const TString data = "payload";
        const TLogoBlobID kept(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID obsolete(100500, 1, 2, 0, data.size(), 0);
        const TActorId client = runtime->AllocateEdgeActor(1);

        // Place the data in all three realms before setting Keep. The transition's repair
        // scan has no missing part to restore in the surviving realm.
        for (const TLogoBlobID& id : {kept, obsolete}) {
            for (ui32 part = 0; part < 3; ++part) {
                const TVDiskID disk = normal->GetVDiskInSubgroup(part, id.Hash());
                env.PutBlob(disk, TLogoBlobID(id, part + 1), data);
            }
        }

        // Allow Keep to be acknowledged in Normal by the two other realms while delaying
        // propagation to the surviving realm. Intra-realm synchronization can still run.
        auto canSync = [](const auto& record) {
            return record.GetSourceVDiskID().GetRing() != survivor
                || record.GetTargetVDiskID().GetRing() == survivor;
        };
        bool resumeSync = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> delayedSync;
        runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEv::EvVCollectGarbage: {
                    const auto& record = event->Get<TEv::TEvVCollectGarbage>()->Record;
                    return !record.KeepSize() || record.GetVDiskID().GetRing() != survivor;
                }
                case TEv::EvVSync:
                case TEv::EvVSyncFull: {
                    bool allowed = event->GetTypeRewrite() == TEv::EvVSync
                        ? canSync(event->Get<TEv::TEvVSync>()->Record)
                        : canSync(event->Get<TEv::TEvVSyncFull>()->Record);
                    if (!resumeSync && !allowed) {
                        delayedSync.emplace_back(node, std::move(event));
                        return false;
                    }
                    return true;
                }
                default:
                    return true;
            }
        };

        auto collect = [&](ui32 counter, bool barrier, TVector<TLogoBlobID>* keep) {
            runtime->WrapInActorContext(client, [&] {
                SendToBSProxy(client, normal->GroupID, new TEv::TEvCollectGarbage(
                    kept.TabletID(), kept.Generation(), counter, kept.Channel(), barrier,
                    barrier ? obsolete.Generation() : 0, barrier ? obsolete.Step() : 0,
                    keep, nullptr, env.Now() + TDuration::Seconds(30), false));
            });
            const auto result = env.WaitForEdgeActorEvent<TEv::TEvCollectGarbageResult>(client, false);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        };
        collect(1, false, new TVector<TLogoBlobID>{kept});

        // Inspect the flag on individual VDisks before the configuration changes.
        // A group GET may finish after reading just one data copy.
        TBlobStorageGroupInfo::TGroupVDisks withKeep(&normal->GetTopology());
        for (const auto& slot : normal->GetVDisks()) {
            const TVDiskID disk = normal->GetVDiskId(slot.OrderNumber);
            env.WithQueueId(disk, NKikimrBlobStorage::GetFastRead, [&](TActorId queue) {
                const auto deadline = env.Now() + TDuration::Seconds(30);
                auto query = TEv::TEvVGet::CreateExtremeIndexQuery(disk, deadline, NKikimrBlobStorage::FastRead);
                query->AddExtremeQuery(kept, 0, 0);
                runtime->Send(new IEventHandle(queue, client, query.release()), client.NodeId());
                const auto response = env.WaitForEdgeActorEvent<TEv::TEvVGetResult>(client, false, deadline);
                UNIT_ASSERT(response);
                const auto& record = response->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                const auto& item = record.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(item.GetBlobID()), kept);
                if (item.GetKeep()) {
                    UNIT_ASSERT_UNEQUAL(disk.FailRealm, survivor);
                    withKeep |= {&normal->GetTopology(), disk};
                }
            });
        }
        UNIT_ASSERT(normal->GetQuorumChecker().CheckQuorumForGroup(withKeep));

        // Let BSC establish the intermediate copy guarantee and move the group to Solo.
        NKikimrBlobStorage::TConfigRequest request;
        auto* policy = request.AddCommand()->MutableDefineDataCenterStates();
        const auto config = env.FetchBaseConfig();
        for (const auto& group : config.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                UNIT_ASSERT_VALUES_EQUAL(group.FailRealmsSize(), 3);
                for (ui32 realm = 0; realm < 3; ++realm) {
                    if (realm != survivor) {
                        auto* dc = policy->AddDataCenter();
                        dc->SetName(group.GetFailRealms(realm).GetDataCenter());
                        dc->SetState(TDataCenterState::EXCLUDED);
                    }
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(policy->DataCenterSize(), 2);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Sim(TDuration::Seconds(60));
        UNIT_ASSERT_UNEQUAL(ui32(env.GetGroupInfo(groupId)->GetMirror3dcState().State), ui32(TGroupState::DC_SOLO_INV1));
        const auto before = env.GetGroupInfo(groupId);
        for (const auto& slot : before->GetVDisks()) {
            auto disk = before->GetVDiskId(slot.OrderNumber);
            if (disk.FailRealm == survivor) {
                const TActorId service = before->GetActorId(slot.OrderNumber);
                const TActorId edge = runtime->AllocateEdgeActor(service.NodeId());
                runtime->Send(new IEventHandle(service, edge, new TEv::TEvVStatus(disk)), service.NodeId());
                auto status = env.WaitForEdgeActorEvent<TEv::TEvVStatusResult>(edge);
                UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT(status->Get()->Record.GetReplicated());
                UNIT_ASSERT(!status->Get()->Record.GetDurabilityReplicated());
            }
        }
        UNIT_ASSERT(!delayedSync.empty());
        resumeSync = true;
        for (auto& [node, event] : delayedSync) {
            runtime->Schedule(runtime->GetClock(), event.release(), nullptr, node);
        }
        for (ui32 i = 0; i < 60 && env.GetGroupInfo(groupId)->GetMirror3dcState().State != TGroupState::DC_SOLO_INV1; ++i) {
            env.Sim(TDuration::Seconds(10));
        }
        const auto solo = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(ui32(solo->GetMirror3dcState().State), ui32(TGroupState::DC_SOLO_INV1));
        UNIT_ASSERT_VALUES_EQUAL(ui32(solo->GetFailRealmState(survivor)), ui32(TDataCenterState::INCLUDED));

        auto checkReadable = [&](const TLogoBlobID& id) {
            runtime->WrapInActorContext(client, [&] {
                SendToBSProxy(client, solo->GroupID, new TEv::TEvGet(id, 0, 0,
                    env.Now() + TDuration::Seconds(30), NKikimrBlobStorage::FastRead));
            });
            const auto result = env.WaitForEdgeActorEvent<TEv::TEvGetResult>(client, false);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            const auto& item = result->Get()->Responses[0];
            UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(item.Buffer.ConvertToString(), data);
        };
        checkReadable(kept);
        checkReadable(obsolete);

        collect(2, true, nullptr);
        env.Sim(TDuration::Seconds(30));

        runtime->FilterFunction = {};
        ui32 survivorMask = 0;
        for (const auto& disk : solo->GetVDisks()) {
            if (disk.VDiskIdShort.FailRealm == survivor) {
                survivorMask |= 1u << disk.OrderNumber;
            }
        }
        for (const auto& disk : solo->GetVDisks()) {
            if (disk.VDiskIdShort.FailRealm != survivor) {
                continue;
            }
            const TVDiskID vdisk = solo->GetVDiskId(disk.OrderNumber);
            const TKeyBarrier key(kept.TabletID(), kept.Channel(), kept.Generation(), 2, false);
            env.WithQueueId(vdisk, NKikimrBlobStorage::GetFastRead, [&](TActorId queue) {
                runtime->Send(new IEventHandle(queue, client, new TEv::TEvVGetBarrier(
                    vdisk, key, key, nullptr, true)), client.NodeId());
                const auto result = env.WaitForEdgeActorEvent<TEv::TEvVGetBarrierResult>(client, false);
                const auto& record = result->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ValuesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetValues(0).GetIngress(), survivorMask);
            });
            env.CompactVDisk(solo->GetActorId(disk.OrderNumber));
        }

        // The pre-transition Keep must survive compaction even though the survivor
        // received the new barrier without the original Keep request.
        checkReadable(kept);
        runtime->FilterFunction = {};
    }

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
