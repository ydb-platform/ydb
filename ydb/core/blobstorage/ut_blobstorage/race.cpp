#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/activity.h>

std::vector<ui32> GetRestartableNodes(TEnvironmentSetup& env) {
    auto config = env.FetchBaseConfig();
    std::vector<ui32> res;

    std::map<std::tuple<ui32, ui32, ui32>, const NKikimrBlobStorage::TBaseConfig::TVSlot*> slots;
    for (const auto& slot : config.GetVSlot()) {
        const auto& id = slot.GetVSlotId();
        slots.emplace(std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()), &slot);
    }

    for (const ui32 nodeId : env.Runtime->GetNodes()) {
        bool badGroups = false;
        for (const auto& group : config.GetGroup()) {
            ui32 numFullyWorking = 0;
            for (const auto& id : group.GetVSlotId()) {
                auto *slot = slots.at({id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()});
                numFullyWorking += slot->GetReady() && nodeId != id.GetNodeId();
            }
            if (numFullyWorking < 6) {
                badGroups = true;
                break;
            }
        }
        if (!badGroups) {
            res.push_back(nodeId);
        }
    }

    return res;
}

bool IssueReassignQuery(TEnvironmentSetup& env) {
    auto config = env.FetchBaseConfig();

    std::map<std::tuple<ui32, ui32, ui32>, const NKikimrBlobStorage::TBaseConfig::TVSlot*> slots;
    for (const auto& slot : config.GetVSlot()) {
        const auto& id = slot.GetVSlotId();
        slots.emplace(std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()), &slot);
    }

    std::vector<const NKikimrBlobStorage::TBaseConfig::TVSlot*> options;

    for (const auto& group : config.GetGroup()) {
        ui32 numFullyWorking = 0;
        std::vector<const NKikimrBlobStorage::TBaseConfig::TVSlot*> all, notready;
        for (const auto& id : group.GetVSlotId()) {
            auto *slot = slots.at(std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()));
            all.push_back(slot);
            if (slot->GetReady()) {
                ++numFullyWorking;
            } else {
                notready.push_back(slot);
            }
        }
        if (numFullyWorking > 6) {
            options.insert(options.end(), all.begin(), all.end());
        } else if (numFullyWorking == 6) {
            options.insert(options.end(), notready.begin(), notready.end());
        }
    }

    Cerr << "NumOptions# " << options.size() << Endl;

    if (options.empty()) {
        return false;
    }

    const NKikimrBlobStorage::TBaseConfig::TVSlot *slot = options[RandomNumber(options.size())];
    const auto& id = slot->GetVSlotId();
    const ui32 nodeId = id.GetNodeId();
    const ui32 pdiskId = id.GetPDiskId();
    std::vector<const NKikimrBlobStorage::TBaseConfig::TPDisk*> pdisks;
    std::map<std::tuple<ui32, ui32>, ui32> usageCount;
    for (const auto& pdisk : config.GetPDisk()) {
        if (pdisk.GetNodeId() == nodeId && pdisk.GetPDiskId() != pdiskId) {
            pdisks.push_back(&pdisk);
            usageCount[std::make_tuple(pdisk.GetNodeId(), pdisk.GetPDiskId())] = pdisk.GetNumStaticSlots();
        }
    }
    for (const auto& slot : config.GetVSlot()) {
        const auto& id = slot.GetVSlotId();
        if (id.GetNodeId() == nodeId && id.GetPDiskId() != pdiskId) {
            ++usageCount[std::make_tuple(id.GetNodeId(), id.GetPDiskId())];
        }
    }
    ui32 minUsage = Max<ui32>();
    for (const auto& [key, value] : usageCount) {
        minUsage = Min(minUsage, value);
    }
    auto pred = [&](const NKikimrBlobStorage::TBaseConfig::TPDisk *pdisk) {
        return usageCount[std::make_tuple(pdisk->GetNodeId(), pdisk->GetPDiskId())] > minUsage;
    };
    pdisks.erase(std::remove_if(pdisks.begin(), pdisks.end(), pred), pdisks.end());
    UNIT_ASSERT(!pdisks.empty());
    const NKikimrBlobStorage::TBaseConfig::TPDisk *target = pdisks[RandomNumber(pdisks.size())];

    NKikimrBlobStorage::TConfigRequest request;
    request.SetIgnoreGroupFailModelChecks(true);

    auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
    reassign->SetGroupId(slot->GetGroupId());
    reassign->SetGroupGeneration(slot->GetGroupGeneration());
    reassign->SetFailRealmIdx(slot->GetFailRealmIdx());
    reassign->SetFailDomainIdx(slot->GetFailDomainIdx());
    reassign->SetVDiskIdx(slot->GetVDiskIdx());
    reassign->MutableTargetPDiskId()->SetNodeId(target->GetNodeId());
    reassign->MutableTargetPDiskId()->SetPDiskId(target->GetPDiskId());

    auto makeStatusString = [&] {
        std::map<TVDiskID, TString> status;
        for (const auto& x : config.GetVSlot()) {
            if (x.GetGroupId() == slot->GetGroupId()) {
                status.emplace(TVDiskID(x.GetGroupId(), x.GetGroupGeneration(), x.GetFailRealmIdx(),
                    x.GetFailDomainIdx(), x.GetVDiskIdx()), x.GetStatus());
            }
        }

        TStringBuilder sb;
        for (const auto& [key, value] : status) {
            if (sb) {
                sb << " ";
            }
            sb << key << ":" << value;
        }
        return sb;
    };

    Cerr << "Reassign# " << SingleLineProto(request) << " Status# " << makeStatusString() << Endl;

    auto response = env.Invoke(request);
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

    return true;
}

void RunGroupReconfigurationRaceTest(TBlobStorageGroupType type) {
    const ui32 numStaticNodes = type.BlobSubgroupSize();
    const ui32 numDynamicNodes = RandomNumber(3u);
    const ui32 staticNodeSenderMask = RandomNumber(1u << numStaticNodes);
    const ui32 partitionedNode = RandomNumber(1 + numStaticNodes + numDynamicNodes);
    const bool cache = RandomNumber(2u);
    if (!numDynamicNodes && !staticNodeSenderMask) {
        return;
    }

    Cerr << "numStaticNodes# " << numStaticNodes << " numDynamicNodes# " << numDynamicNodes
        << " staticNodeSenderMask# " << staticNodeSenderMask << " partitionedNode# " << partitionedNode
        << " cache# " << (cache ? "true" : "false") << Endl;

    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = numStaticNodes + numDynamicNodes,
        .Erasure = type,
        .Cache = cache,
    });
    auto& runtime = env.Runtime;
    runtime->SetLogPriority(NActorsServices::TEST, NLog::PRI_DEBUG);

    ui32 partCounter = 0;
    runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->Recipient == MakeBlobStorageNodeWardenID(partitionedNode) && partCounter++) {
            // keep the first message
            return false;
        }
        return true;
    };

    const ui32 numDrivesPerNode = 4;
    const ui32 numGroups = numDrivesPerNode * numStaticNodes;
    env.CreateBoxAndPool(numDrivesPerNode, numGroups, numStaticNodes);

    auto baseConfig = env.FetchBaseConfig();
    std::unordered_set<ui32> storageNodes;
    for (const auto& vslot : baseConfig.GetVSlot()) {
        storageNodes.insert(vslot.GetVSlotId().GetNodeId());
    }

    auto groups = env.GetGroups();

    std::vector<ui32> senderNodeIds;
    for (const ui32 nodeId : runtime->GetNodes()) {
        if (!storageNodes.count(nodeId) || staticNodeSenderMask & 1 << nodeId - 1) {
            senderNodeIds.push_back(nodeId);
        }
    }

    std::unordered_map<ui32, TActorId> nodeIdToEdge;
    auto getEdgeActor = [&](ui32 nodeId) -> TActorId {
        TActorId& edge = nodeIdToEdge[nodeId];
        if (!edge) {
            edge = runtime->AllocateEdgeActor(nodeId);
        }
        return edge;
    };

    const ui32 numWriters = groups.size(); // one per each group
    ui64 tabletId = 100500;

    std::unordered_set<TActorId, THash<TActorId>> actors;
    for (ui32 i = 0; i < numWriters; ++i) {
        const ui32 nodeId = senderNodeIds[i % senderNodeIds.size()];
        auto *actor = new TActivityActor(tabletId++, groups[i % groups.size()], getEdgeActor(nodeId));
        actors.insert(runtime->Register(actor, nodeId));
    }

    ui32 counter = 100;
    while (counter) {
        // restart node at random basis
        if (counter % 30 == 15) {
            auto nodes = GetRestartableNodes(env);
            if (!nodes.empty()) {
                auto it = nodes.begin();
                std::advance(it, RandomNumber(nodes.size()));
                Cerr << "RestartNode# " << *it << Endl;
                env.RestartNode(*it);
                env.Sim(TDuration::Seconds(30));
                --counter;
                continue;
            }
        } else if (IssueReassignQuery(env)) {
            const TDuration delay = TDuration::MilliSeconds(1 + RandomNumber<ui64>(200));
            env.Sim(delay);
            --counter;
            continue;
        }
        env.Sim(TDuration::Seconds(15));
    }
}

Y_UNIT_TEST_SUITE(GroupReconfigurationRace) {
    Y_UNIT_TEST(Test_block42) {
        RunGroupReconfigurationRaceTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }
}
