#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/activity.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <optional>

constexpr ui32 NumRequiredWorkingVDisks = 6;

std::vector<ui32> GetRestartableNodes(TEnvironmentSetup& env, ui32 partitionedNode) {
    auto config = env.FetchBaseConfig();
    std::vector<ui32> res;

    std::map<std::tuple<ui32, ui32, ui32>, const NKikimrBlobStorage::TBaseConfig::TVSlot*> slots;
    for (const auto& slot : config.GetVSlot()) {
        const auto& id = slot.GetVSlotId();
        slots.emplace(std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()), &slot);
    }

    for (const ui32 nodeId : env.Runtime->GetNodes()) {
        if (nodeId == partitionedNode) {
            continue;
        }
        bool badGroups = false;
        for (const auto& group : config.GetGroup()) {
            ui32 numFullyWorking = 0;
            for (const auto& id : group.GetVSlotId()) {
                auto *slot = slots.at({id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()});
                numFullyWorking += slot->GetReady() && nodeId != id.GetNodeId();
            }
            if (numFullyWorking < NumRequiredWorkingVDisks) {
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

std::optional<NKikimrBlobStorage::TReassignGroupDisk> IssueReassignQuery(TEnvironmentSetup& env, ui32 partitionedNode) {
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
            if (slot->GetReady()) {
                ++numFullyWorking;
            }
            if (id.GetNodeId() != partitionedNode) {
                all.push_back(slot);
                if (!slot->GetReady()) {
                    notready.push_back(slot);
                }
            }
        }
        if (numFullyWorking > NumRequiredWorkingVDisks) {
            options.insert(options.end(), all.begin(), all.end());
        } else if (numFullyWorking == NumRequiredWorkingVDisks) {
            options.insert(options.end(), notready.begin(), notready.end());
        }
    }

    Cerr << "NumOptions# " << options.size() << Endl;

    if (options.empty()) {
        return std::nullopt;
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

    return *reassign;
}

bool IsReassignedGroupUsable(TEnvironmentSetup& env, const NKikimrBlobStorage::TReassignGroupDisk& reassign, ui32 partitionedNode) {
    const auto config = env.FetchBaseConfig();
    ui32 numFullyWorking = 0;
    bool reassignApplied = false;
    for (const auto& slot : config.GetVSlot()) {
        if (slot.GetGroupId() != reassign.GetGroupId()) {
            continue;
        }
        reassignApplied |= slot.GetGroupGeneration() != reassign.GetGroupGeneration();
        numFullyWorking += slot.GetReady() && slot.GetVSlotId().GetNodeId() != partitionedNode;
    }
    return reassignApplied && numFullyWorking >= NumRequiredWorkingVDisks;
}

void WaitForReassignedGroup(TEnvironmentSetup& env, const NKikimrBlobStorage::TReassignGroupDisk& reassign, ui32 partitionedNode) {
    constexpr size_t MaxAttempts = 60;
    for (size_t attempt = 0; attempt < MaxAttempts; ++attempt) {
        if (IsReassignedGroupUsable(env, reassign, partitionedNode)) {
            return;
        }
        env.Sim(TDuration::Seconds(1));
    }
    UNIT_ASSERT_C(false, "Reassigned group did not become usable: " << SingleLineProto(reassign));
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
    runtime->SetLogPriority(NActorsServices::TEST, NLog::PRI_NOTICE);

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
            auto nodes = GetRestartableNodes(env, partitionedNode);
            if (!nodes.empty()) {
                auto it = nodes.begin();
                std::advance(it, RandomNumber(nodes.size()));
                Cerr << "RestartNode# " << *it << Endl;
                env.RestartNode(*it);
                env.Sim(TDuration::Seconds(30));
                --counter;
                continue;
            }
        } else if (const auto reassign = IssueReassignQuery(env, partitionedNode)) {
            WaitForReassignedGroup(env, *reassign, partitionedNode);
            --counter;
            continue;
        }
        env.Sim(TDuration::Seconds(15));
    }
}

Y_UNIT_TEST_SUITE(GroupReconfigurationRace) {
    enum class ECancelPut { Never, OnRace, AfterRestart };

    void RunInFlightPutWhenSecondRealmIsExcluded(ECancelPut cancel) {
        using TState = NKikimrBlobStorage::TGroupState;
        using TRole = NKikimrBlobStorage::TDataCenterState;
        TFeatureFlags flags;
        flags.SetEnablePutBatchingForBlobStorage(false);
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .FeatureFlags = flags,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        ui32 groupId = env.GetGroups().front();
        auto configure = [&](TState::EState state, TRole::EState secondRealm) {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupFailModelChecks(true);
            request.SetIgnoreDegradedGroupsChecks(state == TState::DC_SOLO_INV1);
            NKikimrBlobStorage::TDefineGroupState::TGroupDefinition* group =
                request.AddCommand()->MutableDefineGroupState()->AddGroups();
            group->SetGroupId(groupId);
            group->SetGroupState(state);
            group->AddFailRealmStates(TRole::EXCLUDED);
            group->AddFailRealmStates(secondRealm);
            group->AddFailRealmStates(TRole::INCLUDED);
            NKikimrBlobStorage::TConfigResponse result = env.Invoke(request);
            UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        };
        configure(TState::DC_FAILURE_INV1, TRole::INCLUDED);
        env.Sim(TDuration::Seconds(30));
        TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groupId);
        ui32 generation = info->GroupGeneration;
        TMessageRelevanceOwner owner = std::make_shared<TMessageRelevanceTracker>();
        bool sawRestart = false;
        TString data(1024, 'x');
        TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        TActorId edge = env.Runtime->AllocateEdgeActor(1);
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        std::set<TActorId> queues;
        ui32 drops = 0;
        ui32 queueRaces = 0;
        std::unique_ptr<IEventHandle> result;
        bool delayReplies = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvBunchOfEvents) {
                for (const auto& item : ev->Get<TEvBlobStorage::TEvBunchOfEvents>()->Bunch) {
                    if (item->GetTypeRewrite() == TEvBlobStorage::EvPut && item->Get<TEvBlobStorage::TEvPut>()->Id == id) {
                        sawRestart = true;
                        if (cancel == ECancelPut::AfterRestart) {
                            owner.reset();
                        }
                    }
                }
            }
            if (!owner && ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const NKikimrBlobStorage::TEvVPut& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                if (LogoBlobIDFromLogoBlobID(record.GetBlobID()).FullID() == id) {
                    TVDiskID target = VDiskIDFromVDiskID(record.GetVDiskID());
                    UNIT_ASSERT_C(ev->Recipient != info->GetActorId(target), "cancelled write reached VDisk");
                }
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->Recipient == edge) {
                result = std::move(ev);
                return false;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPutResult) {
                const NKikimrBlobStorage::TEvVPutResult& r = ev->Get<TEvBlobStorage::TEvVPutResult>()->Record;
                if (LogoBlobIDFromLogoBlobID(r.GetBlobID()).FullID() == id) {
                    TVDiskID disk = VDiskIDFromVDiskID(r.GetVDiskID());
                    if (delayReplies && disk.FailRealm == 1 && disk.GroupGeneration == generation
                            && r.GetStatus() == NKikimrProto::OK) {
                        queues.insert(ev->Recipient);
                        held.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                    if (queues.contains(ev->Sender) && r.GetStatus() == NKikimrProto::RACE) {
                        UNIT_ASSERT_VALUES_EQUAL(disk.GroupGeneration, generation + 1);
                        ++queueRaces;
                        if (cancel == ECancelPut::OnRace) {
                            owner.reset();
                        }
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVWindowChange && queues.contains(ev->Recipient)
                    && ev->Get<TEvBlobStorage::TEvVWindowChange>()->Record.GetDropConnection()) {
                ++drops;
            }
            return true;
        };

        // Keep the second realm's successful writes in flight at BSQueue while the old VDisks shut down.
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut({
                .BlobId = id,
                .Buffer = TRope(data),
                .Deadline = TInstant::Max(),
                .Tactic = TEvBlobStorage::TEvPut::TacticMinLatency,
                .ExternalRelevanceWatcher = owner,
            }));
        });
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!held.empty());
        UNIT_ASSERT(!result);
        configure(TState::DC_SOLO_INV1, TRole::EXCLUDED);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(drops);
        delayReplies = false;
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        env.Sim(TDuration::Seconds(30));
        env.Runtime->FilterFunction = {};
        TIntrusivePtr<TBlobStorageGroupInfo> current = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(current->GroupGeneration, generation + 1);
        UNIT_ASSERT(current->GetMirror3dcState().State == TState::DC_SOLO_INV1);
        UNIT_ASSERT(result);
        const TEvBlobStorage::TEvPutResult* putResult = result->Get<TEvBlobStorage::TEvPutResult>();
        UNIT_ASSERT(queueRaces);
        UNIT_ASSERT_VALUES_EQUAL(sawRestart, cancel != ECancelPut::OnRace);
        if (cancel == ECancelPut::Never) {
            UNIT_ASSERT_VALUES_EQUAL_C(putResult->Status, NKikimrProto::OK, putResult->ErrorReason);
        } else {
            UNIT_ASSERT(!owner);
            UNIT_ASSERT_VALUES_EQUAL(putResult->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_STRING_CONTAINS(putResult->ErrorReason, "external cancellation");
        }
    }

    Y_UNIT_TEST(InFlightPutWhenSecondRealmIsExcluded) {
        RunInFlightPutWhenSecondRealmIsExcluded(ECancelPut::Never);
    }

    Y_UNIT_TEST(CancelPutBeforeGenerationRestart) {
        RunInFlightPutWhenSecondRealmIsExcluded(ECancelPut::OnRace);
    }

    Y_UNIT_TEST(CancelPutAfterGenerationRestart) {
        RunInFlightPutWhenSecondRealmIsExcluded(ECancelPut::AfterRestart);
    }

    Y_UNIT_TEST(DisconnectedQueueRepliesRaceForStaleGeneration) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(env.GetGroups().front());
        TVDiskID disk = info->GetVDiskId(0);
        TActorId vdisk = info->GetActorId(0);
        TActorId edge = env.Runtime->AllocateEdgeActor(vdisk.NodeId());
        TActorId queue = env.CreateQueueActor(disk, NKikimrBlobStorage::PutTabletLog, 0, edge.NodeId());
        env.Runtime->Send(new IEventHandle(queue, vdisk, new TEvBlobStorage::TEvVWindowChange(
            NKikimrBlobStorage::PutTabletLog, TEvBlobStorage::TEvVWindowChange::DropConnection)), edge.NodeId());
        auto putThroughQueue = [&](const TVDiskID& target) {
            env.Runtime->Send(new IEventHandle(queue, edge, new TEvBlobStorage::TEvVPut(
                TLogoBlobID(1, 1, 1, 0, 16, 0, 1), TRope(TString(16, 'x')), target, false, nullptr,
                TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), edge.NodeId());
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, false);
        };

        UNIT_ASSERT_VALUES_EQUAL(putThroughQueue(disk)->Get()->Record.GetStatus(), NKikimrProto::NOTREADY);
        TVDiskID stale = disk;
        --stale.GroupGeneration;
        TEvBlobStorage::TEvVPutResult::TPtr result = putThroughQueue(stale);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(result->Get()->Record.GetVDiskID()), disk);
    }

    template<typename TRequest, typename TResult>
    void CheckControlRequestGeneration(NKikimrBlobStorage::EVDiskQueueId queueId, auto makeRequest) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(env.GetGroups().front());
        TVDiskID disk = info->GetVDiskId(0);
        TVDiskID next = disk;
        ++next.GroupGeneration;
        TActorId vdisk = info->GetActorId(0);
        TActorId edge = env.Runtime->AllocateEdgeActor(vdisk.NodeId());
        TActorId queue = env.CreateQueueActor(disk, queueId, 0, edge.NodeId());

        // Leave the requests outstanding at the queue until the disk disconnects.
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return !(ev->Sender == queue && ev->GetTypeRewrite() == TRequest::EventType);
        };
        auto send = [&](const TVDiskID& target, ui64 cookie) {
            env.Runtime->Send(new IEventHandle(queue, edge, makeRequest(target), 0, cookie), edge.NodeId());
        };
        auto expect = [&](ui64 cookie, NKikimrProto::EReplyStatus status) {
            typename TResult::TPtr reply = env.WaitForEdgeActorEvent<TResult>(edge, false,
                env.Runtime->GetClock() + TDuration::Seconds(1));
            UNIT_ASSERT(reply);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), status);
            UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(reply->Get()->Record.GetVDiskID()), next);
        };

        send(disk, 1);
        env.Runtime->Send(new IEventHandle(queue, edge, new TEvVGenerationChange(next,
            MakeIntrusive<TBlobStorageGroupInfo>(info, next, vdisk))), edge.NodeId());
        send(next, 2);
        env.Runtime->Send(new IEventHandle(queue, vdisk, new TEvBlobStorage::TEvVWindowChange(
            queueId, TEvBlobStorage::TEvVWindowChange::DropConnection)), edge.NodeId());
        expect(1, NKikimrProto::RACE);
        expect(2, NKikimrProto::VDISK_ERROR_STATE);
        send(disk, 3);
        expect(3, NKikimrProto::RACE);
        send(next, 4);
        expect(4, NKikimrProto::NOTREADY);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(StatusGenerationOnDisconnect) {
        CheckControlRequestGeneration<TEvBlobStorage::TEvVStatus, TEvBlobStorage::TEvVStatusResult>(
            NKikimrBlobStorage::GetFastRead, [](const TVDiskID& disk) { return new TEvBlobStorage::TEvVStatus(disk); });
    }

    Y_UNIT_TEST(AssimilateGenerationOnDisconnect) {
        CheckControlRequestGeneration<TEvBlobStorage::TEvVAssimilate, TEvBlobStorage::TEvVAssimilateResult>(
            NKikimrBlobStorage::GetAsyncRead, [](const TVDiskID& disk) {
                return new TEvBlobStorage::TEvVAssimilate(disk, {}, {}, {}, false, false);
            });
    }

    Y_UNIT_TEST(Test_block42) {
        RunGroupReconfigurationRaceTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(RestartedEncryptedPutKeepsData) {
        // Without a tenant key the proxy of an encrypted group refuses puts and gets.
        const ui8 keyData[32] = "restarted put test key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "tenant key",
        };
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .Encryption = true,
            .ConfigPreprocessor = [&](ui32 /*nodeId*/, TNodeWardenConfig& config) { config.TenantKey = key; },
        });
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(2, 1);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();

        TString data = TString::Uninitialized(1000);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = 'a' + i % 26;
        }
        const ui64 tabletId = 100500;
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
        const TActorId edge = runtime->AllocateEdgeActor(1);

        // Hold the VDisk puts of this blob until the group is reconfigured, so that they are answered with RACE and
        // the proxy restarts the put after it has encrypted the buffer. Puts of other tablets, such as the
        // controller's log, must go on.
        auto isTestBlob = [&](const NKikimrProto::TLogoBlobID& blobId) {
            return LogoBlobIDFromLogoBlobID(blobId).TabletID() == tabletId;
        };
        bool hold = true;
        ui32 restartedPuts = 0;
        auto countRestart = [&](IEventHandle& ev) {
            const auto *put = ev.Get<TEvBlobStorage::TEvPut>();
            if (put->Id.TabletID() == tabletId && put->RestartCounter) {
                ++restartedPuts;
            }
        };
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        std::unique_ptr<IEventHandle> putResult;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvBunchOfEvents: // the put actor restarts its puts as a bunch
                    for (const auto& item : ev->Get<TEvBlobStorage::TEvBunchOfEvents>()->Bunch) {
                        if (item->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                            countRestart(*item);
                        }
                    }
                    break;
                case TEvBlobStorage::EvVPut:
                    if (hold && isTestBlob(ev->Get<TEvBlobStorage::TEvVPut>()->Record.GetBlobID())) {
                        held.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                    break;
                case TEvBlobStorage::EvVMultiPut: {
                    const auto& record = ev->Get<TEvBlobStorage::TEvVMultiPut>()->Record;
                    if (hold && record.ItemsSize() && isTestBlob(record.GetItems(0).GetBlobID())) {
                        held.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                    break;
                }
                case TEvBlobStorage::EvPut:
                    countRestart(*ev);
                    break;
                case TEvBlobStorage::EvPutResult:
                    if (ev->GetRecipientRewrite() == edge) {
                        putResult = std::move(ev);
                        return false;
                    }
                    break;
            }
            return true;
        };

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(!putResult, "the put finished before the reconfiguration, Status# "
            << NKikimrProto::EReplyStatus_Name(putResult->Get<TEvBlobStorage::TEvPutResult>()->Status)
            << " ErrorReason# " << putResult->Get<TEvBlobStorage::TEvPutResult>()->ErrorReason);
        UNIT_ASSERT(!held.empty());

        // Move one VDisk of the group to the other PDisk of its node.
        const auto config = env.FetchBaseConfig();
        const NKikimrBlobStorage::TBaseConfig::TVSlot *slot = nullptr;
        for (const auto& x : config.GetVSlot()) {
            if (x.GetGroupId() == groupId) {
                slot = &x;
                break;
            }
        }
        UNIT_ASSERT(slot);
        const NKikimrBlobStorage::TBaseConfig::TPDisk *target = nullptr;
        for (const auto& pdisk : config.GetPDisk()) {
            if (pdisk.GetNodeId() == slot->GetVSlotId().GetNodeId() && pdisk.GetPDiskId() != slot->GetVSlotId().GetPDiskId()) {
                target = &pdisk;
                break;
            }
        }
        UNIT_ASSERT(target);
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreGroupFailModelChecks(true);
        auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
        reassign->SetGroupId(groupId);
        reassign->SetGroupGeneration(slot->GetGroupGeneration());
        reassign->SetFailRealmIdx(slot->GetFailRealmIdx());
        reassign->SetFailDomainIdx(slot->GetFailDomainIdx());
        reassign->SetVDiskIdx(slot->GetVDiskIdx());
        reassign->MutableTargetPDiskId()->SetNodeId(target->GetNodeId());
        reassign->MutableTargetPDiskId()->SetPDiskId(target->GetPDiskId());
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(10));

        hold = false;
        for (auto& [nodeId, ev] : held) {
            runtime->Send(ev.release(), nodeId);
        }
        held.clear();
        for (ui32 i = 0; i < 60 && !putResult; ++i) {
            env.Sim(TDuration::Seconds(1));
        }
        runtime->FilterFunction = {};
        UNIT_ASSERT(putResult);
        UNIT_ASSERT_VALUES_EQUAL(putResult->Get<TEvBlobStorage::TEvPutResult>()->Status, NKikimrProto::OK);
        UNIT_ASSERT_C(restartedPuts, "the put was not restarted");

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        const auto getResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_EQUAL(getResult->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
}
