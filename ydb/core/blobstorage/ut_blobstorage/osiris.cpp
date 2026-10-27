#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_osiris.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_recovery.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <util/system/condvar.h>
#include <util/system/info.h>

#include <bit>

#define SINGLE_THREAD 1

namespace {

bool DoTestCase(TBlobStorageGroupType::EErasureSpecies erasure, const std::set<std::pair<ui32, ui32>>& orderNumToPart,
        bool doNotNeedToRestore, const TLogoBlobID& id, const TString& data, IOutputStream *stream) {
    const TBlobStorageGroupType type(erasure);
    const ui32 numDisks = type.BlobSubgroupSize();

    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = numDisks,
        .Erasure = erasure,
        .PrepareRuntime = [&](TTestActorSystem& runtime) {
            runtime.LogStream = stream;
        },
    });

    env.CreateBoxAndPool(1, 1);

    const auto& groups = env.GetGroups();
    Y_ABORT_UNLESS(groups.size() == 1);
    const auto& info = env.GetGroupInfo(groups.front());

    TBlobStorageGroupInfo::TOrderNums orderNums;
    info->GetTopology().PickSubgroup(id.Hash(), orderNums);

    TString temp = data;
    char *buffer = temp.Detach();
    Encrypt(buffer, buffer, 0, temp.size(), id, *info);
    TDataPartSet parts;
    info->Type.SplitData((TBlobStorageGroupType::ECrcMode)id.CrcMode(), temp, parts);

    TString error;

    // put parts to disks
    for (const auto& [orderNum, partIdx] : orderNumToPart) {
        const TActorId& queueId = env.CreateQueueActor(info->GetVDiskId(orderNum), NKikimrBlobStorage::PutTabletLog, 0);
        const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        const TLogoBlobID blobId(id, partIdx + 1);
        TRope buffer = type.PartSize(blobId) ? parts.Parts[partIdx].OwnedString : TRope(TString());
        env.Runtime->Send(new IEventHandle(queueId, sender, new TEvBlobStorage::TEvVPut(blobId, buffer,
            info->GetVDiskId(orderNum), false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)),
            sender.NodeId());
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(sender);
        Y_ABORT_UNLESS(res->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // sync
    env.Sim(TDuration::Seconds(10));

    // obtain config
    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
    Y_ABORT_UNLESS(response.GetSuccess());
    for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
        const TVDiskID vdiskId(vslot.GetGroupId(), vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
            vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
        const auto& v = vslot.GetVSlotId();
        env.Wipe(v.GetNodeId(), v.GetPDiskId(), v.GetVSlotId(), vdiskId);
        env.Sim(TDuration::Seconds(30));
    }

    // collect blob info from cluster
    TSubgroupPartLayout layout;
    std::vector<std::tuple<TVDiskID, TLogoBlobID, NKikimrProto::EReplyStatus>> v;
    for (ui32 i = 0; i < numDisks; ++i) {
        const TActorId& queueId = env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::GetFastRead, 0);
        const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(i), TInstant::Max(),
            NKikimrBlobStorage::FastRead);
        query->AddExtremeQuery(id, 0, 0);
        env.Runtime->Send(new IEventHandle(queueId, sender, query.release()), sender.NodeId());
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender);
        const auto& record = res->Get()->Record;
        Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        for (const auto& blob : record.GetResult()) {
            const auto& id = LogoBlobIDFromLogoBlobID(blob.GetBlobID());
            v.emplace_back(vdiskId, id, blob.GetStatus());
            if (blob.GetStatus() == NKikimrProto::OK || blob.GetStatus() == NKikimrProto::NOT_YET) {
                layout.AddItem(info->GetIdxInSubgroup(vdiskId, id.FullID().Hash()), id.PartId() - 1, info->Type);
            }
        }
    }

    std::sort(v.begin(), v.end());
    for (const auto& item : v) {
        const TVDiskID& vdiskId = std::get<0>(item);
        const TLogoBlobID& blobId = std::get<1>(item);
        const NKikimrProto::EReplyStatus status = std::get<2>(item);
        *stream << "VDiskId# " << vdiskId << " BlobId# " << blobId << " Status# " << NKikimrProto::EReplyStatus_Name(status) << Endl;
        if (doNotNeedToRestore && (status == NKikimrProto::OK || status == NKikimrProto::NOT_YET)) {
            const ui32 orderNum = info->GetOrderNumber(vdiskId);
            if (!orderNumToPart.count(std::make_pair(orderNum, blobId.PartId() - 1))) {
                error = "Excessive part written to disk";
            }
        }
    }

    if (error || doNotNeedToRestore) {
        // skip checking
    } else if (info->GetQuorumChecker().GetBlobState(layout, {&info->GetTopology()}) != TBlobStorageGroupInfo::EBS_FULL) {
        error = "Blob is not in EBS_FULL state";
    } else {
        switch (info->Type.GetErasure()) {
            case TBlobStorageGroupType::ErasureMirror3dc:
            case TBlobStorageGroupType::ErasureMirror3of4:
                break;

            default: {
                ui32 numDistinctParts = 0;
                for (ui32 i = 0; i < info->Type.TotalPartCount(); ++i) {
                    if (layout.GetDisksWithPart(i)) {
                        ++numDistinctParts;
                    }
                }
                if (numDistinctParts != info->Type.TotalPartCount()) {
                    error = "Not enough parts written";
                }
                break;
            }
        }
    }

    *stream << "[";
    bool first = true;
    for (const auto& [orderNum, part] : orderNumToPart) {
        const auto& vdiskId = info->GetVDiskId(orderNum);
        *stream << (std::exchange(first, false) ? "" : " ") << "[" << (int)vdiskId.FailRealm << ":"
            << (int)vdiskId.FailDomain << "]:" << part;
    }
    *stream << "] " << (error ? error : "Valid") << Endl;

    return !error;
}

template<typename F, typename G>
void RunInThreads(size_t numTasks, F&& func, G&& postprocess) {
    using TResult = std::invoke_result_t<F, size_t>;

    std::deque<TResult> output;
    TMutex outputMutex;
    TCondVar outputCV;
    size_t numRunning = 0;

    size_t index = 0;
    TMutex indexMutex;
    auto threadFunc = [&] {
        for (;;) {
            size_t taskIndex;
            with_lock (indexMutex) {
                if (index == numTasks) {
                    break;
                }
                taskIndex = index++;
            }

            TResult result = std::invoke(func, taskIndex);

            with_lock (outputMutex) {
                output.push_back(std::move(result));
            }
            outputCV.Signal();
        }
        with_lock (outputMutex) {
            if (!--numRunning) {
                outputCV.BroadCast();
            }
        }
    };

    std::list<TThread> threads;
    while (threads.size() < NSystemInfo::NumberOfCpus()) {
        threads.emplace_back(threadFunc);
        ++numRunning;
    }
    for (auto& thread : threads) {
        thread.Start();
    }
    for (;;) {
        std::deque<TResult> results;
        with_lock (outputMutex) {
            results.swap(output);
            if (results.empty()) {
                if (numRunning) {
                    outputCV.Wait(outputMutex);
                    continue;
                } else {
                    break;
                }
            }
        }
        for (TResult& item : results) {
            std::invoke(postprocess, item);
        }
    }
    for (auto& thread : threads) {
        thread.Join();
    }
}

void DoTest(TBlobStorageGroupType::EErasureSpecies erasure) {
    const TBlobStorageGroupType type(erasure);
    const ui32 numDisks = type.BlobSubgroupSize();

    const ui32 numRealms = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
    const ui32 numDomains = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : type.BlobSubgroupSize();
    TBlobStorageGroupInfo tempInfo(type, 1, numDomains, numRealms); // group with the same layout as ours
    const auto& topology = tempInfo.GetTopology();

    TString data = "Hello, World!\n";
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);

    TBlobStorageGroupInfo::TOrderNums orderNums;
    topology.PickSubgroup(id.Hash(), orderNums);

    std::vector<std::vector<ui32>> suitablePartsByOrderNum(numDisks);
    for (ui32 i = 0; i < type.BlobSubgroupSize(); ++i) {
        const ui32 orderNum = orderNums[i];
        auto& v = suitablePartsByOrderNum[orderNum];
        v.push_back(0); // no parts
        switch (erasure) {
            case TBlobStorageGroupType::ErasureMirror3dc:
                v.push_back(1 << (i % 3)); // per-dc part indexing
                break;

            case TBlobStorageGroupType::ErasureMirror3of4:
                v.push_back(1 << 2); // meta
                v.push_back(1 << (i & 1)); // data
                break;

            default:
                if (i < type.TotalPartCount()) {
                    v.push_back(1 << i); // main
                } else {
                    for (ui32 j = 1; j < (1 << type.TotalPartCount()); ++j) {
                        if (std::popcount(j) <= 1) {
                            v.push_back(j); // handoff
                        }
                    }
                }
                break;
        }
    }

    std::vector<std::tuple<std::set<std::pair<ui32, ui32>>, bool>> tasks;

    std::vector<size_t> indexes(numDisks, 0);
    for (;;) {
        TSubgroupPartLayout layout;
        std::set<std::pair<ui32, ui32>> orderNumToPart;
        for (ui32 i = 0; i < numDisks; ++i) {
            ui32 mask = suitablePartsByOrderNum[i][indexes[i]];
            while (mask) {
                const ui32 part = CountTrailingZeroBits(mask);
                orderNumToPart.emplace(i, part);
                layout.AddItem(topology.GetIdxInSubgroup(topology.GetVDiskId(i), id.Hash()), part, type);
                mask &= ~(1 << part);
            }
        }
        bool doNotNeedToRestore;
        switch (erasure) {
            case TBlobStorageGroupType::ErasureMirror3dc:
                // In the normal state D=1+1+1, even though W also accepts 2+2.
                doNotNeedToRestore = (layout.GetDisksWithPart(0) && layout.GetDisksWithPart(1)
                    && layout.GetDisksWithPart(2))
                    || (!layout.GetDisksWithPart(0) && !layout.GetDisksWithPart(1) && !layout.GetDisksWithPart(2));
                break;

            case TBlobStorageGroupType::ErasureMirror3of4: {
                auto [data, any] = layout.GetMirror3of4State();
                doNotNeedToRestore = !data || (data >= 3 && any >= 5);
                break;
            }

            default: {
                ui32 numDistinctParts = 0;
                for (ui32 i = 0; i < type.TotalPartCount(); ++i) {
                    if (layout.GetDisksWithPart(i)) {
                        ++numDistinctParts;
                    }
                }
                doNotNeedToRestore = numDistinctParts < type.MinimalRestorablePartCount() || (
                    numDistinctParts == type.TotalPartCount() &&
                    layout.GetInvolvedDisks(&topology).GetNumSetItems() >= type.DataParts() + type.ParityParts() &&
                    layout.CountEffectiveReplicas(type) == numDistinctParts
                );
                break;
            }
        }

        tasks.emplace_back(std::move(orderNumToPart), doNotNeedToRestore);

        bool carry = true;
        for (ui32 i = 0; carry && i < numDisks; ++i) {
            carry = ++indexes[i] == suitablePartsByOrderNum[i].size();
            if (carry) {
                indexes[i] = 0;
            }
        }
        if (carry) {
            break;
        }
    }

    Cerr << "total# " << tasks.size() << Endl;

    ui32 badCases = 0;

#if SINGLE_THREAD
    for (const auto& [orderNumToPart, doNotNeedToRestore] : tasks) {
        badCases += !DoTestCase(erasure, orderNumToPart, doNotNeedToRestore, id, data, &Cerr);
    }
#else
    ui32 done = 0;
    auto func = [&](size_t index) {
        const auto& [orderNumToPart, doNotNeedToRestore] = tasks[index];
        TStringStream stream;
        const bool res = !DoTestCase(erasure, orderNumToPart, doNotNeedToRestore, id, data, &stream);
        return std::make_tuple(stream.Str(), res);
    };
    auto post = [&](const std::tuple<TString, bool>& value) {
        const auto& [log, res] = value;
        Cerr << log;
        badCases += res;
        Cerr << "done " << ++done << " of " << tasks.size() << Endl;
    };
    RunInThreads(tasks.size(), func, post);
#endif

    UNIT_ASSERT_VALUES_EQUAL(badCases, 0);
}

}

Y_UNIT_TEST_SUITE(Osiris) {

    enum class EGuidHistorySupport {
        AllPeers,
        LegacyPeers,
        LegacyRecoveringPeers,
    };

    void TestMirror3dcRealmRecovery(bool wipe, EGuidHistorySupport historySupport = EGuidHistorySupport::AllPeers) {
        using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        const bool legacySources = historySupport == EGuidHistorySupport::LegacyPeers;
        std::array<TFailRealm::EState, 3> states{TFailRealm::N0, TFailRealm::N0, TFailRealm::N0};
        NKikimrBlobStorage::TGroupInfo originalGroup;
        ui32 revision = 0;
        std::map<ui32, ui32> nodeRealms;
        std::array<ui32, 3> osirisPasses{};
        std::array<ui32, 3> lostDataDecisions{};
        std::array<ui32, 3> startupSyncDone{};
        std::array<ui32, 3> guidHistoryRecovered{};
        bool hideRemoteHistory = false;
        std::array<ui32, 3> fullSyncRequests{};
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            const ui32 type = event->GetTypeRewrite();
            if (type == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                auto* record = &event->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (auto& group : *record->MutableServiceSet()->MutableGroups()) {
                    if (!revision) {
                        originalGroup = group;
                    } else if (group.GetGroupID() == originalGroup.GetGroupID()) {
                        group.SetGroupGeneration(originalGroup.GetGroupGeneration() + revision);
                        for (ui32 realm = 0; realm < 3; ++realm) {
                            group.MutableRings(realm)->SetState(states[realm]);
                        }
                    }
                }
            }
            if (type == TEvBlobStorage::EvStartupDataSyncDone && nodeRealms.contains(nodeId)) {
                ++startupSyncDone[nodeRealms.at(nodeId)];
            }
            if (type == TEvBlobStorage::EvVDiskGuidRecovered && nodeRealms.contains(nodeId)) {
                const auto* recovered = event->Get<TEvVDiskGuidRecovered>();
                const auto decision = recovered->Outcome.Decision;
                if (recovered->GuidHistoryComplete) {
                    ++guidHistoryRecovered[nodeRealms.at(nodeId)];
                }
                if (decision == NSyncer::EDecision::LostData) {
                    ++lostDataDecisions[nodeRealms.at(nodeId)];
                }
                Cerr << "GUID recovery node# " << nodeId << " decision# "
                    << NSyncer::EDecisionToStr(decision) << Endl;
            }
            const auto responder = nodeRealms.find(event->Sender.NodeId());
            const bool legacyRecoveringPeer = historySupport == EGuidHistorySupport::LegacyRecoveringPeers
                && responder != nodeRealms.end() && states[responder->second] == TFailRealm::R
                && event->Sender.NodeId() != nodeId;
            if ((hideRemoteHistory || legacySources || legacyRecoveringPeer)
                    && type == TEvBlobStorage::EvVSyncGuidResult) {
                auto& record = event->Get<TEvBlobStorage::TEvVSyncGuidResult>()->Record;
                if (legacySources || legacyRecoveringPeer) {
                    record.ClearGuidHistory();
                } else if (record.HasGuidHistory()) {
                    record.MutableGuidHistory()->Clear();
                }
                record.ClearRecoveredGuid();
            }
            if (type == TEvBlobStorage::EvOsirisDone && nodeRealms.contains(nodeId)) {
                ++osirisPasses[nodeRealms.at(nodeId)];
            }
            if (type == TEvBlobStorage::EvVSyncFull && nodeRealms.contains(event->Sender.NodeId())) {
                ++fullSyncRequests[nodeRealms.at(event->Sender.NodeId())];
            }
            const bool peerRequest = type == TEvBlobStorage::EvVPut || type == TEvBlobStorage::EvVMultiPut
                || type == TEvBlobStorage::EvVGet || type == TEvBlobStorage::EvVSync
                || type == TEvBlobStorage::EvVSyncFull || type == TEvBlobStorage::EvVSyncGuid;
            const auto excluded = [&](ui32 node) {
                const auto it = nodeRealms.find(node);
                return it != nodeRealms.end() && states[it->second] == TFailRealm::F;
            };
            if (peerRequest && (excluded(nodeId) || excluded(event->Sender.NodeId()))) {
                auto undelivered = IEventHandle::ForwardOnNondelivery(std::move(event),
                    TEvents::TEvUndelivered::Disconnected);
                if (undelivered) {
                    env.Runtime->Send(undelivered.release(), nodeId);
                }
                return false;
            }
            return true;
        };
        env.CreateBoxAndPool(1, 1);
        env.UpdateSettings(false, false);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto originalInfo = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(originalGroup.RingsSize(), 3);
        for (ui32 disk = 0; disk < originalInfo->GetTotalVDisksNum(); ++disk) {
            nodeRealms.emplace(originalInfo->GetActorId(disk).NodeId(), originalInfo->GetVDiskId(disk).FailRealm);
        }
        auto setStates = [&](std::array<TFailRealm::EState, 3> next) {
            states = next;
            revision += 100;
            Cerr << "Applying realm states; revision# " << revision << Endl;
            for (ui32 node = 1; node <= 9; ++node) {
                auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, node);
                *update->Record.MutableServiceSet()->AddGroups() = originalGroup;
                env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(node), {}, update.release()), node);
            }
            env.Sim(TDuration::Seconds(5));
            Cerr << "Realm states applied; revision# " << revision << Endl;
        };
        const TString data(4096, 'O');
        const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
        auto checkBlob = [&] {
            ui32 clientNode = 0;
            for (const auto& [node, realm] : nodeRealms) {
                if (states[realm] == TFailRealm::N1) {
                    clientNode = node;
                    break;
                }
            }
            UNIT_ASSERT(clientNode);
            const auto edge = env.Runtime->AllocateEdgeActor(clientNode);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0,
                    env.Runtime->GetClock() + TDuration::Seconds(10), NKikimrBlobStorage::FastRead));
            });
            const auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Responses[0].Buffer.ConvertToString(), data);
        };
        setStates({TFailRealm::N1, TFailRealm::N1, TFailRealm::F});
        env.PutBlob(groupId, blobId, data);
        checkBlob();

        ui32 survivor = 0;
        // Two complete rotations: every realm must become the sole surviving source twice.
        for (ui32 round = 0; round < 6; ++round) {
            const ui32 returning = (survivor + 1) % 3;
            Cerr << "Osiris cycle# " << round << " survivor# " << survivor << " returning# " << returning << Endl;
            std::array<TFailRealm::EState, 3> next{TFailRealm::F, TFailRealm::F, TFailRealm::F};
            next[survivor] = TFailRealm::N1;
            setStates(next);
            checkBlob();

            Cerr << "Fetching slots for wipe" << Endl;
            const auto base = env.FetchBaseConfig();
            Cerr << "Slots fetched" << Endl;
            const ui32 passesBefore = osirisPasses[returning];
            const ui32 lostDataBefore = lostDataDecisions[returning];
            const ui32 startupDoneBefore = startupSyncDone[returning];
            const ui32 historyBefore = guidHistoryRecovered[returning];
            const ui32 requestsBefore = fullSyncRequests[returning];
            for (const auto& slot : base.GetVSlot()) {
                if (wipe && slot.GetGroupId() == groupId && slot.GetFailRealmIdx() == returning) {
                    const auto& location = slot.GetVSlotId();
                    Cerr << "Wiping returning node# " << location.GetNodeId() << Endl;
                    env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(),
                        TVDiskID(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                            slot.GetFailDomainIdx(), slot.GetVDiskIdx()));
                    Cerr << "Wipe accepted for node# " << location.GetNodeId() << Endl;
                }
            }
            next[returning] = TFailRealm::R;
            setStates(next);
            if (!wipe) {
                for (ui32 attempt = 0; attempt < 600 && fullSyncRequests[returning] == requestsBefore; ++attempt) {
                    env.Sim(TDuration::MilliSeconds(100));
                }
                UNIT_ASSERT_C(fullSyncRequests[returning] > requestsBefore,
                    "F->R must trigger full sync even without a wipe");
                checkBlob();
                env.Runtime->FilterFunction = {};
                return;
            }
            for (ui32 attempt = 0; attempt < 1200 && startupSyncDone[returning] < startupDoneBefore + 3; ++attempt) {
                env.Sim(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_C(startupSyncDone[returning] >= startupDoneBefore + 3,
                "Returning realm must finish startup full sync before checking replication; round# " << round);
            UNIT_ASSERT_VALUES_EQUAL_C(guidHistoryRecovered[returning] - historyBefore, legacySources ? 0 : 3,
                "Returning realm must durably restore GUID history; round# " << round);
            auto waitForReplication = [&] {
                // Promote R to N1 only after every returning VDisk reports completed replication.
                bool replicated = false;
                for (ui32 attempt = 0; attempt < 120 && !replicated; ++attempt) {
                    replicated = true;
                    for (ui32 disk = 0; disk < originalInfo->GetTotalVDisksNum(); ++disk) {
                        auto vdiskId = originalInfo->GetVDiskId(disk);
                        if (vdiskId.FailRealm != returning) {
                            continue;
                        }
                        vdiskId.GroupGeneration = originalGroup.GetGroupGeneration() + revision;
                        const auto service = originalInfo->GetActorId(disk);
                        const auto edge = env.Runtime->AllocateEdgeActor(service.NodeId());
                        env.Runtime->Send(new IEventHandle(service, edge,
                            new TEvBlobStorage::TEvVStatus(vdiskId)), service.NodeId());
                        const auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge);
                        const auto& record = response->Get()->Record;
                        replicated &= record.GetStatus() == NKikimrProto::OK && record.GetReplicated();
                    }
                    if (!replicated) {
                        env.Sim(TDuration::Seconds(1));
                    }
                }
                UNIT_ASSERT_C(replicated, "Returning realm must complete replication before R->N1; round# " << round);
            };
            waitForReplication();
            const ui32 expectedPasses = lostDataDecisions[returning] - lostDataBefore;
            UNIT_ASSERT_VALUES_EQUAL_C(osirisPasses[returning] - passesBefore, expectedPasses,
                "Every LostData recovery must finish Osiris; round# " << round);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedPasses, 3,
                "GUID history must prevent FirstRun after every wipe; round# " << round);
            UNIT_ASSERT_C(fullSyncRequests[returning] > requestsBefore,
                "Returning realm must run full sync; round# " << round);
            if (historySupport != EGuidHistorySupport::AllPeers) {
                next[returning] = TFailRealm::N1;
                setStates(next);
                next[survivor] = TFailRealm::F;
                setStates(next);
                checkBlob();
                env.Runtime->FilterFunction = {};
                return;
            }
            // Restart all returning VDisks and prevent sources from replenishing the history.
            // The next rotation can then rely only on history replayed from the local entry point.
            const ui32 restartedHistoryBefore = guidHistoryRecovered[returning];
            const ui32 restartedSyncBefore = startupSyncDone[returning];
            hideRemoteHistory = true;
            for (const auto& [node, realm] : nodeRealms) {
                if (realm == returning) {
                    env.StopNode(node);
                }
            }
            for (const auto& [node, realm] : nodeRealms) {
                if (realm == returning) {
                    env.StartNode(node);
                }
            }
            for (ui32 attempt = 0; attempt < 1200 && (guidHistoryRecovered[returning] < restartedHistoryBefore + 3
                    || startupSyncDone[returning] < restartedSyncBefore + 3); ++attempt) {
                env.Sim(TDuration::MilliSeconds(100));
            }
            hideRemoteHistory = false;
            UNIT_ASSERT_VALUES_EQUAL_C(guidHistoryRecovered[returning] - restartedHistoryBefore, 3,
                "Returning realm must recover GUID history after restart; round# " << round);
            UNIT_ASSERT_C(startupSyncDone[returning] >= restartedSyncBefore + 3,
                "Returning realm must finish startup sync after restart; round# " << round);
            waitForReplication();
            next[returning] = TFailRealm::N1;
            setStates(next);
            next[survivor] = TFailRealm::F;
            setStates(next);
            checkBlob();
            survivor = returning;
        }
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(Mirror3dcCircularRealmRecovery) {
        TestMirror3dcRealmRecovery(true);
    }

    Y_UNIT_TEST(Mirror3dcRealmRecoveryWithoutGuidHistory) {
        TestMirror3dcRealmRecovery(true, EGuidHistorySupport::LegacyPeers);
    }

    Y_UNIT_TEST(Mirror3dcGuidHistoryIgnoresLegacyRecoveringPeers) {
        TestMirror3dcRealmRecovery(true, EGuidHistorySupport::LegacyRecoveringPeers);
    }

    Y_UNIT_TEST(Mirror3dcReturningRealmForcesFullSync) {
        TestMirror3dcRealmRecovery(false);
    }

    Y_UNIT_TEST(Mirror3dcRestoresDurabilityBeyondWriteQuorum) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        const auto info = env.GetGroupInfo(env.GetGroups().front());
        const TString data = "Osiris durability recovery";
        const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
        TBlobStorageGroupInfo::TOrderNums subgroup;
        info->GetTopology().PickSubgroup(blobId.Hash(), subgroup);

        // 2+2 satisfies W, but D=1+1+1 still requires ingress in the third realm.
        TSubgroupPartLayout initialLayout;
        for (const ui32 disk : {0u, 3u, 1u, 4u}) {
            const auto vdiskId = info->GetVDiskId(subgroup[disk]);
            const auto queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0);
            const auto sender = env.Runtime->AllocateEdgeActor(queue.NodeId());
            env.Runtime->Send(new IEventHandle(queue, sender, new TEvBlobStorage::TEvVPut(
                TLogoBlobID(blobId, disk % 3 + 1), TRope(data), vdiskId, false, nullptr,
                TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), sender.NodeId());
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            initialLayout.AddItem(disk, disk % 3, info->Type);
        }
        UNIT_ASSERT(info->GetQuorumChecker().CheckQuorumForSubgroup(
            initialLayout.GetInvolvedDisks(&info->GetTopology())));
        env.Sim(TDuration::Seconds(30));

        const auto recoveringDisk = info->GetVDiskId(subgroup[2]);
        const ui32 recoveringNode = info->GetActorId(subgroup[2]).NodeId();
        ui32 osirisWrites = 0;
        bool osirisDone = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (nodeId == recoveringNode && event->GetTypeRewrite() == TEvBlobStorage::EvAnubisOsirisPut) {
                const auto* put = event->Get<TEvAnubisOsirisPut>();
                if (put->IsOsiris()) {
                    UNIT_ASSERT_VALUES_EQUAL(put->LogoBlobId, TLogoBlobID(blobId, 3));
                    ++osirisWrites;
                }
            }
            if (nodeId == recoveringNode && event->GetTypeRewrite() == TEvBlobStorage::EvOsirisDone) {
                osirisDone = true;
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        const auto config = env.Invoke(request);
        UNIT_ASSERT(config.GetSuccess());
        bool wiped = false;
        for (const auto& slot : config.GetStatus(0).GetBaseConfig().GetVSlot()) {
            const TVDiskID vdiskId(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                slot.GetFailDomainIdx(), slot.GetVDiskIdx());
            if (vdiskId == recoveringDisk) {
                const auto& location = slot.GetVSlotId();
                env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), vdiskId);
                wiped = true;
                break;
            }
        }
        UNIT_ASSERT(wiped);
        for (ui32 attempt = 0; attempt < 600 && !osirisDone; ++attempt) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(osirisDone, "Lost-data recovery must complete its Osiris pass");
        UNIT_ASSERT_VALUES_EQUAL(osirisWrites, 1);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(mirror3dc) { DoTest(TBlobStorageGroupType::ErasureMirror3dc); }
    Y_UNIT_TEST(mirror3of4) { DoTest(TBlobStorageGroupType::ErasureMirror3of4); }
    Y_UNIT_TEST(block42) { DoTest(TBlobStorageGroupType::Erasure4Plus2Block); }

}
