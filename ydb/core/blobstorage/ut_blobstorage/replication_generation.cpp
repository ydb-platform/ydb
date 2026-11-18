#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>

namespace {

TIntrusivePtr<NMonitoring::TDynamicCounters> GetReplCounters(TEnvironmentSetup& env,
        const TIntrusivePtr<TBlobStorageGroupInfo>& info, ui32 vdiskIndex) {
    const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(info->GetActorId(vdiskIndex));
    Y_UNUSED(vslotId);
    const ui32 orderNumber = info->GetTopology().GetOrderNumber(info->GetVDiskId(vdiskIndex));
    auto counters = GetServiceCounters(env.Runtime->GetNode(nodeId)->AppData->Counters, "vdisks");
    for (const auto& [name, value] : std::initializer_list<std::pair<TString, TString>>{
            {"storagePool", env.StoragePoolName},
            {"group", ToString(info->GroupID.GetRawId())},
            {"orderNumber", Sprintf("%02u", orderNumber)},
            {"pdisk", Sprintf("%09u", pdiskId)},
            {"media", "rot"},
            {"subsystem", "repl"}}) {
        counters = counters->FindSubgroup(name, value);
        UNIT_ASSERT_C(counters, "Missing " << name << "=" << value << " for VDisk " << info->GetVDiskId(vdiskIndex));
    }
    return counters;
}

// Hold a real replication plan while its queues connect, optionally replace a
// different group member, then disconnect all sources before granting the plan
// its token. Disks on the disconnected nodes retain their contents.
void CheckReplicationAfterGenerationChange(bool changeGeneration, bool checkAdmission, bool yieldToken = false) {
    constexpr ui32 nodeCount = 9;
    TFeatureFlags featureFlags;
    featureFlags.SetEnableReplTokenPriority(yieldToken);
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .FeatureFlags = std::move(featureFlags),
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const ui32 groupId = env.GetGroups().front();
    auto info = env.GetGroupInfo(groupId);
    const ui32 recoveringNode = info->GetActorId(0).NodeId();
    const TString data(128 * 1024, 'R');
    const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
    env.PutBlob(groupId, blob, data);
    env.WaitForSync(info, blob);

    env.Cleanup();
    for (auto& [key, state] : env.PDiskMockStates) {
        if (key.first == recoveringNode) {
            state.Reset();
        }
    }
    env.Initialize();

    TActorId replActor;
    std::unique_ptr<IEventHandle> started;
    // Track queues by actor identity, independently of their changing VDiskID.
    struct TQueueState {
        TVDiskID VDiskId;
        bool Connected = false;
    };
    std::map<TActorId, std::map<TActorId, TQueueState>> queues;
    ui32 generationSeen = 0;
    ui32 resumes = 0;
    ui32 completedWhileIsolated = 0;
    bool holdPlan = true;
    bool isolated = false;
    ui32 tokenQueries = 0;
    ui32 tokensReceived = 0;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != recoveringNode) {
            return true;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplStarted && holdPlan) {
            UNIT_ASSERT_C(!started, "More than one replication plan on the recovering node");
            replActor = ev->Recipient;
            started = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvProxyQueueState::EventType) {
            const auto* msg = ev->Get<TEvProxyQueueState>();
            queues[ev->Recipient][ev->Sender] = {msg->VDiskId, msg->IsConnected};
        } else if (ev->GetTypeRewrite() == TEvVGenerationChange::EventType && ev->Recipient == replActor) {
            generationSeen = ev->Get<TEvVGenerationChange>()->NewInfo->GroupGeneration;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvQueryReplToken && ev->Sender == replActor) {
            const auto* msg = ev->Get<TEvQueryReplToken>();
            UNIT_ASSERT_VALUES_EQUAL(msg->SupportsYield, yieldToken);
            if (!yieldToken) {
                UNIT_ASSERT_VALUES_EQUAL(msg->Priority, DefaultReplTokenPriority);
            }
            ++tokenQueries;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken && ev->Recipient == replActor) {
            ++tokensReceived;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplResume && ev->Sender == replActor) {
            ++resumes;
        } else if (isolated && ev->GetTypeRewrite() == TEvBlobStorage::EvReplDone
                && ev->Sender == replActor && ev->Cookie == 0) {
            ++completedWhileIsolated;
        }
        return true;
    };
    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    auto connected = [&] {
        ui32 result = 0;
        for (const auto& [actor, state] : queues[replActor]) {
            (void)actor;
            result += state.Connected;
        }
        return result;
    };
    waitUntil([&] { return started && connected() >= info->Type.DataParts(); },
        "Replication plan and connected source queues must exist before reconfiguration");

    if (changeGeneration) {
        std::set<ui32> groupNodes;
        for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
            groupNodes.insert(info->GetActorId(i).NodeId());
        }
        ui32 spare = 1;
        while (groupNodes.contains(spare)) {
            ++spare;
        }
        UNIT_ASSERT(spare <= nodeCount);
        std::optional<ui32> sparePDiskId;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == spare) {
                sparePDiskId = pdisk.GetPDiskId();
                break;
            }
        }
        UNIT_ASSERT_C(sparePDiskId, "The spare node must have a registered PDisk");
        const auto disk = info->GetVDiskId(1);
        NKikimrBlobStorage::TConfigRequest request;
        // the controller may not yet see the restarted disks as ready; only the generation change matters here
        request.SetIgnoreGroupFailModelChecks(true);
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        auto* cmd = request.AddCommand()->MutableReassignGroupDisk();
        cmd->SetGroupId(groupId);
        cmd->SetGroupGeneration(info->GroupGeneration);
        cmd->SetFailRealmIdx(disk.FailRealm);
        cmd->SetFailDomainIdx(disk.FailDomain);
        cmd->SetVDiskIdx(disk.VDisk);
        cmd->MutableTargetPDiskId()->SetNodeId(spare);
        cmd->MutableTargetPDiskId()->SetPDiskId(*sparePDiskId);
        const auto result = env.Invoke(request);
        UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        UNIT_ASSERT_C(result.GetStatus(0).GetSuccess(), result.GetStatus(0).GetErrorDescription());
        waitUntil([&] { return generationSeen > info->GroupGeneration; },
            "The existing replication actor must receive the new generation");
        info = env.GetGroupInfo(groupId);
        waitUntil([&] { return connected() >= info->Type.DataParts(); },
            "Source queues must reconnect after reassignment");
    }

    isolated = true;
    for (ui32 node = 1; node <= nodeCount; ++node) {
        if (node != recoveringNode) {
            env.StopNode(node);
        }
    }
    waitUntil([&] { return connected() == 0; }, "All source queues must report disconnected");
    holdPlan = false;
    env.Runtime->Send(started.release(), recoveringNode);
    waitUntil([&] { return tokensReceived; }, "The broker must grant the held replication plan its token");
    UNIT_ASSERT_VALUES_EQUAL(tokenQueries, 1);
    if (yieldToken) {
        const TActorId sender = env.Runtime->AllocateEdgeActor(recoveringNode);
        env.Runtime->Send(new IEventHandle(replActor, sender, new TEvYieldReplToken), recoveringNode);
        waitUntil([&] { return tokensReceived >= 2; },
            "The replication scheduler must release and reacquire a yielded token");
    }
    // ResumeForce deliberately resumes after five seconds even with no peers.
    // This assertion concerns only the earlier connection-based admission.
    env.Sim(TDuration::Seconds(1));
    const ui32 earlyResumes = resumes;
    env.Sim(TDuration::Seconds(30));
    const ui32 falseCompletions = completedWhileIsolated;
    Cerr << "changeGeneration# " << changeGeneration << " earlyResumes# " << earlyResumes
        << " isolatedResumes# " << resumes << " falseCompletions# " << falseCompletions << Endl;
    UNIT_ASSERT_C(resumes, "The forced-resume path must actually exercise recovery without sources");

    isolated = false;
    for (ui32 node = 1; node <= nodeCount; ++node) {
        if (node != recoveringNode) {
            env.StartNode(node);
        }
    }
    env.Runtime->FilterFunction = {};
    for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
        env.WaitForVDiskRepl(info->GetActorId(i), info->GetVDiskId(i));
    }
    const TActorId edge = env.Runtime->AllocateEdgeActor(recoveringNode);
    auto checkData = [&] {
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blob, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    };
    checkData();
    // Once every member reports recovery complete, the group must again
    // tolerate two failed members. Keep the recovering member online.
    std::set<ui32> stopped;
    for (ui32 i = 0; i != info->GetTotalVDisksNum() && stopped.size() < 2; ++i) {
        const ui32 node = info->GetActorId(i).NodeId();
        if (node != recoveringNode && stopped.insert(node).second) {
            env.StopNode(node);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(stopped.size(), 2);
    checkData();
    UNIT_ASSERT_VALUES_EQUAL_C(falseCompletions, 0, "Recovery reported complete while all sources were offline");
    if (checkAdmission) {
        UNIT_ASSERT_VALUES_EQUAL_C(earlyResumes, 0, "Disconnected sources must not bypass WaitQueues");
    }
}

void CheckHigherPriorityReplicationPreemptsSamePDiskHolder() {
    constexpr ui32 nodeCount = 9;
    using TRuntimeFilter = std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>;
    TRuntimeFilter runtimeFilter;
    TFeatureFlags featureFlags;
    featureFlags.SetEnableReplTokenPriority(true);
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .PrepareRuntime = [&](TTestActorSystem& runtime) {
            runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                return runtimeFilter ? runtimeFilter(nodeId, ev) : true;
            };
        },
        .FeatureFlags = std::move(featureFlags),
    });

    // With one drive per node, VDisks from different groups placed on the same node also use the same PDisk and,
    // consequently, compete in one real replication-broker queue.
    env.CreateBoxAndPool(1, 2);
    env.Sim(TDuration::Minutes(1));
    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
    const auto regularGroup = env.GetGroupInfo(groups[0]);
    const auto urgentGroup = env.GetGroupInfo(groups[1]);

    using TPDiskLocation = std::pair<ui32, ui32>;
    auto getLocations = [](const auto& info) {
        std::map<TPDiskLocation, ui32> locations;
        for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
            const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(info->GetActorId(i));
            Y_UNUSED(vslotId);
            locations.emplace(TPDiskLocation{nodeId, pdiskId}, i);
        }
        return locations;
    };
    const auto regularLocations = getLocations(regularGroup);
    const auto urgentLocations = getLocations(urgentGroup);

    std::optional<TPDiskLocation> sharedLocation;
    for (const auto& [location, index] : regularLocations) {
        Y_UNUSED(index);
        if (urgentLocations.contains(location)) {
            sharedLocation = location;
            break;
        }
    }
    UNIT_ASSERT_C(sharedLocation, "The two groups must have VDisks on a common PDisk");

    std::optional<TPDiskLocation> urgentOnlyLocation;
    for (const auto& [location, index] : urgentLocations) {
        Y_UNUSED(index);
        if (!regularLocations.contains(location)) {
            urgentOnlyLocation = location;
            break;
        }
    }
    UNIT_ASSERT_C(urgentOnlyLocation,
        "The urgent group must have a member outside the regular group to create an additional failure");

    const auto [targetNode, targetPDisk] = *sharedLocation;
    const ui32 regularTargetIndex = regularLocations.at(*sharedLocation);
    const ui32 urgentTargetIndex = urgentLocations.at(*sharedLocation);
    const ui32 urgentOnlyNode = urgentOnlyLocation->first;

    const TString regularData(128 * 1024, 'R');
    const TString urgentData(128 * 1024, 'U');
    const TLogoBlobID regularBlob(1, 1, 1, 0, regularData.size(), 0);
    const TLogoBlobID urgentBlob(2, 1, 1, 0, urgentData.size(), 0);
    env.PutBlob(groups[0], regularBlob, regularData);
    env.PutBlob(groups[1], urgentBlob, urgentData);
    env.WaitForSync(regularGroup, regularBlob);
    env.WaitForSync(urgentGroup, urgentBlob);
    // A single 4+2 blob has no main part on two of the eight group members. Use different blob hashes so every
    // target VDisk has actual replication work rather than possibly containing only a handoff for the sample blob.
    for (ui32 step = 2; step <= 16; ++step) {
        const TLogoBlobID regularExtraBlob(1, 1, step, 0, regularData.size(), 0);
        const TLogoBlobID urgentExtraBlob(2, 1, step, 0, urgentData.size(), 0);
        env.PutBlob(groups[0], regularExtraBlob, regularData);
        env.PutBlob(groups[1], urgentExtraBlob, urgentData);
        env.WaitForSync(regularGroup, regularExtraBlob);
        env.WaitForSync(urgentGroup, urgentExtraBlob);
    }

    env.Cleanup();
    env.PDiskMockStates.at(*sharedLocation).Reset();
    env.PDiskMockStates.at(*urgentOnlyLocation).Reset();

    // Contract under test:
    //  * the scheduler recovering the regular group has one failed member and priority 2;
    //  * the scheduler recovering the urgent group has two failed members and priority 1;
    //  * both schedulers request a token for the same physical PDisk;
    //  * when priority 2 already holds that token, priority 1 makes the broker ask the holder to yield;
    //  * the holder releases the token and the broker grants it to priority 1 before priority 2 reacquires it.
    // Initial queries, the first token, and queue-state notifications are detained only to impose that otherwise
    // timing-dependent ordering. VStatus traffic is not touched, so priorities are calculated from the real group
    // state. All actors involved are the production schedulers, queues, and node-local replication broker.
    struct TInitialQuery {
        TReplTokenPriority Priority;
        std::unique_ptr<IEventHandle> Event;
    };
    std::map<TActorId, TInitialQuery> initialQueries;
    std::unique_ptr<IEventHandle> regularToken;
    TActorId regularScheduler;
    TActorId urgentScheduler;
    bool detainInitialQueries = true;
    bool detainRegularToken = true;
    bool detainQueueStates = true;
    std::set<TActorId> brokerRequests;
    std::map<TActorId, std::map<TActorId, std::unique_ptr<IEventHandle>>> queueStates;
    TVector<TString> handoffEvents;
    TVector<TString> executionEvents;
    runtimeFilter = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != targetNode) {
            return true;
        }
        if (detainQueueStates && ev->GetTypeRewrite() == TEvProxyQueueState::EventType) {
            queueStates[ev->Recipient][ev->Sender] = std::move(ev);
            return false;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvQueryReplToken) {
            const auto* msg = ev->Get<TEvQueryReplToken>();
            if (detainInitialQueries && !brokerRequests.contains(ev->Sender)
                    && msg->PDiskId == targetPDisk && msg->SupportsYield) {
                const auto [it, inserted] = initialQueries.emplace(ev->Sender,
                    TInitialQuery{msg->Priority, nullptr});
                UNIT_ASSERT_VALUES_EQUAL(it->second.Priority, msg->Priority);
                if (inserted) {
                    it->second.Event = std::move(ev);
                }
                return false;
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken
                && detainRegularToken && ev->Recipient == regularScheduler) {
            UNIT_ASSERT(!regularToken);
            regularToken = std::move(ev);
            return false;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvYieldReplToken
                && ev->Recipient == regularScheduler) {
            handoffEvents.emplace_back("yield");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReleaseReplToken
                && ev->Sender == regularScheduler) {
            handoffEvents.emplace_back("release");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken
                && ev->Recipient == urgentScheduler) {
            handoffEvents.emplace_back("urgent grant");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplResume && ev->Sender == urgentScheduler) {
            executionEvents.emplace_back("urgent resume");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReleaseReplToken
                && ev->Sender == urgentScheduler) {
            executionEvents.emplace_back("urgent release");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken
                && ev->Recipient == regularScheduler && !detainRegularToken) {
            executionEvents.emplace_back("regular grant");
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplResume && ev->Sender == regularScheduler) {
            executionEvents.emplace_back("regular resume");
        }
        return true;
    };
    env.Initialize();
    // This node belongs to the urgent group but not the regular group, so keeping it offline creates a stable second
    // failure for the urgent group without changing the regular group's health or disconnecting its source queues.
    env.StopNode(urgentOnlyNode);

    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    waitUntil([&] { return initialQueries.size() == 2; },
        "Both real replication schedulers must request their shared-PDisk token");
    for (const auto& [scheduler, query] : initialQueries) {
        if (query.Priority == 1) {
            UNIT_ASSERT(!urgentScheduler);
            urgentScheduler = scheduler;
        } else if (query.Priority == 2) {
            UNIT_ASSERT(!regularScheduler);
            regularScheduler = scheduler;
        } else {
            UNIT_FAIL("Unexpected replication-token priority");
        }
    }
    UNIT_ASSERT(urgentScheduler);
    UNIT_ASSERT(regularScheduler);

    const auto regularReplCounters = GetReplCounters(env, regularGroup, regularTargetIndex);
    const auto urgentReplCounters = GetReplCounters(env, urgentGroup, urgentTargetIndex);
    const auto regularPriority = regularReplCounters->FindCounter("ReplTokenPriority");
    const auto urgentPriority = urgentReplCounters->FindCounter("ReplTokenPriority");
    const auto regularPreemptions = regularReplCounters->FindCounter("ReplTokenPreemptions");
    const auto urgentPreemptions = urgentReplCounters->FindCounter("ReplTokenPreemptions");
    UNIT_ASSERT(regularPriority && urgentPriority && regularPreemptions && urgentPreemptions);
    UNIT_ASSERT_VALUES_EQUAL(regularPriority->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(urgentPriority->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(regularPreemptions->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(urgentPreemptions->Val(), 0);

    auto numHeldConnectedQueues = [&](const TActorId& scheduler) {
        ui32 result = 0;
        for (const auto& [sender, event] : queueStates[scheduler]) {
            Y_UNUSED(sender);
            result += event && event->Get<TEvProxyQueueState>()->IsConnected;
        }
        return result;
    };
    waitUntil([&] {
        return numHeldConnectedQueues(regularScheduler) >= regularGroup->Type.DataParts()
            && numHeldConnectedQueues(urgentScheduler) >= urgentGroup->Type.DataParts();
    }, "Both schedulers must have enough connected queues withheld to enter WaitQueues deterministically");

    auto sendQueryToBroker = [&](TInitialQuery& query, const TActorId& scheduler) {
        brokerRequests.insert(scheduler);
        query.Event.reset();
        env.Runtime->Send(new IEventHandle(MakeBlobStorageReplBrokerID(), scheduler,
            new TEvQueryReplToken(targetPDisk, query.Priority, true)), targetNode);
    };
    sendQueryToBroker(initialQueries.at(regularScheduler), regularScheduler);
    waitUntil([&] { return bool(regularToken); }, "The regular scheduler must receive the token first");

    // Queue notifications are still withheld, so the holder deterministically enters WaitQueues when it sees token.
    detainRegularToken = false;
    env.Runtime->Send(regularToken.release(), targetNode);
    env.Sim(TDuration::MilliSeconds(100));

    detainInitialQueries = false;
    sendQueryToBroker(initialQueries.at(urgentScheduler), urgentScheduler);
    waitUntil([&] { return handoffEvents.size() >= 3; },
        "The regular scheduler must yield the shared-PDisk token to the urgent scheduler");
    UNIT_ASSERT_VALUES_EQUAL(handoffEvents[0], "yield");
    UNIT_ASSERT_VALUES_EQUAL(handoffEvents[1], "release");
    UNIT_ASSERT_VALUES_EQUAL(handoffEvents[2], "urgent grant");
    UNIT_ASSERT_VALUES_EQUAL(regularPreemptions->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(urgentPreemptions->Val(), 0);

    executionEvents.clear(); // discard the initial regular grant replayed through the filter
    detainQueueStates = false;
    for (auto& [scheduler, states] : queueStates) {
        Y_UNUSED(scheduler);
        for (auto& [sender, event] : states) {
            Y_UNUSED(sender);
            if (event) {
                env.Runtime->Send(event.release(), targetNode);
            }
        }
    }
    waitUntil([&] { return executionEvents.size() >= 4; },
        "Urgent replication must run and release the token before regular replication resumes");
    UNIT_ASSERT_VALUES_EQUAL(executionEvents[0], "urgent resume");
    UNIT_ASSERT_VALUES_EQUAL(executionEvents[1], "urgent release");
    UNIT_ASSERT_VALUES_EQUAL(executionEvents[2], "regular grant");
    UNIT_ASSERT_VALUES_EQUAL(executionEvents[3], "regular resume");

    runtimeFilter = {};
    env.StartNode(urgentOnlyNode);
    for (const auto& info : {regularGroup, urgentGroup}) {
        for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
            env.WaitForVDiskRepl(info->GetActorId(i), info->GetVDiskId(i));
        }
    }

    const TActorId edge = env.Runtime->AllocateEdgeActor(targetNode);
    auto checkData = [&](ui32 groupId, const TLogoBlobID& blob, const TString& data) {
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blob, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    };
    checkData(groups[0], regularBlob, regularData);
    checkData(groups[1], urgentBlob, urgentData);
    UNIT_ASSERT_VALUES_EQUAL(regularPriority->Val(), DefaultReplTokenPriority);
    UNIT_ASSERT_VALUES_EQUAL(urgentPriority->Val(), DefaultReplTokenPriority);
}

void CheckYieldWaitsForActiveQuantumBoundary() {
    constexpr ui32 nodeCount = 9;
    using TRuntimeFilter = std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>;
    TRuntimeFilter runtimeFilter;
    TFeatureFlags featureFlags;
    featureFlags.SetEnableReplTokenPriority(true);
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .PrepareRuntime = [&](TTestActorSystem& runtime) {
            runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                return runtimeFilter ? runtimeFilter(nodeId, ev) : true;
            };
        },
        .FeatureFlags = std::move(featureFlags),
        // Keep a recovery pass longer than one quantum without making the test write hundreds of megabytes.
        .ReplMaxQuantumBytes = 64_KB,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));

    const ui32 groupId = env.GetGroups().front();
    const auto info = env.GetGroupInfo(groupId);
    const auto [targetNode, targetPDisk, targetVSlot] = DecomposeVDiskServiceId(info->GetActorId(0));
    Y_UNUSED(targetVSlot);
    const TString data(128_KB, 'Q');
    TVector<TLogoBlobID> blobs;
    for (ui32 step = 1; step <= 32; ++step) {
        blobs.emplace_back(1, 1, step, 0, data.size(), 0);
        env.PutBlob(groupId, blobs.back(), data);
        env.WaitForSync(info, blobs.back());
    }

    env.Cleanup();
    env.PDiskMockStates.at({targetNode, targetPDisk}).Reset();

    TActorId scheduler;
    TActorId waiter;
    std::unique_ptr<IEventHandle> quantumFinished;
    std::unique_ptr<IEventHandle> waiterToken;
    bool detainQuantumFinished = true;
    ui32 schedulerGrants = 0;
    bool schedulerResumed = false;
    bool yieldReceived = false;
    bool schedulerReleased = false;
    bool waiterGranted = false;
    runtimeFilter = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != targetNode) {
            return true;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvQueryReplToken) {
            const auto* msg = ev->Get<TEvQueryReplToken>();
            if (msg->PDiskId == targetPDisk && msg->SupportsYield) {
                if (!scheduler) {
                    scheduler = ev->Sender;
                }
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, scheduler);
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken && ev->Recipient == scheduler) {
            ++schedulerGrants;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplResume && ev->Sender == scheduler) {
            schedulerResumed = true;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplFinished
                && ev->Recipient == scheduler && schedulerResumed && detainQuantumFinished && !quantumFinished
                && !ev->Get<TEvReplFinished>()->Info->Eof) {
            // Keep the scheduler in Replication while the broker asks it to yield. This makes the quantum boundary
            // observable: a correct scheduler records the request but must not release the token yet.
            quantumFinished = std::move(ev);
            return false;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvYieldReplToken && ev->Recipient == scheduler) {
            yieldReceived = true;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReleaseReplToken && ev->Sender == scheduler) {
            schedulerReleased = true;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken && waiter
                && ev->Recipient == waiter) {
            waiterGranted = true;
            waiterToken = std::move(ev);
            return false;
        }
        return true;
    };
    env.Initialize();

    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    waitUntil([&] { return quantumFinished != nullptr; },
        "A non-final replication quantum must be active and reach its completion event");
    UNIT_ASSERT_VALUES_EQUAL(schedulerGrants, 1);

    const auto replCounters = GetReplCounters(env, info, 0);
    const auto tokenPriority = replCounters->FindCounter("ReplTokenPriority");
    const auto tokenPreemptions = replCounters->FindCounter("ReplTokenPreemptions");
    UNIT_ASSERT(tokenPriority && tokenPreemptions);
    UNIT_ASSERT_VALUES_EQUAL(tokenPriority->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(tokenPreemptions->Val(), 0);

    waiter = env.Runtime->AllocateEdgeActor(targetNode);
    env.Runtime->Send(new IEventHandle(MakeBlobStorageReplBrokerID(), waiter,
        new TEvQueryReplToken(targetPDisk, 0, false)), targetNode);
    waitUntil([&] { return yieldReceived; }, "A higher-priority waiter must request cooperative yield");

    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT_C(!schedulerReleased,
        "An active replication scheduler must retain its token until the current quantum finishes");
    UNIT_ASSERT_C(!waiterGranted, "The broker must not grant a token that is still held by the scheduler");
    UNIT_ASSERT_VALUES_EQUAL(tokenPreemptions->Val(), 1);

    // A repeated request while the same yield is pending must not report another preemption.
    const TActorId duplicateYieldSender = env.Runtime->AllocateEdgeActor(targetNode);
    env.Runtime->Send(new IEventHandle(scheduler, duplicateYieldSender, new TEvYieldReplToken), targetNode);
    env.Sim(TDuration::MilliSeconds(100));
    UNIT_ASSERT_VALUES_EQUAL(tokenPreemptions->Val(), 1);

    detainQuantumFinished = false;
    env.Runtime->Send(quantumFinished.release(), targetNode);
    waitUntil([&] { return schedulerReleased && waiterGranted; },
        "The scheduler must release the token at the quantum boundary and unblock the higher-priority waiter");
    UNIT_ASSERT(waiterToken);
    waiterToken.reset();
    env.Runtime->Send(new IEventHandle(MakeBlobStorageReplBrokerID(), waiter,
        new TEvReleaseReplToken), targetNode);
    waitUntil([&] { return schedulerGrants >= 2; },
        "The replication scheduler must reacquire the token after the higher-priority request releases it");

    runtimeFilter = {};
    for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
        env.WaitForVDiskRepl(info->GetActorId(i), info->GetVDiskId(i));
    }

    const TActorId edge = env.Runtime->AllocateEdgeActor(targetNode);
    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobs.front(), 0, 0, TInstant::Max(),
            NKikimrBlobStorage::FastRead));
    });
    auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    UNIT_ASSERT_VALUES_EQUAL(tokenPriority->Val(), DefaultReplTokenPriority);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(ReplicationGeneration) {
    Y_UNIT_TEST(DisconnectedSourcesWaitWithoutGenerationChange) {
        CheckReplicationAfterGenerationChange(false, true);
    }

    Y_UNIT_TEST(DisconnectedSourcesWaitAfterGenerationChange) {
        CheckReplicationAfterGenerationChange(true, true);
    }

    // Keep durability independent of the admission assertion: a gate failure
    // must not be reported as evidence of lost data.
    Y_UNIT_TEST(DataSurvivesSourceDisconnectionAfterGenerationChange) {
        CheckReplicationAfterGenerationChange(true, false);
    }

    Y_UNIT_TEST(YieldedTokenIsReacquiredWhileWaitingForSources) {
        CheckReplicationAfterGenerationChange(false, true, true);
    }

    Y_UNIT_TEST(HigherPriorityReplicationPreemptsSamePDiskHolder) {
        CheckHigherPriorityReplicationPreemptsSamePDiskHolder();
    }

    Y_UNIT_TEST(YieldWaitsForActiveQuantumBoundary) {
        CheckYieldWaitsForActiveQuantumBoundary();
    }
}
