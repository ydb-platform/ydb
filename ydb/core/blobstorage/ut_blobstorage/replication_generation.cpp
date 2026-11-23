#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>

namespace {

// Hold a real replication plan while its queues connect, optionally replace a
// different group member, then disconnect all sources before granting the plan
// its token. Disks on the disconnected nodes retain their contents.
void CheckReplicationAfterGenerationChange(bool changeGeneration, bool checkAdmission) {
    constexpr ui32 nodeCount = 9;
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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
    bool tokenReceived = false;
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
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplToken && ev->Recipient == replActor) {
            tokenReceived = true;
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
    waitUntil([&] { return tokenReceived; }, "The broker must grant the held replication plan its token");
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
}
