#include <ydb/core/base/tablet.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(NodeWardenCache) {

    Y_UNIT_TEST(RegistrationSnapshotPrecedesQueuedDiskUpdates) {
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
        }};
        env.CreateBoxAndPool(1, 1, 2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto info = env.GetGroupInfo(groupId);
        const ui32 targetNodeId = info->GetActorId(0).NodeId() == 1 ? 2 : 1;
        const auto baseConfig = env.FetchBaseConfig();
        ui32 targetPDiskId = 0;
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == targetNodeId) {
                targetPDiskId = pdisk.GetPDiskId();
            }
        }
        UNIT_ASSERT(targetPDiskId);

        auto& runtime = *env.Runtime;
        const TActorId sender = runtime.AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        bool requestSeen = false;
        bool restarted = false;
        bool registrationSeen = false;
        bool snapshotReceived = false;
        bool holdCommits = true;
        ui32 reassignStep = 0;
        ui32 registrationStep = 0;
        std::vector<std::unique_ptr<IEventHandle>> commits;
        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType &&
                    event->Sender == sender) {
                requestSeen = true;
            } else if (event->GetTypeRewrite() == TEvTablet::TEvCommit::EventType) {
                const auto* msg = event->Get<TEvTablet::TEvCommit>();
                if (msg->TabletID == env.TabletId && requestSeen) {
                    if (!reassignStep) {
                        reassignStep = msg->Step;
                    } else if (registrationSeen) {
                        registrationStep = msg->Step;
                    }
                }
            } else if (event->GetTypeRewrite() == TEvTablet::TEvCommitResult::EventType) {
                const auto* msg = event->Get<TEvTablet::TEvCommitResult>();
                if (holdCommits && msg->TabletID == env.TabletId && reassignStep && msg->Step >= reassignStep) {
                    commits.push_back(std::move(event));
                    return false;
                }
            } else if (restarted && event->GetTypeRewrite() == TEvBlobStorage::TEvControllerRegisterNode::EventType &&
                    event->Sender.NodeId() == targetNodeId) {
                registrationSeen = true;
            } else if (restarted && nodeId == targetNodeId && event->GetTypeRewrite() ==
                    TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                const auto& record = event->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                snapshotReceived |= record.GetComprehensive();
                UNIT_ASSERT_C(record.GetServiceSet().VDisksSize() == 0 || snapshotReceived,
                    "VDisk delta arrived before registration snapshot: " << record.DebugString());
            }
            return true;
        };
        auto waitFor = [&](auto condition, const char* description) {
            const TInstant deadline = env.Now() + TDuration::Seconds(10);
            while (!condition()) {
                UNIT_ASSERT_C(env.Now() < deadline, description);
                env.Sim(TDuration::MilliSeconds(1));
            }
        };

        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* config = request->Record.MutableRequest();
        config->SetIgnoreGroupFailModelChecks(true);
        config->SetIgnoreDegradedGroupsChecks(true);
        config->SetIgnoreDisintegratedGroupsChecks(true);
        auto* cmd = config->AddCommand()->MutableReassignGroupDisk();
        cmd->SetGroupId(groupId);
        cmd->SetGroupGeneration(info->GroupGeneration);
        cmd->SetFailRealmIdx(0);
        cmd->SetFailDomainIdx(0);
        cmd->SetVDiskIdx(0);
        cmd->SetSuppressDonorMode(true);
        cmd->MutableTargetPDiskId()->SetNodeId(targetNodeId);
        cmd->MutableTargetPDiskId()->SetPDiskId(targetPDiskId);
        runtime.SendToPipe(env.TabletId, sender, request.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());

        // Execute prepares a VDisk-only delta for the old registration. Keep Complete pending.
        waitFor([&] { return !commits.empty(); }, "Timed out waiting for the reassign commit");
        restarted = true;
        env.RestartNode(targetNodeId);
        // RegisterNode.Execute switches the controller to the new pipe, but its full snapshot
        // cannot be sent until its own commit is confirmed.
        waitFor([&] {
            return registrationStep && !commits.empty() &&
                commits.back()->Get<TEvTablet::TEvCommitResult>()->Step >= registrationStep;
        }, "Timed out waiting for the new registration commit");
        UNIT_ASSERT(registrationStep > reassignStep);
        UNIT_ASSERT(!snapshotReceived);

        holdCommits = false;
        for (auto& event : commits) {
            runtime.Send(event.release(), env.Settings.ControllerNodeId);
        }
        commits.clear();
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(sender, false,
            env.Now() + TDuration::Seconds(10));
        UNIT_ASSERT(response);
        UNIT_ASSERT_C(response->Get()->Record.GetResponse().GetSuccess(), response->Get()->Record.DebugString());
        waitFor([&] { return snapshotReceived; }, "Timed out waiting for the registration snapshot");
        const auto current = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(current->GetActorId(0).NodeId(), targetNodeId);
        UNIT_ASSERT_VALUES_EQUAL(current->GroupGeneration, info->GroupGeneration + 1);
        env.PutBlob(groupId, TLogoBlobID(1, 1, 1, 0, 4, 0), "test");
        runtime.FilterFunction = {};
    }

    bool TryReadBlob(TEnvironmentSetup& env, ui32 groupId, const TLogoBlobID& blobId, ui32 senderNodeId) {
        const TActorId sender = env.Runtime->AllocateEdgeActor(senderNodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        if (!res) {
            return false;
        }
        auto *msg = res->Get();
        return msg->Status == NKikimrProto::OK && msg->ResponseSz == 1 && msg->Responses[0].Status == NKikimrProto::OK;
    }

    void DoTestRecoveryWhileBscDown(bool useCache, bool expectReadableAfterRestart) {
        const ui32 controllerNodeId = 3;
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = controllerNodeId,
            .Cache = useCache,
        }};

        env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(info->GetTotalVDisksNum(), 1);
        const ui32 vdiskNodeId = info->GetActorId(0).NodeId();
        UNIT_ASSERT_C(vdiskNodeId != controllerNodeId, "VDisk unexpectedly landed on the controller node");

        const ui32 readerNodeId = vdiskNodeId == 1 ? 2 : 1;

        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");

        UNIT_ASSERT_C(TryReadBlob(env, groupId, blobId, readerNodeId),
            "blob must be readable before the restart");

        env.StopNode(controllerNodeId);

        env.RestartNode(vdiskNodeId);
        env.Sim(TDuration::Seconds(60));

        const bool readable = TryReadBlob(env, groupId, blobId, readerNodeId);
        UNIT_ASSERT_VALUES_EQUAL(readable, expectReadableAfterRestart);
    }

    Y_UNIT_TEST(StorageNodeRecoversFromCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/true, /*expectReadableAfterRestart=*/true);
    }

    Y_UNIT_TEST(StorageNodeStuckWithoutCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/false, /*expectReadableAfterRestart=*/false);
    }
}
