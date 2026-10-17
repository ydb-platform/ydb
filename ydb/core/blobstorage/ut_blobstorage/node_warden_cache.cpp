#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(NodeWardenCache) {

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

    Y_UNIT_TEST(SingleDcModeFromRecentGroupRestartsVDisk) {
        TEnvironmentSetup env{{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        }};
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(2));
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        const ui32 index = 0;
        const ui32 targetNode = info->GetActorId(index).NodeId();
        const auto serviceId = info->GetActorId(index);
        const ui32 survivingRealm = info->GetVDiskId(index).FailRealm;
        const TString data = "recent group restart";
        const TLogoBlobID blobId(7100, 1, 1, 0, data.size(), 0);
        env.PutBlob(groupId, blobId, data);

        std::set<TActorId> readinessEdges;
        auto frontActor = [&] {
            const auto edge = env.Runtime->AllocateEdgeActor(targetNode);
            readinessEdges.insert(edge);
            auto request = std::make_unique<TEvBlobStorage::TEvVCheckReadiness>();
            VDiskIDFromVDiskID(info->GetVDiskId(index), request->Record.MutableVDiskID());
            env.Runtime->Send(new IEventHandle(serviceId, edge, request.release()), targetNode);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVCheckReadinessResult>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            return result->Sender;
        };

        for (bool enable : {true, false}) {
            const auto previousActor = frontActor();
            const auto previousGeneration = info->GroupGeneration;
            const auto edge = env.Runtime->AllocateEdgeActor(targetNode);
            readinessEdges.insert(edge);
            std::optional<NKikimrBlobStorage::TGroupInfo> recentGroup;
            std::vector<std::unique_ptr<IEventHandle>> updates;
            const auto wardenId = MakeBlobStorageNodeWardenID(targetNode);

            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                    const auto& record = event->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                    bool changed = false;
                    for (const auto& group : record.GetServiceSet().GetGroups()) {
                        if (group.GetGroupID() == groupId && group.GetGroupGeneration() > previousGeneration) {
                            recentGroup = group;
                            changed = true;
                        }
                    }
                    for (const auto& group : record.GetGroupMetadata()) {
                        changed |= group.GetGroupId() == groupId
                            && group.GetCurrentGeneration() > previousGeneration;
                    }
                    if (changed && event->Recipient == wardenId) {
                        return false;
                    }
                } else if (event->GetTypeRewrite() == TEvBlobStorage::TEvUpdateGroupInfo::EventType
                        && event->Recipient == wardenId) {
                    const auto *msg = event->Get<TEvBlobStorage::TEvUpdateGroupInfo>();
                    if (msg->GroupId.GetRawId() == groupId && msg->GroupGeneration > previousGeneration) {
                        updates.push_back(std::move(event));
                        return false;
                    }
                } else if (event->GetTypeRewrite() == NStorage::TEvNodeWardenQueryCacheResult::EventType
                        && event->GetRecipientRewrite().NodeId() == targetNode) {
                    // BSC also publishes group updates through the distributed cache.
                    // Keep that path from restarting the VDisk before RecentGroup arrives.
                    const auto *msg = event->Get<NStorage::TEvNodeWardenQueryCacheResult>();
                    if (msg->Key == Sprintf("G%08" PRIx32, groupId) && msg->GenerationValue
                            && std::get<0>(*msg->GenerationValue) > previousGeneration) {
                        return false;
                    }
                } else if (event->GetTypeRewrite() == TEvBlobStorage::TEvVCheckReadiness::EventType
                        && event->Recipient == serviceId && !readinessEdges.contains(event->Sender)) {
                    return false;
                }
                return true;
            };

            NKikimrBlobStorage::TConfigRequest request;
            auto *command = request.AddCommand()->MutableSetGroupSingleDcMode();
            command->SetGroupId(groupId);
            command->SetGroupGeneration(previousGeneration);
            command->SetEnableSingleDcMode(enable);
            if (enable) {
                command->SetSurvivingDc(survivingRealm);
            }
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            env.Sim(TDuration::Seconds(1));
            UNIT_ASSERT(recentGroup);
            UNIT_ASSERT_VALUES_EQUAL(recentGroup->GetEnableSingleDcMode(), enable);
            UNIT_ASSERT_VALUES_EQUAL(frontActor(), previousActor);

            const auto nextInfo = TBlobStorageGroupInfo::Parse(*recentGroup, nullptr, nullptr);
            auto readiness = std::make_unique<TEvBlobStorage::TEvVCheckReadiness>();
            VDiskIDFromVDiskID(nextInfo->GetVDiskId(index), readiness->Record.MutableVDiskID());
            readiness->Record.MutableRecentGroup()->CopyFrom(*recentGroup);
            env.Runtime->Send(new IEventHandle(serviceId, edge, readiness.release()), targetNode);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVCheckReadinessResult>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::NOTREADY);
            UNIT_ASSERT_VALUES_EQUAL(result->Sender, previousActor);
            UNIT_ASSERT_VALUES_EQUAL(frontActor(), previousActor);
            UNIT_ASSERT_C(!updates.empty(), "RecentGroup must be forwarded to NodeWarden");

            env.Runtime->FilterFunction = {};
            for (auto& update : updates) {
                env.Runtime->Send(update.release(), targetNode);
            }
            info = nextInfo;
            env.Sim(TDuration::Minutes(2));
            UNIT_ASSERT(frontActor() != previousActor);
            UNIT_ASSERT(TryReadBlob(env, groupId, blobId, targetNode));
            env.PutBlob(groupId, TLogoBlobID(7100, 1, enable ? 2 : 3, 0, data.size(), 0), data);
        }
    }
}
