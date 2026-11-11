#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/library/actors/core/invoke.h>

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

    Y_UNIT_TEST(FirstControllerResponseSurvivesDisconnectDuringCacheWrite) {
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
            .Cache = true,
        }};
        env.CreateBoxAndPool(1, 1, 2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 nodeId = env.GetGroupInfo(groupId)->GetActorId(0).NodeId();
        const ui32 readerNodeId = nodeId == 1 ? 2 : 1;
        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");

        std::unique_ptr<IEventHandle> completion;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNodeId == nodeId && ev->GetTypeRewrite() == TEvents::TEvInvokeResult::EventType) {
                UNIT_ASSERT(!completion);
                completion = std::move(ev);
                return false;
            }
            return true;
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        env.RestartNode(nodeId);
        env.Sim(TDuration::Seconds(1)); // the first response arrives before the five-second cache read
        UNIT_ASSERT(completion);
        env.Runtime->FilterFunction = {};
        env.StopNode(3);
        env.Sim(TDuration::Seconds(10));
        env.Runtime->Send(completion.release(), nodeId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(TryReadBlob(env, groupId, blobId, readerNodeId));
    }

    Y_UNIT_TEST(EmptyComprehensiveResponseSupersedesCache) {
        for (const bool replayCacheFirst : {false, true}) {
            TEnvironmentSetup env{{
                .NodeCount = 3,
                .Erasure = TBlobStorageGroupType::ErasureNone,
                .ControllerNodeId = 3,
                .Cache = true,
            }};
            env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
            env.Sim(TDuration::Seconds(30));
            const ui32 groupId = env.GetGroups().front();
            const auto info = env.GetGroupInfo(groupId);
            const auto vdisk = info->GetActorId(0);
            const ui32 nodeId = vdisk.NodeId();
            const ui32 readerNodeId = nodeId == 1 ? 2 : 1;
            const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
            env.PutBlob(groupId, blobId, "test");
            env.StopNode(3);
            env.RestartNode(nodeId);
            if (replayCacheFirst) {
                env.Sim(TDuration::Seconds(30));
                UNIT_ASSERT(TryReadBlob(env, groupId, blobId, readerNodeId));
            }

            // RegisterNode may return an empty snapshot with no ServiceSet field when the node has
            // no configured services. It must fence a future cache read and retire an earlier replay.
            const auto edge = env.Runtime->AllocateEdgeActor(nodeId);
            auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
            update->Record.SetComprehensive(true);
            UNIT_ASSERT(!update->Record.HasServiceSet());
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge, update.release()), nodeId);
            env.Sim(TDuration::Seconds(30));

            env.Runtime->Send(new IEventHandle(vdisk, edge,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(0)), IEventHandle::FlagTrackDelivery), nodeId);
            const auto gone = env.WaitForEdgeActorEvent<TEvents::TEvUndelivered>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(10));
            UNIT_ASSERT(gone);
        }
    }

    NKikimrBlobStorage::TNodeWardenServiceSet MakeReplacementServices(
            const NKikimrBlobStorage::TNodeWardenCache& cache, ui32 groupId, ui32 pdiskId,
            const TString& diskPath, ui64 guid, ui32 targetGroupId) {
        auto ss = cache.GetServiceSet();
        UNIT_ASSERT_VALUES_EQUAL(ss.PDisksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ss.VDisksSize(), 1);
        ss.ClearGroups();
        for (const auto& group : cache.GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == groupId) {
                ss.AddGroups()->CopyFrom(group);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ss.GroupsSize(), 1);
        auto* pdisk = ss.MutablePDisks(0);
        pdisk->SetPDiskID(pdiskId);
        pdisk->SetPath(diskPath);
        pdisk->SetPDiskGuid(guid);
        auto replaceLocation = [&](auto& loc) {
            loc.SetPDiskID(pdiskId);
            loc.SetPDiskGuid(guid);
            if (TGroupID(targetGroupId).ConfigurationType() == EGroupConfigurationType::Static) {
                loc.SetVDiskSlotID(1); // distconf allocates static slots below BSC's range starting at 1000
            }
        };
        auto* vdisk = ss.MutableVDisks(0);
        vdisk->MutableVDiskID()->SetGroupID(targetGroupId);
        vdisk->MutableVDiskID()->SetGroupGeneration(vdisk->GetVDiskID().GetGroupGeneration() + 1);
        replaceLocation(*vdisk->MutableVDiskLocation());
        auto* group = ss.MutableGroups(0);
        group->SetGroupID(targetGroupId);
        group->SetGroupGeneration(vdisk->GetVDiskID().GetGroupGeneration());
        for (auto& ring : *group->MutableRings()) {
            for (auto& domain : *ring.MutableFailDomains()) {
                for (auto& loc : *domain.MutableVDiskLocations()) {
                    replaceLocation(loc);
                }
            }
        }
        return ss;
    }

    enum class EDelayedControllerUpdate { None, Comprehensive, VDiskOnly, PDiskRestart };

    void CheckStaticReplacement(bool runningNode, bool reusePDiskId, bool replayCacheFirst = true,
            EDelayedControllerUpdate delayedUpdate = EDelayedControllerUpdate::None, bool stopController = true) {
        TFeatureFlags flags;
        flags.SetForceDistconfDisable(true); // this test delivers static configurations itself
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
            .Cache = true,
            .FeatureFlags = flags,
        }};
        env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 nodeId = env.GetGroupInfo(groupId)->GetActorId(0).NodeId();
        NKikimrBlobStorage::TNodeWardenCache cache;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(env.Cache.at(nodeId), &cache));
        const auto& old = cache.GetServiceSet().GetPDisks(0);
        const auto replacement = MakeReplacementServices(cache, groupId, old.GetPDiskID() + !reusePDiskId, old.GetPath(),
            old.GetPDiskGuid() ^ 1, TGroupID(EGroupConfigurationType::Static, 1, 100).GetRaw());
        std::unique_ptr<IEventHandle> delayedCacheResult;
        if (delayedUpdate != EDelayedControllerUpdate::None) {
            env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
                if (recipientNodeId == nodeId && ev->GetTypeRewrite() == TEvents::TEvInvokeResult::EventType) {
                    UNIT_ASSERT(!delayedCacheResult);
                    delayedCacheResult = std::move(ev);
                    return false;
                }
                return true;
            };
            Y_DEFER { env.Runtime->FilterFunction = {}; };
            auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
            if (delayedUpdate == EDelayedControllerUpdate::Comprehensive
                    || delayedUpdate == EDelayedControllerUpdate::PDiskRestart) {
                update->Record.SetComprehensive(true);
                update->Record.MutableServiceSet()->CopyFrom(cache.GetServiceSet());
                if (delayedUpdate == EDelayedControllerUpdate::PDiskRestart) {
                    update->Record.MutableServiceSet()->MutablePDisks(0)->SetEntityStatus(NKikimrBlobStorage::RESTART);
                }
            } else {
                // A read-only slot update need not include its PDisk. It cannot be validated as a
                // cached snapshot after a newer static identity has superseded that disk.
                auto* vdisk = update->Record.MutableServiceSet()->AddVDisks();
                vdisk->CopyFrom(cache.GetServiceSet().GetVDisks(0));
                vdisk->SetReadOnly(true);
            }
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {}, update.release()), nodeId);
            env.Sim(TDuration::Seconds(1));
            UNIT_ASSERT(delayedCacheResult);
        }
        if (stopController) {
            env.StopNode(3);
        }
        if (!runningNode) {
            env.RestartNode(nodeId);
        }
        if (runningNode || replayCacheFirst) {
            env.Sim(TDuration::Seconds(30));
        }

        // BSC's first-free allocator can reuse this ID after host removal. The mock aborts on
        // an unexpected media GUID; model the real PDisk's CORRUPTED response instead.
        ui32 rejectedInitializations = 0;
        ui32 restartRequests = 0;
        ui32 restartReports = 0;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNodeId == nodeId
                    && ev->GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType
                    && ev->Get<TEvBlobStorage::TEvAskWardenRestartPDiskResult>()->PDiskId == old.GetPDiskID()) {
                ++restartRequests;
            }
            if (ev->Sender.NodeId() == nodeId
                    && ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeReport::EventType) {
                for (const auto& report : ev->Get<TEvBlobStorage::TEvControllerNodeReport>()->Record.GetPDiskReports()) {
                    if (report.GetPDiskId() == old.GetPDiskID()
                            && report.GetPhase() == NKikimrBlobStorage::TEvControllerNodeReport::PD_RESTARTED) {
                        ++restartReports;
                    }
                }
            }
            if (recipientNodeId == nodeId && ev->GetTypeRewrite() == NPDisk::TEvYardInit::EventType
                    && ev->Get<NPDisk::TEvYardInit>()->PDiskGuid != old.GetPDiskGuid()) {
                ++rejectedInitializations;
                env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, "PDisk GUID mismatch on reused media"),
                    0, ev->Cookie), nodeId);
                return false;
            }
            return true;
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        const auto edge = env.Runtime->AllocateEdgeActor(nodeId);
        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        config->MutableBlobStorageConfig()->MutableServiceSet()->CopyFrom(replacement);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
            new TEvNodeWardenStorageConfig(config, true, nullptr)), nodeId);
        const auto confirm = env.Runtime->WaitForEdgeActorEvent({edge});
        UNIT_ASSERT_VALUES_EQUAL(confirm->GetTypeRewrite(), ui32(TEvBlobStorage::EvNodeWardenStorageConfigConfirm));
        if (delayedCacheResult) {
            env.Runtime->Send(delayedCacheResult.release(), nodeId);
        }
        env.Sim(TDuration::Seconds(30)); // includes late cache replay in the static-first case
        if (delayedUpdate == EDelayedControllerUpdate::PDiskRestart) {
            UNIT_ASSERT_VALUES_EQUAL(restartRequests, 0);
            UNIT_ASSERT_VALUES_EQUAL(restartReports, 0);
        }
        UNIT_ASSERT(rejectedInitializations);
        const auto& vdisk = replacement.GetVDisks(0);
        const auto& loc = vdisk.GetVDiskLocation();
        env.Runtime->Send(new IEventHandle(MakeBlobStorageVDiskID(nodeId, loc.GetPDiskID(), loc.GetVDiskSlotID()),
            edge, new TEvBlobStorage::TEvVStatus(VDiskIDFromVDiskID(vdisk.GetVDiskID()))), nodeId);
        const auto status = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT(status);
        UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(), NKikimrProto::VDISK_ERROR_STATE);
    }

    Y_UNIT_TEST(StaticReplacementReusesCachedPDiskId) {
        for (const bool replayCacheFirst : {false, true}) {
            CheckStaticReplacement(false, true, replayCacheFirst);
        }
    }

    Y_UNIT_TEST(StaticReplacementWhileControllerDisconnected) {
        CheckStaticReplacement(true, false);
    }

    Y_UNIT_TEST(StaticReplacementReusesConfirmedPDiskIdWhileControllerDisconnected) {
        CheckStaticReplacement(true, true);
    }

    Y_UNIT_TEST(StaticReplacementWhileControllerConnected) {
        for (const bool reusePDiskId : {false, true}) {
            CheckStaticReplacement(true, reusePDiskId, true, EDelayedControllerUpdate::None,
                /*stopController=*/false);
        }
    }

    Y_UNIT_TEST(DelayedControllerResponseWhileConnectedDoesNotResurrectSupersededPDisk) {
        for (const bool reusePDiskId : {false, true}) {
            CheckStaticReplacement(true, reusePDiskId, true, EDelayedControllerUpdate::Comprehensive,
                /*stopController=*/false);
        }
    }

    Y_UNIT_TEST(DelayedVDiskUpdateWhileConnectedDoesNotResurrectSupersededPDisk) {
        for (const bool reusePDiskId : {false, true}) {
            CheckStaticReplacement(true, reusePDiskId, true, EDelayedControllerUpdate::VDiskOnly,
                /*stopController=*/false);
        }
    }

    Y_UNIT_TEST(DelayedPDiskRestartDoesNotResurrectSupersededPDisk) {
        CheckStaticReplacement(true, false, true, EDelayedControllerUpdate::PDiskRestart,
            /*stopController=*/false);
    }

    Y_UNIT_TEST(DelayedPDiskRestartDoesNotResurrectReusedPDiskId) {
        CheckStaticReplacement(true, true, true, EDelayedControllerUpdate::PDiskRestart,
            /*stopController=*/false);
    }

    Y_UNIT_TEST(ConfiguredPDiskRestartPreservesData) {
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
            .Cache = true,
        }};
        env.CreateBoxAndPool(1, 1, 2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 nodeId = env.GetGroupInfo(groupId)->GetActorId(0).NodeId();
        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");
        NKikimrBlobStorage::TNodeWardenCache cache;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(env.Cache.at(nodeId), &cache));
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        auto* pdisk = update->Record.MutableServiceSet()->AddPDisks();
        pdisk->CopyFrom(cache.GetServiceSet().GetPDisks(0));
        pdisk->SetEntityStatus(NKikimrBlobStorage::RESTART);
        ui32 restarts = 0;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNodeId == nodeId
                    && ev->GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType) {
                ++restarts;
            }
            return true;
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {}, update.release()), nodeId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(restarts, 1);
        UNIT_ASSERT(TryReadBlob(env, groupId, blobId, nodeId == 1 ? 2 : 1));
    }

    Y_UNIT_TEST(DelayedControllerResponseDoesNotResurrectSupersededPDisk) {
        CheckStaticReplacement(true, true, true, EDelayedControllerUpdate::Comprehensive);
    }

    Y_UNIT_TEST(DelayedVDiskUpdateDoesNotResurrectSupersededPDisk) {
        for (const bool reusePDiskId : {false, true}) {
            CheckStaticReplacement(true, reusePDiskId, true, EDelayedControllerUpdate::VDiskOnly);
        }
    }

    Y_UNIT_TEST(ControllerUpdateAheadOfStaticConfigIsRestoredAfterCatchUp) {
        TFeatureFlags flags;
        flags.SetForceDistconfDisable(true); // this test delivers static configurations itself
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
            .Cache = true,
            .FeatureFlags = flags,
        }};
        env.CreateBoxAndPool(1, 1, 2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 nodeId = env.GetGroupInfo(groupId)->GetActorId(0).NodeId();
        const ui32 readerNodeId = nodeId == 1 ? 2 : 1;
        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");
        NKikimrBlobStorage::TNodeWardenCache cache;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(env.Cache.at(nodeId), &cache));
        const auto& pdisk = cache.GetServiceSet().GetPDisks(0);

        auto deliverStaticConfig = [&](const NKikimrBlobStorage::TNodeWardenServiceSet& services) {
            const auto edge = env.Runtime->AllocateEdgeActor(nodeId);
            auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
            config->MutableBlobStorageConfig()->MutableServiceSet()->CopyFrom(services);
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
                new TEvNodeWardenStorageConfig(config, true, nullptr)), nodeId);
            const auto confirm = env.Runtime->WaitForEdgeActorEvent({edge});
            UNIT_ASSERT_VALUES_EQUAL(confirm->GetTypeRewrite(), ui32(TEvBlobStorage::EvNodeWardenStorageConfigConfirm));
            env.Sim(TDuration::Seconds(30));
        };

        // The obsolete static configuration holds the path of the disk which BSC knows about.
        deliverStaticConfig(MakeReplacementServices(cache, groupId, pdisk.GetPDiskID() + 1, pdisk.GetPath(),
            pdisk.GetPDiskGuid() ^ 1, TGroupID(EGroupConfigurationType::Static, 1, 100).GetRaw()));
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        update->Record.MutableServiceSet()->CopyFrom(cache.GetServiceSet());
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {}, update.release()), nodeId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(!TryReadBlob(env, groupId, blobId, readerNodeId));

        // Nothing but a new registration can bring the services rejected above.
        deliverStaticConfig({});
        UNIT_ASSERT(TryReadBlob(env, groupId, blobId, readerNodeId));
    }

    Y_UNIT_TEST(StaticReplacementCancelsCachedPDiskWaitingForMetadataReader) {
        TFeatureFlags flags;
        flags.SetForceDistconfDisable(true); // this test delivers static configurations itself
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
            .Cache = true,
            .FeatureFlags = flags,
        }};
        env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 nodeId = env.GetGroupInfo(groupId)->GetActorId(0).NodeId();
        NKikimrBlobStorage::TNodeWardenCache cache;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(env.Cache.at(nodeId), &cache));
        const auto& old = cache.GetServiceSet().GetPDisks(0);
        const ui32 oldId = old.GetPDiskID();
        const TString path = old.GetPath();
        env.StopNode(3);
        env.RestartNode(nodeId);
        const auto edge = env.Runtime->AllocateEdgeActor(nodeId);
        const auto warden = MakeBlobStorageNodeWardenID(nodeId);

        // A successful metadata read keeps its temporary PDisk idle. Cache replay five seconds after
        // boot tries to replace it, but the acknowledgement of temporary-PDisk shutdown is delayed.
        env.Runtime->Send(new IEventHandle(warden, edge, new NStorage::TEvNodeWardenReadMetadata(path)), nodeId);
        const auto metadata = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false);
        UNIT_ASSERT(metadata->Get()->Outcome != NPDisk::EPDiskMetadataOutcome::ERROR);
        std::unique_ptr<IEventHandle> poisonTaken;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNodeId == nodeId && ev->GetTypeRewrite() == TEvents::TEvPoisonTaken::EventType) {
                UNIT_ASSERT(!poisonTaken);
                poisonTaken = std::move(ev);
                return false;
            }
            return true;
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(poisonTaken);

        auto checkVDiskResponds = [&](const auto& ss) {
            const auto& vdisk = ss.GetVDisks(0);
            const auto& loc = vdisk.GetVDiskLocation();
            env.Sim(TDuration::Seconds(5));
            env.Runtime->Send(new IEventHandle(MakeBlobStorageVDiskID(nodeId, loc.GetPDiskID(), loc.GetVDiskSlotID()),
                edge, new TEvBlobStorage::TEvVStatus(VDiskIDFromVDiskID(vdisk.GetVDiskID()))), nodeId);
            UNIT_ASSERT(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, false,
                env.Runtime->GetClock() + TDuration::Seconds(10)));
        };

        const auto replacement = MakeReplacementServices(cache, groupId, oldId + 1, path, old.GetPDiskGuid() ^ 1,
            TGroupID(EGroupConfigurationType::Static, 1, 100).GetRaw());
        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        config->MutableBlobStorageConfig()->MutableServiceSet()->CopyFrom(replacement);
        env.Runtime->Send(new IEventHandle(warden, edge,
            new TEvNodeWardenStorageConfig(config, true, nullptr)), nodeId);
        const auto confirm = env.Runtime->WaitForEdgeActorEvent({edge});
        UNIT_ASSERT_VALUES_EQUAL(confirm->GetTypeRewrite(), ui32(TEvBlobStorage::EvNodeWardenStorageConfigConfirm));
        env.Runtime->FilterFunction = {};
        env.Runtime->Send(poisonTaken.release(), nodeId);
        checkVDiskResponds(replacement);

        // BSC can reuse the cancelled ID on another drive. Its VDisk must start without another
        // service-set replay or a node restart; an orphan waiting marker would block it forever.
        const auto reused = MakeReplacementServices(cache, groupId, oldId, path + ":reused", old.GetPDiskGuid(), groupId);
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        update->Record.SetComprehensive(true);
        update->Record.MutableServiceSet()->CopyFrom(reused);
        env.Runtime->Send(new IEventHandle(warden, edge, update.release()), nodeId);
        checkVDiskResponds(reused);
    }
}
