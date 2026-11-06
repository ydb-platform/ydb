#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_recovery.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_proxyobtain.h>

#include <array>

Y_UNIT_TEST_SUITE(GuidHistory) {

    enum class EScenario {
        AllPeers,
        LegacyNormalPeers,
        LegacyRecoveringPeers,
        DelayedNormalSource,
    };

    void TestRecovery(EScenario scenario) {
        using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        std::array<TFailRealm::EState, 3> states{TFailRealm::N0, TFailRealm::N0, TFailRealm::N0};
        NKikimrBlobStorage::TGroupInfo originalGroup;
        std::map<ui32, ui32> nodeRealms;
        std::map<ui32, ui64> expectedGuids;
        std::array<ui32, 3> historyCompletions{};
        std::array<ui32, 3> guidDecisions{};
        std::array<ui32, 3> lostDataDecisions{};
        std::array<ui32, 3> dataSyncCompletions{};
        std::set<ui32> legacyRecoveringRepliesSeen;
        std::multimap<ui32, std::unique_ptr<IEventHandle>> delayedNormalReplies;
        ui32 revision = 0;
        bool pauseDataSync = false;
        bool hideRemoteHistory = false;
        bool delaySecondNormal = false;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            const ui32 type = event->GetTypeRewrite();
            if (type == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                auto& record = event->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (auto& group : *record.MutableServiceSet()->MutableGroups()) {
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
            const auto responder = nodeRealms.find(event->Sender.NodeId());
            const bool legacyNormalPeer = scenario == EScenario::LegacyNormalPeers
                && responder != nodeRealms.end() && states[responder->second] == TFailRealm::N0;
            const bool legacyRecoveringPeer = scenario == EScenario::LegacyRecoveringPeers
                && responder != nodeRealms.end() && states[responder->second] == TFailRealm::R
                && event->Sender.NodeId() != node;
            if (type == TEvBlobStorage::EvVSyncGuidResult) {
                auto& record = event->Get<TEvBlobStorage::TEvVSyncGuidResult>()->Record;
                if (legacyNormalPeer || legacyRecoveringPeer) {
                    record.ClearGuidHistory();
                    record.ClearRecoveredGuid();
                } else if (hideRemoteHistory) {
                    if (record.HasGuidHistory()) {
                        record.MutableGuidHistory()->Clear();
                    }
                    record.ClearRecoveredGuid();
                }
                if (delaySecondNormal && record.HasReadInfo() && responder != nodeRealms.end()
                        && responder->second == 1 && nodeRealms.contains(node) && nodeRealms.at(node) == 2) {
                    delayedNormalReplies.emplace(node, std::move(event));
                    return false;
                }
                // Exercise an unsupported R reply before normal sources can satisfy the history quorum.
                if (scenario == EScenario::LegacyRecoveringPeers && record.HasReadInfo()
                        && responder != nodeRealms.end() && states[responder->second] == TFailRealm::N0
                        && nodeRealms.contains(node) && states[nodeRealms.at(node)] == TFailRealm::R
                        && !legacyRecoveringRepliesSeen.contains(node)) {
                    delayedNormalReplies.emplace(node, std::move(event));
                    return false;
                }
            }
            if (type == TEvBlobStorage::EvVDiskGuidObtained && scenario == EScenario::LegacyRecoveringPeers) {
                const auto* obtained = event->Get<TEvVDiskGuidObtained>();
                if (!obtained->HasHistory && states[obtained->VDiskId.FailRealm] == TFailRealm::R) {
                    legacyRecoveringRepliesSeen.insert(node);
                    auto [begin, end] = delayedNormalReplies.equal_range(node);
                    while (begin != end) {
                        auto reply = std::move(begin->second);
                        begin = delayedNormalReplies.erase(begin);
                        env.Runtime->Send(reply.release(), node);
                    }
                }
            }
            if (type == TEvBlobStorage::EvVDiskGuidRecovered && nodeRealms.contains(node)) {
                const auto* recovered = event->Get<TEvVDiskGuidRecovered>();
                const ui32 realm = nodeRealms.at(node);
                ++guidDecisions[realm];
                historyCompletions[realm] += recovered->GuidHistoryComplete;
                lostDataDecisions[realm] += recovered->Outcome.Decision == NSyncer::EDecision::LostData;
                if (expectedGuids.contains(node)) {
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(recovered->Outcome.Guid), expectedGuids.at(node));
                }
            }
            if (type == TEvBlobStorage::EvStartupDataSyncDone && nodeRealms.contains(node)) {
                ++dataSyncCompletions[nodeRealms.at(node)];
            }
            const auto excluded = [&](ui32 peer) {
                return nodeRealms.contains(peer) && states[nodeRealms.at(peer)] == TFailRealm::F;
            };
            // Keep data recovery paused: GUID history must recover independently of Hull and Osiris.
            const bool dataRequest = type == TEvBlobStorage::EvVSync || type == TEvBlobStorage::EvVSyncFull;
            const bool guidRequest = type == TEvBlobStorage::EvVSyncGuid;
            bool dropGuidPublication = false;
            if (scenario == EScenario::DelayedNormalSource && guidRequest) {
                const auto& record = event->Get<TEvBlobStorage::TEvVSyncGuid>()->Record;
                dropGuidPublication = record.HasInfo()
                    && VDiskIDFromVDiskID(record.GetSourceVDiskID()).FailRealm == 2
                    && VDiskIDFromVDiskID(record.GetTargetVDiskID()).FailRealm == 0;
            }
            if (dropGuidPublication || (pauseDataSync && dataRequest)
                    || ((dataRequest || guidRequest) && (excluded(node) || excluded(event->Sender.NodeId())))) {
                auto undelivered = IEventHandle::ForwardOnNondelivery(std::move(event),
                    TEvents::TEvUndelivered::Disconnected);
                if (undelivered) {
                    env.Runtime->Send(undelivered.release(), node);
                }
                return false;
            }
            return true;
        };
        env.CreateBoxAndPool(1, 1);
        env.UpdateSettings(false, false);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto info = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(originalGroup.RingsSize(), 3);
        for (ui32 disk = 0; disk < info->GetTotalVDisksNum(); ++disk) {
            nodeRealms.emplace(info->GetActorId(disk).NodeId(), info->GetVDiskId(disk).FailRealm);
        }
        auto readHistory = [&](ui32 owner, ui32 target) {
            auto ownerId = info->GetVDiskId(owner);
            auto targetId = info->GetVDiskId(target);
            ownerId.GroupGeneration = targetId.GroupGeneration = originalGroup.GetGroupGeneration() + revision;
            const auto service = info->GetActorId(target);
            const auto edge = env.Runtime->AllocateEdgeActor(service.NodeId());
            auto query = std::make_unique<TEvBlobStorage::TEvVSyncGuid>(ownerId, targetId);
            query->Record.SetReadGuidHistory(true);
            env.Runtime->Send(new IEventHandle(service, edge, query.release()), service.NodeId());
            const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVSyncGuidResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
            return reply->Get()->Record;
        };
        for (ui32 disk = 0; disk < info->GetTotalVDisksNum(); ++disk) {
            if (info->GetVDiskId(disk).FailRealm == 2) {
                const auto reply = readHistory(disk, scenario == EScenario::DelayedNormalSource ? 3 : 0);
                UNIT_ASSERT_VALUES_EQUAL(reply.GetReadInfo().GetState(), NKikimrBlobStorage::TSyncGuidInfo::Final);
                expectedGuids.emplace(info->GetActorId(disk).NodeId(), reply.GetReadInfo().GetGuid());
                if (scenario == EScenario::DelayedNormalSource) {
                    UNIT_ASSERT_VALUES_EQUAL(readHistory(disk, 0).GetReadInfo().GetState(),
                        NKikimrBlobStorage::TSyncGuidInfo::Empty);
                }
            }
        }
        auto setStates = [&](std::array<TFailRealm::EState, 3> next) {
            states = next;
            revision += 100;
            for (ui32 node = 1; node <= 9; ++node) {
                auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, node);
                *update->Record.MutableServiceSet()->AddGroups() = originalGroup;
                env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(node), {}, update.release()), node);
            }
            env.Sim(TDuration::Seconds(5));
        };
        auto wipeRealm = [&](ui32 realm) {
            const auto base = env.FetchBaseConfig();
            for (const auto& slot : base.GetVSlot()) {
                if (slot.GetGroupId() == groupId && slot.GetFailRealmIdx() == realm) {
                    const auto& location = slot.GetVSlotId();
                    env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(),
                        TVDiskID(groupId, slot.GetGroupGeneration(), realm, slot.GetFailDomainIdx(), slot.GetVDiskIdx()));
                }
            }
        };
        auto waitForDecisions = [&](ui32 realm, ui32 count) {
            for (ui32 attempt = 0; attempt < 600 && lostDataDecisions[realm] < count; ++attempt) {
                env.Sim(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_VALUES_EQUAL(lostDataDecisions[realm], count);
            UNIT_ASSERT_VALUES_EQUAL(guidDecisions[realm], count);
            UNIT_ASSERT_VALUES_EQUAL(historyCompletions[realm],
                scenario == EScenario::LegacyNormalPeers ? 0 : count);
            UNIT_ASSERT_VALUES_EQUAL(dataSyncCompletions[realm], 0);
        };

        pauseDataSync = true;
        if (scenario == EScenario::DelayedNormalSource) {
            // N1 constrains blob placement. GUID publication to B+C is valid even if A never received it.
            setStates({TFailRealm::N1, TFailRealm::N1, TFailRealm::F});
            wipeRealm(2);
            delaySecondNormal = true;
            setStates({TFailRealm::N1, TFailRealm::N1, TFailRealm::R});
            UNIT_ASSERT_VALUES_EQUAL(lostDataDecisions[2], 0);
            UNIT_ASSERT_VALUES_EQUAL(guidDecisions[2], 0);
            UNIT_ASSERT_VALUES_EQUAL(historyCompletions[2], 0);
            UNIT_ASSERT(!delayedNormalReplies.empty());
            delaySecondNormal = false;
            for (auto& [node, reply] : delayedNormalReplies) {
                env.Runtime->Send(reply.release(), node);
            }
            delayedNormalReplies.clear();
            waitForDecisions(2, 3);
            env.Runtime->FilterFunction = {};
            return;
        }
        setStates({TFailRealm::N0, TFailRealm::F, TFailRealm::F});
        wipeRealm(1);
        setStates({TFailRealm::N0, TFailRealm::R, TFailRealm::F});
        waitForDecisions(1, 3);
        UNIT_ASSERT(delayedNormalReplies.empty());
        if (scenario != EScenario::AllPeers) {
            env.Runtime->FilterFunction = {};
            return;
        }

        std::map<ui32, NKikimrBlobStorage::TSyncGuidWitness> savedWitnesses;
        for (ui32 disk = 3; disk < 6; ++disk) {
            const auto reply = readHistory(6, disk);
            UNIT_ASSERT_VALUES_EQUAL(reply.GetReadInfo().GetState(), NKikimrBlobStorage::TSyncGuidInfo::Empty);
            UNIT_ASSERT(reply.HasRecoveredGuid());
            UNIT_ASSERT_VALUES_EQUAL(reply.GetRecoveredGuid().GetGuid(), expectedGuids.at(info->GetActorId(6).NodeId()));
            savedWitnesses.emplace(disk, reply.GetRecoveredGuid());
        }
        hideRemoteHistory = true;
        for (const auto& [node, realm] : nodeRealms) {
            if (realm == 1) {
                env.StopNode(node);
            }
        }
        for (const auto& [node, realm] : nodeRealms) {
            if (realm == 1) {
                env.StartNode(node);
            }
        }
        waitForDecisions(1, 6);
        hideRemoteHistory = false;
        for (const auto& [disk, witness] : savedWitnesses) {
            const auto reply = readHistory(6, disk);
            UNIT_ASSERT_VALUES_EQUAL(reply.GetReadInfo().GetState(), NKikimrBlobStorage::TSyncGuidInfo::Empty);
            UNIT_ASSERT_VALUES_EQUAL(reply.GetRecoveredGuid().SerializeAsString(), witness.SerializeAsString());
        }

        // The next realm must reuse its former GUID using only the persisted witness on realm 1.
        setStates({TFailRealm::F, TFailRealm::N0, TFailRealm::F});
        wipeRealm(2);
        setStates({TFailRealm::F, TFailRealm::N0, TFailRealm::R});
        waitForDecisions(2, 3);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(HistoryQuorumDoesNotUseBlobGuarantees) {
        TestRecovery(EScenario::DelayedNormalSource);
    }

    Y_UNIT_TEST(HistorySurvivesRestartAndRecoversNextRealmGuid) {
        TestRecovery(EScenario::AllPeers);
    }

    Y_UNIT_TEST(LegacyNormalPeersAllowFallback) {
        TestRecovery(EScenario::LegacyNormalPeers);
    }

    Y_UNIT_TEST(LegacyRecoveringPeersDoNotForceFallback) {
        TestRecovery(EScenario::LegacyRecoveringPeers);
    }

    Y_UNIT_TEST(UncommittedReadOnlyWrite) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        });
        env.CreateBoxAndPool(1, 1);
        env.UpdateSettings(false, false);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        auto target = info->GetVDiskId(3);
        bool readOnlySet = false;
        const auto base = env.FetchBaseConfig();
        for (const auto& slot : base.GetVSlot()) {
            if (slot.GetGroupId() == groupId && slot.GetFailRealmIdx() == target.FailRealm
                    && slot.GetFailDomainIdx() == target.FailDomain && slot.GetVDiskIdx() == target.VDisk) {
                const auto& location = slot.GetVSlotId();
                env.SetVDiskReadOnly(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), target, true);
                readOnlySet = true;
            }
        }
        UNIT_ASSERT(readOnlySet);
        env.Sim(TDuration::Seconds(30));
        info = env.GetGroupInfo(groupId);
        target = info->GetVDiskId(3);
        const auto source = info->GetVDiskId(0);
        const auto targetService = info->GetActorId(3);
        auto readHistory = [&] {
            const auto edge = env.Runtime->AllocateEdgeActor(targetService.NodeId());
            auto request = std::make_unique<TEvBlobStorage::TEvVSyncGuid>(source, target);
            request->Record.SetReadGuidHistory(true);
            env.Runtime->Send(new IEventHandle(targetService, edge, request.release()), targetService.NodeId());
            const auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVSyncGuidResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrProto::OK);
            return response->Get()->Record;
        };
        const auto before = readHistory();
        UNIT_ASSERT_VALUES_EQUAL(before.GetReadInfo().GetState(), NKikimrBlobStorage::TSyncGuidInfo::Final);
        const ui64 savedGuid = before.GetReadInfo().GetGuid();
        const ui64 unsavedGuid = savedGuid == 1 ? 2 : 1;
        bool writeObserved = false;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& event) {
            if (node == targetService.NodeId() && event->GetTypeRewrite() == TEvBlobStorage::EvVSyncGuid) {
                const auto& record = event->Get<TEvBlobStorage::TEvVSyncGuid>()->Record;
                if (record.HasInfo()) {
                    // Prevent propagators from replacing the injected value before the read.
                    if (record.GetInfo().GetGuid() != unsavedGuid) {
                        return false;
                    }
                    writeObserved = true;
                }
            }
            return true;
        };
        auto write = std::make_unique<TEvBlobStorage::TEvVSyncGuid>(source, target);
        write->Record.MutableInfo()->SetState(NKikimrBlobStorage::TSyncGuidInfo::Final);
        write->Record.MutableInfo()->SetGuid(unsavedGuid);
        env.Runtime->Send(new IEventHandle(targetService, {}, write.release()), targetService.NodeId());
        env.Sim(TDuration::MilliSeconds(10));
        UNIT_ASSERT(writeObserved);
        const auto after = readHistory();
        UNIT_ASSERT_VALUES_EQUAL(after.GetReadInfo().GetGuid(), savedGuid);
        bool sourceFound = false;
        for (const auto& witness : after.GetGuidHistory().GetWitnesses()) {
            if (TVDiskIdShort(VDiskIDFromVDiskID(witness.GetOwnerVDiskID())) == TVDiskIdShort(source)) {
                sourceFound = true;
                UNIT_ASSERT_VALUES_EQUAL(witness.GetGuid(), savedGuid);
            }
        }
        UNIT_ASSERT(sourceFound);
        env.Runtime->FilterFunction = {};
    }

}
