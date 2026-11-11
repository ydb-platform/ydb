#include "bridge_common.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCMovePDisk) {

    using TPDisk = std::pair<ui32, ui32>;

    void WaitForPDiskStop(TEnvironmentSetup& env) {
        TInstant barrier = env.Runtime->GetClock() + TDuration::Seconds(30);
        bool gotPdiskStop = false;
        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && !gotPdiskStop; }, [&](IEventHandle &witnessedEvent) {
            if (witnessedEvent.GetTypeRewrite() == NPDisk::TEvYardControl::EventType) {
                auto *control = witnessedEvent.Get<NPDisk::TEvYardControl>();
                if (control) {
                    UNIT_ASSERT(control->Action == NPDisk::TEvYardControl::EAction::PDiskStop);
                    gotPdiskStop = true;
                }
            }
        });
        UNIT_ASSERT(gotPdiskStop);
    }

    void WaitForPDiskRestart(TEnvironmentSetup& env) {
        TInstant barrier = env.Runtime->GetClock() + TDuration::Seconds(30);
        bool gotPdiskRestart = false;
        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && !gotPdiskRestart; }, [&](IEventHandle &witnessedEvent) {
            if (witnessedEvent.GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType) {
                    gotPdiskRestart = true;
            }
        });
        UNIT_ASSERT(gotPdiskRestart);
    }

    void StopPDisk(TEnvironmentSetup& env, TPDisk pdisk) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *id = request.AddCommand()->MutableStopPDisk()->MutableTargetPDiskId();
        id->SetNodeId(pdisk.first);
        id->SetPDiskId(pdisk.second);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitForPDiskStop(env);
    }

    NKikimrBlobStorage::TConfigResponse InvokeMovePDisk(TEnvironmentSetup& env, TPDisk source, TPDisk destination) {
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreGroupFailModelChecks(true);
        auto *cmd = request.AddCommand()->MutableMovePDisk();
        auto *sourceId = cmd->MutableSourcePDisk()->MutableTargetPDiskId();
        sourceId->SetNodeId(source.first);
        sourceId->SetPDiskId(source.second);
        auto *destinationId = cmd->MutableDestinationPDisk()->MutableTargetPDiskId();
        destinationId->SetNodeId(destination.first);
        destinationId->SetPDiskId(destination.second);
        request.AddCommand()->MutableQueryBaseConfig(); // the config as the move makes it
        return env.Invoke(request);
    }

    // the config reported by QueryBaseConfig in the request moving the PDisk has the generations the groups get when
    // this request is committed, so they are usable in further commands
    void CheckGenerationsReported(TEnvironmentSetup& env, const NKikimrBlobStorage::TConfigResponse& response,
            TPDisk destination) {
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 2);
        const auto& reported = response.GetStatus(1).GetBaseConfig();
        const auto committed = env.FetchBaseConfig();

        std::map<ui32, ui32> groupGenerations;
        for (const auto& group : committed.GetGroup()) {
            groupGenerations.emplace(group.GetGroupId(), group.GetGroupGeneration());
        }
        UNIT_ASSERT_VALUES_EQUAL(reported.GroupSize(), committed.GroupSize());
        for (const auto& group : reported.GetGroup()) {
            UNIT_ASSERT_VALUES_EQUAL_C(group.GetGroupGeneration(), groupGenerations.at(group.GetGroupId()),
                "GroupId# " << group.GetGroupId());
        }

        std::map<std::tuple<ui32, ui32, ui32>, ui32> vslotGenerations;
        for (const auto& vslot : committed.GetVSlot()) {
            const auto& id = vslot.GetVSlotId();
            vslotGenerations.emplace(std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()),
                vslot.GetGroupGeneration());
        }
        UNIT_ASSERT_VALUES_EQUAL(reported.VSlotSize(), committed.VSlotSize());
        size_t numMoved = 0;
        for (const auto& vslot : reported.GetVSlot()) {
            const auto& id = vslot.GetVSlotId();
            const auto key = std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId());
            UNIT_ASSERT_C(vslotGenerations.contains(key), id.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(vslot.GetGroupGeneration(), vslotGenerations.at(key), id.ShortDebugString());
            numMoved += id.GetNodeId() == destination.first && id.GetPDiskId() == destination.second;
        }
        UNIT_ASSERT_C(numMoved, "the config reported doesn't show VSlots on the destination PDisk");
    }

    void RestartPDisk(TEnvironmentSetup& env, TPDisk pdisk) {
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreGroupFailModelChecks(true);
        auto *id = request.AddCommand()->MutableRestartPDisk()->MutableTargetPDiskId();
        id->SetNodeId(pdisk.first);
        id->SetPDiskId(pdisk.second);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    TPDisk FindGroupPDisk(TEnvironmentSetup& env, ui32 groupId) {
        const auto config = env.FetchBaseConfig();
        for (const auto& vslot : config.GetVSlot()) {
            if (vslot.GetGroupId() == groupId) {
                return {vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId()};
            }
        }
        UNIT_FAIL("no PDisk found for GroupId# " << groupId);
        Y_UNREACHABLE();
    }

    TPDisk PrepareDestinationPDisk(TEnvironmentSetup& env, ui32 nodeId) {
        const auto config = env.FetchBaseConfig();
        std::optional<TPDisk> destination;
        for (const auto& pdisk : config.GetPDisk()) {
            if (pdisk.GetNodeId() == nodeId) {
                destination.emplace(nodeId, pdisk.GetPDiskId());
                break;
            }
        }
        UNIT_ASSERT_C(destination, "no PDisk found on NodeId# " << nodeId);

        for (const auto& vslot : config.GetVSlot()) {
            if (vslot.GetVSlotId().GetNodeId() != destination->first ||
                    vslot.GetVSlotId().GetPDiskId() != destination->second) {
                continue;
            }
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreDisintegratedGroupsChecks(true);
            request.SetIgnoreDegradedGroupsChecks(true);
            request.SetIgnoreGroupFailModelChecks(true);
            auto *cmd = request.AddCommand()->MutableReassignGroupDisk();
            cmd->SetGroupId(vslot.GetGroupId());
            cmd->SetGroupGeneration(vslot.GetGroupGeneration());
            cmd->SetFailRealmIdx(vslot.GetFailRealmIdx());
            cmd->SetFailDomainIdx(vslot.GetFailDomainIdx());
            cmd->SetVDiskIdx(vslot.GetVDiskIdx());
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        return *destination;
    }

    void PDiskMoveBasic(TBlobStorageGroupType::EErasureSpecies erasure, ui32 nodeCount, std::pair<ui32, ui32> newDiskId, ui32 numDrivesPerNode = 1) {
        ui32 numDataCenters = erasure == TBlobStorageGroupType::EErasureSpecies::ErasureMirror3dc ? 3 : 1;

        TEnvironmentSetup env({
            .NodeCount = nodeCount,
            .Erasure = erasure,
            .NumDataCenters = numDataCenters,
        });

        env.UpdateSettings(false, false);

        std::optional<NKikimrBlobStorage::TGroupGeometry> geometry;

        if (erasure == TBlobStorageGroupType::EErasureSpecies::ErasureMirror3dc && nodeCount == 3) {
            // Mirror3dc with 3 nodes is a special case, we need to set up the group geometry
            NKikimrBlobStorage::TGroupGeometry g;
            g.SetNumFailRealms(3);
            g.SetNumFailDomainsPerFailRealm(3);
            g.SetNumVDisksPerFailDomain(1);
            g.SetRealmLevelBegin(10);
            g.SetRealmLevelEnd(20);
            g.SetDomainLevelBegin(10);
            g.SetDomainLevelEnd(256);
            geometry = g;
        }

        // Create multiple groups over our pdisks
        env.CreateBoxAndPool(numDrivesPerNode, 4, 0, NKikimrBlobStorage::EPDiskType::ROT, geometry);

        ui32 sourceNodeId = 1;
        ui32 sourcePDiskId = 1000;

        ui32 destinationNodeId = newDiskId.first;
        ui32 destinationPDiskId = newDiskId.second;

        {
            // But move out all the vdisks from the destination pdisk
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableQueryBaseConfig();
            auto response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
            auto& config = response.GetStatus(0).GetBaseConfig();

            for (const auto& vslot : config.GetVSlot()) {
                if (vslot.GetVSlotId().GetNodeId() != destinationNodeId ||
                    vslot.GetVSlotId().GetPDiskId() != destinationPDiskId) {
                    continue;
                }
                NKikimrBlobStorage::TConfigRequest request;
                auto *cmd = request.AddCommand()->MutableReassignGroupDisk();
                request.SetIgnoreDisintegratedGroupsChecks(true);
                request.SetIgnoreDegradedGroupsChecks(true);
                request.SetIgnoreGroupFailModelChecks(true);
                cmd->SetGroupId(vslot.GetGroupId());
                cmd->SetGroupGeneration(vslot.GetGroupGeneration());
                cmd->SetFailRealmIdx(vslot.GetFailRealmIdx());
                cmd->SetFailDomainIdx(vslot.GetFailDomainIdx());
                cmd->SetVDiskIdx(vslot.GetVDiskIdx());
                auto response = env.Invoke(request);
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            }
        }

        // Put some data on the source disk to later read it from the destination disk.
        auto groupId = env.GetGroups()[0];
        TString data = TString::Uninitialized(1024);
        memset(data.Detach(), 1, data.size());
        TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
        env.PutBlob(groupId, id, data);

        const TPDisk source{sourceNodeId, sourcePDiskId};
        const TPDisk destination{destinationNodeId, destinationPDiskId};
        StopPDisk(env, destination);

        auto srcPDiskState = env.PDiskMockStates[std::make_pair(sourceNodeId, sourcePDiskId)];

        {
            // "Attach" source drive to the destination PDisk.
            // This is a mock operation that simulates the drive being moved.
            TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
            auto* evChangeState = new TEvMoveDrive(srcPDiskState);
            TActorId destinationPDiskActorId = MakeBlobStoragePDiskID(destinationNodeId, destinationPDiskId);
            env.Runtime->Send(new IEventHandle(destinationPDiskActorId, edge, evChangeState), destinationNodeId);
        }

        // Execute the move PDisk command. This command will remove source PDisk.
        const auto response = InvokeMovePDisk(env, source, destination);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        // Wait until destination PDisk is restarted.
        WaitForPDiskRestart(env);
        CheckGenerationsReported(env, response, destination);
        RestartPDisk(env, destination);

        // Wait and read the data back from the destination PDisk.
        env.Sim(TDuration::Seconds(30));

        {
            TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
            env.Runtime->WrapInActorContext(edge, [&] {
                auto* ev = new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
                SendToBSProxy(edge, groupId, ev);
            });
            auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            auto res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Status, NKikimrProto::OK);
            TString readData = res->Responses->Buffer.ConvertToString();
            UNIT_ASSERT_VALUES_EQUAL(readData, data);
        }
    }

    Y_UNIT_TEST(PDiskMove_ErasureNone) {
        PDiskMoveBasic(TBlobStorageGroupType::EErasureSpecies::ErasureNone, 2, {2, 1000});
    }

    Y_UNIT_TEST(PDiskMove_Block42) {
        PDiskMoveBasic(TBlobStorageGroupType::EErasureSpecies::Erasure4Plus2Block, 9, {9, 1000});
    }

    Y_UNIT_TEST(PDiskMove_Mirror3dc) {
        PDiskMoveBasic(TBlobStorageGroupType::EErasureSpecies::ErasureMirror3dc, 12, {4, 1000});
    }

    Y_UNIT_TEST(PDiskMove_Mirror3dc3Nodes) {
        PDiskMoveBasic(TBlobStorageGroupType::EErasureSpecies::ErasureMirror3dc, 3, {1, 1003}, 4);
    }

    TEnvironmentSetup::TSettings MakeBridgeSettings() {
        return {
            .NodeCount = 18,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                location.SetBridgePileName(TStringBuilder() << "pile_" << (nodeId - 1) / 9 + 1);
                location.SetDataCenter("my_dc");
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    Y_UNIT_TEST(PDiskMove_Bridge) {
        TEnvironmentSetup env(MakeBridgeSettings());
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto parts = NBridgeTest::GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);

        const TPDisk destination = PrepareDestinationPDisk(env, 18);
        const TPDisk source = FindGroupPDisk(env, parts[1]);
        const auto proxyBefore = NBridgeTest::GetGroupInfo(env, groupId);
        std::vector<ui32> generationsBefore;
        for (ui32 partId : parts) {
            generationsBefore.push_back(NBridgeTest::GetGroupInfo(env, partId).GetGroupGeneration());
        }

        NBridgeTest::TDataChecker checker(env);
        checker.Write(groupId);
        StopPDisk(env, destination);
        auto srcPDiskState = env.PDiskMockStates[std::make_pair(source.first, source.second)];
        TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        auto *evChangeState = new TEvMoveDrive(srcPDiskState);
        TActorId destinationPDiskActorId = MakeBlobStoragePDiskID(destination.first, destination.second);
        env.Runtime->Send(new IEventHandle(destinationPDiskActorId, edge, evChangeState), destination.first);
        const auto response = InvokeMovePDisk(env, source, destination);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitForPDiskRestart(env);
        CheckGenerationsReported(env, response, destination);
        RestartPDisk(env, destination);
        env.Sim(TDuration::Seconds(30));

        const auto proxyAfter = NBridgeTest::GetGroupInfo(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(proxyAfter.GetGroupGeneration(), proxyBefore.GetGroupGeneration() + 1);
        for (size_t i = 0; i < parts.size(); ++i) {
            const auto partAfter = NBridgeTest::GetGroupInfo(env, parts[i]);
            UNIT_ASSERT_VALUES_EQUAL(partAfter.GetGroupGeneration(), generationsBefore[i] + 1);
            UNIT_ASSERT_VALUES_EQUAL(proxyAfter.GetBridgeGroupState().GetPile(i).GetGroupGeneration(),
                partAfter.GetGroupGeneration());
        }
        checker.Write(groupId);
        checker.CheckAll();
    }

    Y_UNIT_TEST(PDiskMove_RejectCrossPileDestination) {
        TEnvironmentSetup env(MakeBridgeSettings());
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto parts = NBridgeTest::GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);

        const TPDisk destination = PrepareDestinationPDisk(env, 18);
        const TPDisk source = FindGroupPDisk(env, parts[0]);
        const auto proxyBefore = NBridgeTest::GetGroupInfo(env, groupId);
        StopPDisk(env, destination);
        const auto response = InvokeMovePDisk(env, source, destination);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "outside its pile");
        const auto proxyAfter = NBridgeTest::GetGroupInfo(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(proxyAfter.GetGroupGeneration(), proxyBefore.GetGroupGeneration());
    }

}
