#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCMovePDisk) {

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

        {
            // Stop destination PDisk.
            NKikimrBlobStorage::TConfigRequest request;

            NKikimrBlobStorage::TStopPDisk* cmd = request.AddCommand()->MutableStopPDisk();
            auto* pdiskId = cmd->MutableTargetPDiskId();
            pdiskId->SetNodeId(destinationNodeId);
            pdiskId->SetPDiskId(destinationPDiskId);

            auto response = env.Invoke(request);

            UNIT_ASSERT_C(response.GetSuccess(), "Should've stopped");

            WaitForPDiskStop(env);
        }

        auto srcPDiskState = env.PDiskMockStates[std::make_pair(sourceNodeId, sourcePDiskId)];

        {
            // "Attach" source drive to the destination PDisk.
            // This is a mock operation that simulates the drive being moved.
            TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
            auto* evChangeState = new TEvMoveDrive(srcPDiskState);
            TActorId destinationPDiskActorId = MakeBlobStoragePDiskID(destinationNodeId, destinationPDiskId);
            env.Runtime->Send(new IEventHandle(destinationPDiskActorId, edge, evChangeState), destinationNodeId);
        }
        
        {
            // Execute the move PDisk command.
            // This command will remove source PDisk.
            NKikimrBlobStorage::TConfigRequest request;

            NKikimrBlobStorage::TMovePDisk* cmd = request.AddCommand()->MutableMovePDisk();
            request.SetIgnoreGroupFailModelChecks(true);

            auto* sourcePdiskId = cmd->MutableSourcePDisk()->MutableTargetPDiskId();
            sourcePdiskId->SetNodeId(sourceNodeId);
            sourcePdiskId->SetPDiskId(sourcePDiskId);

            auto* destinationPdiskId = cmd->MutableDestinationPDisk()->MutableTargetPDiskId();
            destinationPdiskId->SetNodeId(destinationNodeId);
            destinationPdiskId->SetPDiskId(destinationPDiskId);

            auto response = env.Invoke(request);

            UNIT_ASSERT(response.GetSuccess());
        }

        // Wait until destination PDisk is restarted.
        WaitForPDiskRestart(env);

        {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupFailModelChecks(true);

            NKikimrBlobStorage::TRestartPDisk* cmd = request.AddCommand()->MutableRestartPDisk();
            auto pdiskId = cmd->MutableTargetPDiskId();
            pdiskId->SetNodeId(destinationNodeId);
            pdiskId->SetPDiskId(destinationPDiskId);

            auto response = env.Invoke(request);

            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

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

}
