#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCReadOnlyPDisk) {

    Y_UNIT_TEST(ReadOnlyNotAllowed) {
        TEnvironmentSetup env({
            .NodeCount = 10,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        std::unordered_map<TPDiskId, ui64> diskGuids;

        {
            env.CreateBoxAndPool(1, 10);

            env.Sim(TDuration::Seconds(30));

            auto config = env.FetchBaseConfig();

            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk : config.GetPDisk()) {
                TPDiskId diskId(pdisk.GetNodeId(), pdisk.GetPDiskId());

                diskGuids[diskId] = pdisk.GetGuid();
            }

            env.Sim(TDuration::Seconds(30));
        }

        int i = 0;
        auto it = diskGuids.begin();

        for (; it != diskGuids.end(); it++, i++) {
            auto& diskId =  it->first;

            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreDegradedGroupsChecks(true);

            NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
            auto pdiskId = cmd->MutableTargetPDiskId();
            cmd->SetValue(true);
            pdiskId->SetNodeId(diskId.NodeId);
            pdiskId->SetPDiskId(diskId.PDiskId);

            auto response = env.Invoke(request);

            if (i < 2) {
                // Two disks can be set ReadOnly.
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            } else {
                // Restarting third disk will not be allowed.
                UNIT_ASSERT_C(!response.GetSuccess(), "Restart should've been prohibited");

                UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "Disintegrated");
                break;
            }
        }
    }

    class TDummyActor : public TActor<TDummyActor> {
    public:
        TDummyActor() : TActor(&TThis::StateFunc) {}

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            Y_UNUSED(ev);
        }
    };

    void Invoke(TEnvironmentSetup& env, const NKikimrBlobStorage::TConfigRequest& request) {
        TActorId actorId = env.Runtime->Register(new TDummyActor(), TActorId(), 0, std::nullopt, env.Settings.ControllerNodeId);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        env.Runtime->SendToPipe(env.TabletId, actorId, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
    }

    void CheckDiskIsReadOnly(TEnvironmentSetup& env, const TPDiskId& diskId) {
        auto config = env.FetchBaseConfig();
        
        for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk : config.GetPDisk()) {
            if (pdisk.GetNodeId() == diskId.NodeId && pdisk.GetPDiskId() == diskId.PDiskId) {
                UNIT_ASSERT_VALUES_EQUAL(true, pdisk.GetReadOnly());
                break;
            }
        }

        auto stateIt = env.PDiskMockStates.find(std::pair(diskId.NodeId, diskId.PDiskId));

        UNIT_ASSERT(stateIt != env.PDiskMockStates.end());

        UNIT_ASSERT(stateIt->second->IsDiskReadOnly());
    }

    Y_UNIT_TEST(RestartAndReadOnlyConsecutive) {
        // This test ensures that restart that sets disk to read-only is not lost when regular restart is in progress.
        TEnvironmentSetup env({
            .NodeCount = 10,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        std::unordered_map<TPDiskId, ui64> diskGuids;

        {
            env.CreateBoxAndPool(1, 10);

            env.Sim(TDuration::Seconds(30));

            auto config = env.FetchBaseConfig();

            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk : config.GetPDisk()) {
                TPDiskId diskId(pdisk.GetNodeId(), pdisk.GetPDiskId());

                diskGuids[diskId] = pdisk.GetGuid();
            }

            env.Sim(TDuration::Seconds(30));
        }

        auto& diskId = diskGuids.begin()->first;

        {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreDegradedGroupsChecks(true);

            NKikimrBlobStorage::TRestartPDisk* cmd = request.AddCommand()->MutableRestartPDisk();
            auto pdiskId = cmd->MutableTargetPDiskId();
            pdiskId->SetNodeId(diskId.NodeId);
            pdiskId->SetPDiskId(diskId.PDiskId);

            Invoke(env, request);
        }

        {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreDegradedGroupsChecks(true);

            NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
            auto pdiskId = cmd->MutableTargetPDiskId();
            cmd->SetValue(true);
            pdiskId->SetNodeId(diskId.NodeId);
            pdiskId->SetPDiskId(diskId.PDiskId);

            auto response = env.Invoke(request);

            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        TInstant barrier = env.Runtime->GetClock() + TDuration::Minutes(5);

        bool gotReport = false;

        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && !gotReport; }, [&](IEventHandle &witnessedEvent) {
            switch (witnessedEvent.GetTypeRewrite()) {
                case TEvBlobStorage::TEvControllerNodeReport::EventType: {
                    auto *report = witnessedEvent.Get<TEvBlobStorage::TEvControllerNodeReport>();
                    if (report) {
                        auto& reports = report->Record.GetPDiskReports();
                        UNIT_ASSERT_VALUES_EQUAL(1, reports.size());
                        auto& report = reports[0];
                        auto pdiskId = report.GetPDiskId();
                        auto phase = report.GetPhase();
                        UNIT_ASSERT_VALUES_EQUAL(diskId.PDiskId, pdiskId);
                        UNIT_ASSERT_EQUAL(NKikimrBlobStorage::TEvControllerNodeReport::PD_RESTARTED, phase);
                        gotReport = true;
                    }
                    break;
                }
            }
        });

        UNIT_ASSERT(gotReport);

        CheckDiskIsReadOnly(env, diskId);
    }

    Y_UNIT_TEST(ReadOnlyOneByOne) {
        TEnvironmentSetup env({
            .NodeCount = 10,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        std::unordered_map<TPDiskId, ui64> diskGuids;

        {
            env.CreateBoxAndPool(1, 10);

            env.Sim(TDuration::Seconds(30));

            auto config = env.FetchBaseConfig();

            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk : config.GetPDisk()) {
                TPDiskId diskId(pdisk.GetNodeId(), pdisk.GetPDiskId());

                diskGuids[diskId] = pdisk.GetGuid();
            }

            env.Sim(TDuration::Seconds(30));
        }

        int i = 0;
        auto it = diskGuids.begin();

        for (; it != diskGuids.end(); it++, i++) {
            auto& diskId =  it->first;

            for (auto val : {true, false}) {
                NKikimrBlobStorage::TConfigRequest request;
                request.SetIgnoreDegradedGroupsChecks(true);

                NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
                auto pdiskId = cmd->MutableTargetPDiskId();
                cmd->SetValue(val);
                pdiskId->SetNodeId(diskId.NodeId);
                pdiskId->SetPDiskId(diskId.PDiskId);

                Invoke(env, request);

                TInstant barrier = env.Runtime->GetClock() + TDuration::Minutes(5);
                
                bool gotServiceSetUpdate = false;
                bool gotConfigResponse = false;
                env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && (!gotServiceSetUpdate || !gotConfigResponse); }, [&](IEventHandle &witnessedEvent) {
                    switch (witnessedEvent.GetTypeRewrite()) {
                        case TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType: {
                            auto *serviceSetUpdate = witnessedEvent.Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
                            if (serviceSetUpdate) {
                                const auto &pdisks = serviceSetUpdate->Record.GetServiceSet().GetPDisks();
                                const auto &pdisk = pdisks[0];
                                UNIT_ASSERT_EQUAL(NKikimrBlobStorage::INITIAL, pdisk.GetEntityStatus());
                                UNIT_ASSERT_VALUES_EQUAL(val, pdisk.GetReadOnly());
                                gotServiceSetUpdate = true;
                            }
                            break;
                        }
                        case TEvBlobStorage::TEvControllerConfigResponse::EventType: {
                            auto *configResponse = witnessedEvent.Get<TEvBlobStorage::TEvControllerConfigResponse>();
                            if (configResponse) {
                                const auto &response = configResponse->Record.GetResponse();
                                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
                                gotConfigResponse = true;
                            }
                            break;
                        }
                    }
                });

                UNIT_ASSERT(gotServiceSetUpdate);
                UNIT_ASSERT(gotConfigResponse);

                if (val) {
                    CheckDiskIsReadOnly(env, diskId);
                }

                // Wait for VSlot to become ready after PDisk restart due to ReadOnly status being changed.
                env.Sim(TDuration::Seconds(30));
            }
        }
    }

    auto GetGroupVDisks(TEnvironmentSetup& env) {
        struct TVDisk {
            ui32 NodeId;
            ui32 PDiskId;
            ui32 VSlotId;
            TVDiskID VDiskId;
        };

        std::vector<TVDisk> vdisks;

        auto config = env.FetchBaseConfig();

        auto& group = config.get_idx_group(0);
        
        for (auto& vslot : config.GetVSlot()) {
            if (group.GetGroupId() == vslot.GetGroupId()) {
                auto slotId = vslot.GetVSlotId();
                auto nodeId = slotId.GetNodeId();
                auto pdiskId = slotId.GetPDiskId();
                auto vdiskId = TVDiskID(group.GetGroupId(), group.GetGroupGeneration(), vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                vdisks.push_back({nodeId, pdiskId, slotId.GetVSlotId(), vdiskId});
            }
        }

        return vdisks;
    }

    Y_UNIT_TEST(SetBrokenDiskInBrokenGroupReadOnly) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));

        auto vdisks = GetGroupVDisks(env);

        // Making all vdisks bad, group is disintegrated
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        for (auto& pdisk : env.PDiskActors) {
            env.Runtime->WrapInActorContext(sender, [&] () {
                env.Runtime->Send(new IEventHandle(EvBecomeError, 0, pdisk, sender, nullptr, 0));
            });
        }

        env.Sim(TDuration::Minutes(1));

        // Restarting the owner of an already broken disk in a broken group must be allowed
        auto& [targetNodeId, targetPDiskId, unused1, unused2] = vdisks[0];

        NKikimrBlobStorage::TConfigRequest request;

        NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
        auto pdiskId = cmd->MutableTargetPDiskId();
        cmd->SetValue(true);
        pdiskId->SetNodeId(targetNodeId);
        pdiskId->SetPDiskId(targetPDiskId);

        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        // Wait until pdisk restarts and node warden sends "pdisk restarted" to BSC.
        TInstant barrier = env.Runtime->GetClock() + TDuration::Seconds(30);
        bool gotPdiskReport = false;
        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && !gotPdiskReport; }, [&](IEventHandle &witnessedEvent) {
            if (witnessedEvent.GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeReport::EventType) {
                auto *nodeReport = witnessedEvent.Get<TEvBlobStorage::TEvControllerNodeReport>();
                if (nodeReport) {
                    const auto &pdisks = nodeReport->Record.GetPDiskReports();
                    const auto &pdisk = pdisks[0];
                    UNIT_ASSERT(pdisk.GetPhase() == NKikimrBlobStorage::TEvControllerNodeReport_EPDiskPhase_PD_RESTARTED);
                    gotPdiskReport = true;
                }
            }
        });

        UNIT_ASSERT(gotPdiskReport);

        CheckDiskIsReadOnly(env, {targetNodeId, targetPDiskId});
    }

    Y_UNIT_TEST(SetGoodDiskInBrokenGroupReadOnlyNotAllowed) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
    
        // Making all but one vdisks bad, group is disintegrated
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        for (size_t i = 0; i < env.PDiskActors.size() - 1; i++) {
            env.Runtime->WrapInActorContext(sender, [&] () {
                env.Runtime->Send(new IEventHandle(EvBecomeError, 0, env.PDiskActors[i], sender, nullptr, 0));
            });
        }

        env.Sim(TDuration::Minutes(1));

        ui32 targetNodeId = 0;
        ui32 targetPDiskId = 0;

        for (auto& [k, v] : env.PDiskMockStates) {
            if (v.Get()->GetStateErrorReason().empty()) {
                targetNodeId = k.first;
                targetPDiskId = k.second;
            }
        }

        // However making the owner of a single good disk ReadOnly must be prohibited
        NKikimrBlobStorage::TConfigRequest request;

        NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
        auto pdiskId = cmd->MutableTargetPDiskId();
        cmd->SetValue(true);
        pdiskId->SetNodeId(targetNodeId);
        pdiskId->SetPDiskId(targetPDiskId);

        auto response = env.Invoke(request);

        UNIT_ASSERT_C(!response.GetSuccess(), "ReadOnly should've been prohibited");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "Disintegrated");
    }
}
