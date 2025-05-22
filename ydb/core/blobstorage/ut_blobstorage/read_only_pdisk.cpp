#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCReadOnlyPDisk) {

    NKikimrBlobStorage::TConfigRequest CreateReadOnlyRequest(ui32 nodeId, ui32 pdiskId, bool readOnly, bool ignoreDegraded = false) {
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreDegradedGroupsChecks(ignoreDegraded);

        NKikimrBlobStorage::TSetPDiskReadOnly* cmd = request.AddCommand()->MutableSetPDiskReadOnly();
        auto pdisk = cmd->MutableTargetPDiskId();
        cmd->SetValue(readOnly);
        pdisk->SetNodeId(nodeId);
        pdisk->SetPDiskId(pdiskId);

        return std::move(request);
    }

    NKikimrBlobStorage::TConfigResponse SetReadOnly(TEnvironmentSetup& env, ui32 nodeId, ui32 pdiskId, bool readOnly, bool ignoreDegraded = false) {
        NKikimrBlobStorage::TConfigRequest request = CreateReadOnlyRequest(nodeId, pdiskId, readOnly, ignoreDegraded);

        return env.Invoke(request);
    }

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

            auto response = SetReadOnly(env, diskId.NodeId, diskId.PDiskId, true, true);

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
            auto response = SetReadOnly(env, diskId.NodeId, diskId.PDiskId, true, true);

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
                Invoke(env, CreateReadOnlyRequest(diskId.NodeId, diskId.PDiskId, val, true));

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

        auto response = SetReadOnly(env, targetNodeId, targetPDiskId, true);
        
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
        auto response = SetReadOnly(env, targetNodeId, targetPDiskId, true);

        UNIT_ASSERT_C(!response.GetSuccess(), "ReadOnly should've been prohibited");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "Disintegrated");
    }

    Y_UNIT_TEST(ReadOnlySlay) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.EnableDonorMode();
        env.CreateBoxAndPool(2, 1);
        env.CommenceReplication();
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (ui32 i = 0; i < 100; ++i) {
            const TString buffer = TStringBuilder() << "blob number " << i;
            TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // Wait for sync and stuff.
        env.Sim(TDuration::Seconds(3));

        // Move slot out the disk.
        auto info = env.GetGroupInfo(groupId);
        const TVDiskID& vdiskId = info->GetVDiskId(0);
        const TActorId& vdiskActorId = info->GetActorId(0);

        ui32 targetNodeId, targetPDiskId;
        std::tie(targetNodeId, targetPDiskId, std::ignore) = DecomposeVDiskServiceId(vdiskActorId);

        {
            auto response = SetReadOnly(env, targetNodeId, targetPDiskId, true);

            UNIT_ASSERT_C(response.GetSuccess(), "ReadOnly should've been allowed");   
        }
        
        env.SettlePDisk(vdiskActorId);
        env.Sim(TDuration::Seconds(30));

        // Find our donor and acceptor disks.
        auto baseConfig = env.FetchBaseConfig();
        bool found = false;
        std::pair<ui32, ui32> donorPDiskId;
        std::tuple<ui32, ui32, ui32> acceptor;
        std::tuple<ui32, ui32, ui32> donorId;
        for (const auto& slot : baseConfig.GetVSlot()) {
            if (slot.DonorsSize()) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 1);
                const auto& donor = slot.GetDonors(0);
                const auto& id = donor.GetVSlotId();
                UNIT_ASSERT_VALUES_EQUAL(vdiskActorId, MakeBlobStorageVDiskID(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(donor.GetVDiskId()), vdiskId);
                donorPDiskId = {id.GetNodeId(), id.GetPDiskId()};
                donorId = {id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()};
                const auto& acceptorId = slot.GetVSlotId();
                acceptor = {acceptorId.GetNodeId(), acceptorId.GetPDiskId(), acceptorId.GetVSlotId()};
                found = true;
            }
        }
        UNIT_ASSERT(found);

        // Restart with formatting.
        env.Cleanup();
        const size_t num = env.PDiskMockStates.erase(donorPDiskId);
        UNIT_ASSERT_VALUES_EQUAL(num, 1);
        env.Initialize();

        // Wait for initialization.
        env.Sim(TDuration::Seconds(30));

        // Ensure donor finished its job.
        baseConfig = env.FetchBaseConfig();
        found = false;
        for (const auto& slot : baseConfig.GetVSlot()) {
            const auto& id = slot.GetVSlotId();
            if (std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()) == acceptor) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot.GetStatus(), "REPLICATING");
                found = true;
            }
        }
        UNIT_ASSERT(found);

        // Ensure donor was not slain yet.
        TInstant barrier = env.Runtime->GetClock() + TDuration::Minutes(10);
        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier; }, [&](IEventHandle &witnessedEvent) {
            switch (witnessedEvent.GetTypeRewrite()) {
                case TEvBlobStorage::TEvControllerNodeReport::EventType: {
                    UNIT_ASSERT(false);
                    break;
                }
            }
        });

        // Now make disk not read-only so that it can slay donor vdisk.
        {
            auto response = SetReadOnly(env, targetNodeId, targetPDiskId, false);

            UNIT_ASSERT_C(response.GetSuccess(), "ReadOnly should've been allowed");   
        }

        // Ensure donor vdisk was slain.
        barrier = env.Runtime->GetClock() + TDuration::Minutes(10);
        bool gotNodeReport = false;
        env.Runtime->Sim([&] { return env.Runtime->GetClock() <= barrier && (!gotNodeReport); }, [&](IEventHandle &witnessedEvent) {
            switch (witnessedEvent.GetTypeRewrite()) {
                case TEvBlobStorage::TEvControllerNodeReport::EventType: {
                    auto *nodeReport = witnessedEvent.Get<TEvBlobStorage::TEvControllerNodeReport>();
                    if (nodeReport) {
                        const auto& vdisks = nodeReport->Record.GetVDiskReports();
                        if (vdisks.size() == 1) {
                            auto& vdisk = vdisks[0];
                            const auto& vslotId = vdisk.GetVSlotId();
                            std::tuple<ui32, ui32, ui32> vdiskId = {vslotId.GetNodeId(), vslotId.GetPDiskId(), vslotId.GetVSlotId()};
                            UNIT_ASSERT_VALUES_EQUAL(donorId, vdiskId);
                            UNIT_ASSERT_EQUAL(vdisk.GetPhase(), NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED);
                            gotNodeReport = true;
                        }
                    }
                    break;
                }
            }
        });

        UNIT_ASSERT(gotNodeReport);
    }
}
