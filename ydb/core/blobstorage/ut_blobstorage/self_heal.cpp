#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/generic/hash_set.h>
#include <util/system/compiler.h>

#include <numeric>

Y_UNIT_TEST_SUITE(SelfHeal) {
    void ChangeDiskStatus(TEnvironmentSetup& env, TPDiskId pdiskId, NKikimrBlobStorage::EDriveStatus status,
            NKikimrBlobStorage::TMaintenanceStatus::E maintenanceStatus) {
        NKikimrBlobStorage::TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableUpdateDriveStatus();
        cmd->MutableHostKey()->SetNodeId(pdiskId.NodeId);
        cmd->SetPDiskId(pdiskId.PDiskId);
        cmd->SetStatus(status);
        cmd->SetMaintenanceStatus(maintenanceStatus);
        auto res = env.Invoke(request);
        UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
        UNIT_ASSERT_C(res.GetStatus(0).GetSuccess(), res.GetStatus(0).GetErrorDescription());
    }

    void TestReassignThrottling() {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3dc;
        const ui32 groupsCount = 32;

        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        });

        // create 2 pdisks per node to allow self-healings and
        // allocate groups
        env.CreateBoxAndPool(2, groupsCount);
        env.Sim(TDuration::Minutes(1));

        auto base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), groupsCount);

        ui32 maxReassignsInFlight = 0;

        std::set<TActorId> reassignersInFlight;

        auto catchReassigns = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                for (const auto& command : request.GetCommand()) {
                    if (command.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                        UNIT_ASSERT(!reassignersInFlight.contains(ev->Sender));
                        reassignersInFlight.insert(ev->Sender);
                        maxReassignsInFlight = std::max(maxReassignsInFlight, (ui32)reassignersInFlight.size());
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigResponse::EventType) {
                auto it = reassignersInFlight.find(ev->Recipient);
                if (it != reassignersInFlight.end()) {
                    reassignersInFlight.erase(it);
                }
            }
            return true;
        };

        env.Runtime->FilterFunction = catchReassigns;

        auto pdisk = base.GetPDisk(0);
        // set FAULTY status on the chosen PDisk
        ChangeDiskStatus(env, { pdisk.GetNodeId(), pdisk.GetPDiskId() }, NKikimrBlobStorage::EDriveStatus::FAULTY,
            NKikimrBlobStorage::TMaintenanceStatus::NOT_SET);

        env.Sim(TDuration::Minutes(15));

        UNIT_ASSERT_C(maxReassignsInFlight == 1, "maxReassignsInFlight# " << maxReassignsInFlight);
    }

    Y_UNIT_TEST(ReassignThrottling) {
        TestReassignThrottling();
    }

    struct TPDiskStatus {
        NKikimrBlobStorage::EDriveStatus DriveStatus = NKikimrBlobStorage::ACTIVE;
        NKikimrBlobStorage::TMaintenanceStatus::E MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST;
    };

    using TPDisks = std::vector<TPDiskStatus>;

    constexpr TPDiskStatus Active = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::ACTIVE,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST,
    };

    constexpr TPDiskStatus Inactive = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::INACTIVE,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST,
    };

    constexpr TPDiskStatus Faulty = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::FAULTY,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST,
    };

    constexpr TPDiskStatus ActiveMaintenance = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::ACTIVE,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED,
    };

    constexpr TPDiskStatus InactiveMaintenance = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::INACTIVE,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED,
    };

    constexpr TPDiskStatus FaultyMaintenance = TPDiskStatus{
        .DriveStatus = NKikimrBlobStorage::FAULTY,
        .MaintenanceStatus = NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED,
    };

    void TestMaintenanceRequest(TBlobStorageGroupType erasure, TPDisks pdisks) {
        Y_UNUSED(Inactive);

        const ui32 groupSize = erasure.BlobSubgroupSize();
        Y_VERIFY(pdisks.size() == groupSize, "bad argument");

        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(2, 1);
        env.Sim(TDuration::Minutes(1));

        env.UpdateSettings(false, true, false); // disable self-heal

        auto base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

        std::optional<TBlobStorageGroupInfo::TTopology> topology;
        if (erasure.GetErasure() == TBlobStorageGroupType::Erasure4Plus2Block) {
            topology.emplace(erasure, 1, 8, 1, true);
        } else if (erasure.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
            topology.emplace(erasure, 3, 3, 1, true);
        }

        std::vector<TPDiskId> orderNumberToPDiskId(groupSize);
        std::unordered_map<TPDiskId, ui32> pdiskIdToOrderNumber;

        auto updateMapping = [&](const NKikimrBlobStorage::TBaseConfig& base) {
            pdiskIdToOrderNumber.clear();
            for (const auto& vslot : base.GetVSlot()) {
                TVDiskIdShort vdiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                ui32 orderNumber = topology->GetOrderNumber(vdiskIdShort);
                TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
                orderNumberToPDiskId[orderNumber] = pdiskId;
                pdiskIdToOrderNumber[pdiskId] = orderNumber;
            }

            for (ui32 orderNumber = 0; orderNumber < groupSize; ++orderNumber) {
                TPDiskId pdiskId = orderNumberToPDiskId[orderNumber];
                TPDiskStatus pdiskStatus = pdisks[orderNumber];
                ChangeDiskStatus(env, pdiskId, pdiskStatus.DriveStatus, pdiskStatus.MaintenanceStatus);
            }
        };

        updateMapping(base);

        // check statuses
        base = env.FetchBaseConfig();
        for (const auto& pdisk : base.GetPDisk()) {
            TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
            const auto it = pdiskIdToOrderNumber.find(pdiskId);
            if (it != pdiskIdToOrderNumber.end()) {
                ui32 orderNumber = it->second;
                const TPDiskStatus& expectedStatus = pdisks[orderNumber];
                UNIT_ASSERT(pdisk.GetDriveStatus() == expectedStatus.DriveStatus);
                UNIT_ASSERT(pdisk.GetMaintenanceStatus() == expectedStatus.MaintenanceStatus);
            } else {
                UNIT_ASSERT(pdisk.GetDriveStatus() == NKikimrBlobStorage::ACTIVE);
                UNIT_ASSERT(pdisk.GetMaintenanceStatus() == NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST);
            }
        }

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(180));

        base = env.FetchBaseConfig();
        updateMapping(base);

        for (const auto& pdisk : base.GetPDisk()) {
            TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
            const auto it = pdiskIdToOrderNumber.find(pdiskId);
            if (it != pdiskIdToOrderNumber.end()) {
                UNIT_ASSERT(pdisk.GetDriveStatus() == NKikimrBlobStorage::ACTIVE);
                UNIT_ASSERT(pdisk.GetMaintenanceStatus() == NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST);
            }
        }
    }

#define SELF_HEAL_MAINTENANCE_TEST(name, erasure, pdisks)                           \
    Y_UNIT_TEST(Test##name##erasure) {                                              \
        TestMaintenanceRequest(TBlobStorageGroupType::Erasure##erasure, pdisks);    \
    }

    SELF_HEAL_MAINTENANCE_TEST(OneMaintenanceRequest, Mirror3dc, TPDisks({ Active, Active, Active, ActiveMaintenance, Active, Active, Active, Active, Active }));
    SELF_HEAL_MAINTENANCE_TEST(OneMaintenanceRequest, 4Plus2Block, TPDisks({ Active, Active, ActiveMaintenance, Active, Active, Active, Active, Active }));

    SELF_HEAL_MAINTENANCE_TEST(ThreeMaintenanceRequests, Mirror3dc, TPDisks({ Active, ActiveMaintenance, Active, ActiveMaintenance, Active, Active, Active, ActiveMaintenance, Active }));
    SELF_HEAL_MAINTENANCE_TEST(ThreeMaintenanceRequests, 4Plus2Block, TPDisks({ ActiveMaintenance, ActiveMaintenance, ActiveMaintenance, Active, Active, Active, Active, Active }));

    SELF_HEAL_MAINTENANCE_TEST(TwoMaintenanceRequestsOneFaulty, Mirror3dc, TPDisks({ Active, ActiveMaintenance, Active, ActiveMaintenance, Active, Active, Active, Faulty, Active }));
    SELF_HEAL_MAINTENANCE_TEST(TwoMaintenanceRequestsOneFaulty, 4Plus2Block, TPDisks({ Faulty, ActiveMaintenance, ActiveMaintenance, Active, Active, Active, Active, Active }));

    SELF_HEAL_MAINTENANCE_TEST(OneInactiveMaintenanceRequestOneMaintenanceRequest, Mirror3dc, TPDisks({ Active, Active, Active, Active, Active, InactiveMaintenance, Active, ActiveMaintenance, Active }));
    SELF_HEAL_MAINTENANCE_TEST(OneInactiveMaintenanceRequestOneMaintenanceRequest, 4Plus2Block, TPDisks({ ActiveMaintenance, Active, Active, Active, Active, Active, InactiveMaintenance, Active }));

    SELF_HEAL_MAINTENANCE_TEST(OneFaultyMaintenanceRequestOneMaintenanceRequest, Mirror3dc, TPDisks({ Active, Active, Active, Active, Active, FaultyMaintenance, Active, ActiveMaintenance, Active }));
    SELF_HEAL_MAINTENANCE_TEST(OneFaultyMaintenanceRequestOneMaintenanceRequest, 4Plus2Block, TPDisks({ ActiveMaintenance, Active, Active, Active, Active, Active, FaultyMaintenance, Active }));

    Y_UNIT_TEST(DefaultMaintenanceStatusValue) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3dc;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

        for (const auto& pdisk : base.GetPDisk()) {
            NKikimrBlobStorage::EDriveStatus driveStatus = pdisk.GetDriveStatus();
            NKikimrBlobStorage::TMaintenanceStatus::E maintenanceStatus = pdisk.GetMaintenanceStatus();
            UNIT_ASSERT_C(driveStatus == NKikimrBlobStorage::ACTIVE,
                    "Got DriveStatus# " << NKikimrBlobStorage::EDriveStatus_Name(driveStatus));
            UNIT_ASSERT_C(maintenanceStatus == NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST,
                    "Got MaintenanceStatus# " << NKikimrBlobStorage::TMaintenanceStatus::E_Name(maintenanceStatus));
        }
    }

    Y_UNIT_TEST(MaintenanceStatusNoNewAllocations) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);

        env.UpdateSettings(false, true, false); // disable self-heal

        // set PDisk (9,1000) to ACTIVE + NO_NEW_VDISKS
        ChangeDiskStatus(env, { 9, 1000 }, NKikimrBlobStorage::EDriveStatus::ACTIVE, NKikimrBlobStorage::TMaintenanceStatus::NO_NEW_VDISKS);

        // set PDisk (1,1000) to ACTIVE + LONG_TERM_MAINTENANCE_PLANNED
        ChangeDiskStatus(env, { 1, 1000 }, NKikimrBlobStorage::EDriveStatus::ACTIVE, NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED);

        TActorId reassigner;
        bool reassignSeen = false;
        bool seenReassignFailure = false;

        auto catchReassigns = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (seenReassignFailure) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                for (const auto& command : request.GetCommand()) {
                    if (command.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                        reassignSeen = true;
                        reassigner = ev->Sender;
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigResponse::EventType) {
                if (ev->Recipient != reassigner) {
                    return true;
                }
                const auto& response = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.GetResponse();
                auto& status = response.GetStatus(0);
                bool succeeded = status.GetSuccess();
                UNIT_ASSERT_C(!succeeded, "Reassign was expected to fail");
                seenReassignFailure = true;
            }
            return true;
        };

        auto prevFn = env.Runtime->FilterFunction;
        env.Runtime->FilterFunction = catchReassigns;

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(30));

        {
            auto base = env.FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

            auto& group = base.GetGroup(0);

            bool diskNotMoved = false;

            for (auto& slot : group.GetVSlotId()) {
                TPDiskId pdiskId = { slot.GetNodeId(), slot.GetPDiskId() };
                if (pdiskId == TPDiskId{9, 1000}) {
                    UNIT_ASSERT_C(false, "Expected PDisk (9,1000) to be excluded from group");
                }
                if (pdiskId == TPDiskId{1, 1000}) {
                    diskNotMoved = true;
                }
            }
            UNIT_ASSERT_C(reassignSeen, "Expected to see reassign request");
            UNIT_ASSERT_C(seenReassignFailure, "Expected to see reassign failure");
            UNIT_ASSERT_C(diskNotMoved, "Expected PDisk (1,1000) to not be excluded from group");
        }

        env.Runtime->FilterFunction = prevFn;

        // set PDisk (9,1000) to ACTIVE + NO_REQUEST
        ChangeDiskStatus(env, { 9, 1000 }, NKikimrBlobStorage::EDriveStatus::ACTIVE, NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST);

        env.Sim(TDuration::Seconds(30));

        {
            auto base = env.FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

            auto& group = base.GetGroup(0);

            bool diskMoved = false;

            for (auto& slot : group.GetVSlotId()) {
                TPDiskId pdiskId = { slot.GetNodeId(), slot.GetPDiskId() };
                if (pdiskId == TPDiskId{9, 1000}) {
                    diskMoved = true;
                    break;
                }
            }

            UNIT_ASSERT_C(diskMoved, "Expected PDisk (9,1000) to be included into group");
        }
    }

    Y_UNIT_TEST(SelfHealParameters) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& conf) {
                auto* bscSettings = conf.BlobStorageConfig.MutableBscSettings();
                auto* selfHealSettings = bscSettings->MutableSelfHealSettings();

                selfHealSettings->SetPreferLessOccupiedRack(true);
                selfHealSettings->SetWithAttentionToReplication(true);
            },
        });

        env.CreateBoxAndPool(1, 1);

        env.UpdateSettings(false, true, false); // disable self-heal

        ChangeDiskStatus(env, { 1, 1000 }, NKikimrBlobStorage::EDriveStatus::ACTIVE, NKikimrBlobStorage::TMaintenanceStatus::LONG_TERM_MAINTENANCE_PLANNED);

        bool seenParameters = false;

        auto catchReassigns = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                for (const auto& command : request.GetCommand()) {
                    if (command.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                        auto& reassignCommand = command.GetReassignGroupDisk();
                        if (reassignCommand.GetPreferLessOccupiedRack() && reassignCommand.GetWithAttentionToReplication()) {
                            seenParameters = true;
                        }
                    }
                }
            }
            return true;
        };

        env.Runtime->FilterFunction = catchReassigns;

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(30));

        UNIT_ASSERT(seenParameters);
    }

    TPDiskId GetPDiskIdByVDisk(TEnvironmentSetup& env, ui32 groupId, ui32 ring, ui32 domain, ui32 vdisk) {
        auto base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

        for (const auto& slot : base.GetVSlot()) {
            if (slot.GetGroupId() != groupId) {
                continue;
            }
            ui32 curRing = slot.GetFailRealmIdx();
            ui32 curDomain = slot.GetFailDomainIdx();
            ui32 curVDisk = slot.GetVDiskIdx();
            if (curRing == ring && curDomain == domain && curVDisk == vdisk) {
                return { slot.GetVSlotId().GetNodeId(), slot.GetVSlotId().GetPDiskId() };
            }
        }

        UNIT_FAIL("PDisk for VDisk not found");
        return {};
    }

    auto MakeCatchDiskStatuses = [](ui32 groupId, const THashSet<ui32>& phantomOnlyDomains,
                                    const THashSet<ui32>& faultyDomains,
                                    const THashSet<ui32>& readyFaultyDomains = {}) {
        return [=](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateDiskStatus::EventType) {
                auto* vdiskStatuses = ev->Get<TEvBlobStorage::TEvControllerUpdateDiskStatus>()->Record.MutableVDiskStatus();

                for (auto& status : *vdiskStatuses) {
                    auto& vdiskId = status.GetVDiskId();

                    if (vdiskId.GetGroupID() != groupId) {
                        continue;
                    }

                    const ui32 ring = vdiskId.GetRing();
                    const ui32 domain = vdiskId.GetDomain();
                    const ui32 vdisk = vdiskId.GetVDisk();

                    if (ring != 0 || vdisk != 0) {
                        continue;
                    }

                    if (phantomOnlyDomains.contains(domain)) {
                        // this VDisk is REPLICATING with only phantom blobs remaining
                        status.SetOnlyPhantomsRemain(true);
                        status.SetStatus(NKikimrBlobStorage::EVDiskStatus::REPLICATING);
                    } else if (readyFaultyDomains.contains(domain)) {
                        // this VDisk remains available even though its PDisk is marked FAULTY
                        status.SetOnlyPhantomsRemain(false);
                        status.SetStatus(NKikimrBlobStorage::EVDiskStatus::READY);
                    } else if (faultyDomains.contains(domain)) {
                        // this VDisk's PDisk is FAULTY, so it doesn't report its status
                        return false;
                    }
                }
            }
            return true;
        };
    };

    Y_UNIT_TEST(SelfHealOnlyPhantom) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);

        env.UpdateSettings(false, true, false); // disable self-heal

        const ui32 groupId = env.GetGroups().at(0);

        TPDiskId originalPDiskId = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);

        ChangeDiskStatus(env, originalPDiskId, NKikimrBlobStorage::EDriveStatus::FAULTY, NKikimrBlobStorage::TMaintenanceStatus::NOT_SET);

        env.Runtime->FilterFunction = MakeCatchDiskStatuses(groupId, /*phantomOnlyDomains=*/{0}, /*faultyDomains=*/{1});

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(30));

        TPDiskId newPDiskId = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);

        UNIT_ASSERT_C(newPDiskId != originalPDiskId, "Expected VDisk (0, 1, 0) to be moved");
    }

    Y_UNIT_TEST(SelfHealDoesNotMoveReadyFaultyDiskWithPhantomsOnly) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);
        env.UpdateSettings(false, true, false); // disable self-heal

        const ui32 groupId = env.GetGroups().at(0);
        const TPDiskId originalPDiskId = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);

        ChangeDiskStatus(env, originalPDiskId, NKikimrBlobStorage::EDriveStatus::FAULTY,
            NKikimrBlobStorage::TMaintenanceStatus::NOT_SET);

        env.Runtime->FilterFunction = MakeCatchDiskStatuses(groupId, /*phantomOnlyDomains=*/{0},
            /*faultyDomains=*/{}, /*readyFaultyDomains=*/{1});

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(30));

        const TPDiskId newPDiskId = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);
        UNIT_ASSERT_C(newPDiskId == originalPDiskId, "Expected ready VDisk (0, 1, 0) not to be moved");
    }

    Y_UNIT_TEST(SelfHealWithTwoFailedDisksAndOnePhantomOnly) {
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(1, 1);

        env.UpdateSettings(false, true, false); // disable self-heal

        const ui32 groupId = env.GetGroups().at(0);

        TPDiskId originalPDiskId1 = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);
        TPDiskId originalPDiskId2 = GetPDiskIdByVDisk(env, groupId, 0, 2, 0);

        ChangeDiskStatus(env, originalPDiskId1, NKikimrBlobStorage::EDriveStatus::FAULTY, NKikimrBlobStorage::TMaintenanceStatus::NOT_SET);
        ChangeDiskStatus(env, originalPDiskId2, NKikimrBlobStorage::EDriveStatus::FAULTY, NKikimrBlobStorage::TMaintenanceStatus::NOT_SET);

        env.Runtime->FilterFunction = MakeCatchDiskStatuses(groupId, /*phantomOnlyDomains=*/{0}, /*faultyDomains=*/{1, 2});

        env.UpdateSettings(true, true, false); // enable self-heal
        env.Sim(TDuration::Seconds(30));

        TPDiskId newPDiskId1 = GetPDiskIdByVDisk(env, groupId, 0, 1, 0);
        TPDiskId newPDiskId2 = GetPDiskIdByVDisk(env, groupId, 0, 2, 0);

        UNIT_ASSERT_C(newPDiskId1 == originalPDiskId1, "Expected VDisk (0, 1, 0) not to be moved");
        UNIT_ASSERT_C(newPDiskId2 == originalPDiskId2, "Expected VDisk (0, 2, 0) not to be moved");
    }

    struct TVDiskState {
        NKikimrBlobStorage::EVDiskStatus Status = NKikimrBlobStorage::EVDiskStatus::READY;
        bool OnlyPhantomsRemain = false;

        bool IsReady() const {
            return Status == NKikimrBlobStorage::EVDiskStatus::READY;
        }

        bool IsOperational() const {
            return Status >= NKikimrBlobStorage::EVDiskStatus::REPLICATING;
        }

        bool IsReplicatingWithPhantomsOnly() const {
            return Status == NKikimrBlobStorage::EVDiskStatus::REPLICATING && OnlyPhantomsRemain;
        }
    };

    using TVDiskKey = std::tuple<ui32, ui32, ui32, ui32>;
    using TVDiskStates = std::map<TVDiskKey, TVDiskState>;

    TVDiskKey MakeVDiskKey(ui32 groupId, ui32 ring, ui32 domain, ui32 vdisk) {
        return {groupId, ring, domain, vdisk};
    }

    TVDiskKey MakeVDiskKey(const NKikimrBlobStorage::TBaseConfig::TVSlot& slot) {
        return MakeVDiskKey(slot.GetGroupId(), slot.GetFailRealmIdx(), slot.GetFailDomainIdx(), slot.GetVDiskIdx());
    }

    NKikimrBlobStorage::TConfigResponse InvokeConfigRequest(TEnvironmentSetup& env,
            const NKikimrBlobStorage::TConfigRequest& request, bool selfHeal) {
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        ev->SelfHeal = selfHeal;
        env.Runtime->SendToPipe(env.TabletId, sender, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(sender);
        return response->Get()->Record.GetResponse();
    }

    void ReportVDiskStates(TEnvironmentSetup& env, const NKikimrBlobStorage::TBaseConfig& base,
            const TVDiskStates& states) {
        std::map<std::pair<ui32, ui32>, ui64> pdiskGuids;
        for (const auto& pdisk : base.GetPDisk()) {
            pdiskGuids.emplace(std::make_pair(pdisk.GetNodeId(), pdisk.GetPDiskId()), pdisk.GetGuid());
        }

        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerRegisterNode>();
        ev->Record.SetNodeID(env.Settings.ControllerNodeId);
        for (const auto& slot : base.GetVSlot()) {
            const TVDiskState& state = states.at(MakeVDiskKey(slot));
            const auto& vslotId = slot.GetVSlotId();
            auto *item = ev->Record.AddVDiskStatus();
            item->SetNodeId(vslotId.GetNodeId());
            item->SetPDiskId(vslotId.GetPDiskId());
            item->SetVSlotId(vslotId.GetVSlotId());
            item->SetPDiskGuid(pdiskGuids.at(std::make_pair(vslotId.GetNodeId(), vslotId.GetPDiskId())));
            item->SetStatus(state.Status);
            item->SetOnlyPhantomsRemain(state.OnlyPhantomsRemain);
            auto *vdiskId = item->MutableVDiskId();
            vdiskId->SetGroupID(slot.GetGroupId());
            vdiskId->SetGroupGeneration(slot.GetGroupGeneration());
            vdiskId->SetRing(slot.GetFailRealmIdx());
            vdiskId->SetDomain(slot.GetFailDomainIdx());
            vdiskId->SetVDisk(slot.GetVDiskIdx());
        }
        env.Runtime->SendToPipe(env.TabletId, sender, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(sender);
    }

    enum class EExpectedResult {
        Success,
        Degraded,
        Disintegrated,
        FitDegraded,
        FitDisintegrated,
    };

    EExpectedResult EvaluateReassign(const TBlobStorageGroupInfo::TTopology& topology,
            const TVector<TVDiskState>& states, ui32 targetOrderNumber, bool selfHeal) {
        const TBlobStorageGroupInfo::IQuorumChecker& checker = topology.GetQuorumChecker();
        TBlobStorageGroupInfo::TGroupVDisks nonOperational(&topology);
        for (ui32 orderNumber = 0; orderNumber < states.size(); ++orderNumber) {
            if (!states[orderNumber].IsOperational()) {
                nonOperational |= {&topology, topology.GetVDiskId(orderNumber)};
            }
        }
        if (!checker.CheckFailModelForGroup(nonOperational)) {
            return EExpectedResult::FitDisintegrated;
        } else if (checker.IsDegraded(nonOperational)) {
            return EExpectedResult::FitDegraded;
        }

        TBlobStorageGroupInfo::TGroupVDisks failed(&topology);
        for (ui32 orderNumber = 0; orderNumber < states.size(); ++orderNumber) {
            if (!states[orderNumber].IsReady()) {
                failed |= {&topology, topology.GetVDiskId(orderNumber)};
            }
        }
        failed |= {&topology, topology.GetVDiskId(targetOrderNumber)};

        if (!checker.CheckFailModelForGroup(failed)) {
            return EExpectedResult::Disintegrated;
        }

        if (selfHeal) {
            for (ui32 orderNumber = 0; orderNumber < states.size(); ++orderNumber) {
                if (orderNumber != targetOrderNumber && states[orderNumber].IsReplicatingWithPhantomsOnly()) {
                    failed = failed - TBlobStorageGroupInfo::TGroupVDisks(&topology, topology.GetVDiskId(orderNumber));
                    break;
                }
            }
        }
        return checker.IsDegraded(failed) ? EExpectedResult::Degraded : EExpectedResult::Success;
    }

    void AssertReassignResult(const NKikimrBlobStorage::TConfigResponse& response, EExpectedResult expected,
            ui32 groupId, const TString& context) {
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetSuccess(), expected == EExpectedResult::Success,
            context << " Response# " << response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(response.GroupsGetDegradedSize(), expected == EExpectedResult::Degraded ? 1 : 0,
            context << " Response# " << response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(response.GroupsGetDisintegratedSize(),
            expected == EExpectedResult::Disintegrated ? 1 : 0, context << " Response# " << response.DebugString());
        if (expected == EExpectedResult::Degraded) {
            UNIT_ASSERT_VALUES_EQUAL_C(response.GetGroupsGetDegraded(0), groupId, context);
        } else if (expected == EExpectedResult::Disintegrated) {
            UNIT_ASSERT_VALUES_EQUAL_C(response.GetGroupsGetDisintegrated(0), groupId, context);
        } else if (expected == EExpectedResult::FitDegraded || expected == EExpectedResult::FitDisintegrated) {
            UNIT_ASSERT_VALUES_EQUAL_C(response.StatusSize(), 1, context << " Response# " << response.DebugString());
            const auto failReason = expected == EExpectedResult::FitDegraded
                ? NKikimrBlobStorage::TConfigResponse::TStatus::kMayGetDegraded
                : NKikimrBlobStorage::TConfigResponse::TStatus::kMayLoseData;
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(response.GetStatus(0).GetFailReason()),
                static_cast<ui32>(failReason),
                context << " Response# " << response.DebugString());
        }
    }

    Y_UNIT_TEST(RandomizedVDiskStates) {
        constexpr ui32 numGroups = 64;
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TBlobStorageGroupInfo::TTopology topology(erasure, 1, erasure.BlobSubgroupSize(), 1, true);
        TEnvironmentSetup env({
            .NodeCount = erasure.BlobSubgroupSize() + 4,
            .Erasure = erasure,
        });

        env.CreateBoxAndPool(4, numGroups);
        env.Sim(TDuration::Minutes(1));
        env.UpdateSettings(true, true, false);

        const NKikimrBlobStorage::TBaseConfig base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), numGroups);

        struct TTestCase {
            const NKikimrBlobStorage::TBaseConfig::TGroup *Group = nullptr;
            TVector<const NKikimrBlobStorage::TBaseConfig::TVSlot*> SlotsByOrderNumber;
            TVector<TVDiskState> States;
            ui32 TargetOrderNumber = 0;
        };

        TVDiskStates states;
        TVector<TTestCase> testCases;
        testCases.reserve(numGroups);
        for (const auto& group : base.GetGroup()) {
            TTestCase& testCase = testCases.emplace_back();
            testCase.Group = &group;
            testCase.SlotsByOrderNumber.resize(topology.GetTotalVDisksNum());
            testCase.States.resize(topology.GetTotalVDisksNum());

            for (const auto& slot : base.GetVSlot()) {
                if (slot.GetGroupId() == group.GetGroupId()) {
                    const TVDiskIdShort vdiskId(slot.GetFailRealmIdx(), slot.GetFailDomainIdx(), slot.GetVDiskIdx());
                    testCase.SlotsByOrderNumber[topology.GetOrderNumber(vdiskId)] = &slot;
                }
            }

            TVector<ui32> orderNumbers(topology.GetTotalVDisksNum());
            std::iota(orderNumbers.begin(), orderNumbers.end(), 0);
            for (ui32 i = 0; i + 1 < orderNumbers.size(); ++i) {
                std::swap(orderNumbers[i], orderNumbers[i + RandomNumber<ui32>(orderNumbers.size() - i)]);
            }
            testCase.TargetOrderNumber = orderNumbers[0];

            // Keep every important validation outcome covered for every seed while randomizing VDisk positions.
            // Every fifth case adds a fully randomized mix of the same readiness/operational state classes.
            switch (testCases.size() % 5) {
                case 0: // SelfHeal succeeds only because it may ignore the phantoms-only VDisk
                    testCase.States[orderNumbers[0]].Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    testCase.States[orderNumbers[1]] = {NKikimrBlobStorage::EVDiskStatus::REPLICATING, true};
                    break;

                case 1: // evicting a third VDisk disintegrates the group, even for SelfHeal
                    testCase.States[orderNumbers[1]].Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    testCase.States[orderNumbers[2]] = {NKikimrBlobStorage::EVDiskStatus::REPLICATING, true};
                    break;

                case 2: // non-operational VDisks trigger the earlier group fitter check
                    testCase.States[orderNumbers[0]].Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    testCase.States[orderNumbers[1]].Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    break;

                case 3: // a healthy group accepts an ordinary reassign
                    break;

                case 4: {
                    const ui32 numNonReady = 1 + RandomNumber<ui32>(4);
                    for (ui32 i = 0; i < numNonReady; ++i) {
                        switch (RandomNumber<ui32>(3)) {
                            case 0:
                                testCase.States[orderNumbers[i]].Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                                break;
                            case 1:
                                testCase.States[orderNumbers[i]].Status = NKikimrBlobStorage::EVDiskStatus::REPLICATING;
                                break;
                            case 2:
                                testCase.States[orderNumbers[i]] = {
                                    NKikimrBlobStorage::EVDiskStatus::REPLICATING, true};
                                break;
                        }
                    }
                    testCase.TargetOrderNumber = RandomNumber<ui32>(orderNumbers.size());
                    break;
                }
            }

            for (ui32 orderNumber = 0; orderNumber < testCase.States.size(); ++orderNumber) {
                UNIT_ASSERT(testCase.SlotsByOrderNumber[orderNumber]);
                states.emplace(MakeVDiskKey(*testCase.SlotsByOrderNumber[orderNumber]), testCase.States[orderNumber]);
            }
        }

        env.Runtime->FilterFunction = [&states](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateDiskStatus::EventType) {
                auto *statuses = ev->Get<TEvBlobStorage::TEvControllerUpdateDiskStatus>()->Record.MutableVDiskStatus();
                for (auto& status : *statuses) {
                    const auto& vdiskId = status.GetVDiskId();
                    const auto it = states.find(MakeVDiskKey(vdiskId.GetGroupID(), vdiskId.GetRing(),
                        vdiskId.GetDomain(), vdiskId.GetVDisk()));
                    if (it != states.end()) {
                        status.SetStatus(it->second.Status);
                        status.SetOnlyPhantomsRemain(it->second.OnlyPhantomsRemain);
                    }
                }
            }
            return true;
        };
        ReportVDiskStates(env, base, states);

        ui32 selfHealSuccesses = 0;
        ui32 disintegratedRejections = 0;
        for (ui32 index = 0; index < testCases.size(); ++index) {
            const TTestCase& testCase = testCases[index];
            const auto *target = testCase.SlotsByOrderNumber[testCase.TargetOrderNumber];

            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupReserve(true);
            request.SetAllowUnusableDisks(true);
            request.SetIgnoreDisintegratedGroupsChecks(true);
            auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
            reassign->SetGroupId(testCase.Group->GetGroupId());
            reassign->SetGroupGeneration(testCase.Group->GetGroupGeneration());
            reassign->SetFailRealmIdx(target->GetFailRealmIdx());
            reassign->SetFailDomainIdx(target->GetFailDomainIdx());
            reassign->SetVDiskIdx(target->GetVDiskIdx());

            const TString context = TStringBuilder() << "Case# " << index << " GroupId# "
                << testCase.Group->GetGroupId() << " TargetOrderNumber# " << testCase.TargetOrderNumber;
            const EExpectedResult regularExpected = EvaluateReassign(topology, testCase.States,
                testCase.TargetOrderNumber, false);
            NKikimrBlobStorage::TConfigResponse response = InvokeConfigRequest(env, request, false);
            AssertReassignResult(response, regularExpected, testCase.Group->GetGroupId(), context + " SelfHeal# false");

            if (regularExpected != EExpectedResult::Success) {
                const EExpectedResult selfHealExpected = EvaluateReassign(topology, testCase.States,
                    testCase.TargetOrderNumber, true);
                response = InvokeConfigRequest(env, request, true);
                AssertReassignResult(response, selfHealExpected, testCase.Group->GetGroupId(),
                    context + " SelfHeal# true");
                selfHealSuccesses += selfHealExpected == EExpectedResult::Success;
                disintegratedRejections += selfHealExpected == EExpectedResult::Disintegrated;
            }
        }

        UNIT_ASSERT_C(selfHealSuccesses, "Randomized test did not cover a SelfHeal-only successful reassign");
        UNIT_ASSERT_C(disintegratedRejections,
            "Randomized test did not cover fail-model rejection with IgnoreDisintegratedGroupsChecks=true");
    }
}
