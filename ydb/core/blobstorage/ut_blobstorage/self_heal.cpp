#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <util/system/compiler.h>

Y_UNIT_TEST_SUITE(SelfHeal) {
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
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto* cmd = request.AddCommand()->MutableUpdateDriveStatus();
            cmd->MutableHostKey()->SetNodeId(pdisk.GetNodeId());
            cmd->SetPDiskId(pdisk.GetPDiskId());
            cmd->SetStatus(NKikimrBlobStorage::FAULTY);
            auto res = env.Invoke(request);
            UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
            UNIT_ASSERT_C(res.GetStatus(0).GetSuccess(), res.GetStatus(0).GetErrorDescription());
        }

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
                NKikimrBlobStorage::TConfigRequest request;
                auto* cmd = request.AddCommand()->MutableUpdateDriveStatus();
                cmd->MutableHostKey()->SetNodeId(pdiskId.NodeId);
                cmd->SetPDiskId(pdiskId.PDiskId);
                cmd->SetStatus(pdiskStatus.DriveStatus);
                cmd->SetMaintenanceStatus(pdiskStatus.MaintenanceStatus);
                auto res = env.Invoke(request);
                UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
                UNIT_ASSERT_C(res.GetStatus(0).GetSuccess(), res.GetStatus(0).GetErrorDescription());
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
        env.Sim(TDuration::Minutes(180));

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
}
