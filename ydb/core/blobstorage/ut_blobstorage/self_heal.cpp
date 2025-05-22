#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

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
}
