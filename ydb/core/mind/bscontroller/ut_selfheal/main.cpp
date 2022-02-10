#include <ydb/core/mind/bscontroller/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h> 

#include "env.h"

Y_UNIT_TEST_SUITE(BsControllerTest) {

    Y_UNIT_TEST(SelfHeal) {
        const ui32 numNodes = 32;
        const ui32 numDisksPerNode = 2;
        const ui32 numGroups = numNodes * numDisksPerNode;
        TEnvironmentSetup env(numNodes);

        NKikimrBlobStorage::TConfigRequest request;
        env.DefineBox(1, {{"/dev/disk1"}, {"/dev/disk2"}}, {{1, numNodes}}, &request);
        env.DefineStoragePool(1, 1, numGroups, NKikimrBlobStorage::ROT, {}, &request);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        env.WaitForNodeWardensToConnect();

        request.Clear();
        auto *cmd = request.AddCommand()->MutableEnableSelfHeal();
        cmd->SetEnable(true);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        std::set<TPDiskId> active, faulty;

        request = {};
        env.QueryBaseConfig(&request);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
            active.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
        }

        UNIT_ASSERT_VALUES_EQUAL(active.size(), numNodes * numDisksPerNode);

        auto move = [&](auto& from, auto& to, NKikimrBlobStorage::EDriveStatus status) {
            auto it = from.begin();
            std::advance(it, RandomNumber(from.size()));
            Ctest << "PDisk# " << *it
                << " setting status to " << NKikimrBlobStorage::EDriveStatus_Name(status)
                << Endl;
            request = {};
            env.UpdateDriveStatus(*it, status, &request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            to.insert(from.extract(it));
        };

        for (size_t i = 0; i < 32; ++i) {
            env.Wait(TDuration::Seconds(300));
            if (faulty.size() < 8) {
                move(active, faulty, NKikimrBlobStorage::FAULTY);
            } else {
                move(faulty, active, NKikimrBlobStorage::ACTIVE);
            }
            env.Wait(TDuration::Seconds(300));

            request = {};
            env.QueryBaseConfig(&request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                if (pdisk.GetDriveStatus() == NKikimrBlobStorage::ACTIVE) {
                    UNIT_ASSERT(active.count(pdiskId));
                } else {
                    UNIT_ASSERT(pdisk.GetDriveStatus() == NKikimrBlobStorage::FAULTY);
                    UNIT_ASSERT(faulty.count(pdiskId));
                }
            }
            for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
                const auto& id = vslot.GetVSlotId();
                const TPDiskId pdiskId(id.GetNodeId(), id.GetPDiskId());
                UNIT_ASSERT(active.count(pdiskId));
            }
        }
    }

}
