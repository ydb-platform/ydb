#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCRestartPDisk) {
    
    Y_UNIT_TEST(RestartNotAllowed) {
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

            NKikimrBlobStorage::TRestartPDisk* cmd = request.AddCommand()->MutableRestartPDisk();
            auto pdiskId = cmd->MutableTargetPDiskId();
            pdiskId->SetNodeId(diskId.NodeId);
            pdiskId->SetPDiskId(diskId.PDiskId);

            auto response = env.Invoke(request);

            if (i < 2) {
                // Two disks can be restarted. 
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            } else {
                // Restarting third disk will not be allowed.
                UNIT_ASSERT_C(!response.GetSuccess(), "Restart should've been prohibited");

                UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "ExpectedStatus# DISINTEGRATED");
                break;
            }
        }
    }
    
    Y_UNIT_TEST(RestartOneByOne) {
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

            NKikimrBlobStorage::TRestartPDisk* cmd = request.AddCommand()->MutableRestartPDisk();
            auto pdiskId = cmd->MutableTargetPDiskId();
            pdiskId->SetNodeId(diskId.NodeId);
            pdiskId->SetPDiskId(diskId.PDiskId);

            auto response = env.Invoke(request);
            
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            // Wait for VSlot to become ready after PDisk restart.
            env.Sim(TDuration::Seconds(30));
        }
    }
}
