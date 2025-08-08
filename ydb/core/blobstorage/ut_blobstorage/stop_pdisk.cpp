#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCStopPDisk) {

    Y_UNIT_TEST(PDiskStop) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));

        auto it = env.PDiskMockStates.begin();
        TPDiskMockState* targetState = it->second.Get();
        ui32 targetNodeId = it->first.first;
        ui32 targetPDiskId = it->first.second;

        NKikimrBlobStorage::TConfigRequest request;

        NKikimrBlobStorage::TStopPDisk* cmd = request.AddCommand()->MutableStopPDisk();
        auto* pdiskId = cmd->MutableTargetPDiskId();
        pdiskId->SetNodeId(targetNodeId);
        pdiskId->SetPDiskId(targetPDiskId);

        auto response = env.Invoke(request);

        UNIT_ASSERT_C(response.GetSuccess(), "Should've stopped");

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

        UNIT_ASSERT_VALUES_EQUAL(targetState->GetStateErrorReason(), "Stopped by control message");
    }
}
