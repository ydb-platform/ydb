#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <util/system/info.h>


enum class EState {
    OK,
    FORMAT
};

struct TReplTestSettings {
    ui32 BlobSize;
    ui32 MinHugeBlobSize;
    ui32 MinHugeBlobSizeInRepl;
};

void DoTestCase(const TReplTestSettings& settings) {
    using E = EState;
    std::vector<EState> states = {E::OK, E::FORMAT, E::FORMAT, E::OK, E::OK, E::OK, E::OK, E::OK};
    ui32 nodeCount = states.size();
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = TBlobStorageGroupType::EErasureSpecies::Erasure4Plus2Block,
        .ControllerNodeId = 1,
        .MinHugeBlobInBytes = settings.MinHugeBlobSize,
        .UseFakeConfigDispatcher = true,
    });

    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));

    auto groupId = env.GetGroups()[0];
    TString data = TString::Uninitialized(settings.BlobSize);
    memset(data.Detach(), 1, data.size());
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);
    env.PutBlob(groupId, id, data);
    env.WaitForSync(env.GetGroupInfo(groupId), id);

    auto checkBlob = [&] {
        TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
        auto *msg = res->Get();
        Y_ABORT_UNLESS(msg->ResponseSz == 1);
        return msg->Responses[0].Status;
    };

    Y_ABORT_UNLESS(checkBlob() == NKikimrProto::OK);
    env.Cleanup();

    for (auto& [key, state] : env.PDiskMockStates) {
        if (states[key.first - 1] == E::FORMAT) {
            state.Reset();
        }
    }

    env.Initialize();

    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> detainedMsgs;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplStarted) {
            detainedMsgs.emplace_back(nodeId, std::move(ev));
            return false;
        }
        return true;
    };

    env.Sim(TDuration::Minutes(10));
    Y_ABORT_UNLESS(checkBlob() == NKikimrProto::OK);
    Y_ABORT_IF(detainedMsgs.empty());

    env.Runtime->FilterFunction = {};

    // replication is about to start, updating the minHugeBlobSize in skeleton and resuming replication
    for (auto& [nodeId, detainedEv] : detainedMsgs) {
        TActorId edge = env.Runtime->AllocateEdgeActor(nodeId);

        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto perfConfig = NKikimrConfig::TBlobStorageConfig_TVDiskPerformanceConfig();
        perfConfig.SetPDiskType(PDiskTypeToPDiskType(env.Settings.DiskType));
        perfConfig.SetMinHugeBlobSizeInBytes(settings.MinHugeBlobSizeInRepl);
        auto* vdiskTypes = request->Record.MutableConfig()->MutableBlobStorageConfig()->MutableVDiskPerformanceSettings()->MutableVDiskTypes();
        vdiskTypes->Add(std::move(perfConfig));

        env.Runtime->Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(nodeId), edge, request.Release()), nodeId);
        Y_ABORT_UNLESS(env.Runtime->WaitForEdgeActorEvent({edge})->CastAsLocal<NConsole::TEvConsole::TEvConfigNotificationResponse>());

        env.Runtime->Send(detainedEv.release(), nodeId);
    }

    // waiting for replication to complete 
    env.WaitForSync(env.GetGroupInfo(groupId), id);
        
    UNIT_ASSERT_EQUAL(checkBlob(),  NKikimrProto::OK);
}

Y_UNIT_TEST_SUITE(MinHugeChangeOnReplication) {

    Y_UNIT_TEST(MinHugeDecreased) {
        DoTestCase(TReplTestSettings{ 
            .BlobSize = 200u << 10, 
            .MinHugeBlobSize = 64u << 10,
            .MinHugeBlobSizeInRepl = 512u << 10,
        });
    }

    Y_UNIT_TEST(MinHugeIncreased) {
        DoTestCase(TReplTestSettings{ 
            .BlobSize = 200u << 10, 
            .MinHugeBlobSize = 512u << 10,
            .MinHugeBlobSizeInRepl = 10u << 10,
        });
    }

}
