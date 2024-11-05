#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/protos/pdiskfit.pb.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <util/system/event.h>

using namespace NActors;
using namespace NKikimr;

class TStateManager;

struct TFakeVDiskParams {
    // 0 means no limit
    ui32 LogsToBeSent = 0;

    // LogRecord size distribution
    ui32 SizeMin = 1000;
    ui32 SizeMax = 100000;

    ui32 LsnToKeepCount = 1000;
    double LogCutProbability = 1.0 / 500;
};


IActor *CreateFakeVDisk(const TVDiskID& vdiskId, const TActorId& pdiskServiceId, ui64 pdiskGuid,
        TStateManager *stateManager, TFakeVDiskParams params);

class TBasicTest : public TActorBootstrapped<TBasicTest> {
    TAutoEvent *StopEvent = nullptr;
    TStateManager *StateManager = nullptr;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TPDiskConfig> PDiskConfig;
    TActorId PDiskServiceId;
    const ui32 NumVDisks;
    bool InduceLogSplicing;

public:
    TBasicTest(ui32 numVDisks, bool induceLogSplicing)
        : NumVDisks(numVDisks)
        , InduceLogSplicing(induceLogSplicing)
    {}

    template<typename TEnv>
    void Run(TEnv *env, TAutoEvent *stopEvent, TStateManager *stateManager) {
        StopEvent = stopEvent;
        StateManager = stateManager;
        Counters = env->Counters;
        PDiskConfig = new TPDiskConfig(env->PDiskFilePath, env->PDiskGuid, 1,
                TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        PDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        PDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        env->ActorSystem->Register(this);
    }

    void Bootstrap(const TActorContext& ctx) {
        CreatePDiskActor(ctx);
        TVector<TActorId> actors;
        for (ui32 i = 0; i < NumVDisks; ++i) {
            TVDiskID vdiskId(TGroupId::FromValue(i), 0, 0, 0, 0);
            TFakeVDiskParams params;
            if (InduceLogSplicing) {
                params.LogCutProbability = 1e-3;
                params.SizeMin = 4000;
                params.SizeMax = 4000;
                if (i == 0) {
                    params.LogsToBeSent = 100;
                }
            }
            TActorId actorId = ctx.ExecutorThread.ActorSystem->Register(CreateFakeVDisk(vdiskId, PDiskServiceId,
                    PDiskConfig->PDiskGuid, StateManager, params));
            actors.push_back(actorId);
        }
        for (const TActorId& actor : actors) {
            ctx.Send(actor, new TEvents::TEvBootstrap);
        }
        Become(&TBasicTest::StateFunc);
    }

    void CreatePDiskActor(const TActorContext& ctx) {
        Y_ABORT_UNLESS(Counters);
        Y_ABORT_UNLESS(ctx.ExecutorThread.ActorSystem);
        Y_ABORT_UNLESS(PDiskConfig);
        Y_ABORT_UNLESS(AppData(ctx));
        std::unique_ptr<IActor> pdiskActor(CreatePDisk(PDiskConfig, NPDisk::TMainKey{ .Keys = { 1 }, .IsInitialized = true },
                Counters->GetSubgroup("subsystem", "pdisk")));
        const TActorId actorId = ctx.ExecutorThread.ActorSystem->Register(pdiskActor.release(), TMailboxType::Simple,
                AppData(ctx)->SystemPoolId);
        PDiskServiceId = MakeBlobStoragePDiskID(ctx.ExecutorThread.ActorSystem->NodeId, PDiskConfig->PDiskId);
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(PDiskServiceId, actorId);
    }

    void Finish(const TActorContext& ctx) {
        LOG_NOTICE(ctx, NActorsServices::TEST, "TBasicTest::Finish called");
        Die(ctx);
        Y_ABORT_UNLESS(StopEvent);
        StopEvent->Signal();
    }

    STFUNC(StateFunc) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            default: Y_ABORT("unexpected message 0x%08" PRIx32, type);
        }
    }
};
