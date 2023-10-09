#include "helpers.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/tablet/bootstrapper.h>

namespace NKikimr {

    TTabletStorageInfo* CreateTestTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
            TBlobStorageGroupType::EErasureSpecies erasure, ui32 groupId)
    {
        THolder<TTabletStorageInfo> x(new TTabletStorageInfo());

        x->TabletID = tabletId;
        x->TabletType = tabletType;
        x->Channels.resize(5);

        for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
            x->Channels[channel].Channel = channel;
            x->Channels[channel].Type = TBlobStorageGroupType(erasure);
            x->Channels[channel].History.resize(1);
            x->Channels[channel].History[0].FromGeneration = 0;
            x->Channels[channel].History[0].GroupID = groupId;
        }

        return x.Release();
    }

    TActorId CreateTestBootstrapper(TTestActorRuntime &runtime, TTabletStorageInfo *info,
            std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex)
    {
        TIntrusivePtr<TBootstrapperInfo> bi(new TBootstrapperInfo(new TTabletSetupInfo(op, TMailboxType::Simple, 0, TMailboxType::Simple, 0)));
        return runtime.Register(CreateBootstrapper(info, bi.Get()), nodeIndex);
    }

    NTabletPipe::TClientConfig GetPipeConfigWithRetries()
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        return pipeConfig;
    }

    struct TPDiskReplyChecker : IReplyChecker {
        ~TPDiskReplyChecker()
        {
        }

        void OnRequest(IEventHandle *request) override {
            if (request->Type == TEvBlobStorage::EvMultiLog) {
                NPDisk::TEvMultiLog *evLogs = request->Get<NPDisk::TEvMultiLog>();
                LastLsn = evLogs->LsnSeg.Last;
            } else {
                LastLsn = {};
            }
        }

        bool IsWaitingForMoreResponses(IEventHandle *response) override {
            if (!LastLsn) {
                return false;
            }
            Y_VERIFY_S(response->Type == TEvBlobStorage::EvLogResult, "expected EvLogResult "
                    << (ui64)TEvBlobStorage::EvLogResult << ", but given " << response->Type);
            NPDisk::TEvLogResult *evResult = response->Get<NPDisk::TEvLogResult>();
            ui64 responseLastLsn = evResult->Results.back().Lsn;
            return *LastLsn > responseLastLsn;
        }

        TMaybe<ui64> LastLsn;
    };

    void TStrandedPDiskServiceFactory::Create(const TActorContext &ctx, ui32 pDiskID,
            const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId)
    {
        Y_UNUSED(ctx);
        Y_ABORT_UNLESS(!Runtime.IsRealThreads());
        Y_ABORT_UNLESS(nodeId >= Runtime.GetNodeId(0) && nodeId < Runtime.GetNodeId(0) + Runtime.GetNodeCount());
        ui32 nodeIndex = nodeId - Runtime.GetNodeId(0);
        Runtime.BlockOutputForActor(TActorId(nodeId, "actorsystem"));

        TActorId actorId = Runtime.Register(CreatePDisk(cfg, mainKey, Runtime.GetAppData(0).Counters), nodeIndex, poolId, TMailboxType::Revolving);
        TActorId pDiskServiceId = MakeBlobStoragePDiskID(nodeId, pDiskID);

        Runtime.BlockOutputForActor(pDiskServiceId);
        Runtime.BlockOutputForActor(actorId);
        auto factory = CreateStrandingDecoratorFactory(&Runtime, []{ return MakeHolder<TPDiskReplyChecker>(); });
        IActor* wrappedActor = factory->Wrap(actorId, true, TVector<TActorId>());
        TActorId wrappedActorId = Runtime.Register(wrappedActor, nodeIndex, poolId, TMailboxType::Revolving);
        Runtime.RegisterService(pDiskServiceId, wrappedActorId, nodeIndex);
    }
}
