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

    TActorId StartTestTablet(TTestActorRuntime &runtime, TTabletStorageInfo *info,
            std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op, ui32 nodeIndex)
    {
        auto setup = MakeIntrusive<TTabletSetupInfo>(op, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
        return runtime.Register(CreateTablet({}, info, setup.Get(), 0), nodeIndex);
    }

    NTabletPipe::TClientConfig GetPipeConfigWithRetries()
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        return pipeConfig;
    }

}
