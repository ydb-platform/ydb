#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TStateStorageSelfhealActor : public TActorBootstrapped<TStateStorageSelfhealActor> {
        const ui32 WaitForConfigStep;
        ui32 StateStorageReconfigurationStep;
        const TActorId Sender;
        const ui64 Cookie;
        NKikimrBlobStorage::TStateStorageConfig CurrentConfig;
        NKikimrBlobStorage::TStateStorageConfig TargetConfig;

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        void HandleWakeup();
        void Finish(TResult::EStatus result);
        bool RequestChangeStateStorage();
        void PassAway();
        void HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev);

    public:
        TStateStorageSelfhealActor(TActorId sender, ui64 cookie, ui32 waitForConfigStep
            , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig);

        void Bootstrap(TActorId parentId);

        STFUNC(StateFunc);
    };
}
