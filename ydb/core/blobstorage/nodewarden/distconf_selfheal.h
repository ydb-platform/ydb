#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TStateStorageSelfhealActor : public TActorBootstrapped<TStateStorageSelfhealActor> {
        enum EReconfigurationStep {
            NONE = 0,
            INTRODUCE_NEW_GROUP,
            MAKE_NEW_GROUP_READWRITE,
            MAKE_PREVIOUS_GROUP_WRITEONLY,
            DELETE_PREVIOUS_GROUP,
            INVALID_RECONFIGURATION_STEP
        };

        const TDuration WaitForConfigStep;
        EReconfigurationStep StateStorageReconfigurationStep;
        const TActorId Sender;
        const ui64 Cookie;
        NKikimrBlobStorage::TStateStorageConfig CurrentConfig;
        NKikimrBlobStorage::TStateStorageConfig TargetConfig;
        bool AllowNextStep = true;

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        void HandleWakeup();
        void Finish(TResult::EStatus result, const TString& errorReason = "");
        void RequestChangeStateStorage();
        void PassAway();
        void HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev);
        EReconfigurationStep GetNextStep(EReconfigurationStep prevStep);

    public:
        TStateStorageSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
            , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig);

        void Bootstrap(TActorId parentId);

        STFUNC(StateFunc);
    };
}
