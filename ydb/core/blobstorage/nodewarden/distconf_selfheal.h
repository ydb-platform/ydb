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
        ui32 PilesCount;

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        void HandleWakeup();
        void Finish(TResult::EStatus result, const TString& errorReason = "");
        void RequestChangeStateStorage();
        void PassAway();
        void HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev);
        EReconfigurationStep GetNextStep(EReconfigurationStep prevStep);

    public:
        TStateStorageSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
            , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig, ui32 pilesCount);

        void Bootstrap(TActorId parentId);

        STFUNC(StateFunc);
    };

    class TStateStorageReassignNodeSelfhealActor : public TActorBootstrapped<TStateStorageReassignNodeSelfhealActor> {
        const TDuration WaitForConfigStep;
        const TActorId Sender;
        const ui64 Cookie;
        bool AllowNextStep = false;
        bool FinishReassign = false;
        ui32 NodeFrom;
        ui32 NodeTo;
        bool NeedReconfigSS;
        bool NeedReconfigSSB;
        bool NeedReconfigSB;

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        void HandleWakeup();
        void Finish(TResult::EStatus result, const TString& errorReason = "");
        void RequestChangeStateStorage(bool disable);
        void HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev);

    public:
        TStateStorageReassignNodeSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
            , ui32 nodeFrom, ui32 nodeTo, bool needReconfigSS, bool needReconfigSSB, bool needReconfigSB);

        void Bootstrap(TActorId parentId);

        STFUNC(StateFunc);
    };
}
