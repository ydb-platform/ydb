#include "distconf.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {
    static constexpr TDuration DefaultWaitForConfigStep = TDuration::Minutes(1);
    static constexpr TDuration MaxWaitForConfigStep = TDuration::Minutes(10);

    TStateStorageSelfhealActor::TStateStorageSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
        , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig)
        : WaitForConfigStep(waitForConfigStep > TDuration::Seconds(0) && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , StateStorageReconfigurationStep(NONE)
        , Sender(sender)
        , Cookie(cookie)
        , CurrentConfig(currentConfig)
        , TargetConfig(targetConfig)
    {}

    void TStateStorageSelfhealActor::RequestChangeStateStorage() {
        Y_ABORT_UNLESS(StateStorageReconfigurationStep > NONE && StateStorageReconfigurationStep < INVALID_RECONFIGURATION_STEP);
        auto request = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        NKikimrBlobStorage::TStateStorageConfig *config = request->Record.MutableReconfigStateStorage();
        auto fillRingGroupsForCurrentCfg = [&](auto *cfg, auto *currentCfg) {
            if (currentCfg->RingGroupsSize()) {
                for (ui32 i : xrange(currentCfg->RingGroupsSize())) {
                    auto &rg = currentCfg->GetRingGroups(i);
                    if (rg.GetWriteOnly()) {
                        continue;
                    }
                    auto *ringGroup = cfg->AddRingGroups();
                    ringGroup->CopyFrom(rg);
                    ringGroup->SetWriteOnly(StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
                }
            } else {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(currentCfg->GetRing());
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
            }
        };
        auto fillRingGroups = [&](auto mutableFunc) {
            auto *targetCfg = (TargetConfig.*mutableFunc)();
            if (targetCfg->RingGroupsSize() == 0) {
                return;
            }
            auto *cfg = (config->*mutableFunc)();
            auto *currentCfg = (CurrentConfig.*mutableFunc)();
            if (StateStorageReconfigurationStep < MAKE_PREVIOUS_GROUP_WRITEONLY) {
                fillRingGroupsForCurrentCfg(cfg, currentCfg);
            }

            for (ui32 i : xrange(targetCfg->RingGroupsSize())) {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(targetCfg->GetRingGroups(i));
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == INTRODUCE_NEW_GROUP);
            }
            if (StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY) {
                fillRingGroupsForCurrentCfg(cfg, currentCfg);
            }
        };

        fillRingGroups(&NKikimrBlobStorage::TStateStorageConfig::MutableStateStorageConfig);
        fillRingGroups(&NKikimrBlobStorage::TStateStorageConfig::MutableStateStorageBoardConfig);
        fillRingGroups(&NKikimrBlobStorage::TStateStorageConfig::MutableSchemeBoardConfig);
        STLOG(PRI_DEBUG, BS_NODE, NW70, "TStateStorageSelfhealActor::RequestChangeStateStorage",
                (StateStorageReconfigurationStep, (ui32)StateStorageReconfigurationStep), (StateStorageConfig, config));

        AllowNextStep = false;
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
    }

    void TStateStorageSelfhealActor::Bootstrap(TActorId /*parentId*/) {
        StateStorageReconfigurationStep = INTRODUCE_NEW_GROUP;
        RequestChangeStateStorage();
        Schedule(WaitForConfigStep, new TEvents::TEvWakeup());
        Become(&TThis::StateFunc);
    }

    void TStateStorageSelfhealActor::Finish(TResult::EStatus result, const TString& errorReason) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        auto *record = &ev->Record;
        record->SetStatus(result);
        if (!errorReason.empty()) {
            record->SetErrorReason(errorReason);
        }
        TActivationContext::Send(new IEventHandle(Sender, SelfId(), ev.release(), 0, Cookie));
        PassAway();
    }

    TStateStorageSelfhealActor::EReconfigurationStep TStateStorageSelfhealActor::GetNextStep(TStateStorageSelfhealActor::EReconfigurationStep prevStep) {
        switch(prevStep) {
            case NONE:
                return INTRODUCE_NEW_GROUP;
            case INTRODUCE_NEW_GROUP:
                return MAKE_NEW_GROUP_READWRITE;
            case MAKE_NEW_GROUP_READWRITE:
                return MAKE_PREVIOUS_GROUP_WRITEONLY;
            case MAKE_PREVIOUS_GROUP_WRITEONLY:
                return DELETE_PREVIOUS_GROUP;
            default:
                Y_ABORT("Invalid reconfiguration step");
        }
        return INVALID_RECONFIGURATION_STEP;
    }

    void TStateStorageSelfhealActor::HandleWakeup() {
        if (!AllowNextStep) {
            STLOG(PRI_ERROR, BS_NODE, NW78, "TStateStorageSelfhealActor::HandleWakeup aborted. Previous reconfiguration step not finished yet.", (StateStorageReconfigurationStep, (ui32)StateStorageReconfigurationStep));
            Finish(TResult::ERROR, "Previous reconfiguration step not finished yet.");
            return;
        }
        if (StateStorageReconfigurationStep == DELETE_PREVIOUS_GROUP) {
            Finish(TResult::OK);
            return;
        }
        StateStorageReconfigurationStep = GetNextStep(StateStorageReconfigurationStep);
        RequestChangeStateStorage();
        Schedule(WaitForConfigStep, new TEvents::TEvWakeup());
    }

    void TStateStorageSelfhealActor::HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        if (ev->Get()->Record.GetStatus() != TResult::OK) {
            STLOG(PRI_ERROR, BS_NODE, NW72, "TStateStorageSelfhealActor::HandleResult aborted. ", (Reason, ev->Get()->Record.GetErrorReason()));
            Finish(TResult::ERROR, ev->Get()->Record.GetErrorReason());
        } else {
            AllowNextStep = true;
        }
    }

    void TStateStorageSelfhealActor::PassAway() {
        StateStorageReconfigurationStep = INVALID_RECONFIGURATION_STEP;
        TActorBootstrapped::PassAway();
    }

    STFUNC(TStateStorageSelfhealActor::StateFunc) {
        STRICT_STFUNC_BODY(
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, HandleResult);
        )
    }
}
