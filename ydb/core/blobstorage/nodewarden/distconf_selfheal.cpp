#include "distconf.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {
    static const ui32 DefaultWaitForConfigStep = 60;
    static const ui32 MaxWaitForConfigStep = DefaultWaitForConfigStep * 10;

    TStateStorageSelfhealActor::TStateStorageSelfhealActor(TActorId sender, ui64 cookie, ui32 waitForConfigStep
        , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig)
        : WaitForConfigStep(waitForConfigStep > 0 && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , StateStorageReconfigurationStep(NONE)
        , Sender(sender)
        , Cookie(cookie)
        , CurrentConfig(currentConfig)
        , TargetConfig(targetConfig)
    {}

    bool TStateStorageSelfhealActor::RequestChangeStateStorage() {
        Y_ABORT_UNLESS(StateStorageReconfigurationStep > NONE && StateStorageReconfigurationStep < INVALID_RECONFIGURATION_STEP);
        auto request = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        NKikimrBlobStorage::TStateStorageConfig *config = request->Record.MutableReconfigStateStorage();
        auto fillRingGroups = [&](auto *cfg, auto *currentCfg) {
            if (currentCfg->RingGroupsSize()) {
                for (ui32 i : xrange(currentCfg->RingGroupsSize())) {
                    auto *ringGroup = cfg->AddRingGroups();
                    ringGroup->CopyFrom(currentCfg->GetRingGroups(i));
                    ringGroup->SetWriteOnly(StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
                }
            } else {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(currentCfg->GetRing());
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
            }
        };
        auto process = [&](auto mutableFunc) {
            auto *cfg = (config->*mutableFunc)();
            auto *currentCfg = (CurrentConfig.*mutableFunc)();
            if (StateStorageReconfigurationStep < DELETE_PREVIOUS_GROUP) {
                fillRingGroups(cfg, currentCfg);
            }
            auto *targetCfg = (TargetConfig.*mutableFunc)();
            Y_ABORT_UNLESS(targetCfg->RingGroupsSize());
            for (ui32 i : xrange(targetCfg->RingGroupsSize())) {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(targetCfg->GetRingGroups(i));
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == INTRODUCE_NEW_GROUP);
            }
            if (StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY) {
                fillRingGroups(cfg, currentCfg);
            }
            return true;
        };

        #define F(NAME) \
        if (!process(&NKikimrBlobStorage::TStateStorageConfig::Mutable##NAME##Config)) { \
            return false; \
        }
        F(StateStorage)
        F(StateStorageBoard)
        F(SchemeBoard)
        #undef F
        STLOG(PRI_DEBUG, BS_NODE, NW52, "TStateStorageSelfhealActor::RequestChangeStateStorage",
                (StateStorageReconfigurationStep, (ui32)StateStorageReconfigurationStep), (StateStorageConfig, config));

        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
        return true;
    }

    void TStateStorageSelfhealActor::Bootstrap(TActorId /*parentId*/) {
        StateStorageReconfigurationStep = INTRODUCE_NEW_GROUP;
        if (!RequestChangeStateStorage()) {
            Finish(TResult::ERROR);
            return;
        }
        Schedule(TDuration::Seconds(WaitForConfigStep), new TEvents::TEvWakeup());
        Become(&TThis::StateFunc);
    }

    void TStateStorageSelfhealActor::Finish(TResult::EStatus result) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        auto *record = &ev->Record;
        record->SetStatus(result);
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
        StateStorageReconfigurationStep = GetNextStep(StateStorageReconfigurationStep);
        if (!RequestChangeStateStorage()) {
            Finish(TResult::ERROR);
            return;
        }
        if (StateStorageReconfigurationStep == DELETE_PREVIOUS_GROUP) {
            Finish(TResult::OK);
        } else {
            Schedule(TDuration::Seconds(WaitForConfigStep), new TEvents::TEvWakeup());
        }
    }

    void TStateStorageSelfhealActor::HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == TResult::OK);
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
