#include "distconf.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {
    static const ui32 DefaultWaitForConfigStep = 60;
    static const ui32 MaxWaitForConfigStep = DefaultWaitForConfigStep * 10;

    TStateStorageSelfhealActor::TStateStorageSelfhealActor(TActorId sender, ui64 cookie, ui32 waitForConfigStep
        , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig)
        : WaitForConfigStep(waitForConfigStep > 0 && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , StateStorageReconfigurationStep(0)
        , Sender(sender)
        , Cookie(cookie)
        , CurrentConfig(currentConfig)
        , TargetConfig(targetConfig)
    {}

    bool TStateStorageSelfhealActor::RequestChangeStateStorage() {
        Y_ABORT_UNLESS(StateStorageReconfigurationStep > 0 && StateStorageReconfigurationStep < 5);
        auto request = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        NKikimrBlobStorage::TStateStorageConfig *config = request->Record.MutableReconfigStateStorage();
        auto fillRingGroups = [&](auto *cfg, auto *currentCfg) {
            if (currentCfg->RingGroupsSize()) {
                for (ui32 i : xrange(currentCfg->RingGroupsSize())) {
                    auto *ringGroup = cfg->AddRingGroups();
                    ringGroup->CopyFrom(currentCfg->GetRingGroups(i));
                    ringGroup->SetWriteOnly(StateStorageReconfigurationStep == 3);
                }
            } else {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(currentCfg->GetRing());
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == 3);
            }
        };
        auto process = [&](auto mutableFunc) {
            auto *cfg = (config->*mutableFunc)();
            auto *currentCfg = (CurrentConfig.*mutableFunc)();
            if (StateStorageReconfigurationStep < 3) {
                fillRingGroups(cfg, currentCfg);
            }
            auto *targetCfg = (TargetConfig.*mutableFunc)();
            Y_ABORT_UNLESS(targetCfg->RingGroupsSize());
            for (ui32 i : xrange(targetCfg->RingGroupsSize())) {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(targetCfg->GetRingGroups(i));
                ringGroup->SetWriteOnly(StateStorageReconfigurationStep == 1);
            }
            if (StateStorageReconfigurationStep == 3) {
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
                (StateStorageReconfigurationStep, StateStorageReconfigurationStep), (StateStorageConfig, config));

        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
        return true;
    }

    void TStateStorageSelfhealActor::Bootstrap(TActorId /*parentId*/) {
        StateStorageReconfigurationStep = 1;
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

    void TStateStorageSelfhealActor::HandleWakeup() {
        StateStorageReconfigurationStep++;
        if (!RequestChangeStateStorage()) {
            Finish(TResult::ERROR);
            return;
        }
        if (StateStorageReconfigurationStep == 4) {
            Finish(TResult::OK);
        } else {
            Schedule(TDuration::Seconds(WaitForConfigStep), new TEvents::TEvWakeup());
        }
    }

    void TStateStorageSelfhealActor::HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == TResult::OK);
    }

    void TStateStorageSelfhealActor::PassAway() {
        StateStorageReconfigurationStep = 0;
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
