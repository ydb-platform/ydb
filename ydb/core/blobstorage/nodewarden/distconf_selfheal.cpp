#include "distconf.h"
#include "distconf_selfheal.h"

namespace NKikimr::NStorage {
    static constexpr TDuration DefaultWaitForConfigStep = TDuration::Minutes(1);
    static constexpr TDuration MaxWaitForConfigStep = TDuration::Minutes(10);

    TStateStorageSelfhealActor::TStateStorageSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
        , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig, ui32 pilesCount)
        : WaitForConfigStep(waitForConfigStep > TDuration::Seconds(0) && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , StateStorageReconfigurationStep(NONE)
        , Sender(sender)
        , Cookie(cookie)
        , CurrentConfig(currentConfig)
        , TargetConfig(targetConfig)
        , PilesCount(pilesCount)
    {}

    void TStateStorageSelfhealActor::RequestChangeStateStorage() {
        Y_ABORT_UNLESS(StateStorageReconfigurationStep > NONE && StateStorageReconfigurationStep < INVALID_RECONFIGURATION_STEP);
        auto request = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        NKikimrBlobStorage::TStateStorageConfig *config = request->Record.MutableReconfigStateStorage();
        auto setWriteOnly = [](auto* ringGroup, bool writeOnly) {
            if (writeOnly) {
                ringGroup->SetWriteOnly(true);
            } else {
                ringGroup->ClearWriteOnly();
            }
        };
        auto fillRingGroupsForCurrentCfg = [&](auto *cfg, auto *currentCfg) {
            if (currentCfg->RingGroupsSize()) {
                for (ui32 i : xrange(currentCfg->RingGroupsSize())) {
                    auto &rg = currentCfg->GetRingGroups(i);
                    if (rg.GetWriteOnly()) {
                        continue;
                    }
                    auto *ringGroup = cfg->AddRingGroups();
                    ringGroup->CopyFrom(rg);
                    setWriteOnly(ringGroup, StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY || i > PilesCount);
                }
            } else {
                auto *ringGroup = cfg->AddRingGroups();
                ringGroup->CopyFrom(currentCfg->GetRing());
                setWriteOnly(ringGroup, StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
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
                setWriteOnly(ringGroup, StateStorageReconfigurationStep == INTRODUCE_NEW_GROUP);
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

    TStateStorageReassignNodeSelfhealActor::TStateStorageReassignNodeSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
        , ui32 nodeFrom, ui32 nodeTo, bool needReconfigSS, bool needReconfigSSB, bool needReconfigSB)
        : WaitForConfigStep(waitForConfigStep > TDuration::Seconds(0) && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , Sender(sender)
        , Cookie(cookie)
        , NodeFrom(nodeFrom)
        , NodeTo(nodeTo)
        , NeedReconfigSS(needReconfigSS)
        , NeedReconfigSSB(needReconfigSSB)
        , NeedReconfigSB(needReconfigSB)
    {}

    void TStateStorageReassignNodeSelfhealActor::Bootstrap(TActorId /*parentId*/) {
        RequestChangeStateStorage(true);
        Schedule(WaitForConfigStep, new TEvents::TEvWakeup());
        Become(&TThis::StateFunc);
    }

    void TStateStorageReassignNodeSelfhealActor::RequestChangeStateStorage(bool disable) {
        auto request = std::make_unique<TEvNodeConfigInvokeOnRoot>();
        auto *cmd = request->Record.MutableReassignStateStorageNode();
        cmd->SetFrom(NodeFrom);
        cmd->SetTo(NodeTo);
        cmd->SetStateStorage(NeedReconfigSS);
        cmd->SetStateStorageBoard(NeedReconfigSSB);
        cmd->SetSchemeBoard(NeedReconfigSB);
        cmd->SetDisableRing(disable);
        AllowNextStep = false;
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
        STLOG(PRI_ERROR, BS_NODE, NW72, "StateStorageReassignNodeSelfhealActor::RequestChangeStateStorage", (cmd, cmd));
    }

    void TStateStorageReassignNodeSelfhealActor::Finish(TResult::EStatus result, const TString& errorReason) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        auto *record = &ev->Record;
        record->SetStatus(result);
        if (!errorReason.empty()) {
            record->SetErrorReason(errorReason);
        }
        TActivationContext::Send(new IEventHandle(Sender, SelfId(), ev.release(), 0, Cookie));
        PassAway();
    }

    void TStateStorageReassignNodeSelfhealActor::HandleResult(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        if (ev->Get()->Record.GetStatus() != TResult::OK) {
            STLOG(PRI_ERROR, BS_NODE, NW72, "TStateStorageReassignNodeSelfhealActor::HandleResult aborted. ", (Reason, ev->Get()->Record.GetErrorReason()));
            Finish(TResult::ERROR, ev->Get()->Record.GetErrorReason());
        } else {
            AllowNextStep = true;
        }
    }

    void TStateStorageReassignNodeSelfhealActor::HandleWakeup() {
        if (!AllowNextStep) {
            STLOG(PRI_ERROR, BS_NODE, NW78, "TStateStorageReassignNodeSelfhealActor::HandleWakeup aborted. Previous reconfiguration step not finished yet.");
            Finish(TResult::ERROR, "Previous reconfiguration step not finished yet.");
            return;
        }
        if (FinishReassign) {
            Finish(TResult::OK);
            return;
        }
        FinishReassign = true;
        RequestChangeStateStorage(false);
        Schedule(WaitForConfigStep, new TEvents::TEvWakeup());
    }

    STFUNC(TStateStorageReassignNodeSelfhealActor::StateFunc) {
        STRICT_STFUNC_BODY(
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, HandleResult);
        )
    }
}
