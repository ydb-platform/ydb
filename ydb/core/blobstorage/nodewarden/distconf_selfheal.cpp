#include "distconf.h"
#include "distconf_selfheal.h"

#include <ydb/core/base/nodestate.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tabletid.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {
    // Registered in the keeper's mailbox, like its config-refill actors.
    class TDistributedConfigKeeper::TStateStorageSelfHealRefreshActor
        : public TActorBootstrapped<TStateStorageSelfHealRefreshActor> {
        TDistributedConfigKeeper *const Keeper;
        const std::weak_ptr<TLifetimeToken> Lifetime;
        const ui64 ScepterId;
        TActorId CmsPipe;

        bool IsCurrent() const {
            return !Lifetime.expired() && Keeper->StateStorageSelfHealRefreshActor == SelfId()
                && Keeper->Scepter && Keeper->Scepter->Id == ScepterId;
        }

        void Request() {
            if (!CmsPipe) {
                CmsPipe = Register(NTabletPipe::CreateClient(SelfId(), MakeCmsID(),
                    NTabletPipe::TClientRetryPolicy::WithRetries()));
            }
            NTabletPipe::SendData(SelfId(), CmsPipe, new TEvNodeConfigGetSelfHealState);
            Schedule(Keeper->Cfg->SelfHealStateRequestTimeout, new TEvents::TEvWakeup);
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
            if (ev->Get()->ClientId == CmsPipe && ev->Get()->Status != NKikimrProto::OK) {
                CmsPipe = {};
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
            if (ev->Get()->ClientId == CmsPipe) {
                CmsPipe = {};
            }
        }

        void Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
            if (ev->Get()->Record.HasSelfHealNodesStateUpdate()) {
                // Enter the keeper's invocation queue atomically with the
                // tenure check; a later loss of the scepter aborts this queue.
                InvokeOtherActor(*Keeper, &TDistributedConfigKeeper::HandleInvokeOnRoot, std::move(ev));
                PassAway();
            }
        }

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
            YDB_LOG_NOTICE("CMS rejected State Storage selfheal state refresh",
                {"status", ev->Get()->Record.GetStatus()},
                {"reason", ev->Get()->Record.GetErrorReason()});
            PassAway();
        }

        void PassAway() override {
            NTabletPipe::CloseAndForgetClient(SelfId(), CmsPipe);
            if (!Lifetime.expired() && Keeper->StateStorageSelfHealRefreshActor == SelfId()) {
                Keeper->StateStorageSelfHealRefreshActor.reset();
            }
            TActorBootstrapped::PassAway();
        }

    public:
        explicit TStateStorageSelfHealRefreshActor(TDistributedConfigKeeper *keeper)
            : Keeper(keeper)
            , Lifetime(keeper->LifetimeToken)
            , ScepterId(keeper->Scepter->Id)
        {}

        void Bootstrap() {
            if (!IsCurrent()) {
                return PassAway();
            }
            Become(&TThis::StateWork);
            Request();
        }

        STFUNC(StateWork) {
            if (!IsCurrent()) {
                return PassAway();
            }
            STRICT_STFUNC_BODY(
                cFunc(TEvents::TSystem::Poison, PassAway);
                cFunc(TEvents::TSystem::Wakeup, Request);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(TEvNodeConfigInvokeOnRoot, Handle);
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
            )
        }
    };

    void TDistributedConfigKeeper::StartStateStorageSelfHealRefresh() {
        if (Scepter && SelfManagementEnabled && StorageConfig->HasStateStorageConfig()
                && !StateStorageSelfHealRefreshStarted) {
            StateStorageSelfHealRefreshStarted = true;
            StateStorageSelfHealRefreshActor = RegisterWithSameMailbox(new TStateStorageSelfHealRefreshActor(this));
        }
    }

    std::optional<ui32> ChooseStateStorageSelfHealSource(const NKikimrConfig::TStateStorageConfig& config,
            ui32 pileId, const std::unordered_map<ui32, ui32>& nodeStates) {
        std::vector<ui32> readable;
        for (ui32 i : xrange(config.RingGroupsSize())) {
            const auto& group = config.GetRingGroups(i);
            if (group.GetBridgePileId() == pileId && !group.GetWriteOnly()) {
                readable.push_back(i);
            }
        }
        if (readable.size() == 1) {
            return readable.front();
        }
        const auto info = BuildStateStorageInfo(config);

        struct THealth {
            ui32 BadRings = 0;
            ui32 UnavailableRings = 0;
            ui32 NToSelect = 0;
            bool GuaranteedQuorum = false;
            bool PossibleQuorum = false;

            bool BetterThan(const THealth& other) const {
                if (GuaranteedQuorum != other.GuaranteedQuorum) {
                    return GuaranteedQuorum;
                }
                if (PossibleQuorum != other.PossibleQuorum) {
                    return PossibleQuorum;
                }
                return ui64(BadRings) * other.NToSelect < ui64(other.BadRings) * NToSelect;
            }
        };
        std::optional<ui32> selected;
        THealth selectedHealth;
        for (ui32 i : readable) {
            const auto& group = info->RingGroups[i];
            THealth health;
            health.NToSelect = group.NToSelect;
            for (const auto& ring : group.Rings) {
                bool bad = ring.IsDisabled;
                bool hasGoodReplica = false;
                for (const auto& replica : ring.Replicas) {
                    const auto it = nodeStates.find(replica.NodeId());
                    if (it == nodeStates.end() || it->second == ENodeState::UNKNOWN) {
                        return std::nullopt; // Do not retire a readable source without observations.
                    }
                    bad |= it->second != ENodeState::GOOD;
                    hasGoodReplica |= it->second == ENodeState::GOOD;
                }
                health.BadRings += bad;
                health.UnavailableRings += ring.IsDisabled || !hasGoodReplica;
            }
            const ui32 majority = health.NToSelect / 2 + 1;
            // A request selects NToSelect rings, then one replica per ring.
            // Count a partly unhealthy ring as bad: its selected replica may fail.
            health.GuaranteedQuorum = health.BadRings <= health.NToSelect - majority;
            health.PossibleQuorum = group.Rings.size() - health.UnavailableRings >= majority;
            if (!selected || health.BetterThan(selectedHealth)) {
                selected = i;
                selectedHealth = health;
            }
        }
        return selected;
    }

    void TDistributedConfigKeeper::CancelStateStorageSelfHealRefresh() {
        if (StateStorageSelfHealRefreshActor) {
            Send(*StateStorageSelfHealRefreshActor, new TEvents::TEvPoison);
            StateStorageSelfHealRefreshActor.reset();
        }
    }

    static constexpr TDuration DefaultWaitForConfigStep = TDuration::Minutes(1);
    static constexpr TDuration MaxWaitForConfigStep = TDuration::Minutes(10);

    TStateStorageSelfhealActor::TStateStorageSelfhealActor(TActorId sender, ui64 cookie, TDuration waitForConfigStep
        , NKikimrBlobStorage::TStateStorageConfig&& currentConfig, NKikimrBlobStorage::TStateStorageConfig&& targetConfig,
        bool prepareCurrentConfig)
        : WaitForConfigStep(waitForConfigStep > TDuration::Seconds(0) && waitForConfigStep < MaxWaitForConfigStep ? waitForConfigStep : DefaultWaitForConfigStep)
        , StateStorageReconfigurationStep(NONE)
        , Sender(sender)
        , Cookie(cookie)
        , CurrentConfig(currentConfig)
        , TargetConfig(targetConfig)
        , PrepareCurrentConfig(prepareCurrentConfig)
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
                    setWriteOnly(ringGroup, StateStorageReconfigurationStep == MAKE_PREVIOUS_GROUP_WRITEONLY);
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
            if (StateStorageReconfigurationStep == PREPARE_CURRENT_GROUPS) {
                // Retire surplus readable versions before they may be removed.
                // Replaying this step after another root loss is safe as well.
                cfg->CopyFrom(*currentCfg);
                return;
            }
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
        YDB_LOG_DEBUG("TStateStorageSelfhealActor::RequestChangeStateStorage",
            {"marker", "NW70"},
            {"stateStorageReconfigurationStep", (ui32)StateStorageReconfigurationStep},
            {"stateStorageConfig", config});

        AllowNextStep = false;
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
    }

    void TStateStorageSelfhealActor::Bootstrap(TActorId /*parentId*/) {
        StateStorageReconfigurationStep = PrepareCurrentConfig ? PREPARE_CURRENT_GROUPS : INTRODUCE_NEW_GROUP;
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
            case PREPARE_CURRENT_GROUPS:
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
            YDB_LOG_ERROR("TStateStorageSelfhealActor::HandleWakeup aborted. Previous reconfiguration step not finished yet",
                {"marker", "NW78"},
                {"stateStorageReconfigurationStep", (ui32)StateStorageReconfigurationStep});
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
            YDB_LOG_ERROR("TStateStorageSelfhealActor::HandleResult aborted",
                {"marker", "NW72"},
                {"reason", ev->Get()->Record.GetErrorReason()});
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
        YDB_LOG_ERROR("StateStorageReassignNodeSelfhealActor::RequestChangeStateStorage",
            {"marker", "NW72"},
            {"cmd", cmd});
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
            YDB_LOG_ERROR("TStateStorageReassignNodeSelfhealActor::HandleResult aborted",
                {"marker", "NW72"},
                {"reason", ev->Get()->Record.GetErrorReason()});
            Finish(TResult::ERROR, ev->Get()->Record.GetErrorReason());
        } else {
            AllowNextStep = true;
        }
    }

    void TStateStorageReassignNodeSelfhealActor::HandleWakeup() {
        if (!AllowNextStep) {
            YDB_LOG_ERROR("TStateStorageReassignNodeSelfhealActor::HandleWakeup aborted. Previous reconfiguration step not finished yet",
                {"marker", "NW78"});
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
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, HandleResult);
        )
    }
}
