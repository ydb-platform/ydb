#include "coordinator_impl.h"

namespace NKikimr::NFlatTxCoordinator {

    void TTxCoordinator::Handle(TEvTxProxy::TEvSubscribeLastStep::TPtr& ev) {
        TActorId pipeServerId = ev->Recipient;
        auto itPipeServer = PipeServers.find(pipeServerId);
        if (Y_UNLIKELY(itPipeServer == PipeServers.end())) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                "Unexpected TEvSubscribeLastStep from " << ev->Sender
                << " at coordinator " << TabletID()
                << " without an active pipe server");
            return;
        }

        auto* msg = ev->Get();
        if (Y_UNLIKELY(msg->Record.GetCoordinatorID() != TabletID())) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                "Unexpected TEvSubscribeLastStep from " << ev->Sender
                << " at coordinator " << TabletID()
                << " for coordinator " << msg->Record.GetCoordinatorID());
            return;
        }

        TLastStepSubscriber* subscriber;

        auto itSubscriber = LastStepSubscribers.find(ev->Sender);
        if (itSubscriber != LastStepSubscribers.end()) {
            subscriber = &itSubscriber->second;

            if (msg->Record.GetSeqNo() <= subscriber->SeqNo) {
                // Ignore messages that are out of sequence
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                    "Ignored TEvSubscribeLastStep from " << ev->Sender
                    << " at coordinator " << TabletID()
                    << " with seqNo " << msg->Record.GetSeqNo()
                    << " existing seqNo " << subscriber->SeqNo);
                return;
            }

            auto itPrevServer = PipeServers.find(subscriber->PipeServer);
            Y_ABORT_UNLESS(itPrevServer != PipeServers.end());
            itPrevServer->second.LastStepSubscribers.erase(ev->Sender);
        } else {
            subscriber = &LastStepSubscribers[ev->Sender];
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
            "Processing TEvSubscribeLastStep from " << ev->Sender
            << " at coordinator " << TabletID()
            << " with seqNo " << msg->Record.GetSeqNo()
            << " and cookie " << ev->Cookie);

        subscriber->PipeServer = pipeServerId;
        subscriber->InterconnectSession = ev->InterconnectSession;
        subscriber->SeqNo = msg->Record.GetSeqNo();
        subscriber->Cookie = ev->Cookie;

        auto res = itPipeServer->second.LastStepSubscribers.emplace(ev->Sender, subscriber);
        Y_ABORT_UNLESS(res.second);

        if (VolatileState.LastPlanned > 0) {
            NotifyUpdatedLastStep(ev->Sender, *subscriber);
        }
    }

    void TTxCoordinator::Handle(TEvTxProxy::TEvUnsubscribeLastStep::TPtr& ev) {
        TActorId pipeServerId = ev->Recipient;
        auto itPipeServer = PipeServers.find(pipeServerId);
        if (Y_UNLIKELY(itPipeServer == PipeServers.end())) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                "Unexpected TEvUnsubscribeLastStep from " << ev->Sender
                << " at coordinator " << TabletID()
                << " without an active pipe server");
            return;
        }

        auto* msg = ev->Get();
        if (Y_UNLIKELY(msg->Record.GetCoordinatorID() != TabletID())) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                "Unexpected TEvSubscribeLastStep from " << ev->Sender
                << " at coordinator " << TabletID()
                << " for coordinator " << msg->Record.GetCoordinatorID());
            return;
        }

        auto itSubscriber = LastStepSubscribers.find(ev->Sender);
        if (itSubscriber == LastStepSubscribers.end()) {
            return;
        }

        auto& subscriber = itSubscriber->second;
        if (pipeServerId == subscriber.PipeServer && msg->Record.GetSeqNo() == subscriber.SeqNo) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
                "Processing TEvUnsubscribeLastStep from " << ev->Sender
                << " at coordinator " << TabletID()
                << " with seqNo " << msg->Record.GetSeqNo());
            itPipeServer->second.LastStepSubscribers.erase(ev->Sender);
            LastStepSubscribers.erase(itSubscriber);
        }
    }

    void TTxCoordinator::NotifyUpdatedLastStep() {
        while (!PendingSiblingSteps.empty()) {
            auto it = PendingSiblingSteps.begin();
            if (VolatileState.LastPlanned < *it) {
                break;
            }
            PendingSiblingSteps.erase(it);
        }
        for (const auto& pr : LastStepSubscribers) {
            NotifyUpdatedLastStep(pr.first, pr.second);
        }
    }

    void TTxCoordinator::NotifyUpdatedLastStep(const TActorId& actorId, const TLastStepSubscriber& subscriber) {
        SendViaSession(
            subscriber.InterconnectSession,
            actorId,
            new TEvTxProxy::TEvUpdatedLastStep(TabletID(), subscriber.SeqNo, VolatileState.LastPlanned),
            0, subscriber.Cookie);
    }

    void TTxCoordinator::SubscribeToSiblings() {
        // We subscribe the first time we see non-empty coordinators list
        if (!Siblings.empty() || Config.Coordinators.empty()) {
            return;
        }

        for (ui64 coordinatorId : Config.Coordinators) {
            if (coordinatorId == TabletID()) {
                // We never subscribe to ourselves
                continue;
            }

            auto& state = Siblings[coordinatorId];
            state.CoordinatorId = coordinatorId;
            SubscribeToSibling(state);
        }
    }

    void TTxCoordinator::SubscribeToSibling(TSiblingState& state) {
        if (!state.Subscribed) {
            auto pipeCache = MakePipePerNodeCacheID(false);
            Send(pipeCache, new TEvPipeCache::TEvForward(
                    new TEvTxProxy::TEvSubscribeLastStep(state.CoordinatorId, ++state.SeqNo),
                    state.CoordinatorId,
                    true));
            state.Subscribed = true;
        }
    }

    void TTxCoordinator::UnsubscribeFromSiblings() {
        for (auto& pr : Siblings) {
            if (pr.second.Subscribed) {
                auto pipeCache = MakePipePerNodeCacheID(false);
                Send(pipeCache, new TEvPipeCache::TEvForward(
                        new TEvTxProxy::TEvUnsubscribeLastStep(pr.first, pr.second.SeqNo),
                        pr.first,
                        false));
                Send(pipeCache, new TEvPipeCache::TEvUnlink(pr.first));
                pr.second.Subscribed = false;
            }
        }
    }

    void TTxCoordinator::Handle(TEvTxProxy::TEvUpdatedLastStep::TPtr& ev) {
        auto* msg = ev->Get();
        ui64 coordinatorId = msg->Record.GetCoordinatorID();
        ui64 seqNo = msg->Record.GetSeqNo();
        if (auto* state = Siblings.FindPtr(coordinatorId); state && state->Subscribed && state->SeqNo == seqNo) {
            // Receiving TEvUpdateLastStep confirmed this sibling supports
            // subscriptions and will notify us on new steps, this is later
            // used to decide if we may relax plan step ticking.
            if (!state->Confirmed) {
                ++SiblingsConfirmed;
                state->Confirmed = true;
            }
        }

        ui64 step = msg->Record.GetLastStep();
        if (step > VolatileState.LastPlanned && PendingSiblingSteps.insert(step).second) {
            SchedulePlanTickExact(step);
        }
    }

    void TTxCoordinator::Handle(TEvTxProxy::TEvRequirePlanSteps::TPtr& ev) {
        ui64 volatileLeaseMs = VolatilePlanLeaseMs;
        bool usesVolatilePlanning = volatileLeaseMs > 0;

        auto* msg = ev->Get();
        for (ui64 step : msg->Record.GetPlanSteps()) {
            if (!usesVolatilePlanning) {
                // Note: we want to align requested steps to plan resolution
                // when volatile planning is not used. Otherwise extra steps
                // are cheap and reduce latency.
                step = AlignPlanStep(step);
            }
            // Note: this is not a sibling step, but it behaves similar enough
            // so we reuse the same queue here.
            if (step > VolatileState.LastPlanned && PendingSiblingSteps.insert(step).second) {
                SchedulePlanTickExact(step);
            }
        }
    }

    void TTxCoordinator::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto* msg = ev->Get();
        if (auto* state = Siblings.FindPtr(msg->TabletId); state && state->Subscribed) {
            state->Subscribed = false;
            if (state->Confirmed) {
                bool wasAllConfirmed = SiblingsConfirmed == Siblings.size();
                --SiblingsConfirmed;
                state->Confirmed = false;
                if (wasAllConfirmed) {
                    // Switching to unconfirmed mode, next nick may change
                    SchedulePlanTick();
                }
            }
            // TODO: add retry delay
            SubscribeToSibling(*state);
        }
    }

} // namespace NKikimr::NFlatTxCoordinator
