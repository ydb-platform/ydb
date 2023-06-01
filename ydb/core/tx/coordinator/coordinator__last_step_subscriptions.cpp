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
            Y_VERIFY(itPrevServer != PipeServers.end());
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
        Y_VERIFY(res.second);

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

} // namespace NKikimr::NFlatTxCoordinator
