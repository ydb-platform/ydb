#include "datashard_impl.h"

namespace NKikimr::NDataShard {

void TDataShard::OnYellowChannelsChanged() {
    if (!IsAnyChannelYellowStop()) {
        NotifyOverloadSubscribers(ERejectReason::YellowChannels);
    }
}

void TDataShard::OnRejectProbabilityRelaxed() {
    NotifyOverloadSubscribers(ERejectReason::OverloadByProbability);
    for (auto& ev : DelayedS3UploadRows) {
        TActivationContext::Send(ev.Release());
    }
    DelayedS3UploadRows.clear();
}

bool TDataShard::HasPipeServer(const TActorId& pipeServerId) {
    return PipeServers.contains(pipeServerId);
}

bool TDataShard::AddOverloadSubscriber(const TActorId& pipeServerId, const TActorId& actorId, ui64 seqNo, ERejectReasons reasons) {
    auto it = PipeServers.find(pipeServerId);
    if (it != PipeServers.end()) {
        bool wasEmpty = it->second.OverloadSubscribers.empty();
        auto& entry = it->second.OverloadSubscribers[actorId];
        if (entry.SeqNo <= seqNo) {
            entry.SeqNo = seqNo;
            // Increment counter for every new reason
            EnumerateRejectReasons(reasons - entry.Reasons, [&](ERejectReason reason) {
                OverloadSubscribersByReason[RejectReasonIndex(reason)]++;
            });
            entry.Reasons |= reasons;
        }
        if (wasEmpty) {
            PipeServersWithOverloadSubscribers.PushBack(&it->second);
        }
        return true;
    }
    return false;
}

void TDataShard::NotifyOverloadSubscribers(ERejectReason reason) {
    if (OverloadSubscribersByReason[RejectReasonIndex(reason)] == 0) {
        // Avoid spending time when we know it is pointless
        return;
    }
    ERejectReasons reasons = MakeRejectReasons(reason);

    TPipeServersWithOverloadSubscribers left;
    while (!PipeServersWithOverloadSubscribers.Empty()) {
        TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
        for (auto it = pipeServer->OverloadSubscribers.begin(); it != pipeServer->OverloadSubscribers.end();) {
            auto current = it++;
            const TActorId& actorId = current->first;
            TOverloadSubscriber& entry = current->second;
            if ((entry.Reasons & reasons) != reasons) {
                // Reasons don't match
                continue;
            }
            entry.Reasons -= reasons;
            OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
            if (entry.Reasons == ERejectReasons::None) {
                SendViaSession(
                    pipeServer->InterconnectSession,
                    actorId,
                    SelfId(),
                    new TEvDataShard::TEvOverloadReady(TabletID(), entry.SeqNo));
                pipeServer->OverloadSubscribers.erase(current);
            }
        }
        if (!pipeServer->OverloadSubscribers.empty()) {
            left.PushBack(pipeServer);
        }
    }
    PipeServersWithOverloadSubscribers.Append(left);
}

void TDataShard::NotifyAllOverloadSubscribers() {
    bool clearedSubscribers = false;
    while (!PipeServersWithOverloadSubscribers.Empty()) {
        TPipeServerInfo* pipeServer = PipeServersWithOverloadSubscribers.PopFront();
        for (auto it = pipeServer->OverloadSubscribers.begin(); it != pipeServer->OverloadSubscribers.end(); ++it) {
            const TActorId& actorId = it->first;
            TOverloadSubscriber& entry = it->second;
            SendViaSession(
                pipeServer->InterconnectSession,
                actorId,
                SelfId(),
                new TEvDataShard::TEvOverloadReady(TabletID(), entry.SeqNo));
        }
        pipeServer->OverloadSubscribers.clear();
        clearedSubscribers = true;
    }

    if (clearedSubscribers) {
        for (int i = 0; i < RejectReasonCount; ++i) {
            OverloadSubscribersByReason[i] = 0;
        }
    }
}

void TDataShard::DiscardOverloadSubscribers(TPipeServerInfo& pipeServer) {
    for (auto it = pipeServer.OverloadSubscribers.begin(); it != pipeServer.OverloadSubscribers.end(); ++it) {
        TOverloadSubscriber& entry = it->second;
        EnumerateRejectReasons(entry.Reasons, [&](ERejectReason reason) {
            OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
        });
    }
    pipeServer.OverloadSubscribers.clear();
    PipeServersWithOverloadSubscribers.Remove(&pipeServer);
}

void TDataShard::Handle(TEvDataShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    auto* msg = ev->Get();
    if (auto* pipeServer = PipeServers.FindPtr(ev->Recipient)) {
        auto it = pipeServer->OverloadSubscribers.find(ev->Sender);
        if (it != pipeServer->OverloadSubscribers.end()) {
            if (it->second.SeqNo == msg->Record.GetSeqNo()) {
                EnumerateRejectReasons(it->second.Reasons, [&](ERejectReason reason) {
                    OverloadSubscribersByReason[RejectReasonIndex(reason)]--;
                });
                pipeServer->OverloadSubscribers.erase(it);
                if (pipeServer->OverloadSubscribers.empty()) {
                    PipeServersWithOverloadSubscribers.Remove(pipeServer);
                }
            }
        }
    }
}

} // namespae NKikimr::NDataShard
