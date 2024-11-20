#include "event.h"

namespace NKikimr::NBsQueue {

IEventBase *TEventHolder::MakeErrorReply(NKikimrProto::EReplyStatus status, const TString& errorReason,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& deserItems,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& deserBytes) {
    auto callback = [&](auto *event) -> IEventBase* {
        using T = std::remove_pointer_t<decltype(event)>;
        std::unique_ptr<T> temp;

        if (!event) {
            // if there is no local event in holder, we have to deserialize it
            temp.reset(static_cast<T*>(T::Load(Buffer.Get())));
            event = temp.get();
            ++*deserItems;
            *deserBytes += ByteSize;
        }

        auto res = std::make_unique<TMatchingResultTypeT<T>>();
        res->MakeError(status, std::move(errorReason), event->Record);
        return res.release();
    };

    return Apply(callback);
}

void TEventHolder::SendToVDisk(const TActorContext& ctx, const TActorId& remoteVDisk, ui64 queueCookie, ui64 msgId,
        ui64 sequenceId, bool sendMeCostSettings, NWilson::TTraceId traceId, const NBackpressure::TQueueClientId& clientId,
        const TBSQueueTimer& processingTimer) {
    // check that we are not discarded yet
    Y_ABORT_UNLESS(Type != 0);

    auto processMsgQoS = [&](auto& record) {
        // prepare extra buffer with some changed params
        NKikimrBlobStorage::TMsgQoS& msgQoS = *record.MutableMsgQoS();
        if (sendMeCostSettings) {
            msgQoS.SetSendMeCostSettings(true);
        }
        NKikimrBlobStorage::TMessageId& id = *msgQoS.MutableMsgId();
        id.SetMsgId(msgId);
        id.SetSequenceId(sequenceId);
        clientId.Serialize(&msgQoS);

        // update in sender queue duration
        TDuration inSenderQueue = TDuration::Seconds(processingTimer.Passed());
        NKikimrBlobStorage::TExecTimeStats& execTimeStats = *msgQoS.MutableExecTimeStats();
        execTimeStats.SetInSenderQueue(inSenderQueue.GetValue());
        LWTRACK(DSQueueVPutIsSent, Orbit, inSenderQueue.SecondsFloat() * 1e3);
    };

    const ui32 flags = IEventHandle::MakeFlags(InterconnectChannel, IEventHandle::FlagTrackDelivery);

    if (LocalEvent) {
        auto callback = [&](auto *ev) -> std::unique_ptr<IEventBase> {
            using T = std::remove_pointer_t<decltype(ev)>;
            processMsgQoS(ev->Record);
            auto clone = std::make_unique<T>();
            clone->Record.CopyFrom(ev->Record);
            for (ui32 i = 0, count = ev->GetPayloadCount(); i < count; ++i) {
                clone->AddPayload(TRope(ev->GetPayload(i)));
            }
            return clone;
        };
        ctx.Send(remoteVDisk, Apply(callback).release(), flags, queueCookie, std::move(traceId));
    } else {
        // FIXME: ensure that MsgQoS has the same field identifier in all structures
        NKikimrBlobStorage::TEvVPut record;
        processMsgQoS(record);

        // serialize that extra buffer
        TString buf;
        const bool status = record.SerializeToString(&buf);
        Y_ABORT_UNLESS(status);

        // send it to disk
        ctx.ExecutorThread.Send(new IEventHandle(Type, flags, remoteVDisk, ctx.SelfID,
            MakeIntrusive<TEventSerializedData>(*Buffer, std::move(buf)), queueCookie, nullptr, std::move(traceId)));
    }
}

void TEventHolder::Discard() {
    if (std::exchange(Type, 0)) {
        BSProxyCtx->Queue.Subtract(ByteSize);
        Buffer.Reset();
        LocalEvent.reset();
    }
}

} // NKikimr::NBsQueue
