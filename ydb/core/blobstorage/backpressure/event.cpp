#include "event.h"

#include <ydb/library/actors/core/event_pb.h>

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
        const TBSQueueTimer& processingTimer, const ::NMonitoring::TDynamicCounters::TCounterPtr& serItems,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& serBytes) {
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

    if (Local && Event) {
        auto callback = [&](auto *ev) -> std::unique_ptr<IEventBase> {
            using T = std::remove_pointer_t<decltype(ev)>;
            auto clone = std::make_unique<T>();
            clone->Record.CopyFrom(ev->Record);
            processMsgQoS(clone->Record);
            for (ui32 i = 0, count = ev->GetPayloadCount(); i < count; ++i) {
                clone->AddPayload(TRope(ev->GetPayload(i)));
            }
            return clone;
        };
        ctx.Send(remoteVDisk, Apply(callback).release(), flags, queueCookie, std::move(traceId));
        return;
    }

    // FIXME: ensure that MsgQoS has the same field identifier in all structures
    NKikimrBlobStorage::TEvVPut record;
    processMsgQoS(record);

    // serialize that extra buffer
    TString buf;
    const bool status = record.SerializeToString(&buf);
    Y_ABORT_UNLESS(status);

    TIntrusivePtr<TEventSerializedData> buffer;
    if (Event) {
        TAllocChunkSerializer serializer;
        const bool success = Event->SerializeToArcadiaStream(&serializer);
        Y_ABORT_UNLESS(success);
        buffer = serializer.Release(Event->CreateSerializationInfo(false));
        ++*serItems;
        *serBytes += buffer->GetSize();
        // keep section sizes consistent after appending MsgQoS.
        if (const auto& info = buffer->GetSerializationInfo(); !info.Sections.empty()) {
            TEventSerializationInfo updated(info);
            updated.Sections.push_back(TEventSectionInfo{0, buf.size(), 0, 0, true});
            buffer->SetSerializationInfo(std::move(updated));
        }
        buffer->Append(std::move(buf));
    } else {
        // keep the original buffer intact for retransmission.
        buffer = MakeIntrusive<TEventSerializedData>(*Buffer, std::move(buf));
    }

    // send it to disk
    ctx.Send(new IEventHandle(Type, flags, remoteVDisk, ctx.SelfID, std::move(buffer), queueCookie, nullptr,
        std::move(traceId)));
}

void TEventHolder::Discard() {
    if (std::exchange(Type, 0)) {
        BSProxyCtx->Queue.Subtract(ByteSize);
        Buffer.Reset();
        Event.reset();
    }
}

} // NKikimr::NBsQueue
