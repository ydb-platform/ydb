#include "msgbus_tabletreq.h"
#include <ydb/core/keyvalue/keyvalue_events.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui32 DefaultTimeoutMs = 1000 * 90; // 90 seconds is a good default
    const ui64 MaxAllowedTimeoutMs = 1000 * 60 * 30; // 30 minutes is an instanely long request
}

template <typename ResponseType>
class TMessageBusKeyValue
        : public TMessageBusSimpleTabletRequest<TMessageBusKeyValue<ResponseType>, TEvKeyValue::TEvResponse, NKikimrServices::TActivity::FRONT_KV_REQUEST> {
    using TBase = TMessageBusSimpleTabletRequest<TMessageBusKeyValue<ResponseType>, TEvKeyValue::TEvResponse, NKikimrServices::TActivity::FRONT_KV_REQUEST>;
public:
    NKikimrClient::TKeyValueRequest RequestProto;
    ui64 TabletId;

    TMessageBusKeyValue(TBusMessageContext &msg, ui64 tabletId, bool withRetry, TDuration timeout)
        : TBase(msg, tabletId, withRetry, timeout, false)
        , RequestProto(static_cast<TBusKeyValue *>(msg.GetMessage())->Record)
        , TabletId(tabletId)
    {}

    void Handle(TEvKeyValue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
        TEvKeyValue::TEvResponse *msg = ev->Get();
        TAutoPtr<ResponseType> response(new ResponseType());
        CopyProtobufsByFieldName(response->Record, msg->Record);
        TBase::SendReplyAndDie(response.Release(), ctx);
    }

    TEvKeyValue::TEvRequest* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest());
        request->Record = RequestProto;
        return request.Release();
    }

     NBus::TBusMessage* CreateErrorReply(EResponseStatus status, const TActorContext &ctx,
            const TString& text = TString()) override {
        LOG_WARN_S(ctx, NKikimrServices::MSGBUS_REQUEST, "TMessageBusKeyValue TabletId# " << TabletId
            << " status# " << status << " text# \"" << text << "\" Marker# MBKV2" << Endl);

        TAutoPtr<ResponseType> response(new ResponseType());
        response->Record.SetStatus(status);
        if (text) {
            response->Record.SetErrorReason(text);
        } else {
            TStringStream str;
            str << "TMessageBusKeyValue unknown error, TabletId# " << TabletId;
            str << " status# " << status;
            str << " Marker# MBKV1" << Endl;
            response->Record.SetErrorReason(str.Str());
        }
        return response.Release();
    }
};

IActor* CreateMessageBusKeyValue(NKikimr::NMsgBusProxy::TBusMessageContext &msg) {
    auto &record = static_cast<TBusKeyValue *>(msg.GetMessage())->Record;

    const ui64 tabletId = record.GetTabletId();
    const bool withRetry = true;
    TDuration timeout = TDuration::MilliSeconds(DefaultTimeoutMs);
    TInstant now(TInstant::Now());
    bool doSetDeadline = true;
    if (record.HasDeadlineInstantMs()) {
        ui64 nowMs = now.MilliSeconds();
        ui64 deadlineMs = record.GetDeadlineInstantMs();
        if (deadlineMs >= nowMs) {
            ui64 timeoutMs = deadlineMs - nowMs;
            if (timeoutMs > MaxAllowedTimeoutMs) {
                // overwrite the deadline since there is no sense in allowing requests longer than MaxAllowedTimeoutMs
                timeout = TDuration::MilliSeconds(MaxAllowedTimeoutMs);
            } else {
                doSetDeadline = false;
                timeout = TDuration::MilliSeconds(timeoutMs);
            }
        } else {
            doSetDeadline = false;
            timeout = TDuration::MilliSeconds(0);
        }
    }
    if (doSetDeadline) {
        TInstant deadlineInstant = now + timeout;
        ui64 deadlineInstantMs = deadlineInstant.MilliSeconds();
        record.SetDeadlineInstantMs(deadlineInstantMs);
    }
    if (msg.GetMessage()->GetHeader()->Type == MTYPE_CLIENT_OLD_KEYVALUE) {
        return new TMessageBusKeyValue<TBusKeyValueResponse>(msg, tabletId, withRetry, timeout);
    } else {
        return new TMessageBusKeyValue<TBusResponse>(msg, tabletId, withRetry, timeout);
    }
}

}
}
