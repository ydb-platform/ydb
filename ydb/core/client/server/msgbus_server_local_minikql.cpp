#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusLocalMKQL : public TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TMessageBusLocalMKQL, TEvTablet::TEvLocalMKQLResponse, NKikimrServices::TActivity::FRONT_LOCAL_MQKL>> {
    const NKikimrClient::TLocalMKQL Request;
public:
    TMessageBusLocalMKQL(TBusMessageContext &msg, const NKikimrClient::TLocalMKQL &request, ui64 tabletId, bool withRetry, TDuration timeout, bool connectToFollower)
        : TMessageBusSecureRequest(msg, tabletId, withRetry, timeout, connectToFollower)
        , Request(request)
    {
        SetSecurityToken(static_cast<TBusTabletLocalMKQL*>(msg.GetMessage())->Record.GetSecurityToken());
        SetRequireAdminAccess(true);
        SetPeerName(msg.GetPeerName());
    }

    void Handle(TEvTablet::TEvLocalMKQLResponse::TPtr &ev, const TActorContext &ctx) {
        const auto &record = ev->Get()->Record;

        const auto replyStatus = (record.GetStatus() == NKikimrProto::OK) ? MSTATUS_OK : MSTATUS_ERROR;
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(replyStatus));

        response->Record.SetTabletId(TabletID);
        if (record.HasExecutionEngineEvaluatedResponse())
            *response->Record.MutableExecutionEngineEvaluatedResponse() = record.GetExecutionEngineEvaluatedResponse();
        if (record.HasEngineStatus())
            response->Record.SetExecutionEngineStatus(record.GetEngineStatus());
        if (record.HasEngineResponseStatus())
            response->Record.SetExecutionEngineResponseStatus(record.GetEngineResponseStatus());

        if (record.HasMiniKQLErrors())
            response->Record.SetMiniKQLErrors(record.GetMiniKQLErrors());

        if (record.HasCompileResults())
            *response->Record.MutableMiniKQLCompileResults() = record.GetCompileResults();

        return SendReplyAndDie(response.Release(), ctx);
    }

    TEvTablet::TEvLocalMKQL* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);

        TAutoPtr<TEvTablet::TEvLocalMKQL> req = new TEvTablet::TEvLocalMKQL();
        auto *pgm = req->Record.MutableProgram();
        *pgm = Request.GetProgram();

        return req.Release();
    }
};

IActor* CreateMessageBusLocalMKQL(TBusMessageContext &msg) {
    const auto &record = static_cast<TBusTabletLocalMKQL *>(msg.GetMessage())->Record;

    if (!record.HasProgram())
        return nullptr;

    const bool connectToFollower = record.HasConnectToFollower() ? record.GetConnectToFollower() : false;
    const ui64 tabletId = record.GetTabletID();
    const bool withRetry = record.HasWithRetry() ? record.GetWithRetry() : false;
    const TDuration timeout = TDuration::MilliSeconds(record.HasTimeout() ? record.GetTimeout() : DefaultTimeout);

    return new TMessageBusLocalMKQL(msg, record, tabletId, withRetry, timeout, connectToFollower);
}

}}
