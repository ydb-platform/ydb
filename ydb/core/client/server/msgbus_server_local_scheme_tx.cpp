#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusLocalSchemeTx : public TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TMessageBusLocalSchemeTx,
        TEvTablet::TEvLocalSchemeTxResponse, NKikimrServices::TActivity::FRONT_LOCAL_TXRQ>> {
    using TBase = TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<TMessageBusLocalSchemeTx,
    TEvTablet::TEvLocalSchemeTxResponse, NKikimrServices::TActivity::FRONT_LOCAL_TXRQ>>;
    NKikimrClient::TLocalSchemeTx Request;
public:
    TMessageBusLocalSchemeTx(TBusMessageContext &msg, NKikimrClient::TLocalSchemeTx &request, ui64 tabletId, bool withRetry, TDuration timeout, bool connectToFollower)
        : TMessageBusSecureRequest(msg, tabletId, withRetry, timeout, connectToFollower)
        , Request()
    {
        Request.Swap(&request);
        TBase::SetSecurityToken(Request.GetSecurityToken());
        TBase::SetRequireAdminAccess(true);
        TBase::SetPeerName(msg.GetPeerName());
    }

    void Handle(TEvTablet::TEvLocalSchemeTxResponse::TPtr &ev, const TActorContext &ctx) {
        auto &record = ev->Get()->Record;

        const auto replyStatus = (record.GetStatus() == NKikimrProto::OK) ? MSTATUS_OK : MSTATUS_ERROR;
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(replyStatus));

        response->Record.SetTabletId(TabletID);
        response->Record.MutableLocalDbScheme()->Swap(record.MutableFullScheme());

        if (record.HasErrorReason())
            response->Record.SetErrorReason(record.GetErrorReason());

        return SendReplyAndDie(response.Release(), ctx);
    }

    TEvTablet::TEvLocalSchemeTx* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);

        TAutoPtr<TEvTablet::TEvLocalSchemeTx> req = new TEvTablet::TEvLocalSchemeTx();
        req->Record.MutableSchemeChanges()->Swap(Request.MutableSchemeChanges());
        req->Record.SetDryRun(Request.GetDryRun());

        return req.Release();
    }
};

IActor* CreateMessageBusLocalSchemeTx(TBusMessageContext &msg) {
    auto &record = static_cast<TBusTabletLocalSchemeTx *>(msg.GetMessage())->Record;

    const bool connectToFollower = record.HasConnectToFollower() ? record.GetConnectToFollower() : false;
    const ui64 tabletId = record.GetTabletID();
    const bool withRetry = record.HasWithRetry() ? record.GetWithRetry() : false;
    const TDuration timeout = TDuration::MilliSeconds(record.HasTimeout() ? record.GetTimeout() : DefaultTimeout);

    return new TMessageBusLocalSchemeTx(msg, record, tabletId, withRetry, timeout, connectToFollower);
}

}}
