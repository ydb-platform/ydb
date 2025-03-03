#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusBlobStorageDescribe
    : public TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<
        TMessageBusBlobStorageDescribe,
        TEvBlobStorage::TEvControllerDescribeResponse,
        NKikimrServices::TActivity::FRONT_BSADM_CONFIG
      >>
{
    NKikimrBlobStorage::TDescribeRequest Request;

public:
    TMessageBusBlobStorageDescribe(TBusMessageContext &msg, ui64 tabletId,
            const NKikimrBlobStorage::TDescribeRequest &request, bool withRetry, TDuration timeout,
            const TString& token)
        : TMessageBusSecureRequest(msg, tabletId, withRetry, timeout, false)
        , Request(request)
    {
        SetSecurityToken(token);
        SetRequireAdminAccess(true);
        SetPeerName(msg.GetPeerName());
    }

    void Handle(TEvBlobStorage::TEvControllerDescribeResponse::TPtr &ev, const TActorContext &ctx) {
        auto &pb = *ev->Get()->Record.MutableResponse();
        TAutoPtr<TBusBlobStorageDescribeResponse> response(new TBusBlobStorageDescribeResponse);
        response->Record.SetStatus(MSTATUS_OK);
        pb.Swap(response->Record.MutableResponse());
        SendReplyAndDie(response.Release(), ctx);
    }

    TEvBlobStorage::TEvControllerDescribeRequest *MakeReq(const TActorContext&) {
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerDescribeRequest>();
        auto &record = ev->Record;
        Request.Swap(record.MutableRequest());
        return ev.Release();
    }
};

IActor* CreateMessageBusBlobStorageDescribe(TBusMessageContext &msg) {
    const NKikimrClient::TBlobStorageDescribeRequest &record = static_cast<TBusBlobStorageDescribeRequest*>(msg.GetMessage())->Record;
    const ui64 tabletId = MakeBSControllerID();
    return new TMessageBusBlobStorageDescribe(msg, tabletId, record.GetRequest(), true,
        TDuration::MilliSeconds(DefaultTimeout), record.GetSecurityToken());
}

}
}
