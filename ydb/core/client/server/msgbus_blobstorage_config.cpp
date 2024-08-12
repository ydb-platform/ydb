#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusBlobStorageConfig
    : public TMessageBusSecureRequest<TMessageBusSimpleTabletRequest<
        TMessageBusBlobStorageConfig,
        TEvBlobStorage::TEvControllerConfigResponse,
        NKikimrServices::TActivity::FRONT_BSADM_CONFIG
      >>
{
    NKikimrBlobStorage::TConfigRequest Request;

public:
    TMessageBusBlobStorageConfig(TBusMessageContext &msg, ui64 tabletId,
            const NKikimrBlobStorage::TConfigRequest &request, bool withRetry, TDuration timeout,
            const TString& token)
        : TMessageBusSecureRequest(msg, tabletId, withRetry, timeout, false)
        , Request(request)
    {
        SetSecurityToken(token);
        SetRequireAdminAccess(true);
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr &ev, const TActorContext &ctx) {
        auto &pb = *ev->Get()->Record.MutableResponse();
        TAutoPtr<TBusResponse> response(new TBusResponse);
        response->Record.SetStatus(MSTATUS_OK);
        pb.Swap(response->Record.MutableBlobStorageConfigResponse());
        SendReplyAndDie(response.Release(), ctx);
    }

    TEvBlobStorage::TEvControllerConfigRequest *MakeReq(const TActorContext&) {
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto &record = ev->Record;
        Request.SetUserSID(GetUserSID());
        Request.Swap(record.MutableRequest());
        return ev.Release();
    }
};

IActor* CreateMessageBusBlobStorageConfig(TBusMessageContext &msg) {
    const NKikimrClient::TBlobStorageConfigRequest &record = static_cast<TBusBlobStorageConfigRequest*>(msg.GetMessage())->Record;
    const ui64 tabletId = MakeBSControllerID();
    return new TMessageBusBlobStorageConfig(msg, tabletId, record.GetRequest(), true,
        TDuration::MilliSeconds(DefaultTimeout), record.GetSecurityToken());
}

}
}
