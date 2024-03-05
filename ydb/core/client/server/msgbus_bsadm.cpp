#include "msgbus_tabletreq.h"
#include "msgbus_securereq.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusBSAdmGroupReconfigureWipe : public TMessageBusSecureRequest<
        TMessageBusSimpleTabletRequest<
        TMessageBusBSAdmGroupReconfigureWipe,
        TEvBlobStorage::TEvControllerGroupReconfigureWipeResult,
        NKikimrServices::TActivity::FRONT_BSADM_RECONF_REPLACE>> {
    TAutoPtr<TEvBlobStorage::TEvControllerGroupReconfigureWipe> Ev;
public:
    TMessageBusBSAdmGroupReconfigureWipe(TBusMessageContext &msg, ui64 tabletId,
            TAutoPtr<TEvBlobStorage::TEvControllerGroupReconfigureWipe> ev, bool withRetry, TDuration timeout)
        : TMessageBusSecureRequest(msg, tabletId, withRetry, timeout, false)
        , Ev(ev)
    {
        SetSecurityToken(static_cast<TBusBSAdm*>(msg.GetMessage())->Record.GetSecurityToken());
        SetRequireAdminAccess(true);
    }

    void Handle(TEvBlobStorage::TEvControllerGroupReconfigureWipeResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBlobStorage::TEvControllerGroupReconfigureWipeResult &record = ev->Get()->Record;

        const auto resultStatus = record.GetStatus();
        switch (resultStatus) {
        case NKikimrProto::OK:
        case NKikimrProto::ALREADY:
            {
                TAutoPtr<TBusResponse> response(new TBusResponse());
                auto &x = response->Record;
                x.SetStatus(MSTATUS_OK);

                return SendReplyAndDie(response.Release(), ctx);
            }
        default:
            return SendReplyAndDie(
                new TBusResponseStatus(MSTATUS_ERROR, Sprintf("Error status returned %d with reason: %s", resultStatus,
                        record.HasErrorReason() ? record.GetErrorReason().c_str() : "empty")), ctx);
        }
    }

    TEvBlobStorage::TEvControllerGroupReconfigureWipe* MakeReq(const TActorContext&) {
        return Ev.Release();
    }
};

IActor* CreateMessageBusBSAdm(TBusMessageContext &msg) {
    const NKikimrClient::TBSAdm &record = static_cast<TBusBSAdm *>(msg.GetMessage())->Record;
    const ui64 tabletId = MakeBSControllerID();

    if (record.HasGroupReconfigureWipe()) {
        const auto &x = record.GetGroupReconfigureWipe();
        if (!x.HasLocation()) {
            return nullptr;
        }
        const auto &location = x.GetLocation();
        if (!location.HasNodeId() || !location.HasPDiskId() || !location.HasVSlotId()) {
            return nullptr;
        }

        TAutoPtr<TEvBlobStorage::TEvControllerGroupReconfigureWipe> req(
            new TEvBlobStorage::TEvControllerGroupReconfigureWipe(
                location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId()));

        return new TMessageBusBSAdmGroupReconfigureWipe(msg, tabletId, req, true,
            TDuration::MilliSeconds(DefaultTimeout));
    }

    return nullptr;
}

}
}
