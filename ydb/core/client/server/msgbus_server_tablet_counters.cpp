#include "msgbus_tabletreq.h"

#include <ydb/core/protos/tablet_counters.pb.h>

namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui64 DefaultTimeout = 90000;
}

class TMessageBusTabletCounters : public TMessageBusSimpleTabletRequest<TMessageBusTabletCounters, TEvTablet::TEvGetCountersResponse, NKikimrServices::TActivity::FRONT_GETCOUNTERS> {
    NKikimrClient::TTabletCountersRequest Request;
public:
    TMessageBusTabletCounters(TBusMessageContext &msg, NKikimrClient::TTabletCountersRequest &request, ui64 tabletId,
                          bool withRetry, TDuration timeout, bool connectToFollower)
        : TMessageBusSimpleTabletRequest(msg, tabletId, withRetry, timeout, connectToFollower)
        , Request()
    {
        Request.Swap(&request);
    }

    void Handle(TEvTablet::TEvGetCountersResponse::TPtr &ev, const TActorContext &ctx) {
        auto &record = ev->Get()->Record;

        TAutoPtr<TBusResponse> response(new TBusResponseStatus(MSTATUS_OK));
        response->Record.SetTabletId(TabletID);
        response->Record.MutableTabletCounters()->Swap(record.MutableTabletCounters());

        return SendReplyAndDie(response.Release(), ctx);
    }

    TEvTablet::TEvGetCounters* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        return new TEvTablet::TEvGetCounters();
    }
};

IActor* CreateMessageBusTabletCountersRequest(TBusMessageContext &msg) {
    auto &record = static_cast<TBusTabletCountersRequest*>(msg.GetMessage())->Record;

    const bool connectToFollower = record.HasConnectToFollower() ? record.GetConnectToFollower() : false;
    const ui64 tabletId = record.GetTabletID();
    const bool withRetry = record.HasWithRetry() ? record.GetWithRetry() : false;
    const TDuration timeout = TDuration::MilliSeconds(record.HasTimeout() ? record.GetTimeout() : DefaultTimeout);

    return new TMessageBusTabletCounters(msg, record, tabletId, withRetry, timeout, connectToFollower);
}

}}
