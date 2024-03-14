#include "msgbus_servicereq.h"
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>
namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    const ui32 DefaultTimeout = 90000;
}

template <typename ResponseType>
class TMessageBusLocalEnumerateTablets: public TMessageBusLocalServiceRequest<TMessageBusLocalEnumerateTablets<ResponseType>, NKikimrServices::TActivity::FRONT_ENUMERATE_TABLETS> {
    using TBase = TMessageBusLocalServiceRequest<TMessageBusLocalEnumerateTablets<ResponseType>, NKikimrServices::TActivity::FRONT_ENUMERATE_TABLETS>;
    ui64 DomainUid;
    ui32 NodeId;
    TTabletTypes::EType TabletType;
    bool IsFiltered;
    bool IsOk;
    bool IsNodeIdPresent;
public:
    TMessageBusLocalEnumerateTablets(TBusMessageContext &msg, TDuration timeout)
        : TBase(msg, timeout)
        , DomainUid(0)
        , NodeId(0)
        , TabletType()
        , IsFiltered(false)
        , IsOk(true)
        , IsNodeIdPresent(false)
    {
        const auto &record = static_cast<TBusLocalEnumerateTablets*>(msg.GetMessage())->Record;
        IsOk = IsOk && record.HasDomainUid();
        if (record.HasNodeId()) {
            IsNodeIdPresent = true;
            NodeId = record.GetNodeId();
        }
        if (IsOk) {
            DomainUid = record.GetDomainUid();
            if (record.HasTabletType()) {
                IsFiltered = true;
                TabletType = record.GetTabletType();
            }
        }
    }

    void Handle(TEvLocal::TEvEnumerateTabletsResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimrLocal::TEvEnumerateTabletsResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        THolder<ResponseType> response(new ResponseType());
        if (record.GetStatus() != NKikimrProto::OK) {
            response->Record.SetStatus(MSTATUS_ERROR);
            response->Record.SetErrorReason(Sprintf("Local response is not OK (Status# %s), Marker# LE1",
                NKikimrProto::EReplyStatus_Name(record.GetStatus()).data()));
            TBase::SendReplyAndDie(response.Release(), ctx);
        } else {
            response->Record.SetStatus(MSTATUS_OK);
            for (ui32 i = 0; i < record.TabletInfoSize(); ++i) {
                auto &srcInfo = record.GetTabletInfo(i);
                auto *dstInfo = response->Record.AddTabletInfo();
                if (srcInfo.HasTabletId()) {
                    dstInfo->SetTabletId(srcInfo.GetTabletId());
                }
                if (srcInfo.HasTabletType()) {
                    dstInfo->SetTabletType(srcInfo.GetTabletType());
                }
            }
            TBase::SendReplyAndDie(response.Release(), ctx);
        }
    }

    TActorId MakeServiceID(const TActorContext &ctx) {
        auto &domainsInfo = AppData(ctx)->DomainsInfo;
        if (!domainsInfo->Domain || domainsInfo->GetDomain()->DomainUid != DomainUid) {
            // Report details in CreateErrorReply
            TActorId invalidId;
            return invalidId;
        }
        ui32 nodeId = IsNodeIdPresent ? NodeId : ctx.SelfID.NodeId();
        ui64 hiveId = domainsInfo->GetHive();
        return MakeLocalRegistrarID(nodeId, hiveId);
    }

    TEvLocal::TEvEnumerateTablets* MakeReq(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        if (IsFiltered) {
            return new TEvLocal::TEvEnumerateTablets(TabletType);
        }
        return new TEvLocal::TEvEnumerateTablets();
    }

    NBus::TBusMessage* CreateErrorReply(EResponseStatus status, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        Y_UNUSED(status);
        ui64 nodeId = IsNodeIdPresent ? NodeId : ctx.SelfID.NodeId();
        THolder<ResponseType> response(new ResponseType());
        response->Record.SetStatus(MSTATUS_ERROR);
        response->Record.SetErrorReason(Sprintf("Invalid DomainUid# %" PRIu64 ", NodeId# %" PRIu64
            " or kikimr hive/domain/node configuration, Marker# LE3", (ui64)DomainUid, (ui64)nodeId));
        return response.Release();
    }

    void HandleTimeout(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(MSTATUS_TIMEOUT, ""));
        TBase::SendReplyAndDie(response.Release(), ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        THolder<ResponseType> response(new ResponseType());
        ui64 nodeId = IsNodeIdPresent ? NodeId : ctx.SelfID.NodeId();
        response->Record.SetStatus(MSTATUS_ERROR);
        response->Record.SetErrorReason(Sprintf("Request was not delivered to Local, NodeId# %" PRIu64
            ", Marker# LE2", (ui64)nodeId));
        TBase::SendReplyAndDie(response.Release(), ctx);

    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLocal::TEvEnumerateTabletsResult, Handle);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

IActor* CreateMessageBusLocalEnumerateTablets(TBusMessageContext &msg) {
    //const auto &record = static_cast<TBusLocalEnumerateTablets*>(msg.GetMessage())->Record;
    //const TDuration timeout = TDuration::MilliSeconds(record.HasTimeout() ? record.GetTimeout() : DefaultTimeout);
    const TDuration timeout = TDuration::MilliSeconds(DefaultTimeout);

    if (msg.GetMessage()->GetHeader()->Type == MTYPE_CLIENT_OLD_LOCAL_ENUMERATE_TABLETS) {
        return new TMessageBusLocalEnumerateTablets<TBusLocalEnumerateTabletsResult>(msg, timeout);
    } else {
        return new TMessageBusLocalEnumerateTablets<TBusResponse>(msg, timeout);
    }
}

}
}
