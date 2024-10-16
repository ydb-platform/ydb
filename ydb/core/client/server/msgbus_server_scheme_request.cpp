#include <ydb/core/base/ticket_parser.h>
#include "msgbus_server.h"
#include "msgbus_server_request.h"
#include "msgbus_server_proxy.h"
#include "msgbus_server_persqueue.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

template <typename T>
class TMessageBusServerSchemeRequest : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerSchemeRequest<T>>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerSchemeRequest<T>>>;
    THolder<T> Request;

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        const TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
        // completion
            return ReplyWithResult(MSTATUS_OK, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
        // completion
            return ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress:
            return ReplyWithResult(MSTATUS_INPROGRESS, msg->Record, ctx);
        default:
            return ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
        }
    }

    void ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus &result, const TActorContext &ctx);

    void FillStatus(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus &result, TBusResponse* response)
    {
        response->Record.SetStatus(status);
        if (result.HasPathId()) {
            response->Record.MutableFlatTxId()->SetPathId(result.GetPathId());
        }

        if (result.HasPathCreateTxId()) {
            response->Record.MutableFlatTxId()->SetTxId(result.GetPathCreateTxId());
        } else if (result.HasPathDropTxId()) {
            response->Record.MutableFlatTxId()->SetTxId(result.GetPathDropTxId());
        } else if (result.HasTxId()) {
            response->Record.MutableFlatTxId()->SetTxId(result.GetTxId());
        }

        if (result.HasSchemeShardTabletId())
            response->Record.MutableFlatTxId()->SetSchemeShardTabletId(result.GetSchemeShardTabletId());

        if (result.HasSchemeShardReason()) {
            response->Record.SetErrorReason(result.GetSchemeShardReason());
        }

        if (result.HasSchemeShardStatus()) {
            response->Record.SetSchemeStatus(result.GetSchemeShardStatus());
        }

        if (result.HasStatus()) {
            response->Record.SetProxyErrorCode(result.GetStatus());
        }
    }


public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_SCHEME_REQUEST; }

    template <TEvBusProxy::EEv EvId>
    TMessageBusServerSchemeRequest(TEvBusProxy::TEvMsgBusRequest<EvId>* msg)
        : TBase(msg->MsgContext)
        , Request(static_cast<T*>(msg->MsgContext.ReleaseMessage()))
    {
        TBase::SetSecurityToken(Request->Record.GetSecurityToken());
        TBase::SetRequireAdminAccess(true);
        TBase::SetPeerName(msg->MsgContext.GetPeerName());
    }

    //STFUNC(StateWork)
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        SendProposeRequest(ctx);
        TBase::Become(&TMessageBusServerSchemeRequest::StateWork);
    }

    void SendProposeRequest(const TActorContext &ctx);
};

template <>
void TMessageBusServerSchemeRequest<TBusPersQueue>::SendProposeRequest(const TActorContext &ctx) {
    TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> req(new TEvTxUserProxy::TEvProposeTransaction());
    NKikimrTxUserProxy::TEvProposeTransaction &record = req->Record;
    record.SetPeerName(GetPeerName());

    if (Request->Record.HasMetaRequest() && Request->Record.GetMetaRequest().HasCmdCreateTopic()) {
        const auto& cmd = Request->Record.GetMetaRequest().GetCmdCreateTopic();
        auto *transaction = record.MutableTransaction()->MutableModifyScheme();
        transaction->SetWorkingDir(TopicPrefix(ctx));
        transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup);
        auto *pqgroup = transaction->MutableCreatePersQueueGroup();
        pqgroup->SetName(cmd.GetTopic());
        pqgroup->SetTotalGroupCount(cmd.GetNumPartitions());
        pqgroup->SetPartitionPerTablet(cmd.GetNumPartitionsPerTablet());
        pqgroup->MutablePQTabletConfig()->MergeFrom(cmd.GetConfig());
    }

    if (Request->Record.HasMetaRequest() && Request->Record.GetMetaRequest().HasCmdChangeTopic()) {
        const auto& cmd = Request->Record.GetMetaRequest().GetCmdChangeTopic();
        auto *transaction = record.MutableTransaction()->MutableModifyScheme();
        transaction->SetWorkingDir(TopicPrefix(ctx));
        transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);
        auto *pqgroup = transaction->MutableAlterPersQueueGroup();
        pqgroup->SetName(cmd.GetTopic());
        if (cmd.HasNumPartitions())
            pqgroup->SetTotalGroupCount(cmd.GetNumPartitions());
        if (cmd.HasConfig())
            pqgroup->MutablePQTabletConfig()->MergeFrom(cmd.GetConfig());
    }

    if (Request->Record.HasMetaRequest() && Request->Record.GetMetaRequest().HasCmdDeleteTopic()) {
        const auto& cmd = Request->Record.GetMetaRequest().GetCmdDeleteTopic();
        auto *transaction = record.MutableTransaction()->MutableModifyScheme();
        transaction->SetWorkingDir(TopicPrefix(ctx));
        transaction->SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
        auto *pqgroup = transaction->MutableDrop();
        pqgroup->SetName(cmd.GetTopic());
    }

    req->Record.SetUserToken(TBase::GetSerializedToken());

    ctx.Send(MakeTxProxyID(), req.Release());
}

template <>
void TMessageBusServerSchemeRequest<TBusPersQueue>::ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus &result, const TActorContext &ctx) {
    TAutoPtr<TBusResponse> response(new TBusResponse());
    FillStatus(status, result, response.Get());
    if (result.GetSchemeShardStatus() == NKikimrScheme::StatusPathDoesNotExist) {
        response->Record.SetErrorCode(NPersQueue::NErrorCode::UNKNOWN_TOPIC);
    } else if (status == MSTATUS_OK || status == MSTATUS_INPROGRESS)
        response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
    else
        response->Record.SetErrorCode(NPersQueue::NErrorCode::ERROR);
    if (result.HasSchemeShardReason()) {
        response->Record.SetErrorReason(result.GetSchemeShardReason());
    }
    SendReplyAutoPtr(response);
    Request.Destroy();
    Die(ctx);
}

template <>
void TMessageBusServerSchemeRequest<TBusSchemeOperation>::SendProposeRequest(const TActorContext &ctx) {
    TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> req(new TEvTxUserProxy::TEvProposeTransaction());
    NKikimrTxUserProxy::TEvProposeTransaction &record = req->Record;
    record.SetPeerName(GetPeerName());

    if (!Request->Record.HasTransaction()) {
        return HandleError(MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::Unknown, "Malformed request: no modify scheme transaction provided", ctx);
    }

    if (!Request->Record.GetTransaction().HasModifyScheme()) {
        return HandleError(MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::Unknown, "Malformed request: no modify scheme request body provided", ctx);
    }

    bool needAdminCheck = false;
    switch (Request->Record.GetTransaction().GetModifyScheme().GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions:
            needAdminCheck = true;
            break;
        default:
            break;
    }

    const auto& allowedSIDs(AppData(ctx)->AdministrationAllowedSIDs);

    if (needAdminCheck) {
        if (allowedSIDs.empty()) {
            needAdminCheck = false;
        }
    }

    if (needAdminCheck) {
        if (!TBase::IsUserAdmin()) {
            return TBase::HandleError(
                        EResponseStatus::MSTATUS_ERROR,
                        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied,
                        "Access denied",
                        ctx);
        }
    }

    record.MutableTransaction()->MutableModifyScheme()->MergeFrom(Request->Record.GetTransaction().GetModifyScheme());
    req->Record.SetUserToken(TBase::GetSerializedToken());
    ctx.Send(MakeTxProxyID(), req.Release());
}

template <>
void TMessageBusServerSchemeRequest<TBusSchemeOperation>::ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus &result, const TActorContext &ctx) {
    TAutoPtr<TBusResponse> response(new TBusResponse());

    FillStatus(status, result, response.Get());

    SendReplyAutoPtr(response);
    Request.Destroy();
    Die(ctx);
}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvPersQueue::TPtr& ev, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TMessageBusServerProxy::Handle");

    TEvBusProxy::TEvPersQueue *msg = ev->Get();
    const auto& rec = static_cast<TBusPersQueue *>(msg->MsgContext.GetMessage())->Record;
    if (rec.HasMetaRequest() && (rec.GetMetaRequest().HasCmdCreateTopic()
                                 || rec.GetMetaRequest().HasCmdChangeTopic()
                                 || rec.GetMetaRequest().HasCmdDeleteTopic())) {
        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TMessageBusServerProxy::Handle new TMessageBusServerSchemeRequest");

        ctx.Register(new TMessageBusServerSchemeRequest<TBusPersQueue>(ev->Get()), TMailboxType::HTSwap, AppData()->UserPoolId);
        return;
    }
    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "TMessageBusServerProxy::Handle CreateMessageBusServerPersQueue");

    ctx.Register(CreateMessageBusServerPersQueue(msg->MsgContext, PqMetaCache));
}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvFlatTxRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TMessageBusServerSchemeRequest<TBusSchemeOperation>(ev->Get()));
}

}
}
