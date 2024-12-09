#include "msgbus_server.h"
#include "msgbus_server_request.h"
#include "msgbus_server_proxy.h"
#include "msgbus_securereq.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

namespace NKikimr {
namespace NMsgBusProxy {

template <typename ResponseType>
class TMessageBusServerFlatDescribeRequest : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerFlatDescribeRequest<ResponseType>>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerFlatDescribeRequest<ResponseType>>>;
    THolder<TBusSchemeDescribe> Request;
    NYql::TIssueManager IssueManager;

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        auto &mutableRecord = *ev->Get()->MutableRecord();
        TAutoPtr<ResponseType> response(new ResponseType());
        response->Record.SetSchemeStatus(mutableRecord.GetStatus());
        const auto status = mutableRecord.GetStatus();
        if (status == NKikimrScheme::StatusSuccess) {
            response->Record.SetStatus(MSTATUS_OK);
            response->Record.SetPath(mutableRecord.GetPath());
            response->Record.MutablePathDescription()->Swap(mutableRecord.MutablePathDescription());
            response->Record.SetStatusCode(NKikimrIssues::TStatusIds::SUCCESS);
        } else {
            response->Record.SetStatus(MSTATUS_ERROR);
            response->Record.SetErrorReason(mutableRecord.GetReason());

            switch (status) {
            case NKikimrScheme::StatusPathDoesNotExist:
                response->Record.SetStatusCode(NKikimrIssues::TStatusIds::PATH_NOT_EXIST);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::PATH_NOT_EXIST));
                break;
            default:
                response->Record.SetStatusCode(NKikimrIssues::TStatusIds::ERROR);
                IssueManager.RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR));
                break;
            }
        }

        if (IssueManager.GetIssues())
            IssuesToMessage(IssueManager.GetIssues(), response->Record.MutableIssues());

        TBase::SendReplyAutoPtr(response);
        Request.Destroy();
        this->Die(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_SCHEME_DESCRIBE; }

    TMessageBusServerFlatDescribeRequest(TEvBusProxy::TEvFlatDescribeRequest* msg)
        : TBase(msg->MsgContext)
        , Request(static_cast<TBusSchemeDescribe*>(msg->MsgContext.ReleaseMessage()))
    {
        TBase::SetSecurityToken(Request->Record.GetSecurityToken());
        TBase::SetPeerName(msg->MsgContext.GetPeerName());
    }

    //STFUNC(StateWork)
    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        SendRequest(ctx);
        TBase::Become(&TMessageBusServerFlatDescribeRequest::StateWork);
    }

    void SendRequest(const TActorContext& ctx) {
        TAutoPtr<TEvTxUserProxy::TEvNavigate> req(new TEvTxUserProxy::TEvNavigate());
        NKikimrSchemeOp::TDescribePath* record = req->Record.MutableDescribePath();

        if (Request->Record.HasPath()) {
            record->SetPath(Request->Record.GetPath());
        } else {
            record->SetSchemeshardId(Request->Record.GetSchemeshardId());
            record->SetPathId(Request->Record.GetPathId());
        }
        if (Request->Record.HasOptions()) {
            auto options = record->MutableOptions();
            options->CopyFrom(Request->Record.GetOptions());
        }
        req->Record.SetUserToken(TBase::GetSerializedToken());
        ctx.Send(MakeTxProxyID(), req.Release());
    }
};

IActor* CreateMessageBusServerProxy(TMessageBusServer* server) {
    return new TMessageBusServerProxy(server);
}

TBusResponse* ProposeTransactionStatusToResponse(EResponseStatus status,
                                                 const NKikimrTxUserProxy::TEvProposeTransactionStatus &result) {
    TAutoPtr<TBusResponse> response(new TBusResponse());
    response->Record.SetStatus(status);
    response->Record.SetProxyErrorCode(result.GetStatus());

    response->Record.SetStatusCode(result.GetStatusCode());
    response->Record.MutableIssues()->CopyFrom(result.GetIssues());

    if (result.HasExecutionEngineStatus())
        response->Record.SetExecutionEngineStatus(result.GetExecutionEngineStatus());
    if (result.HasExecutionEngineResponseStatus())
        response->Record.SetExecutionEngineResponseStatus(result.GetExecutionEngineResponseStatus());
    if (result.HasMiniKQLCompileResults())
        response->Record.MutableMiniKQLCompileResults()->CopyFrom(result.GetMiniKQLCompileResults());
    if (result.HasMiniKQLErrors())
        response->Record.SetMiniKQLErrors(result.GetMiniKQLErrors());
    if (result.HasDataShardErrors())
        response->Record.SetDataShardErrors(result.GetDataShardErrors());
    if (result.ComplainingDataShardsSize())
        response->Record.MutableComplainingDataShards()->CopyFrom(result.GetComplainingDataShards());
    if (result.UnresolvedKeysSize())
        response->Record.MutableUnresolvedKeys()->CopyFrom(result.GetUnresolvedKeys());
    if (result.HasHadFollowerReads())
        response->Record.SetHadFollowerReads(result.GetHadFollowerReads());

    if (result.HasTxId())
        response->Record.SetTxId(result.GetTxId());
    if (result.HasStep())
        response->Record.SetStep(result.GetStep());

    if (result.HasSchemeShardStatus())
        response->Record.SetSchemeStatus(result.GetSchemeShardStatus());
    if (result.HasSchemeShardReportedId())
        response->Record.SetSchemeTagId(result.GetSchemeShardReportedId());

    if (result.HasTimings())
        response->Record.MutableProxyTimings()->CopyFrom(result.GetTimings());

    return response.Release();
}

//void TMessageBusServerProxy::Handle(TEvBusProxy::TEvRequest::TPtr& ev, const TActorContext& ctx); // see msgbus_server_request.cpp

//void TMessageBusServerProxy::Handle(TEvBusProxy::TEvPersQueue::TPtr& ev, const TActorContext& ctx); // see msgbus_server_scheme_request.cpp
//void TMessageBusServerProxy::Handle(TEvBusProxy::TEvFlatTxRequest::TPtr& ev, const TActorContext& ctx); // see msgbus_server_scheme_request.cpp

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvFlatDescribeRequest::TPtr& ev, const TActorContext& ctx) {
    TEvBusProxy::TEvFlatDescribeRequest *msg = ev->Get();
    if (msg->MsgContext.GetMessage()->GetHeader()->Type == MTYPE_CLIENT_OLD_FLAT_DESCRIBE_REQUEST) {
        ctx.Register(new TMessageBusServerFlatDescribeRequest<TBusOldFlatDescribeResponse>(ev->Get()));
    } else {
        ctx.Register(new TMessageBusServerFlatDescribeRequest<TBusResponse>(ev->Get()));
    }
}

//void TMessageBusServerProxy::Handle(TEvBusProxy::TEvDbSchema::TPtr& ev, const TActorContext& ctx); // see msgbus_server_db.cpp
//void TMessageBusServerProxy::Handle(TEvBusProxy::TEvDbOperation::TPtr& ev, const TActorContext& ctx); // see msgbus_server_db.cpp

void TMessageBusServerProxy::Bootstrap(const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::MSGBUS_PROXY, "TMessageBusServerProxy::Bootstrap");

    SelfID = ctx.SelfID;

    TxProxy = MakeTxProxyID();

    SchemeCacheCounters = GetServiceCounters(AppData(ctx)->Counters, "pqproxy|cache");
    DbOperationsCounters = new TMessageBusDbOpsCounters(AppData(ctx)->Counters);

    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(AppData(ctx), SchemeCacheCounters);
    SchemeCache = ctx.ExecutorThread.RegisterActor(CreateSchemeBoardSchemeCache(cacheConfig.Get()));
    PqMetaCache = CreatePersQueueMetaCacheV2Id();

    if (Server) {
        Server->InitSession(ctx.ExecutorThread.ActorSystem, ctx.SelfID);
    }

    Become(&TThis::StateFunc);
}

}
}
