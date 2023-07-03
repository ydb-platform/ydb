#include "service_query.h"
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/query/service_query.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>


namespace NKikimr::NGRpcService {

namespace {

//TODO move to common
bool CheckSession(const TString& sessionId, IRequestCtxBase* ctx) {
    if (sessionId.empty()) {
        ctx->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty session id"));
        return false;
    }

    return true;
}

using TEvAttachSessionRequest = TGrpcRequestNoOperationCall<Ydb::Query::AttachSessionRequest,
    Ydb::Query::SessionState>;

class TAttachSessionRPC : public TActorBootstrapped<TAttachSessionRPC> {
public:
    TAttachSessionRPC(std::unique_ptr<IRequestNoOpCtx> request)
        : Request(std::move(request))
    {}

    void Bootstrap() {
        Become(&TAttachSessionRPC::AttachingState);
        DoAttach();
    }

    void AttachingState(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvPingSessionResponse, HandleAttachin);
            hFunc(NKqp::TEvKqp::TEvProcessResponse, HandleAttachin);
        }
    }

    void ReadyState(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, HandleReady);
        }
    }

private:
   void SubscribeClientLost() {
        auto selfId = this->SelfId();
        auto as = TActivationContext::ActorSystem();

        Request->SetFinishAction([selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup());
        });
   }

   void DoAbortTx() {
       auto ev = std::make_unique<NKqp::TEvKqp::TEvCloseSessionRequest>();
       ev->Record.MutableRequest()->SetSessionId(SessionId);
       Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.release());
   }

    void DoAttach() {
        auto ev = std::make_unique<NKqp::TEvKqp::TEvPingSessionRequest>();
        auto req = dynamic_cast<TEvAttachSessionRequest*>(Request.get());
        Y_VERIFY(req, "unexpected request type");

        const auto sessionId = req->GetProtoRequest()->session_id();

        if (CheckSession(sessionId, req)) {
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            SessionId = sessionId;
        } else {
            return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST);
        }

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.release());
    }

    void HandleReady(TEvents::TEvWakeup::TPtr&) {
        DoAbortTx();
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR);
    }

    void HandleAttachin(NKqp::TEvKqp::TEvPingSessionResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        // Do not try to attach to closing session
        const bool sessionExpired = record.GetWorkerIsClosing();
        if (sessionExpired) {
            return ReplyFinishStream(Ydb::StatusIds::NOT_FOUND);
        }

        SubscribeClientLost();

        SendAttachResult(record.GetStatus());

        Become(&TAttachSessionRPC::ReadyState);
    }

    void SendAttachResult(Ydb::StatusIds::StatusCode status) {
        Ydb::Query::SessionState resp;
        resp.set_status(status);

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD resp.SerializeToString(&out);

        Request->SendSerializedResult(std::move(out), status);
    }

    template<typename TResp>
    void ReplyResponseError(const TResp& kqpResponse) {
        if (kqpResponse.HasError()) {
            Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, kqpResponse.GetError()));
        }
        return ReplyFinishStream(kqpResponse.GetYdbStatus());
    }

    void HandleAttachin(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            // KQP should not send TEvProcessResponse with SUCCESS for CreateSession rpc.
            // We expect TEvKqp::TEvPingSessionResponse instead.
            static const TString err = "Unexpected TEvProcessResponse with success status for PingSession request";
            Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, err));
            ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR);
        } else {
            return ReplyResponseError(record);
        }
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        Request->FinishStream();
        this->PassAway();
    }

    std::unique_ptr<IRequestNoOpCtx> Request;
    TString SessionId;
};

}

namespace NQuery {

void DoAttachSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& provider) {
    provider.RegisterActor(new TAttachSessionRPC(std::move(p)));
}

}
}
