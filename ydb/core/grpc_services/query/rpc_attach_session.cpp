#include "service_query.h"
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/query/service_query.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>


namespace NKikimr::NGRpcService {

namespace {

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
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqp::TEvPingSessionResponse, HandleAttaching);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void ReadyState(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvWakeup, HandleReady);
                hFunc(NKqp::TEvKqp::TEvCloseSessionResponse, HandleReady);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
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
        if (!req) {
            return InternalError("unexpected request type");
        }

        const auto sessionId = req->GetProtoRequest()->session_id();

        if (CheckSession(sessionId, req)) {
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ActorIdToProto(SelfId(), ev->Record.MutableRequest()->MutableExtSessionCtrlActorId());
            SessionId = sessionId;
        } else {
            return ReplyFinishStream(Ydb::StatusIds::BAD_REQUEST);
        }

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.release());
    }

    void HandleReady(TEvents::TEvWakeup::TPtr&) {
        DoAbortTx();
        // Any status to finish stream
        ReplyFinishStream(Ydb::StatusIds::SUCCESS);
    }

    void HandleReady(NKqp::TEvKqp::TEvCloseSessionResponse::TPtr& ev) {
        const auto &event = ev->Get()->Record;
        if (event.GetResponse().GetSessionId() == SessionId &&
            event.GetResponse().GetClosed() &&
            event.GetStatus() == Ydb::StatusIds::SUCCESS)
        {
            ReplyFinishStream(Ydb::StatusIds::SUCCESS);
        } else {
            InternalError("unexpected TEvCloseSessionResponse response");
        }
    }

    void HandleAttaching(NKqp::TEvKqp::TEvPingSessionResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        // Do not try to attach to closing session
        const bool sessionExpired = record.GetWorkerIsClosing();
        if (sessionExpired) {
            return ReplyFinishStream(Ydb::StatusIds::NOT_FOUND);
        }

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            if (record.GetIssues().size() > 0) {
                Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, record.GetIssues().at(0).message()));
            }

            return ReplyFinishStream(record.GetStatus());
        }

        if (record.GetResponse().GetSessionStatus() != Ydb::Table::KeepAliveResult::SESSION_STATUS_READY) {
            return ReplyFinishStream(Ydb::StatusIds::SESSION_BUSY);
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

    void InternalError(const TString& message) {
        Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message));
        ReplyFinishStream(Ydb::StatusIds::INTERNAL_ERROR);
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TAttachSessionRPC in state " << state << " received unexpected event " <<
            ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void ReplyFinishStream(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        Request->FinishStream(status);
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
