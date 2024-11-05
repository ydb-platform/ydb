#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/query/service_query.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>

#include "rpc_common.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/util/wilson.h>

#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvCreateSessionTableRequest = TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
    Ydb::Table::CreateSessionResponse>;

using TEvDeleteSessionTableRequest = TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
    Ydb::Table::DeleteSessionResponse>;

using TEvDeleteSessionQueryRequest = TGrpcRequestOperationCall<Ydb::Query::DeleteSessionRequest,
    Ydb::Query::DeleteSessionResponse>;

class TCreateSessionRPC : public TActorBootstrapped<TCreateSessionRPC> {
public:
    TCreateSessionRPC(IRequestCtx* msg)
        : Request(msg)
        , Span(TWilsonGrpc::RequestActor, msg->GetWilsonTraceId(), "CreateSessionRpcActor")
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TCreateSessionRPC::StateWork);

        auto now = TInstant::Now();
        const auto& deadline = Request->GetDeadline();

        if (deadline <= now) {
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                SelfId() << " Request deadline has expired for " << now - deadline << " seconds");

            Reply(Ydb::StatusIds::TIMEOUT);
            return;
        }

        auto selfId = this->SelfId();
        auto as = TActivationContext::ActorSystem();

        Request->SetFinishAction([selfId, as]() {
            as->Send(selfId, new TEvents::TEvWakeup);
        });

        CreateSessionImpl();
    }

private:
    void CreateSessionImpl() {
        const auto& traceId = Request->GetTraceId();
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.SetDeadlineUs(Request->GetDeadline().MicroSeconds());
        SetClientIdentitySettings(ev, *Request);

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (Request->HasClientCapability(NYdb::YDB_CLIENT_CAPABILITY_SESSION_BALANCER)) {
            ev->Record.SetCanCreateRemoteSession(true);
            ev->Record.SetSupportsBalancing(true);
        }

        SetDatabase(ev, *Request);

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release(), 0, 0, Span.GetTraceId());
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        Span.Event("client_lost", {});
        ClientLost = true;
    }

    void DoCloseSession(const TActorContext& ctx, const TString& sessionId) {
        Ydb::Table::DeleteSessionRequest request;
        request.set_session_id(sessionId);

        auto cb = [](const Ydb::Table::DeleteSessionResponse&){};

        auto database = Request->GetDatabaseName().GetOrElse("");

        using TEvDeleteSessionRequest = TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
            Ydb::Table::DeleteSessionResponse>;

        auto actorId = NRpcService::DoLocalRpcSameMailbox<TEvDeleteSessionRequest>(
            std::move(request), std::move(cb), database, Request->GetSerializedToken(), ctx);

        LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
            SelfId() << " Client lost, session " << sessionId << " will be closed by " << actorId);
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.GetResourceExhausted()) {
            auto responseCode = grpc::StatusCode::RESOURCE_EXHAUSTED;
            Request->ReplyWithRpcStatus(responseCode, record.GetError());
            Span.EndError("Resource exhausted");
            Die(ctx);
            return;
        }

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            if (ClientLost) {
                DoCloseSession(ctx, kqpResponse.GetSessionId());
                // We already lost the client, so the client should not see this status
                Reply(Ydb::StatusIds::INTERNAL_ERROR);
            } else {
                SendSessionResult(kqpResponse);
                Span.EndOk();
                PassAway();
                return;
            }
        } else {
            return ReplyResponseError(record);
        }
    }

private:
    virtual void SendSessionResult(const NKikimrKqp::TCreateSessionResponse& kqpResponse) = 0;

    template<typename TResp>
    void ReplyResponseError(const TResp& kqpResponse) {
        if (kqpResponse.HasError()) {
            Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, kqpResponse.GetError()));
        }
        return Reply(kqpResponse.GetYdbStatus());
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span, status);
        this->PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NProtoBuf::Message* resp) {
        Request->Reply(resp, status);
        NWilson::EndSpanWithStatus(Span, status);
        this->PassAway();
    }

protected:
    std::shared_ptr<IRequestCtx> Request;
    NWilson::TSpan Span;

private:
    bool ClientLost = false;
};

class TCreateSessionTableService : public TCreateSessionRPC {
    using TCtx = IRequestOpCtx;

public:
    using TCreateSessionRPC::TCreateSessionRPC;
    static TCreateSessionRPC* New(TCtx* ctx) {
        return new TCreateSessionTableService(ctx);
    }

private:
    void SendSessionResult(const NKikimrKqp::TCreateSessionResponse& kqpResponse) override {
        Ydb::Table::CreateSessionResult result;
        result.set_session_id(kqpResponse.GetSessionId());
        static_cast<TCtx*>(Request.get())->SendResult(result, Ydb::StatusIds::SUCCESS);
    };
};

class TCreateSessionQueryService : public TCreateSessionRPC {
public:
    using TCreateSessionRPC::TCreateSessionRPC;
    static TCreateSessionRPC* New(IRequestNoOpCtx* ctx) {
        return new TCreateSessionQueryService(ctx);
    }

private:
    void SendSessionResult(const NKikimrKqp::TCreateSessionResponse& kqpResponse) override {
        using TRes = Ydb::Query::CreateSessionResponse;
        auto res = google::protobuf::Arena::CreateMessage<TRes>(Request->GetArena());;
        res->set_status(Ydb::StatusIds::SUCCESS);
        res->set_session_id(kqpResponse.GetSessionId());

        if (kqpResponse.HasNodeId())
            res->set_node_id(kqpResponse.GetNodeId());

        Request->Reply(res, Ydb::StatusIds::SUCCESS);
    };
};

class TDeleteSessionRPC : public TActorBootstrapped<TDeleteSessionRPC> {
public:
    TDeleteSessionRPC(IRequestCtx* msg)
        : Request(msg)
        , Span(TWilsonGrpc::RequestActor, msg->GetWilsonTraceId(), "DeleteSessionRpcActor")
    {}

    void Bootstrap(const TActorContext&) {
        DeleteSessionImpl();
    }

private:
    void DeleteSessionImpl() {
        const auto sessionId = GetSessionId();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();

        if (CheckSession(sessionId, Request.get())) {
            ev->Record.MutableRequest()->SetSessionId(sessionId);
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST);
        }

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release(), 0, 0, Span.GetTraceId()); //no respose will be sended, so don't wait for anything
        Reply(Ydb::StatusIds::SUCCESS);
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span, status);
        this->PassAway();
    }

    virtual const TString& GetSessionId() const = 0;

protected:
    std::shared_ptr<IRequestCtx> Request;
    NWilson::TSpan Span;
};

class TDeleteSessionTableService : public TDeleteSessionRPC {
    using TCtx = IRequestOpCtx;

public:
    using TDeleteSessionRPC::TDeleteSessionRPC;
    static TDeleteSessionRPC* New(TCtx* ctx) {
        return new TDeleteSessionTableService(ctx);
    }

private:
    const TString& GetSessionId() const override {
        return TEvDeleteSessionTableRequest::GetProtoRequest(Request)->session_id();
    };
};

class TDeleteSessionQueryService : public TDeleteSessionRPC {
public:
    using TDeleteSessionRPC::TDeleteSessionRPC;
    static TDeleteSessionRPC* New(IRequestNoOpCtx* ctx) {
        return new TDeleteSessionQueryService(ctx);
    }

private:
    const TString& GetSessionId() const override {
        return TEvDeleteSessionQueryRequest::GetProtoRequest(Request)->session_id();
    };
};

void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(TCreateSessionTableService::New(ctx.release()));
}

template<>
IActor* TEvCreateSessionTableRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return TCreateSessionTableService::New(msg);
}

void DoDeleteSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& provider) {
    provider.RegisterActor(TDeleteSessionTableService::New(p.release()));
}

template<>
IActor* TEvDeleteSessionTableRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return TDeleteSessionTableService::New(msg);
}

namespace NQuery {

void DoCreateSession(std::unique_ptr<IRequestNoOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(TCreateSessionQueryService::New(ctx.release()));
}

void DoDeleteSession(std::unique_ptr<IRequestNoOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(TDeleteSessionQueryService::New(ctx.release()));
}

}

} // namespace NGRpcService
} // namespace NKikimr
