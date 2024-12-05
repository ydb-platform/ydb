#include "service_debug.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/public/api/protos/ydb_debug.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

using namespace Ydb;

namespace {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

using TEvKqpProxyRequest = TGrpcRequestNoOperationCall<Debug::KqpProxyRequest, Debug::KqpProxyResponse>;

class TExecuteKqpPingRPC : public TActorBootstrapped<TExecuteKqpPingRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::OTHER;
    }

    TExecuteKqpPingRPC(TEvKqpProxyRequest* request)
        : Request_(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TThis::StateWork);

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(NKqp::TEvKqp::TEvProxyPingResponse, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void Proceed(const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " sending ping to KQP proxy");
        if (!ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), new NKqp::TEvKqp::TEvProxyPingRequest())) {
            LOG_ERROR_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " failed to send ping");
            ReplyWithResult(StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvProxyPingResponse::TPtr&, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " got ping response");
        ReplyWithResult(StatusIds::SUCCESS, ctx);
    }

private:
    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);
        ReplyWithResult(StatusIds::INTERNAL_ERROR, TActivationContext::AsActorContext());
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TExecuteKqpPingRPC in state " << state << " received unexpected event "
            << ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

private:
    std::shared_ptr<TEvKqpProxyRequest> Request_;
};

////////////////////////////////////////////////////////////////////////////////

using TEvSchemeCacheRequest = TGrpcRequestNoOperationCall<Debug::SchemeCacheRequest, Debug::SchemeCacheResponse>;

class TExecuteSchemeCachePingRPC : public TActorBootstrapped<TExecuteSchemeCachePingRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::OTHER;
    }

    TExecuteSchemeCachePingRPC(TEvSchemeCacheRequest* request)
        : Request_(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TThis::StateWork);

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void Proceed(const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " sending ping to SchemeCache");

        auto* request = new TEvTxProxySchemeCache::TEvNavigateKeySet(new NSchemeCache::TSchemeCacheNavigate());
        if (!ctx.Send(MakeSchemeCacheID(), request)) {
            LOG_ERROR_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " failed to send ping to SchemeCache");
            ReplyWithResult(StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " got ping response from SchemeCache");
        ReplyWithResult(StatusIds::SUCCESS, ctx);
    }

private:
    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);
        ReplyWithResult(StatusIds::INTERNAL_ERROR, TActivationContext::AsActorContext());
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TExecuteSchemeCachePingRPC in state " << state <<
            " received unexpected event " << ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

private:
    std::shared_ptr<TEvSchemeCacheRequest> Request_;
};

////////////////////////////////////////////////////////////////////////////////

using TEvTxProxyRequest = TGrpcRequestNoOperationCall<Debug::TxProxyRequest, Debug::TxProxyResponse>;

class TExecuteTxProxyPingRPC : public TActorBootstrapped<TExecuteTxProxyPingRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::OTHER;
    }

    TExecuteTxProxyPingRPC(TEvTxProxyRequest* request)
        : Request_(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TThis::StateWork);

        Proceed(ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void Proceed(const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " sending ping to TxProxy");
        if (!ctx.Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId)) {
            LOG_ERROR_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " failed to send ping to TxProxy");
            ReplyWithResult(StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr&, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " got ping response from TxProxy");
        ReplyWithResult(StatusIds::SUCCESS, ctx);
    }

private:
    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);
        ReplyWithResult(StatusIds::INTERNAL_ERROR, TActivationContext::AsActorContext());
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TExecuteTxProxyPingRPC in state " << state << " received unexpected event "
            << ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

private:
    std::shared_ptr<TEvTxProxyRequest> Request_;
};

} // anonymous

////////////////////////////////////////////////////////////////////////////////

void DoGrpcProxyPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // we are in the GRPC proxy already (or in the check actor in case of auth check),
    // thus ready to reply right here
    using TRequest = TGrpcRequestNoOperationCall<Debug::GrpcProxyRequest, Debug::GrpcProxyResponse>;
    TRequest* request = dynamic_cast<TRequest *>(p.get());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper in DoGrpcProxyPing");
    request->ReplyWithYdbStatus(StatusIds::SUCCESS);
}

void DoKqpPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* request = dynamic_cast<TEvKqpProxyRequest*>(p.release());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper in DoKqpPing");
    f.RegisterActor(new TExecuteKqpPingRPC(request));
}

void DoSchemeCachePing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* request = dynamic_cast<TEvSchemeCacheRequest*>(p.release());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper in DoSchemeCachePing");
    f.RegisterActor(new TExecuteSchemeCachePingRPC(request));
}

void DoTxProxyPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* request = dynamic_cast<TEvTxProxyRequest*>(p.release());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper in DoTxProxyPing");
    f.RegisterActor(new TExecuteTxProxyPingRPC(request));
}

} // namespace NKikimr::NGRpcService
