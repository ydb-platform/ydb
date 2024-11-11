#include "service_debug.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>

#include <ydb/public/api/protos/ydb_debug.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvKqpProxyRequest = TGrpcRequestNoOperationCall<Ydb::Debug::KqpProxyRequest, Ydb::Debug::KqpProxyResponse>;

class TExecutePingRPC : public TActorBootstrapped<TExecutePingRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::OTHER;
    }

    TExecutePingRPC(TEvKqpProxyRequest* request)
        : Request_(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        this->Become(&TExecutePingRPC::StateWork);

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
            ReplyWithResult(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvProxyPingResponse::TPtr&, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::RPC_REQUEST, this->SelfId() << " got ping response");
        ReplyWithResult(Ydb::StatusIds::SUCCESS, ctx);
    }

private:
    void ReplyWithResult(Ydb::StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);
        ReplyWithResult(Ydb::StatusIds::INTERNAL_ERROR, TActivationContext::AsActorContext());
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TExecutePingRPC in state " << state << " received unexpected event " <<
            ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }


private:
    std::shared_ptr<TEvKqpProxyRequest> Request_;
};

} // anonymous

void DoGrpcProxyPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    // we are in the GRPC proxy already (or in the check actor in case of auth check),
    // thus ready to reply right here
    using TRequest = TGrpcRequestNoOperationCall<Ydb::Debug::GrpcProxyRequest, Ydb::Debug::GrpcProxyResponse>;
    TRequest* request = dynamic_cast<TRequest *>(p.get());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper");
    request->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
}

void DoKqpPing(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    auto* request = dynamic_cast<TEvKqpProxyRequest*>(p.release());
    Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper");
    f.RegisterActor(new TExecutePingRPC(request));
}

} // namespace NKikimr::NGRpcService
