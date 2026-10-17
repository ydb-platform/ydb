#include "grpc_request_proxy.h"
#include "rpc_calls.h"

#include <ydb/core/grpc_streaming/grpc_streaming.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr {
namespace NGRpcService {

class TBiStreamPingRequestRPC : public TActorBootstrapped<TBiStreamPingRequestRPC> {
    using TBase = TActorBootstrapped<TBiStreamPingRequestRPC>;
    using IContext = NGRpcServer::IGRpcStreamingContext<
        TEvBiStreamPingRequest::TRequest,
        TEvBiStreamPingRequest::TResponse>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TBiStreamPingRequestRPC(TEvBiStreamPingRequest* msg)
        : Request_(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Request_->Attach(SelfId());
        Request_->Read();
        Become(&TBiStreamPingRequestRPC::StateWork);
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvReadFinished, success",
            {"success", ev->Get()->Success});
        auto req = static_cast<const TEvBiStreamPingRequest::TRequest&>(ev->Get()->Record);

        if (req.copy()) {
            Resp_.set_payload(req.payload());
        }
        Request_->RefreshToken("someInvalidNewToken", ctx, SelfId());
    }

    void Handle(TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_ERROR_CTX(ctx, "Received TEvRefreshTokenResponse, Authenticated",
            {"authenticated", ev->Get()->Authenticated});
        Request_->Write(std::move(Resp_));
        Ydb::StatusIds::StatusCode status = ev->Get()->Authenticated ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::UNAUTHORIZED;
        auto grpcStatus = grpc::Status(ev->Get()->Authenticated ?
            grpc::StatusCode::OK : grpc::StatusCode::UNAUTHENTICATED,
            "");
        Request_->Finish(status, grpcStatus);
        PassAway();
    }

    void PassAway() override {
        if (Request_) {
            // Write to audit log if it is needed and we have not written yet.
            Request_->AuditLogRequestEnd(Ydb::StatusIds::SUCCESS);
        }

        TActorBootstrapped::PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);
        }
    }
private:
    std::unique_ptr<TEvBiStreamPingRequest> Request_;
    TEvBiStreamPingRequest::TResponse Resp_;
};

void TGRpcRequestProxyHandleMethods::Handle(TEvBiStreamPingRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TBiStreamPingRequestRPC(ev->Release().Release()));
}

} // namespace NGRpcService
} // namespace NKikimr
