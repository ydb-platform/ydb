#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_auth.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

namespace NKikimr {
namespace NGRpcService {

static TString GetSdkBuildInfo(NYdbGrpc::IRequestContextBase* reqCtx) {
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]};
}

void TGRpcAuthService::SetServerOptions(const NYdbGrpc::TServerOptions& options) {
    // !!! WARN: The login request should be available without auth token !!!
    // Until we have only one rpc call in this service we can just disable auth
    // for service. In case of adding other rpc calls consider this and probably
    // implement switch via TRequestAuxSettings for particular call
    auto op = options;
    op.UseAuth = false;
    TBase::SetServerOptions(op);
}

void TGRpcAuthService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif

#define ADD_REQUEST_LIMIT(NAME, CB, RATE_LIMITER_MODE, AUDIT_MODE) \
    MakeIntrusive<TGRpcRequest<Ydb::Auth::NAME##Request, Ydb::Auth::NAME##Response, TGRpcAuthService>>     \
        (this, this->GetService(), CQ_,                                                                    \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                   \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));     \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                    \
                    new TGrpcRequestOperationCall<Ydb::Auth::NAME##Request, Ydb::Auth::NAME##Response>     \
                        (ctx, &CB, TRequestAuxSettings{                                                    \
                            .RlMode = RATE_LIMITER_MODE,                                                   \
                            .AuditMode = AUDIT_MODE,                                                       \
                        }));                                                                               \
            }, &Ydb::Auth::V1::AuthService::AsyncService::Request ## NAME,                                 \
            #NAME, logger, getCounterBlock("login", #NAME))->Run();

    ADD_REQUEST_LIMIT(Login, DoLoginRequest, TRateLimiterMode::Off, TAuditMode::Auditable)

#undef ADD_REQUEST_LIMIT

}

} // namespace NGRpcService
} // namespace NKikimr
