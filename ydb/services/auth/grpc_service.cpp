#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

static TString GetSdkBuildInfo(NGrpc::IRequestContextBase* reqCtx) {
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]};
}

void TGRpcAuthService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Auth::IN, Ydb::Auth::OUT, TGRpcAuthService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase* reqCtx) { \
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx)); \
           ACTION; \
        }, &Ydb::Auth::V1::AuthService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("login", #NAME))->Run();

    ADD_REQUEST(Login, LoginRequest, LoginResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvLoginRequest(reqCtx));
    })

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
