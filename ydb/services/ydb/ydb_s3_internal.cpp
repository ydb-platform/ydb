#include "ydb_s3_internal.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbS3InternalService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::S3Internal::IN, Ydb::S3Internal::OUT, TGRpcYdbS3InternalService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::S3Internal::V1::S3InternalService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("s3_internal", #NAME))->Run();

    ADD_REQUEST(S3Listing, S3ListingRequest, S3ListingResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvS3ListingRequest(ctx));
    })
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
