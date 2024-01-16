#include "ydb_s3_internal.h"

#include <ydb/core/grpc_services/service_s3_listing.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbS3InternalService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::S3Internal::IN, Ydb::S3Internal::OUT, TGRpcYdbS3InternalService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::S3Internal::IN, Ydb::S3Internal::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr})); \
        }, &Ydb::S3Internal::V1::S3InternalService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("s3listing", #NAME))->Run();

    ADD_REQUEST(S3Listing, S3ListingRequest, S3ListingResponse, DoS3ListingRequest);
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
