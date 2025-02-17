#include "ydb_object_storage.h"

#include <ydb/core/grpc_services/service_object_storage.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbObjectStorageService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::ObjectStorage::IN, Ydb::ObjectStorage::OUT, TGRpcYdbObjectStorageService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestNoOperationCall<Ydb::ObjectStorage::IN, Ydb::ObjectStorage::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr})); \
        }, &Ydb::ObjectStorage::V1::ObjectStorageService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("object-storage-list", #NAME))->Run();

    ADD_REQUEST(List, ListingRequest, ListingResponse, DoObjectStorageListingRequest);
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
