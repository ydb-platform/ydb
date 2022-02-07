#include "grpc_request_proxy.h"
#include "rpc_calls.h"

namespace NKikimr {
namespace NGRpcService {

IActor* CreateGrpcS3ListingRequest(TAutoPtr<TEvS3ListingRequest> request);

void TGRpcRequestProxy::Handle(TEvS3ListingRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(CreateGrpcS3ListingRequest(ev->Release().Release()));
}

} // namespace NKikimr
} // namespace NGRpcService
