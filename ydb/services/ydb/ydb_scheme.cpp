#include "ydb_scheme.h"

#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbSchemeService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response, TGRpcYdbSchemeService>> \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response>      \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));                       \
            }, &Ydb::Scheme::V1::SchemeService::AsyncService::Request ## NAME,                                  \
            #NAME, logger, getCounterBlock("scheme", #NAME))->Run();

    ADD_REQUEST(MakeDirectory, DoMakeDirectoryRequest)
    ADD_REQUEST(RemoveDirectory, DoRemoveDirectoryRequest)
    ADD_REQUEST(ListDirectory, DoListDirectoryRequest)
    ADD_REQUEST(DescribePath, DoDescribePathRequest)
    ADD_REQUEST(ModifyPermissions, DoModifyPermissionsRequest)
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
