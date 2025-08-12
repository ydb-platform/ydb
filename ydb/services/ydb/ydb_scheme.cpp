#include "ydb_scheme.h"

#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbSchemeService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, AUDIT_MODE)                                                                        \
    MakeIntrusive<TGRpcRequest<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response, TGRpcYdbSchemeService>>  \
        (this, &Service_, CQ_,                                                                                   \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                         \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                 \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                          \
                    new TGrpcRequestOperationCall<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response>       \
                        (ctx, &CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr, AUDIT_MODE}));  \
            }, &Ydb::Scheme::V1::SchemeService::AsyncService::Request ## NAME,                                   \
            #NAME, logger, getCounterBlock("scheme", #NAME))->Run();

    ADD_REQUEST(MakeDirectory, DoMakeDirectoryRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(RemoveDirectory, DoRemoveDirectoryRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(ListDirectory, DoListDirectoryRequest, TAuditMode::NonModifying())
    ADD_REQUEST(DescribePath, DoDescribePathRequest, TAuditMode::NonModifying())
    ADD_REQUEST(ModifyPermissions, DoModifyPermissionsRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Acl))
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
