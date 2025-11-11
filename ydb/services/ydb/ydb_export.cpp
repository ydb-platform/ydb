#include "ydb_export.h"

#include <ydb/core/grpc_services/service_export.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbExportService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, AUDIT_MODE) \
    MakeIntrusive<TGRpcRequest<Ydb::Export::IN, Ydb::Export::OUT, TGRpcYdbExportService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::Export::IN, Ydb::Export::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr, AUDIT_MODE})); \
        }, &Ydb::Export::V1::ExportService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("export", #NAME))->Run();

    ADD_REQUEST(ExportToYt, ExportToYtRequest, ExportToYtResponse, DoExportToYtRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
    ADD_REQUEST(ExportToS3, ExportToS3Request, ExportToS3Response, DoExportToS3Request, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
