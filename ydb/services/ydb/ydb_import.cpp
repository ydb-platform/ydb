#include "ydb_import.h"

#include <ydb/core/grpc_services/service_import.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbImportService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, AUDIT_MODE) \
    MakeIntrusive<TGRpcRequest<Ydb::Import::IN, Ydb::Import::OUT, TGRpcYdbImportService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::Import::IN, Ydb::Import::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr, AUDIT_MODE})); \
        }, &Ydb::Import::V1::ImportService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("import", #NAME))->Run();

    ADD_REQUEST(ImportFromS3, ImportFromS3Request, ImportFromS3Response, DoImportFromS3Request, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));
    ADD_REQUEST(ImportData, ImportDataRequest, ImportDataResponse, DoImportDataRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ExportImport));

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
