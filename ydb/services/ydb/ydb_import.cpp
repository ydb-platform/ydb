#include "ydb_import.h"

#include <ydb/core/grpc_services/service_import.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbImportService::TGRpcYdbImportService(NActors::TActorSystem *system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{ }

void TGRpcYdbImportService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbImportService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYdbImportService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbImportService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYdbImportService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::Import::IN, Ydb::Import::OUT, TGRpcYdbImportService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::Import::IN, Ydb::Import::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr})); \
        }, &Ydb::Import::V1::ImportService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("import", #NAME))->Run();

    ADD_REQUEST(ImportFromS3, ImportFromS3Request, ImportFromS3Response, DoImportFromS3Request);
    ADD_REQUEST(ImportData, ImportDataRequest, ImportDataResponse, DoImportDataRequest);

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
