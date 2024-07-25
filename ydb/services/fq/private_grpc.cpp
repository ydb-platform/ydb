#include "private_grpc.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_fq_internal.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/grpc/server/dbg_printer.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcFqPrivateTaskService::TGRpcFqPrivateTaskService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcFqPrivateTaskService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcFqPrivateTaskService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcFqPrivateTaskService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcFqPrivateTaskService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcFqPrivateTaskService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                                  \
MakeIntrusive<TGRpcRequest<Fq::Private::NAME##Request, Fq::Private::NAME##Response, TGRpcFqPrivateTaskService, TSecurityTextFormatPrinter<Fq::Private::NAME##Request>, TSecurityTextFormatPrinter<Fq::Private::NAME##Response>>>(                                                                  \
    this, &Service_, CQ_,                                                                                      \
    [this](NYdbGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_,                                                                \
            new TGrpcRequestOperationCall<Fq::Private::NAME##Request, Fq::Private::NAME##Response>             \
                (ctx, &CB));                                                                                   \
    },                                                                                                         \
    &Fq::Private::V1::FqPrivateTaskService::AsyncService::Request##NAME,                                       \
    #NAME, logger, getCounterBlock("fq_internal", #NAME))                                                      \
    ->Run();                                                                                                   \
    /**/

    ADD_REQUEST(PingTask, DoFqPrivatePingTaskRequest)

    ADD_REQUEST(GetTask, DoFqPrivateGetTaskRequest)

    ADD_REQUEST(WriteTaskResult, DoFqPrivateWriteTaskResultRequest)

    ADD_REQUEST(NodesHealthCheck, DoFqPrivateNodesHealthCheckRequest)

    ADD_REQUEST(CreateRateLimiterResource, DoFqPrivateCreateRateLimiterResourceRequest)
    ADD_REQUEST(DeleteRateLimiterResource, DoFqPrivateDeleteRateLimiterResourceRequest)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr

namespace NYdbGrpc {

template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::PingTaskRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::PingTaskResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::GetTaskRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::GetTaskResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::WriteTaskResultRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::WriteTaskResultResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::NodesHealthCheckRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::NodesHealthCheckResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::CreateRateLimiterResourceRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::CreateRateLimiterResourceResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::DeleteRateLimiterResourceRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<Fq::Private::DeleteRateLimiterResourceResponse>>(const NProtoBuf::Message& message, bool ok);

}
