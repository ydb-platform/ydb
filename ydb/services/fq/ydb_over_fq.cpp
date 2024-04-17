#include "ydb_over_fq.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/ydb_over_fq/service.h>
#include <ydb/library/protobuf_printer/security_printer.h>

namespace NKikimr::NGRpcService {

TGRpcYdbOverFqService::TGRpcYdbOverFqService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcYdbOverFqService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbOverFqService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYdbOverFqService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbOverFqService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGrpcTableOverFqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME) \
MakeIntrusive<TGRpcRequest<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response, TGrpcTableOverFqService, TSecurityTextFormatPrinter<Ydb::Table::NAME##Request>, TSecurityTextFormatPrinter<Ydb::Table::NAME##Response>>>( \
    this, &Service_, CQ_, \
    [this](NYdbGrpc::IRequestContextBase *ctx) { \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
        auto op_call = new TGrpcRequestOperationCall<Ydb::Table::NAME##Request, Ydb::Table::NAME##Response>( \
            ctx, NYdbOverFq::Get##NAME##Executor(GRpcRequestProxyId_)); \
        ActorSystem_->Send(GRpcRequestProxyId_, op_call); \
    }, \
    &Ydb::Table::V1::TableService::AsyncService::Request##NAME, \
    #NAME, logger, getCounterBlock("ydb_over_fq", #NAME)) \
    ->Run();

    ADD_REQUEST(ExecuteDataQuery)
    ADD_REQUEST(ExplainDataQuery)
    ADD_REQUEST(CreateSession)
    ADD_REQUEST(KeepAlive)
    ADD_REQUEST(DescribeTable)

#undef ADD_REQUEST
}

void TGrpcSchemeOverFqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#define ADD_REQUEST(NAME) \
MakeIntrusive<TGRpcRequest<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response, TGrpcSchemeOverFqService, TSecurityTextFormatPrinter<Ydb::Scheme::NAME##Request>, TSecurityTextFormatPrinter<Ydb::Scheme::NAME##Response>>>( \
    this, &Service_, CQ_, \
    [this](NYdbGrpc::IRequestContextBase *ctx) { \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
        auto op_call = new TGrpcRequestOperationCall<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response>( \
            ctx, NYdbOverFq::Get##NAME##Executor(GRpcRequestProxyId_)); \
        ActorSystem_->Send(GRpcRequestProxyId_, op_call); \
    }, \
    &Ydb::Scheme::V1::SchemeService::AsyncService::Request##NAME, \
    #NAME, logger, getCounterBlock("ydb_over_fq", #NAME)) \
    ->Run();

    ADD_REQUEST(ListDirectory)

#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
