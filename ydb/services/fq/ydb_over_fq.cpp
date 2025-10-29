#include "ydb_over_fq.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/ydb_over_fq/service.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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

#ifdef SETUP_YDB_FQ_METHOD
#error SETUP_YDB_FQ_METHOD macro already defined
#endif

#define SETUP_YDB_FQ_METHOD(methodName, inputType, outputType, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                          \
        inputType,                                                  \
        outputType,                                                 \
        NYdbOverFq::Get##methodName##Executor(GRpcRequestProxyId_), \
        rlMode,                                                     \
        requestType,                                                \
        YDB_API_DEFAULT_COUNTER_BLOCK(ydb_over_fq, methodName),     \
        auditMode,                                                  \
        COMMON,                                                     \
        operationCallClass,                                         \
        GRpcRequestProxyId_,                                        \
        CQ_,                                                        \
        nullptr,                                                    \
        nullptr)

void TGrpcTableOverFqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Table;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    SETUP_YDB_FQ_METHOD(ExecuteDataQuery, ExecuteDataQueryRequest, ExecuteDataQueryResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(ExplainDataQuery, ExplainDataQueryRequest, ExplainDataQueryResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(CreateSession, CreateSessionRequest, CreateSessionResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(DeleteSession, DeleteSessionRequest, DeleteSessionResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(KeepAlive, KeepAliveRequest, KeepAliveResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(DescribeTable, DescribeTableRequest, DescribeTableResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_YDB_FQ_METHOD(StreamExecuteScanQuery, ExecuteScanQueryRequest, ExecuteScanQueryPartialResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestNoOperationCall);
}

void TGrpcSchemeOverFqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Scheme;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    SETUP_YDB_FQ_METHOD(ListDirectory, ListDirectoryRequest, ListDirectoryResponse, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
}

#undef SETUP_YDB_FQ_METHOD

} // namespace NKikimr::NGRpcService
