#ifndef GRPC_METHOD_SETUP_H
#define GRPC_METHOD_SETUP_H

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_mon.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_request.h>
#include <ydb/library/grpc/server/grpc_request_base.h>

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <type_traits>

// Implies using namespace for request/response types
#define YDB_API_DEFAULT_REQUEST_TYPE(methodName) Y_CAT(methodName, Request)
#define YDB_API_DEFAULT_RESPONSE_TYPE(methodName) Y_CAT(methodName, Response)

// Implies usage from inside grpc service class, derived from TGrpcServiceBase
#define SETUP_RUNTIME_EVENT_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, counterName, auditMode, runtimeEventType, operationCallClass, grpcProxyId) \
    MakeIntrusive<::NKikimr::NGRpcService::TGRpcRequest<                                                  \
        inputType,                                                                                        \
        outputType,                                                                                       \
        std::remove_reference_t<decltype(*this)>>>                                                        \
    (                                                                                                     \
        this,                                                                                             \
        &Service_,                                                                                        \
        CQ_,                                                                                              \
        [this, proxyId = grpcProxyId](::NYdbGrpc::IRequestContextBase* reqCtx) {                          \
            ::NKikimr::NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer());                \
            ActorSystem_->Send(proxyId, new operationCallClass<                                           \
                inputType,                                                                                \
                outputType,                                                                               \
                ::NKikimr::NGRpcService::NRuntimeEvents::EType::runtimeEventType>(reqCtx, methodCallback, \
                    ::NKikimr::NGRpcService::TRequestAuxSettings {                                        \
                        .RlMode = ::NKikimr::NGRpcService::TRateLimiterMode::rlMode,                      \
                        .AuditMode = auditMode,                                                           \
                        .RequestType = ::NKikimr::NJaegerTracing::ERequestType::requestType,              \
                    }));                                                                                  \
        },                                                                                                \
        &TGrpcAsyncService::Y_CAT(Request, methodName),                                                   \
        Y_STRINGIZE(methodName),                                                                          \
        logger,                                                                                           \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                                \
    )->Run()


// Common macro for gRPC methods setup
#define SETUP_METHOD(methodName, methodCallback, rlMode, requestType, counterName, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName, \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName), \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName), \
        methodCallback, \
        rlMode, \
        requestType, \
        counterName, \
        auditMode, \
        COMMON, \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall, \
        GRpcRequestProxyId_)

#endif // GRPC_METHOD_SETUP_H
