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
#define YDB_API_DEFAULT_COUNTER_BLOCK(counterName, methodName) getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))

// Implies usage from inside grpc service class, derived from TGrpcServiceBase
#define SETUP_RUNTIME_EVENT_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, counterBlock, auditMode, runtimeEventType, operationCallClass, grpcProxyId, cq, limiter, customAttributeProcessorCallback) \
    MakeIntrusive<::NKikimr::NGRpcService::TGRpcRequest<                                                  \
        inputType,                                                                                        \
        outputType,                                                                                       \
        std::remove_reference_t<decltype(*this)>>>                                                        \
    (                                                                                                     \
        this,                                                                                             \
        &Service_,                                                                                        \
        cq,                                                                                               \
        [this, proxyId = grpcProxyId](::NYdbGrpc::IRequestContextBase* reqCtx) {                          \
            ::NKikimr::NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer());                \
            ActorSystem_->Send(proxyId, new operationCallClass<                                           \
                inputType,                                                                                \
                outputType,                                                                               \
                ::NKikimr::NGRpcService::NRuntimeEvents::EType::runtimeEventType>(reqCtx, methodCallback, \
                    ::NKikimr::NGRpcService::TRequestAuxSettings {                                        \
                        .RlMode = rlMode,                                                                 \
                        .CustomAttributeProcessor = customAttributeProcessorCallback,                     \
                        .AuditMode = auditMode,                                                           \
                        .RequestType = ::NKikimr::NJaegerTracing::ERequestType::requestType,              \
                    }));                                                                                  \
        },                                                                                                \
        &TGrpcAsyncService::Y_CAT(Request, methodName),                                                   \
        Y_STRINGIZE(methodName),                                                                          \
        logger,                                                                                           \
        counterBlock,                                                                                     \
        limiter                                                                                           \
    )->Run()

// Common macro for gRPC methods setup
// Use RLSWITCH or RLMODE macro for rlMode
#define SETUP_METHOD(methodName, methodCallback, rlMode, requestType, counterName, auditMode)             \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                                                \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                         \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                                        \
        methodCallback,                                                                                   \
        rlMode,                                                                                           \
        requestType,                                                                                      \
        YDB_API_DEFAULT_COUNTER_BLOCK(counterName, methodName),                                           \
        auditMode,                                                                                        \
        COMMON,                                                                                           \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,                                               \
        GRpcRequestProxyId_,                                                                              \
        CQ_,                                                                                              \
        nullptr,                                                                                          \
        nullptr)

#endif // GRPC_METHOD_SETUP_H
