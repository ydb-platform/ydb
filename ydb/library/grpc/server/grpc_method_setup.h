#ifndef GRPC_METHOD_SETUP_H
#define GRPC_METHOD_SETUP_H

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

#include <type_traits>

// Implies using namespace for request/response types
#define YDB_API_DEFAULT_REQUEST_TYPE(methodName) Y_CAT(methodName, Request)
#define YDB_API_DEFAULT_RESPONSE_TYPE(methodName) Y_CAT(methodName, Response)

// Implies usage from inside grpc service class, derived from TGrpcServiceBase
#define SETUP_RUNTIME_EVENT_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, counterName, auditMode, runtimeEventType, operationCallClass, grpcProxyId) \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                         \
        inputType,                                                                                    \
        outputType,                                                                                   \
        std::remove_reference_t<decltype(*this)>>>                                                    \
    (                                                                                                 \
        this,                                                                                         \
        &Service_,                                                                                    \
        CQ_,                                                                                          \
        [this, proxyId = grpcProxyId](NYdbGrpc::IRequestContextBase* reqCtx) {                        \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer());                       \
            ActorSystem_->Send(proxyId, new operationCallClass<                                       \
                inputType,                                                                            \
                outputType,                                                                           \
                NRuntimeEvents::EType::runtimeEventType>(reqCtx, methodCallback,                      \
                    TRequestAuxSettings {                                                             \
                        .RlMode = TRateLimiterMode::rlMode,                                           \
                        .AuditMode = auditMode,                                                       \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                     \
                    }));                                                                              \
        },                                                                                            \
        &TGrpcAsyncService::Y_CAT(Request, methodName),                                               \
        Y_STRINGIZE(methodName),                                                                      \
        logger,                                                                                       \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                            \
    )->Run()


// Common macro for gRPC methods setup
#define SETUP_METHOD(methodName, methodCallback, rlMode, requestType, counterName, auditMode)    \
    SETUP_RUNTIME_EVENT_METHOD(methodName, YDB_API_DEFAULT_REQUEST_TYPE(methodName), YDB_API_DEFAULT_RESPONSE_TYPE(methodName), methodCallback, rlMode, requestType, counterName, auditMode, COMMON, TGrpcRequestOperationCall, GRpcRequestProxyId_)

#endif // GRPC_METHOD_SETUP_H
