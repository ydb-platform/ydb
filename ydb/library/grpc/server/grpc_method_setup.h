#ifndef GRPC_METHOD_SETUP_H
#define GRPC_METHOD_SETUP_H

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

#define SETUP_METHOD_WITH_TYPE(methodName, method, rlMode, requestType, serviceType, counterName, auditMode, runtimeEventType) \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                                                  \
        Ydb::serviceType::Y_CAT(methodName, Request),                                                                          \
        Ydb::serviceType::Y_CAT(methodName, Response),                                                                         \
        T##serviceType##GRpcService>>                                                                                          \
    (                                                                                                                          \
        this,                                                                                                                  \
        &Service_,                                                                                                             \
        CQ,                                                                                                                    \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                                        \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                                                 \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                                               \
                Ydb::serviceType::Y_CAT(methodName, Request),                                                                  \
                Ydb::serviceType::Y_CAT(methodName, Response),                                                                 \
                TYdbGrpcMethodAccessorTraits<                                                                                  \
                    Ydb::serviceType::Y_CAT(methodName, Request),                                                              \
                    Ydb::serviceType::Y_CAT(methodName, Response), true>,                                                      \
                NRuntimeEvents::EType::runtimeEventType>(reqCtx, &method,                                                      \
                    TRequestAuxSettings {                                                                                      \
                        .RlMode = TRateLimiterMode::rlMode,                                                                    \
                        .AuditMode = auditMode,                                                                                \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                                              \
                    }));                                                                                                       \
        },                                                                                                                     \
        &Ydb::serviceType::V1::Y_CAT(serviceType, Service)::AsyncService::Y_CAT(Request, methodName),                          \
        Y_STRINGIZE(serviceType) "/" Y_STRINGIZE(methodName),                                                                  \
        logger,                                                                                                                \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                                                     \
    )->Run()

// Общий макрос для настройки методов gRPC
#define SETUP_METHOD(methodName, method, rlMode, requestType, serviceType, counterName, auditMode)                             \
    SETUP_METHOD_WITH_TYPE(methodName, method, rlMode, requestType, serviceType, counterName, auditMode, UNKNOWN)

#endif // GRPC_METHOD_SETUP_H
