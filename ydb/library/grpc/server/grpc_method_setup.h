#ifndef GRPC_METHOD_SETUP_H
#define GRPC_METHOD_SETUP_H

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

// Общий макрос для настройки методов gRPC
#define SETUP_METHOD(methodName, method, rlMode, requestType, serviceType, counterName, auditMode)    \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                         \
        Ydb::serviceType::Y_CAT(methodName, Request),                                                 \
        Ydb::serviceType::Y_CAT(methodName, Response),                                                \
        T##serviceType##GRpcService>>                                                                 \
    (                                                                                                 \
        this,                                                                                         \
        &Service_,                                                                                    \
        CQ,                                                                                           \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                               \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                        \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                      \
                Ydb::serviceType::Y_CAT(methodName, Request),                                         \
                Ydb::serviceType::Y_CAT(methodName, Response)>(reqCtx, &method,                       \
                    TRequestAuxSettings {                                                             \
                        .RlMode = TRateLimiterMode::rlMode,                                           \
                        .AuditMode = auditMode,                                                       \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                     \
                    }));                                                                              \
        },                                                                                            \
        &Ydb::serviceType::V1::Y_CAT(serviceType, Service)::AsyncService::Y_CAT(Request, methodName), \
        Y_STRINGIZE(serviceType) "/" Y_STRINGIZE(methodName),                                         \
        logger,                                                                                       \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                            \
    )->Run()

#endif // GRPC_METHOD_SETUP_H
