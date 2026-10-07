#include "classic_grpc_service_adapter.h"

namespace NKikimr::NGRpcService {

    namespace {

        // Shared ordering for method registration and typed request slots.
        enum EMethod {
#define NBS1_COMPAT_METHOD_INDEX(name, ...) name,
            NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(NBS1_COMPAT_METHOD_INDEX)
#undef NBS1_COMPAT_METHOD_INDEX
        };

    } // namespace

    TClassicNbsGrpcServiceAdapter::TClassicNbsGrpcServiceAdapter()
    {
#define NBS1_COMPAT_REGISTER_METHOD(name, ...)                  \
    AddMethod(new grpc::internal::RpcServiceMethod(             \
        "/NCloud.NBlockStore.NProto.TBlockStoreService/" #name, \
        grpc::internal::RpcMethod::NORMAL_RPC,                  \
        nullptr));                                              \
    MarkMethodAsync(EMethod::name);

        NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(NBS1_COMPAT_REGISTER_METHOD)

#undef NBS1_COMPAT_REGISTER_METHOD
    }

    // static
    const char* TClassicNbsGrpcServiceAdapter::service_full_name()
    {
        return "NCloud.NBlockStore.NProto.TBlockStoreService";
    }

#define NBS1_COMPAT_DEFINE_REQUEST(name, ...)                                             \
    void TClassicNbsGrpcServiceAdapter::Request##name(                                    \
        grpc::ServerContext* context,                                                     \
        NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto::T##name##Request* request,        \
        grpc::ServerAsyncResponseWriter<                                                  \
            NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto::T##name##Response>* response, \
        grpc::CompletionQueue* callQueue,                                                 \
        grpc::ServerCompletionQueue* notificationQueue,                                   \
        void* tag) {                                                                      \
        RequestAsyncUnary(EMethod::name, context, request, response,                      \
                          callQueue, notificationQueue, tag);                             \
    }

    NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(NBS1_COMPAT_DEFINE_REQUEST)

#undef NBS1_COMPAT_DEFINE_REQUEST

} // namespace NKikimr::NGRpcService
