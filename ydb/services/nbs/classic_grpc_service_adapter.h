#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/request.h>

#include <grpcpp/impl/service_type.h>
#include <grpcpp/impl/proto_utils.h>
#include <grpcpp/support/async_unary_call.h>

namespace NKikimr::NGRpcService {

    // Binds isolated NBS2 protobuf types to the original NBS1 unary RPC paths.
    // Implements the generated AsyncService interface expected by the YDB server.
    class TClassicNbsGrpcServiceAdapter final: public grpc::Service {
    public:
        using AsyncService = TClassicNbsGrpcServiceAdapter;

        // Registers the supported methods, in the same order as Request<Name>.
        TClassicNbsGrpcServiceAdapter();

        // Returns the external service name, not the isolated protobuf package.
        static const char* service_full_name();

#define NBS1_COMPAT_DECLARE_REQUEST(name, ...)                                            \
    /* Arms a typed request on the YDB-owned completion queues. */                        \
    void Request##name(                                                                   \
        grpc::ServerContext* context,                                                     \
        NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto::T##name##Request* request,        \
        grpc::ServerAsyncResponseWriter<                                                  \
            NYdb::NBS::NNbs1CompatApi::NBlockStore::NProto::T##name##Response>* response, \
        grpc::CompletionQueue* callQueue,                                                 \
        grpc::ServerCompletionQueue* notificationQueue,                                   \
        void* tag);

        NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(NBS1_COMPAT_DECLARE_REQUEST)

#undef NBS1_COMPAT_DECLARE_REQUEST
    };

} // namespace NKikimr::NGRpcService
