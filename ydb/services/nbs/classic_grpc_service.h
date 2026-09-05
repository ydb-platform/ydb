#pragma once

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/grpc/service.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>

namespace NKikimr::NGRpcService {

    // Exposes the supported classic NBS subset on a YDB-owned gRPC server.
    class TClassicNbsGrpcService final
        : public NYdbGrpc::TGrpcServiceBase<
              NCloud::NBlockStore::NProto::TBlockStoreService> {
    public:
        explicit TClassicNbsGrpcService(
            NCloud::NBlockStore::IBlockStorePtr blockStore);

        // Registers handlers for every RPC in the supported subset.
        void InitService(
            grpc::ServerCompletionQueue* cq,
            NYdbGrpc::TLoggerPtr logger) override;

    private:
        template <typename TMethod>
        void HandleRequest(NYdbGrpc::IRequestContextBase* requestContext);

        void SetupIncomingRequests(
            grpc::ServerCompletionQueue* cq,
            NYdbGrpc::TLoggerPtr logger);

    private:
        const NCloud::NBlockStore::IBlockStorePtr BlockStore;
    };

} // namespace NKikimr::NGRpcService
