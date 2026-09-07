#pragma once

#include "classic_grpc_service_adapter.h"

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

#include <ydb/library/grpc/server/grpc_server.h>

namespace NKikimr::NGRpcService {

    // Exposes the supported classic NBS subset on a YDB-owned gRPC server.
    class TClassicNbsGrpcService final
        : public NYdbGrpc::TGrpcServiceBase<
              TClassicNbsGrpcServiceAdapter> {
    public:
        explicit TClassicNbsGrpcService(
            NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr blockStore);

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
        const NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr BlockStore;
    };

} // namespace NKikimr::NGRpcService
