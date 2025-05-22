#pragma once

#include "ydb/public/api/grpc/draft/ydb_ymq_v1.grpc.pb.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

    class TGRpcYmqService : public TGrpcServiceBase<Ydb::Ymq::V1::YmqService>
    {
    public:
        using TGrpcServiceBase<Ydb::Ymq::V1::YmqService>::TGrpcServiceBase;
    private:
        void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
    };

}
