#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

    class TGRpcDataStreamsService : public TGrpcServiceBase<Ydb::DataStreams::V1::DataStreamsService>
    {
    public:
        using TGrpcServiceBase<Ydb::DataStreams::V1::DataStreamsService>::TGrpcServiceBase;
    private:
        void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
    };

}
