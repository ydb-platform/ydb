#pragma once

#include "ydb/public/api/grpc/draft/ydb_sqs_topic_v1.grpc.pb.h"
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

    class TGRpcSqsTopicService : public TGrpcServiceBase<Ydb::SqsTopic::V1::SqsTopicService>
    {
    public:
        using TGrpcServiceBase<Ydb::SqsTopic::V1::SqsTopicService>::TGrpcServiceBase;
    private:
        void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
    };

}
