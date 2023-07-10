#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbQueryService
    : public TGrpcServiceBase<Ydb::Query::V1::QueryService>
{
public:
    using TGrpcServiceBase<Ydb::Query::V1::QueryService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NKikimr::NGRpcService
