#pragma once

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_udf_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbUdfService
    : public TGrpcServiceBase<Ydb::Udf::V1::UdfService>
{
public:
    using TGrpcServiceBase<Ydb::Udf::V1::UdfService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

};

} // namespace NGRpcService
} // namespace NKikimr
