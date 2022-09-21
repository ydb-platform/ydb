#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/draft/ydb_s3_internal_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbS3InternalService
    : public TGrpcServiceBase<Ydb::S3Internal::V1::S3InternalService>
{
public:
    using TGrpcServiceBase<Ydb::S3Internal::V1::S3InternalService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
