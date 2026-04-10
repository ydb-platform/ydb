#pragma once

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_secret_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbSecretService
    : public TGrpcServiceBase<Ydb::Secret::V1::SecretService>
{
public:
    using TGrpcServiceBase<Ydb::Secret::V1::SecretService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

};

} // namespace NGRpcService
} // namespace NKikimr
