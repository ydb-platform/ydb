#pragma once

#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

 class TGRpcAuthService : public TGrpcServiceBase<Ydb::Auth::V1::AuthService>
 {
     using TBase = TGrpcServiceBase<Ydb::Auth::V1::AuthService>;
 public:
     using TGrpcServiceBase<Ydb::Auth::V1::AuthService>::TGrpcServiceBase;

 private:
     void SetServerOptions(const NYdbGrpc::TServerOptions& options) override;
     void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
 };

} // namespace NGRpcService
} // namespace NKikimr
