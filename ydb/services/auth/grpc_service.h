#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

 class TGRpcAuthService : public TGrpcServiceBase<Ydb::Auth::V1::AuthService>
 {
 public:
     using TGrpcServiceBase<Ydb::Auth::V1::AuthService>::TGrpcServiceBase;

  private:
     void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
 };

} // namespace NGRpcService
} // namespace NKikimr
