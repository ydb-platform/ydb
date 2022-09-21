#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

 class TGRpcDiscoveryService
     : public TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>
 {
  public:
     using TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>::TGrpcServiceBase;

  private:
     void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

 };

} // namespace NGRpcService
} // namespace NKikimr
