#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/core/grpc_services/auth_processor/dynamic_node_auth_processor.h>


namespace NKikimr {
namespace NGRpcService {

 class IRequestOpCtx;
 class IFacilityProvider;

 class TGRpcDiscoveryService
     : public TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>
 {
  public:
     using TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>::TGrpcServiceBase;

     void SetDynamicNodeAuthParams(const TDynamicNodeAuthorizationParams& dynamicNodeAuthorizationParams);

  private:
     void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
     TDynamicNodeAuthorizationParams DynamicNodeAuthorizationParams = {};
     std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> NodeRegistrationRequest;
 };

} // namespace NGRpcService
} // namespace NKikimr
