#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/draft/ydb_dynamic_config_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcDynamicConfigService
    : public TGrpcServiceBase<Ydb::DynamicConfig::V1::DynamicConfigService>
{
public:
    using TGrpcServiceBase<Ydb::DynamicConfig::V1::DynamicConfigService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
