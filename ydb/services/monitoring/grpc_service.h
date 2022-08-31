#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

 class TGRpcMonitoringService : public TGrpcServiceBase<Ydb::Monitoring::V1::MonitoringService>
 {
 public:
     using TGrpcServiceBase<Ydb::Monitoring::V1::MonitoringService>::TGrpcServiceBase;
 private:
     void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
 };

} // namespace NGRpcService
} // namespace NKikimr
