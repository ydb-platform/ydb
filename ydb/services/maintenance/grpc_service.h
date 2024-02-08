#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/draft/ydb_maintenance_v1.grpc.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>

namespace NKikimr::NGRpcService {

class TGRpcMaintenanceService: public TGrpcServiceBase<Ydb::Maintenance::V1::MaintenanceService> {
public:
    using TGrpcServiceBase<Ydb::Maintenance::V1::MaintenanceService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

}
