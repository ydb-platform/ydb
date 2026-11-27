#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/draft/ydb_database_management_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcDatabaseManagementService final : public TGrpcServiceBase<Ydb::DatabaseManagement::V1::DatabaseManagementService> {
public:
    using TGrpcServiceBase::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
};

} // namespace NKikimr::NGRpcService
