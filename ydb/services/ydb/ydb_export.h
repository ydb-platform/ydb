#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_export_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbExportService
    : public TGrpcServiceBase<Ydb::Export::V1::ExportService>
{
public:
    using TGrpcServiceBase<Ydb::Export::V1::ExportService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

};

} // namespace NGRpcService
} // namespace NKikimr
