#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbImportService
    : public TGrpcServiceBase<Ydb::Import::V1::ImportService>
{
public:
    using TGrpcServiceBase<Ydb::Import::V1::ImportService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
