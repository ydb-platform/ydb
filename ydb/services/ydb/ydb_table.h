#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbTableService
    : public TGrpcServiceBase<Ydb::Table::V1::TableService>
{
public:
    using TGrpcServiceBase<Ydb::Table::V1::TableService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
