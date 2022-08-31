#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcOperationService
    : public TGrpcServiceBase<Ydb::Operation::V1::OperationService>
{
public:
    using TGrpcServiceBase<Ydb::Operation::V1::OperationService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
