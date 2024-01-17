#pragma once

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash_set.h>

#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NKesus {

class TKesusGRpcService
    : public ::NKikimr::NGRpcService::TGrpcServiceBase<Ydb::Coordination::V1::CoordinationService>
{
    class TContextBase;
    class TSessionContext;

public:
    using ::NKikimr::NGRpcService::TGrpcServiceBase<Ydb::Coordination::V1::CoordinationService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

}
}
