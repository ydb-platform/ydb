#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbSchemeService
    : public TGrpcServiceBase<Ydb::Scheme::V1::SchemeService>
{
public:
    using TGrpcServiceBase<Ydb::Scheme::V1::SchemeService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

};

} // namespace NGRpcService
} // namespace NKikimr
