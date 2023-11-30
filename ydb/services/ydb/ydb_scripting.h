#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbScriptingService
    : public TGrpcServiceBase<Ydb::Scripting::V1::ScriptingService>
{
public:
    using TGrpcServiceBase<Ydb::Scripting::V1::ScriptingService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
