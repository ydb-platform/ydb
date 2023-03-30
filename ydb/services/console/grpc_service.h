#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/draft/ydb_console_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcConsoleService
    : public TGrpcServiceBase<Ydb::Console::V1::ConsoleService>
{
public:
    using TGrpcServiceBase<Ydb::Console::V1::ConsoleService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
