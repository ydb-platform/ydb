#pragma once

#include <ydb/public/api/grpc/draft/ydb_logstore_v1.grpc.pb.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbLogStoreService
    : public TGrpcServiceBase<Ydb::LogStore::V1::LogStoreService>
{
public:
    using TGrpcServiceBase<Ydb::LogStore::V1::LogStoreService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

}
