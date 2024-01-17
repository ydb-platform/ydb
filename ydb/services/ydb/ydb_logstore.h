#pragma once

#include <ydb/public/api/grpc/draft/ydb_logstore_v1.grpc.pb.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbLogStoreService
    : public TGrpcServiceBase<Ydb::LogStore::V1::LogStoreService>
{
public:
    using TGrpcServiceBase<Ydb::LogStore::V1::LogStoreService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

}
