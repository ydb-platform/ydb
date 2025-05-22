#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/draft/ydb_replication_v1.grpc.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>

namespace NKikimr::NGRpcService {

class TGRpcReplicationService: public TGrpcServiceBase<Ydb::Replication::V1::ReplicationService> {
public:
    using TGrpcServiceBase<Ydb::Replication::V1::ReplicationService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

}
