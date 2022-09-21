#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <ydb/public/api/grpc/draft/ydb_experimental_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbExperimentalService
    : public TGrpcServiceBase<Ydb::Experimental::V1::ExperimentalService>
{
public:
    using TGrpcServiceBase<Ydb::Experimental::V1::ExperimentalService>::TGrpcServiceBase;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
