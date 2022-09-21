#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/draft/ydb_long_tx_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbLongTxService
    : public TGrpcServiceBase<Ydb::LongTx::V1::LongTxService>
{
public:
    using TGrpcServiceBase<Ydb::LongTx::V1::LongTxService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);
};

} // namespace NGRpcService
} // namespace NKikimr
