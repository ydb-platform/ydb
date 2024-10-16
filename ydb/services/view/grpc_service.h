#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/draft/ydb_view_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcViewService: public TGrpcServiceBase<Ydb::View::V1::ViewService> {
public:
    using TGrpcServiceBase<Ydb::View::V1::ViewService>::TGrpcServiceBase;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
};

}
