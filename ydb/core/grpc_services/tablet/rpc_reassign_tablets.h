#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

namespace NKikimr::NGRpcService {

using TEvReassignTabletsRequest = TGrpcRequestOperationCall<
    Ydb::Tablet::ReassignTabletsRequest,
    Ydb::Tablet::ReassignTabletsResponse>;

} // namespace NKikimr::NGRpcService
