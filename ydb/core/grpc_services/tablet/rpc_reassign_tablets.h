#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

namespace NKikimr::NGRpcService {

using TEvReassignTabletsRequest = TGrpcRequestNoOperationCall<
    Ydb::Tablet::ReassignTabletsRequest,
    Ydb::Tablet::ReassignTabletsResponse>;

using TEvStopReassignTabletsRequest = TGrpcRequestNoOperationCall<
    Ydb::Tablet::StopReassignTabletsRequest,
    Ydb::Tablet::StopReassignTabletsResponse>;

} // namespace NKikimr::NGRpcService
