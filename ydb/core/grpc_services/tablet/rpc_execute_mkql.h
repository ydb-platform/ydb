#pragma once
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

namespace NKikimr::NGRpcService {

using TEvExecuteTabletMiniKQLRequest = TGrpcRequestNoOperationCall<
    Ydb::Tablet::ExecuteTabletMiniKQLRequest,
    Ydb::Tablet::ExecuteTabletMiniKQLResponse>;

} // namespace NKikimr::NGRpcService
