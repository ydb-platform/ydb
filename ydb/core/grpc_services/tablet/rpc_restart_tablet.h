#pragma once
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

namespace NKikimr::NGRpcService {

using TEvRestartTabletRequest = TGrpcRequestNoOperationCall<
    Ydb::Tablet::RestartTabletRequest,
    Ydb::Tablet::RestartTabletResponse>;

} // namespace NKikimr::NGRpcService
