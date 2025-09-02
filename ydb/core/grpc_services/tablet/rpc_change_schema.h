#pragma once
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

namespace NKikimr::NGRpcService {

using TEvChangeTabletSchemaRequest = TGrpcRequestNoOperationCall<
    Ydb::Tablet::ChangeTabletSchemaRequest,
    Ydb::Tablet::ChangeTabletSchemaResponse>;

} // namespace NKikimr::NGRpcService
