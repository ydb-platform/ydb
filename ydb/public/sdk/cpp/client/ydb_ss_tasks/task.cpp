#include "task.h"
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_export_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
namespace NSchemeShard {

/// YT
TBackgroundProcessesResponse::TBackgroundProcessesResponse(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    Metadata_.Id = GetProto().DebugString();
}

const TBackgroundProcessesResponse::TMetadata& TBackgroundProcessesResponse::Metadata() const {
    return Metadata_;
}

} // namespace NExport
} // namespace NYdb
