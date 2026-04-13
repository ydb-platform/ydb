#include "utils.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/sql/v1/node.h>

namespace NKikimr::NKqp {

TString TStreamingQueryMeta::GetTablesPath() {
    return JoinPath({".metadata", InternalTablesPath});
}

TStreamingQuerySettings& TStreamingQuerySettings::FromProto(const NKikimrSchemeOp::TStreamingQueryProperties& info) {
    for (const auto& [name, value] : info.GetProperties()) {
        if (name == TStreamingQueryMeta::TSqlSettings::QUERY_TEXT_FEATURE) {
            QueryText = value;
        } else if (name == TStreamingQueryMeta::TProperties::Run) {
            Run = value == "true";
        } else if (name == TStreamingQueryMeta::TProperties::ResourcePool) {
            ResourcePool = value;
        } else if (name == TStreamingQueryMeta::TProperties::QueryTextRevision) {
            QueryTextRevision = TryFromString<ui64>(value).GetOrElse(0);
        } else if (name == TStreamingQueryMeta::TProperties::StreamingDisposition) {
            StreamingDisposition = std::make_shared<NYql::NPq::NProto::StreamingDisposition>();
            Y_VALIDATE(StreamingDisposition->ParseFromString(value), "Failed to parse StreamingDisposition");
        }
    }

    return *this;
}

}  // namespace NKikimr::NKqp
