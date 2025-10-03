#include "utils.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <yql/essentials/sql/v1/node.h>

namespace NKikimr::NKqp {

TStreamingQuerySettings& TStreamingQuerySettings::FromProto(const NKikimrSchemeOp::TStreamingQueryProperties& info) {
    for (const auto& [name, value] : info.GetProperties()) {
        if (name == TStreamingQueryMeta::TSqlSettings::QUERY_TEXT_FEATURE) {
            QueryText = value;
        } else if (name == TStreamingQueryMeta::TProperties::Run) {
            Run = value == "true";
        } else if (name == TStreamingQueryMeta::TProperties::ResourcePool) {
            ResourcePool = value;
        }
    }

    return *this;
}

}  // namespace NKikimr::NKqp
