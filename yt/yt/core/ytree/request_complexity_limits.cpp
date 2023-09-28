#include "request_complexity_limits.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/core/ytree/proto/request_complexity_limits.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void TReadRequestComplexityOverrides::ApplyTo(TReadRequestComplexity& complexity) const noexcept
{
    auto apply = [] (i64& target, std::optional<i64> override) {
        target = override.value_or(target);
    };

    apply(complexity.NodeCount, NodeCount);
    apply(complexity.ResultSize, ResultSize);
}

void TReadRequestComplexityOverrides::Validate(TReadRequestComplexity max) const
{
    TError error;

    auto doCheck = [&] (TStringBuf fieldName, std::optional<i64> override, i64 max) {
        if (override && *override > max) {
            error.SetCode(NYT::EErrorCode::Generic);
            error.SetMessage("Read request complexity limits too large");
            error = error << TErrorAttribute(TString(fieldName), *override);
        }
    };

    doCheck("node_count", NodeCount, max.NodeCount);
    doCheck("result_size", ResultSize, max.ResultSize);

    error.ThrowOnError();
}

void FromProto(
    TReadRequestComplexityOverrides* original,
    const NProto::TReadRequestComplexityLimits& serialized)
{
    original->NodeCount = YT_PROTO_OPTIONAL(serialized, node_count);
    original->ResultSize = YT_PROTO_OPTIONAL(serialized, result_size);
}

void ToProto(
    NProto::TReadRequestComplexityLimits* serialized,
    const TReadRequestComplexityOverrides& original)
{
    if (original.NodeCount) {
        serialized->set_node_count(*original.NodeCount);
    }
    if (original.ResultSize) {
        serialized->set_result_size(*original.ResultSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NYTree
