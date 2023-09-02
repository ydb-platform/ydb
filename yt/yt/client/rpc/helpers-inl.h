#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt_proto/yt/client/misc/proto/workload.pb.h>

#include <yt/yt/core/rpc/service.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TContextPtr>
TWorkloadDescriptor GetRequestWorkloadDescriptor(const TContextPtr& context)
{
    using NYT::FromProto;
    const auto& header = context->GetRequestHeader();
    auto extensionId = NYT::NProto::TWorkloadDescriptorExt::workload_descriptor;
    if (header.HasExtension(extensionId)) {
        return FromProto<TWorkloadDescriptor>(header.GetExtension(extensionId));
    }
    // COMPAT(babenko): drop descriptor from request body
    return FromProto<TWorkloadDescriptor>(context->Request().workload_descriptor());
}

template <class TRequestPtr>
void SetRequestWorkloadDescriptor(
    const TRequestPtr& request,
    const TWorkloadDescriptor& workloadDescriptor)
{
    using NYT::ToProto;
    auto extensionId = NYT::NProto::TWorkloadDescriptorExt::workload_descriptor;
    auto* ext = request->Header().MutableExtension(extensionId);
    ToProto(ext, workloadDescriptor);
    // COMPAT(babenko): drop descriptor from request body
    ToProto(request->mutable_workload_descriptor(), workloadDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
