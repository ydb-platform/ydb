#pragma once

#include "public.h"

#include <yt/yt/client/misc/workload.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TContextPtr>
TWorkloadDescriptor GetRequestWorkloadDescriptor(
    const TContextPtr& context);
template <class TRequestPtr>
void SetRequestWorkloadDescriptor(
    const TRequestPtr& request,
    const TWorkloadDescriptor& workloadDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
