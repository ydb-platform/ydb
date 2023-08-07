#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NServiceDiscovery {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((EndpointSetDoesNotExist) (20000))
    ((EndpointResolveFailed)   (20001))
    ((UnknownResolveStatus)    (20002))
);

DECLARE_REFCOUNTED_STRUCT(IServiceDiscovery)

struct TEndpoint;
struct TEndpointSet;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery
