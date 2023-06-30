#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDriver)
DECLARE_REFCOUNTED_STRUCT(IProxyDiscoveryCache)

DECLARE_REFCOUNTED_CLASS(TDriverConfig)

struct TCommandDescriptor;
struct TDriverRequest;
struct TEtag;
struct TProxyDiscoveryRequest;
struct TProxyDiscoveryResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
