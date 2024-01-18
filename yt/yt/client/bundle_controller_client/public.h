#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TInstanceResources)
DECLARE_REFCOUNTED_STRUCT(TBundleTargetConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleConfigDescriptor)

struct TBundleConfigDescriptor;

struct TCpuLimits;
struct TMemoryLimits;
struct TInstanceResources;
struct TBundleTargetConfig;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleControllerClient
