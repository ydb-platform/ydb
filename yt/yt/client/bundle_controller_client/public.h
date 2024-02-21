#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TInstanceResources)
DECLARE_REFCOUNTED_STRUCT(TDefaultInstanceConfig)
DECLARE_REFCOUNTED_STRUCT(TInstanceSize)
DECLARE_REFCOUNTED_STRUCT(TBundleTargetConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleConfigDescriptor)
DECLARE_REFCOUNTED_STRUCT(TBundleConfigConstraints)
DECLARE_REFCOUNTED_STRUCT(TBundleResourceQuota)

struct TBundleConfigDescriptor;

struct TCpuLimits;
struct TMemoryLimits;
struct TInstanceResources;
struct TDefaultInstanceConfig;
struct TInstanceSize;
struct TBundleTargetConfig;
struct TBundleConfigConstraints;
struct TBundleResourceQuota;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleControllerClient
