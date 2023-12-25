#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBundleConfigDescriptor)

struct TBundleConfigDescriptor;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TInstanceResources)

struct TCpuLimits;
struct TMemoryLimits;
struct TInstanceResources;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleControllerClient
