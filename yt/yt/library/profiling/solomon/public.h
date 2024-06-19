#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TShardConfig)
DECLARE_REFCOUNTED_STRUCT(TSolomonExporterConfig)
DECLARE_REFCOUNTED_STRUCT(TSolomonProxyConfig)

DECLARE_REFCOUNTED_CLASS(TSolomonExporter)
DECLARE_REFCOUNTED_CLASS(TSolomonRegistry)
DECLARE_REFCOUNTED_CLASS(TSolomonProxy)

DECLARE_REFCOUNTED_STRUCT(IEndpointProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
