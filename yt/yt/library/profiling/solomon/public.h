#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TShardConfig)
DECLARE_REFCOUNTED_STRUCT(TSolomonExporterConfig)
DECLARE_REFCOUNTED_CLASS(TSolomonExporter)
DECLARE_REFCOUNTED_CLASS(TSolomonRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
