#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateSensorService(
    TSolomonExporterConfigPtr config,
    TSolomonRegistryPtr registry,
    TSolomonExporterPtr exporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
