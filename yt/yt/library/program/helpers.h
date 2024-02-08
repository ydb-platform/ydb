#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config);
void ReconfigureSingletons(
    const TSingletonsConfigPtr& config,
    const TSingletonsDynamicConfigPtr& dynamicConfig);

void StartDiagnosticDump(const TDiagnosticDumpConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
