#pragma once

#include "defs.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NConsole {

IActor* CreateJaegerTracingConfigurator(TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> tracingConfigurator,
                                                NKikimrConfig::TTracingConfig cfg,
                                                ui32 configItemKind,
                                                bool userFacing);

} // namespace NKikimr::NConsole
