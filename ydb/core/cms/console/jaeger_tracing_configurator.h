#pragma once

#include "defs.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/console_config.pb.h>

namespace NKikimr::NConsole {

// `configItemKind` is the CMS item this channel subscribes to for live reconfig; `userFacing`
// selects which field of the pushed config to read (user-facing vs the default dev channel).
IActor* CreateJaegerTracingConfigurator(TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> tracingConfigurator,
                                                NKikimrConfig::TTracingConfig cfg,
                                                ui32 configItemKind = static_cast<ui32>(NKikimrConsole::TConfigItem::TracingConfigItem),
                                                bool userFacing = false);

} // namespace NKikimr::NConsole
