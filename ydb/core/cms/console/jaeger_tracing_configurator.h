#pragma once

#include "defs.h"

#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/protos/config.pb.h>

#include <array>

namespace NKikimr::NConsole {

enum class ETracingConfigKind : ui8 {
    Dev,
    UserFacing,
};

IActor* CreateJaegerTracingConfigurator(TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> tracingConfigurator,
                                        NKikimrConfig::TTracingConfig cfg,
                                        ETracingConfigKind configKind);

std::array<IActor*, 2> CreateJaegerTracingConfigurators(
    TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> devConfigurator,
    TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> userConfigurator,
    const NKikimrConfig::TAppConfig& config);

} // namespace NKikimr::NConsole
