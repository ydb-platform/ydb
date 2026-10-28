#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>
#include <functional>

namespace NKikimr::NInMemoryMetricsMonitoring {

struct TConfig {
    ui32 ExecutorPool = 0;
    std::function<void(NActors::TActorSystem&, const NActors::TActorId&)> RegisterPage;
};

class TInMemoryMetricsMonitoring : public NActors::ISubSystem {};

std::unique_ptr<TInMemoryMetricsMonitoring> MakeInMemoryMetricsMonitoring(TConfig config);

} // namespace NKikimr::NInMemoryMetricsMonitoring
