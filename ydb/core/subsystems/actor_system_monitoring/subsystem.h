#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <functional>
#include <vector>

namespace NKikimr::NActorSystemMonitoring {

struct TPoolConfig {
    TString Name;
    bool IsIo = false;
    TString Threads = "runtime default";
    TString MinThreads = "runtime default";
    TString MaxThreads = "runtime default";
    TString Priority = "runtime default";
    TString SharedThreads = "runtime default";
    TString Parameters;
};

struct TConfig {
    ui32 ExecutorPool = 0;
    TDuration SamplePeriod = TDuration::Seconds(1);
    std::vector<TPoolConfig> Pools;
    TString SystemParameters;
    bool AutoConfigured = false;
    std::function<void(NActors::TActorSystem&, const NActors::TActorId&)> RegisterPage;
};

class TActorSystemMonitoring : public NActors::ISubSystem {};

std::unique_ptr<TActorSystemMonitoring> MakeActorSystemMonitoring(TConfig config);

} // namespace NKikimr::NActorSystemMonitoring
