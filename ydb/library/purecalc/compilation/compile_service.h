#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>

#include <yql/essentials/public/purecalc/common/fwd.h>
#include <yql/essentials/public/issue/yql_issue.h>


namespace NYdb::NPurecalc {

struct TPurecalcCompileSettings {
    bool EnabledLLVM = false;

    std::strong_ordering operator<=>(const TPurecalcCompileSettings& other) const = default;
};

class IProgramHolder : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProgramHolder>;

public:
    // Perform program creation and saving
    virtual void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) = 0;
};

enum EEv : ui32 {
    EvPurecalcCompileRequest = EventSpaceBegin(NKikimr::TKikimrEvents::ES_PURECALC_COMPILER),
    EvPurecalcCompileResponse,
    EvPurecalcCompileAbort,
    EvEnd,
};

// Compilation events
struct TEvPurecalcCompileRequest : public NActors::TEventLocal<TEvPurecalcCompileRequest, EEv::EvPurecalcCompileRequest> {
    TEvPurecalcCompileRequest(IProgramHolder::TPtr programHolder, const TPurecalcCompileSettings& settings)
        : ProgramHolder(std::move(programHolder))
        , Settings(settings)
    {}

    IProgramHolder::TPtr ProgramHolder;
    TPurecalcCompileSettings Settings;
};

struct TEvPurecalcCompileResponse : public NActors::TEventLocal<TEvPurecalcCompileResponse, EEv::EvPurecalcCompileResponse> {
    TEvPurecalcCompileResponse(NYql::TIssues issues)
        : Status(0)
        , Issues(std::move(issues))
    {}

    explicit TEvPurecalcCompileResponse(IProgramHolder::TPtr programHolder)
        : ProgramHolder(std::move(programHolder))
        , Status(1)
    {}

    IProgramHolder::TPtr ProgramHolder;  // Same holder that passed into TEvPurecalcCompileRequest
    bool Status;
    NYql::TIssues Issues;
};

struct TEvPurecalcCompileAbort : public NActors::TEventLocal<TEvPurecalcCompileAbort, EEv::EvPurecalcCompileAbort> {};

struct TCompileServiceConfig {
    ui64 ParallelCompilationLimit = 1;
};

NActors::IActor* CreatePurecalcCompileService(const TCompileServiceConfig& config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
