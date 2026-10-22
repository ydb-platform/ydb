#pragma once
#include "initialization.h"

#include <ydb/library/actors/core/monotonic.h>

#include <library/cpp/lwtrace/shuttle.h>

#include <optional>

namespace NKikimr::NColumnShard {

class TStartupTimeline {
private:
    const ui64 TabletId;
    const TCSInitialization& Counters;
    const TMonotonic CreateInstant = TMonotonic::Now();
    std::optional<TMonotonic> ExecutorActivatedInstant;
    TMonotonic TxInitSchemaFinishInstant;
    TMonotonic TxInitCreateInstant;
    TMonotonic TxInitFinishInstant;
    NLWTrace::TOrbit Orbit;

    void TraceStageFinished(const TString& stage, const TDuration duration);

public:
    TStartupTimeline(const ui64 tabletId, const TCSInitialization& counters);

    NLWTrace::TOrbit& MutableOrbit() {
        return Orbit;
    }

    void OnExecutorActivated();
    void OnTxInitSchemaFinished();
    void OnTxInitCreated();
    void OnTxInitFinished();
    void OnSwitchedToWork(const ui64 portionsCount);
};

}   // namespace NKikimr::NColumnShard
