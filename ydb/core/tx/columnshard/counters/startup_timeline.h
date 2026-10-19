#pragma once
#include "initialization.h"

#include <ydb/library/actors/core/monotonic.h>

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

    void LogStageFinished(const TString& stage, const TDuration duration) const;

public:
    TStartupTimeline(const ui64 tabletId, const TCSInitialization& counters)
        : TabletId(tabletId)
        , Counters(counters)
    {
    }

    void OnExecutorActivated();
    void OnTxInitSchemaFinished();
    void OnTxInitCreated();
    void OnTxInitFinished();
    void OnSwitchedToWork(const ui64 portionsCount);
};

}   // namespace NKikimr::NColumnShard
