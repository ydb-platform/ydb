#pragma once
#include "common/owner.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

class TCSInitialization: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    const NMonitoring::THistogramPtr HistogramTabletInitializationMs;
    const NMonitoring::THistogramPtr HistogramTxInitDurationMs;
    const NMonitoring::THistogramPtr HistogramTxUpdateSchemaDurationMs;
    const NMonitoring::THistogramPtr HistogramTxInitSchemaDurationMs;
    const NMonitoring::THistogramPtr HistogramActivateExecutorFromActivationDurationMs;
    const NMonitoring::THistogramPtr HistogramSwitchToWorkFromActivationDurationMs;
    const NMonitoring::THistogramPtr HistogramSwitchToWorkFromCreateDurationMs;

public:
    void OnTxInitFinished(const TDuration d) const {
        HistogramTxInitDurationMs->Collect(d.MilliSeconds());
    }

    void OnTxUpdateSchemaFinished(const TDuration d) const {
        HistogramTxUpdateSchemaDurationMs->Collect(d.MilliSeconds());
    }

    void OnTxInitSchemaFinished(const TDuration d) const {
        HistogramTxInitSchemaDurationMs->Collect(d.MilliSeconds());
    }

    void OnActivateExecutor(const TDuration fromCreate) const {
        HistogramActivateExecutorFromActivationDurationMs->Collect(fromCreate.MilliSeconds());
    }
    void OnSwitchToWork(const TDuration fromStart, const TDuration fromCreate) const {
        HistogramSwitchToWorkFromActivationDurationMs->Collect(fromStart.MilliSeconds());
        HistogramSwitchToWorkFromCreateDurationMs->Collect(fromCreate.MilliSeconds());
    }

    TCSInitialization(TCommonCountersOwner& owner)
        : TBase(owner, "stage", "initialization")
        , HistogramTabletInitializationMs(TBase::GetHistogram("TabletInitializationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxInitDurationMs(TBase::GetHistogram("TxInitDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxUpdateSchemaDurationMs(TBase::GetHistogram("TxInitDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramTxInitSchemaDurationMs(TBase::GetHistogram("TxInitSchemaDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramActivateExecutorFromActivationDurationMs(
              TBase::GetHistogram("ActivateExecutorFromActivationDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramSwitchToWorkFromActivationDurationMs(
              TBase::GetHistogram("SwitchToWorkFromActivationDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32)))
        , HistogramSwitchToWorkFromCreateDurationMs(
              TBase::GetHistogram("SwitchToWorkFromCreateDurationMs", NMonitoring::ExponentialHistogram(15, 2, 32))) {
    }
};

}
