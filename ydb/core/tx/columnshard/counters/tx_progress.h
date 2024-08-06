#pragma once
#include "common/owner.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NColumnShard {

class TTxProgressCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    using TOpType = TString;

    class TProgressCounters: public TCommonCountersOwner {
    private:
        using TBase = TCommonCountersOwner;

    public:
        NMonitoring::TDynamicCounters::TCounterPtr RegisterTx;
        NMonitoring::TDynamicCounters::TCounterPtr RegisterTxWithDeadline;
        NMonitoring::TDynamicCounters::TCounterPtr StartProposeOnExecute;
        NMonitoring::TDynamicCounters::TCounterPtr StartProposeOnComplete;
        NMonitoring::TDynamicCounters::TCounterPtr FinishProposeOnExecute;
        NMonitoring::TDynamicCounters::TCounterPtr FinishProposeOnComplete;
        NMonitoring::TDynamicCounters::TCounterPtr FinishPlannedTx;
        NMonitoring::TDynamicCounters::TCounterPtr AbortTx;

        TProgressCounters(const TCommonCountersOwner& owner)
            : TBase(owner)
            , RegisterTx(TBase::GetDeriviative("RegisterTx"))
            , RegisterTxWithDeadline(TBase::GetDeriviative("RegisterTxWithDeadline"))
            , StartProposeOnExecute(TBase::GetDeriviative("StartProposeOnExecute"))
            , StartProposeOnComplete(TBase::GetDeriviative("StartProposeOnComplete"))
            , FinishProposeOnExecute(TBase::GetDeriviative("FinishProposeOnExecute"))
            , FinishProposeOnComplete(TBase::GetDeriviative("FinishProposeOnComplete"))
            , FinishPlannedTx(TBase::GetDeriviative("FinishPlannedTx"))
            , AbortTx(TBase::GetDeriviative("AbortTx")) {
        }
    };

    THashMap<TOpType, TProgressCounters> CountersByOpType;

public:
    void OnRegisterTx(const TOpType& opType) {
        GetSubGroup(opType).RegisterTx->Add(1);
    }

    void OnRegisterTxWithDeadline(const TOpType& opType) {
        GetSubGroup(opType).RegisterTxWithDeadline->Add(1);
    }

    void OnStartProposeOnExecute(const TOpType& opType) {
        GetSubGroup(opType).StartProposeOnExecute->Add(1);
    }

    void OnStartProposeOnComplete(const TOpType& opType) {
        GetSubGroup(opType).StartProposeOnComplete->Add(1);
    }

    void OnFinishProposeOnExecute(const TOpType& opType) {
        GetSubGroup(opType).FinishProposeOnExecute->Add(1);
    }

    void OnFinishProposeOnComplete(const TOpType& opType) {
        GetSubGroup(opType).FinishProposeOnComplete->Add(1);
    }

    void OnFinishPlannedTx(const TOpType& opType) {
        GetSubGroup(opType).FinishPlannedTx->Add(1);
    }

    void OnAbortTx(const TOpType& opType) {
        GetSubGroup(opType).AbortTx->Add(1);
    }

    TTxProgressCounters(TCommonCountersOwner& owner)
        : TBase(owner, "TxProgress") {
    }

private:
    TProgressCounters& GetSubGroup(const TOpType& opType) {
        auto findSubGroup = CountersByOpType.FindPtr(opType);
        if (findSubGroup) {
            return *findSubGroup;
        }

        auto subGroup = TBase::CreateSubGroup("operation", opType);
        return CountersByOpType.emplace(opType, subGroup).first->second;
    }
};

}
