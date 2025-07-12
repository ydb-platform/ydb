#pragma once
#include "owner.h"

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/serialized_enum.h>

#include <map>
#include <set>

namespace NKikimr::NOlap::NCounters {

class TStateCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    const NMonitoring::TDynamicCounters::TCounterPtr Volume;
    const NMonitoring::TDynamicCounters::TCounterPtr Add;
    const NMonitoring::TDynamicCounters::TCounterPtr Remove;
    const NMonitoring::TDynamicCounters::TCounterPtr Duration;

    TStateCounters(TCommonCountersOwner& base, const TString& stateId)
        : TBase(base, "state_id", stateId)
        , Volume(TBase::GetValue("Count"))
        , Add(TBase::GetDeriviative("Add"))
        , Remove(TBase::GetDeriviative("Remove"))
        , Duration(TBase::GetDeriviative("Duration/Us")) {
    }
};

template <class EState>
class TStateSignalsOwner: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<TStateCounters> States;

public:
    void ExchangeState(const std::optional<EState> from, const std::optional<EState> to, const std::optional<TDuration> d, const ui32 size = 1) {
        if (from) {
            AFL_VERIFY((ui32)*from < States.size())("from", from)("size", States.size());
            States[(ui32)*from].Volume->Sub(size);
            States[(ui32)*from].Remove->Add(size);
            if (d) {
                States[(ui32)*from].Duration->Add(d->MicroSeconds());
            }
        }
        if (to) {
            AFL_VERIFY((ui32)*to < States.size())("to", to)("size", States.size());
            States[(ui32)*to].Volume->Add(size);
            States[(ui32)*to].Add->Add(size);
        }
    }

    TStateSignalsOwner(TCommonCountersOwner& base, const TString& stateName)
        : TBase(base, "states", stateName) {
        for (auto&& i : GetEnumAllValues<EState>()) {
            while (States.size() < (ui32)i) {
                States.emplace_back(*this, "UNDEFINED");
            }
            States.emplace_back(*this, ::ToString(i));
        }
    }
};

template <class EState>
class TStateSignalsOperator {
private:
    std::shared_ptr<TStateSignalsOwner<EState>> Signals;

public:
    class TGuard: public TMoveOnly {
    private:
        std::optional<TMonotonic> CurrentStateInstant;
        std::optional<EState> CurrentState;
        std::shared_ptr<TStateSignalsOwner<EState>> Signals;

    public:
        EState GetStage() const {
            AFL_VERIFY(CurrentState);
            return *CurrentState;
        }

        void SetState(const std::optional<EState> state) {
            std::optional<TDuration> d;
            const TMonotonic current = TMonotonic::Now();
            if (CurrentStateInstant) {
                d = current - *CurrentStateInstant;
            }
            Signals->ExchangeState(CurrentState, state, d);
            CurrentState = state;
            CurrentStateInstant = current;
        }

        TGuard(const std::shared_ptr<TStateSignalsOwner<EState>>& owner, const std::optional<EState> start)
            : Signals(owner)
        {
            SetState(start);
        }

        ~TGuard() {
            SetState(std::nullopt);
        }
    };

    TStateSignalsOperator(NColumnShard::TCommonCountersOwner& base, const TString& stateName) {
        Signals = std::make_shared<TStateSignalsOwner<EState>>(base, stateName);
    }

    TGuard BuildGuard(const std::optional<EState> startState) {
        return TGuard(Signals, startState);
    }
};

}   // namespace NKikimr::NOlap::NCounters
