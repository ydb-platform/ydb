#pragma once
#include "owner.h"

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/serialized_enum.h>

#include <map>
#include <memory>
#include <mutex>
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

// A state's signals are registered on its first transition, so a state never entered is not exported.
template <class EState>
class TStateSignalsOwner: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<TString> StateNames;
    std::vector<std::once_flag> Registered;
    std::vector<std::unique_ptr<TStateCounters>> States;

    TStateCounters& GetState(const EState state) {
        const ui32 index = (ui32)state;
        AFL_VERIFY(index < StateNames.size())("state", state)("size", StateNames.size());
        std::call_once(Registered[index], [&] {
            States[index] = std::make_unique<TStateCounters>(*this, StateNames[index]);
        });
        return *States[index];
    }

public:
    void ExchangeState(const std::optional<EState> from, const std::optional<EState> to, const std::optional<TDuration> d, const ui32 size = 1) {
        if (from) {
            TStateCounters& state = GetState(*from);
            state.Volume->Sub(size);
            state.Remove->Add(size);
            if (d) {
                state.Duration->Add(d->MicroSeconds());
            }
        }
        if (to) {
            TStateCounters& state = GetState(*to);
            state.Volume->Add(size);
            state.Add->Add(size);
        }
    }

    TStateSignalsOwner(TCommonCountersOwner& base, const TString& stateName)
        : TBase(base, "states", stateName) {
        for (auto&& i : GetEnumAllValues<EState>()) {
            while (StateNames.size() < (ui32)i) {
                StateNames.emplace_back("UNDEFINED");
            }
            StateNames.emplace_back(::ToString(i));
        }
        Registered = std::vector<std::once_flag>(StateNames.size());
        States.resize(StateNames.size());
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
