#pragma once
#include "owner.h"

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <map>
#include <set>

namespace NKikimr::NOlap::NCounters {

template <enum EState>
class TStateSignalsOwner: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> StateVolume;

public:
    void ExchangeState(const std::optional<EState> from, const std::optional<EState> to, const ui32 size = 1) {
        if (from) {
            AFL_VERIFY((ui32)*from < StateVolume.size())("from", from)("size", StateVolume.size());
            StateVolume[(ui32)*from]->Sub(size);
        }
        if (to) {
            AFL_VERIFY((ui32)*to < StateVolume.size())("to", to)("size", StateVolume.size());
            StateVolume[(ui32)*to]->Add(size);
        }
    }

    TStateSignalsOwner(TCommonCountersOwner& base, const TString& stateName)
        : TBase(base, "states", stateName) {
        for (auto&& i : GetEnumAllValues<EState>()) {
            while (StateVolume.size() < (ui32)i) {
                StateVolume.emplace_back(TBase::GetValue("state_id", "UNDEFINED"));
            }
            StateVolume.emplace_back(TBase::GetValue("state_id", ::ToString(i)));
        }
    }
};

template <enum EState>
class TStateSignalsOperator {
private:
    std::shared_ptr<TStateSignalsOwner<EState>> Signals;

public:
    class TGuard: public TMoveOnly {
    private:
        std::optional<EState> CurrentState;
        std::shared_ptr<TStateSignalsOwner<EState>> Signals;

    public:
        void SetState(const EState state) {
            Signals->ExchangeState(CurrentState, state);
            CurrentState = state;
        }

        TGuard(const std::optional<EState> start)
            : CurrentState(start) {
            Signals->ExchangeState(std::nullopt, CurrentState);
        }

        ~TGuard() {
            Signals->ExchangeState(CurrentState, std::nullopt);
        }
    };

    TStateSignalsOperator(TCommonCountersOwner& base, const TString& stateName) {
        Signals = std::make_shared<TStateSignalsOwner>(base, stateName);
    }

    TGuard BuildGuard(const std::optional<EState> startState) {
        return TGuard(startState);
    }
};

}   // namespace NKikimr::NOlap::NCounters
