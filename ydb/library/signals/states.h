#pragma once
#include "owner.h"

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/serialized_enum.h>

#include <map>
#include <set>

namespace NKikimr::NOlap::NCounters {

template <class EState>
class TStateSignalsOwner: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> StateVolume;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> StateAdd;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> StateRemove;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> StateDuration;

    void AddCountersForGroup(TCommonCountersOwner& group) {
        StateVolume.emplace_back(group.GetValue("Count"));
        StateAdd.emplace_back(group.GetDeriviative("Add"));
        StateRemove.emplace_back(group.GetDeriviative("Remove"));
        StateDuration.emplace_back(group.GetDeriviative("Duration/Us"));
    }

public:
    void ExchangeState(const std::optional<EState> from, const std::optional<EState> to, const std::optional<TDuration> d, const ui32 size = 1) {
        if (from) {
            AFL_VERIFY((ui32)*from < StateVolume.size())("from", from)("size", StateVolume.size());
            StateVolume[(ui32)*from]->Sub(size);
            StateRemove[(ui32)*from]->Add(size);
            if (d) {
                StateDuration->Add(d->MicroSeconds());
            }
        }
        if (to) {
            AFL_VERIFY((ui32)*to < StateVolume.size())("to", to)("size", StateVolume.size());
            StateVolume[(ui32)*to]->Add(size);
            StateAdd[(ui32)*to]->Add(size);
        }
    }

    TStateSignalsOwner(TCommonCountersOwner& base, const TString& stateName)
        : TBase(base, "states", stateName) {
        auto undefinedGroup = TBase::CreateSubGroup("state_id", "UNDEFINED");
        for (auto&& i : GetEnumAllValues<EState>()) {
            while (StateVolume.size() < (ui32)i) {
                AddCountersForGroup(undefinedGroup);
            }
            AddCountersForGroup(TBase::CreateSubGroup("state_id", ::ToString(i)));
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

        TGuard(const std::optional<EState> start) {
            SetState(state);
        }

        ~TGuard() {
            SetState(std::nullopt);
        }
    };

    TStateSignalsOperator(NColumnShard::TCommonCountersOwner& base, const TString& stateName) {
        Signals = std::make_shared<TStateSignalsOwner<EState>>(base, stateName);
    }

    TGuard BuildGuard(const std::optional<EState> startState) {
        return TGuard(startState);
    }
};

}   // namespace NKikimr::NOlap::NCounters
