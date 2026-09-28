#pragma once

#include "defs.h"

#include <ydb/core/util/actor_monotonic_time_provider.h>

#include <optional>

namespace NKikimr {

    struct TDbStatYieldPolicy {
        ui64 StepsBeforeMeasures = 100'000;
        TDuration QuantumDuration = TDuration::MilliSeconds(50);
        TDuration DelayBetweenQuanta = TDuration::MilliSeconds(100);
    };

    class TDbStatYieldChecker {
    public:
        TDbStatYieldChecker(
                std::optional<TDbStatYieldPolicy> policy,
                TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
            : Policy(std::move(policy))
            , MonotonicTimeProvider(std::move(monotonicTimeProvider))
        {
            if (Policy) {
                if (!MonotonicTimeProvider) {
                    MonotonicTimeProvider = CreateActorSystemMonotonicTimeProvider();
                }
                QuantumStart = MonotonicTimeProvider->Now();
            }
        }

        bool StepAndCheckForYield() {
            if (!Policy || !Policy->StepsBeforeMeasures || ++Steps < Policy->StepsBeforeMeasures) {
                return false;
            }

            Steps = 0;
            const TMonotonic now = MonotonicTimeProvider->Now();
            if (now - QuantumStart > Policy->QuantumDuration) {
                QuantumStart = now;
                return true;
            }
            return false;
        }

    private:
        std::optional<TDbStatYieldPolicy> Policy;
        TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> MonotonicTimeProvider;
        TMonotonic QuantumStart = TMonotonic::Zero();
        ui64 Steps = 0;
    };

    template <class TKey, class TMemRec>
    struct TDbStatYieldedState {
        // The last key fully processed by the previous quantum. TMemRec remains
        // a template parameter to keep the traversal interface tied to a DB type.
        TKey LastProcessedKey;
    };

} // namespace NKikimr
