#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/util/actor_monotonic_time_provider.h>

#include <optional>
#include <variant>

namespace NKikimr {

    struct TDbStatYieldPolicy {
        const ui64 StepsBeforeMeasures = 100'000;
        const TDuration QuantumDuration = TDuration::MilliSeconds(50);
        const TDuration DelayBetweenQuanta = TDuration::MilliSeconds(100);
    };

    struct TDbStatYieldChecker {
        TDbStatYieldChecker(
                std::optional<TDbStatYieldPolicy> policy,
                TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> monotonicTimeProvider = {})
            : Policy(std::move(policy))
            , MonotonicTimeProvider(std::move(monotonicTimeProvider))
            , QuantumStart(TMonotonic::Zero())
        {
            if (Policy) {
                if (!MonotonicTimeProvider) {
                    MonotonicTimeProvider = CreateActorSystemMonotonicTimeProvider();
                }
                QuantumStart = MonotonicTimeProvider->Now();
            }
        }

        std::optional<TDbStatYieldPolicy> Policy;
        TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> MonotonicTimeProvider;
        TMonotonic QuantumStart;

        ui64 Step = 0;

        bool StepAndCheckForYield() {
            ++Step;
            if (Policy && Policy->StepsBeforeMeasures && Step >= Policy->StepsBeforeMeasures) {
                Step = 0;
                const TMonotonic now = MonotonicTimeProvider->Now();
                if (now - QuantumStart > Policy->QuantumDuration) {
                    QuantumStart = now;
                    return true;
                }
            }
            return false;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TDbStatYieldedState
    // Snapshot of the traversal position, that allows to resume DB traversal
    // after a yield. A yield releases and later re-captures the Hull snapshot,
    // so iterators become invalid across yields. Therefore we only remember the
    // key to continue the processing from (and the phase of the traversal), and
    // re-seek into the freshly captured snapshot on resume.
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TDbStatYieldedState {
        enum class EFreshSegment {
            Cur = 0,
            Dreg,
            Old,
        };

        // Position inside the fresh part: which segment and the next key to process
        struct TFreshPosition {
            EFreshSegment Segment;
            TKey Key;
        };

        // Position inside the level (SST) part.
        struct TLevelPosition {
            using TUnsortedLevelDiscriminator = ui64;
            using TSortedLevelDiscriminator = TKey;

            // Pair (Level, Discriminator) unambiguously specifies the processed SST
            ui32 Level = 0;
            std::variant<TUnsortedLevelDiscriminator, TSortedLevelDiscriminator> Discriminator;
            TKey Key;
        };

        std::variant<TFreshPosition, TLevelPosition> Position;
    };

} // NKikimr
