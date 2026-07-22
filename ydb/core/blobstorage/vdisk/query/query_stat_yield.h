#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

#include <optional>
#include <variant>

namespace NKikimr {

    struct TDbStatYieldPolicy {
        const ui64 StepsBeforeMeasures = 100'000;
        const TDuration QuantDuration = TDuration::MilliSeconds(50);
        const TDuration DelayBetweenQuants = TDuration::MilliSeconds(100);
    };

    struct TDbStatYieldChecker {
        TDbStatYieldChecker(std::optional<TDbStatYieldPolicy> policy)
            : Policy(std::move(policy))
            , PhaseStart(TActivationContext::Monotonic())
        {}

        std::optional<TDbStatYieldPolicy> Policy;
        TMonotonic PhaseStart;

        ui64 Step = 0;

        bool StepAndCheckForYield() {
            ++Step;
            if (Policy && Policy->StepsBeforeMeasures && Step >= Policy->StepsBeforeMeasures) {
                Step = 0;
                if (TActivationContext::Monotonic() - PhaseStart > Policy->QuantDuration) {
                    PhaseStart = TActivationContext::Monotonic();
                    return true;
                }
            }
            return false;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TDbStatYeildedState
    // Snapshot of the traversal position, that allows to resume DB traversal
    // after a yield. A yield releases and later re-captures the Hull snapshot,
    // so iterators become invalid across yields. Therefore we only remember the
    // key to continue the processing from (and the phase of the traversal), and
    // re-seek into the freshly captured snapshot on resume.
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    struct TDbStatYeildedState {
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

        // Position inside the level (SST) part: the next key to process
        struct TLevelPosition {
            TKey Key;
        };

        std::variant<TFreshPosition, TLevelPosition> Position;
    };

} // NKikimr
