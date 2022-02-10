#pragma once
#include "defs.h"

#include <util/generic/ptr.h>

namespace NKikimr {
    namespace NBackpressure {
        class TFlowRecord : public TThrRefBase {
        protected:
            TAtomic PredictedDelayNs = 0;

        public:
            inline void SetPredictedDelayNs(TAtomicBase value) {
                AtomicSet(PredictedDelayNs, value);
            }

            inline TAtomicBase GetPredictedDelayNs() {
                return AtomicGet(PredictedDelayNs);
            }

            // TODO(cthulhu): Improve the prediction, add parameters.
            // TAtomic PredictedSpeedBytesPerSecond;
        };

    } // NBackpressure
} // NKikimr
