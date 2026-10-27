#include "hullds_sst_indexhigh.h"

namespace NKikimr {

    using TRecHigh = TLogoBlobSstRecHigh;

    TTrackableVector<TRecHigh> BuildLogoBlobSstIndexHigh(const TMemoryConsumer& consumer,
            const TTrackableVector<TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>>& linearIndex) {
        TTrackableVector<TRecHigh> indexHigh{TMemoryConsumer(consumer)};
        if (linearIndex.empty()) {
            return indexHigh;
        }
        size_t highCount = 1;
        for (size_t i = 1; i < linearIndex.size(); ++i) {
            highCount += !TRecHigh(linearIndex[i].GetKey().LogoBlobID())
                .SameKey(TRecHigh(linearIndex[i - 1].GetKey().LogoBlobID()));
        }
        indexHigh.reserve(highCount);
        for (size_t i = 0; i < linearIndex.size(); ++i) {
            const TRecHigh high(linearIndex[i].GetKey().LogoBlobID());
            if (indexHigh.empty() || !high.SameKey(indexHigh.back())) {
                if (!indexHigh.empty()) {
                    indexHigh.back().LowRangeEndIndex = i;
                }
                indexHigh.push_back(high);
            }
        }
        indexHigh.back().LowRangeEndIndex = linearIndex.size();
        Y_DEBUG_ABORT_UNLESS(indexHigh.size() == highCount);
        return indexHigh;
    }

} // NKikimr
