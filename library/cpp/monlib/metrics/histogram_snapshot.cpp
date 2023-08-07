#include "histogram_snapshot.h"

#include <util/stream/output.h>

#include <iostream>


namespace NMonitoring {

    IHistogramSnapshotPtr ExplicitHistogramSnapshot(TConstArrayRef<TBucketBound> bounds, TConstArrayRef<TBucketValue> values, bool shrinkBuckets) {
        Y_ENSURE(bounds.size() == values.size(),
                 "mismatched sizes: bounds(" << bounds.size() <<
                 ") != buckets(" << values.size() << ')');

        size_t requiredSize = shrinkBuckets ? std::min(bounds.size(), static_cast<size_t>(HISTOGRAM_MAX_BUCKETS_COUNT)) : bounds.size();
        auto snapshot = TExplicitHistogramSnapshot::New(requiredSize);
        if (requiredSize < bounds.size()) {
            auto remains = bounds.size() % requiredSize;
            auto divided = bounds.size() / requiredSize;
            size_t idx{bounds.size()};

            for (size_t i = requiredSize; i > 0; --i) {
                Y_ENSURE(idx > 0);
                (*snapshot)[i - 1].first = bounds[idx - 1];
                (*snapshot)[i - 1].second = 0;

                auto repeat = divided;
                if (remains > 0) {
                    ++repeat;
                    --remains;
                }
                for (; repeat > 0; --repeat) {
                    Y_ENSURE(idx > 0);
                    (*snapshot)[i - 1].second += values[idx - 1];
                    --idx;
                }
            }
        } else {
            for (size_t i = 0; i != bounds.size(); ++i) {
                (*snapshot)[i].first = bounds[i];
                (*snapshot)[i].second = values[i];
            }
        }

        return snapshot;
    }

} // namespace NMonitoring

namespace {

template <typename TStream>
auto& Output(TStream& os, const NMonitoring::IHistogramSnapshot& hist) {
    os << TStringBuf("{");

    ui32 i = 0;
    ui32 count = hist.Count();

    if (count > 0) {
        for (; i < count - 1; ++i) {
            os << hist.UpperBound(i) << TStringBuf(": ") << hist.Value(i);
            os << TStringBuf(", ");
        }

        if (hist.UpperBound(i) == Max<NMonitoring::TBucketBound>()) {
            os << TStringBuf("inf: ") << hist.Value(i);
        } else {
            os << hist.UpperBound(i) << TStringBuf(": ") << hist.Value(i);
        }
    }

    os << TStringBuf("}");

    return os;
}

} // namespace

std::ostream& operator<<(std::ostream& os, const NMonitoring::IHistogramSnapshot& hist) {
    return Output(os, hist);
}

template <>
void Out<NMonitoring::IHistogramSnapshot>(IOutputStream& os, const NMonitoring::IHistogramSnapshot& hist) {
    Output(os, hist);
}
