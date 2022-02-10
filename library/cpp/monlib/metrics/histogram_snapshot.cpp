#include "histogram_snapshot.h" 
 
#include <util/stream/output.h>

#include <iostream>


namespace NMonitoring { 
 
    IHistogramSnapshotPtr ExplicitHistogramSnapshot(TConstArrayRef<TBucketBound> bounds, TConstArrayRef<TBucketValue> values) {
        Y_ENSURE(bounds.size() == values.size(),
                 "mismatched sizes: bounds(" << bounds.size() <<
                 ") != buckets(" << values.size() << ')');
 
        auto snapshot = TExplicitHistogramSnapshot::New(bounds.size());
 
        for (size_t i = 0; i != bounds.size(); ++i) {
            (*snapshot)[i].first = bounds[i];
            (*snapshot)[i].second = values[i];
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
