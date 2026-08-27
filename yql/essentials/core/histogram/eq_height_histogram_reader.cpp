#include "eq_height_histogram_reader.h"

#include <yql/essentials/utils/yql_panic.h>

#include <utility>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NKikimr {

TEqHeightHistogram::TEqHeightHistogram(const TEqHeightHistogramResult& result) {
    bool isFinalizeRejected = result.ByteSizeLong() == 0;
    if (isFinalizeRejected) {
        return;
    }

    TotalCount_ = result.GetTotalCount();
    MaxRankError_ = result.GetMaxRankError();

    const int numBuckets = result.BucketsSize();
    YQL_ENSURE(numBuckets > 0, "malformed eq-height histogram result: zero buckets");
    YQL_ENSURE(result.GetNumBuckets() > 0,
               "malformed eq-height histogram result: zero NumBuckets field");
    Buckets_.reserve(numBuckets);
    for (int i = 0; i < numBuckets; ++i) {
        const auto& protoBucket = result.GetBuckets(i);
        TBucketRecord record;
        record.UpperBound = TString(protoBucket.GetUpperBound());
        record.CumulativeCount = protoBucket.GetCumulativeCount();
        YQL_ENSURE(record.CumulativeCount <= TotalCount_,
                   "malformed eq-height histogram result: cumulative count exceeds total");
        if (i > 0) {
            const TBucketRecord& prev = Buckets_.back();
            YQL_ENSURE(record.UpperBound > prev.UpperBound,
                       "malformed eq-height histogram result: buckets not strictly increasing");
            YQL_ENSURE(record.CumulativeCount > prev.CumulativeCount,
                       "malformed eq-height histogram result: cumulative count not increasing");
        }
        Buckets_.push_back(std::move(record));
    }
    // Finalize makes the last bucket's CumulativeCount equal the total.
    YQL_ENSURE(Buckets_.back().CumulativeCount == TotalCount_,
               "malformed eq-height histogram result: last cumulative count != total");
}

ui64 TEqHeightHistogram::GetTotalCount() const {
    return TotalCount_;
}

ui64 TEqHeightHistogram::GetMaxRankError() const {
    return MaxRankError_;
}

bool TEqHeightHistogram::IsExact() const {
    return MaxRankError_ == 0;
}

size_t TEqHeightHistogram::GetNumBuckets() const {
    return Buckets_.size();
}

TEqHeightHistogram::TBucket TEqHeightHistogram::GetBucket(size_t i) const {
    const auto& record = Buckets_.at(i);
    return {record.UpperBound, record.CumulativeCount};
}

ui64 TEqHeightHistogram::EstimateLessOrEqual(TStringBuf key) const {
    if (Buckets_.empty()) {
        return 0;
    }
    auto it = UpperBound(Buckets_.begin(), Buckets_.end(), key,
                         [](TStringBuf k, const TBucketRecord& record) {
                             return k < record.UpperBound;
                         });
    if (it == Buckets_.begin()) {
        return 0;
    }
    --it;
    return it->CumulativeCount;
}

} // namespace NKikimr
