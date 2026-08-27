#pragma once

#include <yql/essentials/core/histogram/proto/eq_height_histogram.pb.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr {

// Parsed finalized equi-height histogram (the result from Finalize()). Keys are
// memcomparable byte strings, same as TEqHeightHistogramBuilder: callers encode
// them with NMiniKQL::TPresortEncoder.
//
// The object owns the bucket key strings. EstimateLessOrEqual returns a lower
// bound on the number of rows with key <= the argument and does not interpolate
// inside a bucket.
class TEqHeightHistogram {
public:
    struct TBucket {
        TStringBuf UpperBound;    // last key in this bucket
        ui64 CumulativeCount = 0; // rows with key <= UpperBound
    };

    explicit TEqHeightHistogram(const TEqHeightHistogramResult& result);

    ui64 GetTotalCount() const;
    ui64 GetMaxRankError() const;
    bool IsExact() const;
    size_t GetNumBuckets() const;
    TBucket GetBucket(size_t i) const;
    ui64 EstimateLessOrEqual(TStringBuf key) const;

private:
    struct TBucketRecord {
        TString UpperBound;
        ui64 CumulativeCount = 0;
    };

    TVector<TBucketRecord> Buckets_;
    ui64 TotalCount_ = 0;
    ui64 MaxRankError_ = 0;
};

} // namespace NKikimr
