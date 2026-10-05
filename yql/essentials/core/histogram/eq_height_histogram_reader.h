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
// The object owns the bucket key strings. Estimate* returns a lower bound on
// the number of rows matching the predicate and does not interpolate inside a
// bucket.
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
    ui64 EstimateLess(TStringBuf key) const;
    ui64 EstimateGreaterOrEqual(TStringBuf key) const;
    ui64 EstimateGreater(TStringBuf key) const;
    ui64 EstimateEqual(TStringBuf key) const;

    ui64 EstimateRangeGreaterLess(TStringBuf left, TStringBuf right) const;
    ui64 EstimateRangeGreaterLessOrEqual(TStringBuf left, TStringBuf right) const;
    ui64 EstimateRangeGreaterOrEqualLess(TStringBuf left, TStringBuf right) const;
    ui64 EstimateRangeGreaterOrEqualLessOrEqual(TStringBuf left, TStringBuf right) const;

private:
    struct TBucketRecord {
        TString UpperBound;
        ui64 CumulativeCount = 0;
    };

    struct TBound {
        ui64 Less = 0;
        ui64 LessOrEqual = 0;
    };

    TBound FindBound(TStringBuf key) const;

    TVector<TBucketRecord> Buckets_;
    ui64 TotalCount_ = 0;
    ui64 MaxRankError_ = 0;
};

} // namespace NKikimr
