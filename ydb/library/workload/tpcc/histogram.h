#pragma once

#include <vector>
#include <cstdint>
#include <algorithm>

namespace NYdb::NTPCC {

class THistogram {
public:
    using uint64_t = std::uint64_t;

    // [0; 1), [1; 2), ... [hdrTill - 1; hdrTill), [hdrTill; hdrTill * 2), [hdrTill * 2;), [maxValue; +inf)
    THistogram(uint64_t hdrTill, uint64_t maxValue);

    void RecordValue(uint64_t value);
    void Add(const THistogram& other);
    void Sub(const THistogram& other);
    uint64_t GetValueAtPercentile(double percentile) const;
    void Reset();

private:
    size_t GetBucketIndex(uint64_t value) const;
    uint64_t GetBucketUpperBound(size_t bucketIndex) const;
    size_t GetTotalBuckets() const;
    size_t GetExponentialBucketCount() const;

private:
    uint64_t HdrTill_;
    uint64_t MaxValue_;
    std::vector<uint64_t> Buckets_;
    uint64_t TotalCount_;
    uint64_t MaxRecordedValue_;  // Track the maximum recorded value
};

} // namespace NYdb::NTPCC
