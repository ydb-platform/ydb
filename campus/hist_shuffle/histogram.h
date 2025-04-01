#pragma once

#include <util/system/types.h>
#include <vector>
#include <iostream>


struct Bucket {
    i64 left;
    i64 right;
    ui64 count;
};

struct FreqBucket {
    i64 left;
    i64 right;
    double count;
};

template<class TBucket>
struct Histogram {
    std::vector<TBucket> buckets;
};

Histogram<FreqBucket> multiMerge(const std::vector<Histogram<Bucket>>& sources);

struct Partition {
    i64 left;
    i64 right;
    i32 out;
};

struct Bin {
    std::vector<const FreqBucket*> buckets;
    ui64 sum;
};

using BinArray = std::vector<Bin>;

BinArray multifit(const Histogram<FreqBucket>& hist, ui32 partitionsNum);

template<class TBucket>
inline std::ostream& operator<<(std::ostream& out, const Histogram<TBucket>& hist) {
    out << "{ ";
    size_t sz = hist.buckets.size();
    for (size_t i = 0; i < sz; i++) {
        const TBucket& b = hist.buckets[i];
        out << "[" << b.left << " (" << b.count << ") " << b.right << "]";
        if (i != sz - 1) out << ", ";
    }
    out << "}";
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const BinArray& arr) {
    out << "{ ";
    size_t sz = arr.size();
    for (size_t i = 0; i < sz; i++) {
        const Bin& b = arr[i];
        out << "[" << b.sum << "]";
        if (i != sz - 1) out << ", ";
    }
    out << "}";
    return out;
}