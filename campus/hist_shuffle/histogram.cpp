#include "histogram.h"

#include <algorithm>
#include <cassert>
#include <queue>
#include <unordered_map>

struct pair_hash
{
    std::size_t operator() (const std::pair<ui32, ui32> &pair) const
    {
        return pair.first << 32 + pair.second;
    }
};

struct Border {
    i64 value;
    bool open;
    ui32 sourceIndex;
    ui32 bucketIndex;
    const Bucket* source;
};

Histogram<FreqBucket> multiMerge(const std::vector<Histogram<Bucket>>& sources) {
    using bucketKey = std::pair<ui32, ui32>;

    Histogram<FreqBucket> result;

    std::unordered_map<bucketKey, const Bucket*, pair_hash> currentBuckets;
    auto cmp = [](const Border& left, const Border& right) {
        if (left.value == right.value) {
            if (left.open && right.open) return false;
            return left.open;
        }
        return left.value > right.value;
    };
    std::priority_queue<Border, std::vector<Border>, decltype(cmp)> pq;
    int sourceI = 0;
    for (const auto& hist : sources) {
        int bucketI = 0;
        for (auto& bucket : hist.buckets) {
            pq.emplace(bucket.left, true, sourceI, bucketI, &bucket);
            pq.emplace(bucket.right, false, sourceI, bucketI, &bucket);
            bucketI++;
        }
        sourceI++;
    }

    Border currBorder = pq.top();
    pq.pop();
    //YQL_ENSURE(currBorder.open);

    i64 currLine = currBorder.value;
    currentBuckets.emplace(std::make_pair(currBorder.sourceIndex, currBorder.bucketIndex), currBorder.source);
    while (!pq.empty() && pq.top().value == currLine) {
        //YQL_ENSURE(pq.top().open); // maybe just skip
        Border border = pq.top();
        pq.pop();
        currentBuckets.emplace(std::make_pair(border.sourceIndex, border.bucketIndex), border.source);
    }

    while (!pq.empty()) {
        // nextline should not be equal to curr, aggregate everything
        i64 nextLine = pq.top().value;

        // calculate new interval for current buckets
        double count = 0;
        for (const auto& [k, bucket] : currentBuckets) {
            ui64 len = bucket->right - bucket->left;
            ui64 range = nextLine - currLine;
            count += static_cast<double>(bucket->count) * range / len; // approx
        }

        result.buckets.emplace_back(currLine, nextLine, count);

        // get all closing borders on the line, remove from current
        while(!pq.empty() && pq.top().value == nextLine && !pq.top().open) {
            Border closing = pq.top();
            currentBuckets.erase(std::make_pair(closing.sourceIndex, closing.bucketIndex));
            pq.pop();
        }

        // get all opening borders on the line, add to curr
        while(!pq.empty() && pq.top().value == nextLine && pq.top().open) {
            Border opening = pq.top();
            currentBuckets.emplace(std::make_pair(opening.sourceIndex, opening.bucketIndex), opening.source);
            pq.pop();
        }

        currLine = nextLine;
    }

    return result;
}


static BinArray ffd(const std::vector<const FreqBucket*> &sortedBuckets, ui64 bin_size) {
    BinArray result;
    result.push_back(Bin {});

    std::cerr << "run ffd with size " << bin_size << "\n";
    
    for (const FreqBucket* bucket : sortedBuckets) {
        ui64 value = static_cast<ui64>(bucket->count);
        if (value > bin_size) {
            std::cerr << "bin size " << bin_size << " smaller than value " << value << "\n";
            assert(value <= bin_size);
        }
        size_t i = 0;
        for ( ; i < result.size(); i++) {
            if (result[i].sum + value <= bin_size) {
                result[i].buckets.push_back(bucket);
                result[i].sum += value;
                break;
            }
        }
        // didn't find bin, create new
        if (i == result.size()) {
            result.push_back(Bin { std::vector {bucket}, value });
        }
    }

    return result;
}

BinArray multifit(const Histogram<FreqBucket>& hist, ui32 partitionsNum) {
    ui64 sum = 0;
    ui64 max = static_cast<ui64>(hist.buckets[0].count);

    std::vector<const FreqBucket*> sorted;
    sorted.reserve(hist.buckets.size());
    for (const auto& bucket : hist.buckets) {
        sorted.push_back(&bucket);
        sum += static_cast<ui64>(bucket.count);
        if (static_cast<ui64>(bucket.count) > max) {
            max = static_cast<ui64>(bucket.count);
        }
    }

    std::sort(sorted.begin(), sorted.end(), [](const FreqBucket* left, const FreqBucket* right) {return left->count > right->count;});

    ui64 lower_size = std::max(sum / partitionsNum, max);
    ui64 upper_size = std::max(2 * sum / partitionsNum, max);

    std::cerr << "lower " << lower_size << " upper " << upper_size << "\n";

    for (int i = 0; i < 10; i++) {
        ui64 bin_size = (lower_size + upper_size) / 2;
        auto first_fit = ffd(sorted, bin_size);
        auto ffd_bin_count = first_fit.size();
        if (ffd_bin_count <= partitionsNum) {
            upper_size = bin_size;
        } else {
            lower_size = bin_size;
        }
    }
    
    return ffd(sorted, upper_size);
}

