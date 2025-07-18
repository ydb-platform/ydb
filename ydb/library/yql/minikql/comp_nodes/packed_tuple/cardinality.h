#pragma once

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/generic/buffer.h>
#include <limits>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

class CardinalityEstimator {
public:
    CardinalityEstimator(ui64 buckets = 1)
        : Buckets_(buckets)
    {
    }

    template <typename HashT>
    ui64 Estimate(
        ui64 lhsTuplesCount,
        const TVector<HashT>& lhsHashes,
        ui64 rhsTuplesCount,
        const TVector<HashT>& rhsHashes, // r - build, l - probe
        ui64 step = 100)
    {
        // Fill histograms
        TVector<THashMap<HashT, ui64>> lhsHist(Buckets_);
        TVector<THashMap<HashT, ui64>> rhsHist(Buckets_);

        for (auto hash: lhsHashes) {
            lhsHist[hash % lhsHist.size()][hash]++;
        }

        for (auto hash: rhsHashes) {
            rhsHist[hash % rhsHist.size()][hash]++;
        }

        auto estimateV = [step](const auto& bucket, ui64 tuplesCountPerBucket) {
            // Estimate V(R, a), i.e. number of distinct values
            ui64 v = 0;
            // Heuristic #1: if we found few distinct values, then V(R, a) == number of distinct values
            if (step * 1000 /* treshold */ * bucket.size() < tuplesCountPerBucket) {
                v = bucket.size();
            } else {
                // Heuristic #2: else V(R, a) is close to tuplesCountPerBucket
                ui64 maxCnt = 1;
                for (auto [_, cnt]: bucket) {
                    maxCnt = std::max<ui64>(maxCnt, cnt);
                }
                v = tuplesCountPerBucket / maxCnt; // assume uniform distr
            }
            return v;
        };

        auto estimateSelectivity = [step](const auto* lBucket, const auto* rBucket) {
            if (lBucket->size() < rBucket->size()) {
                std::swap(lBucket, rBucket);
            }

            auto total = 0;
            auto lMatched = 0;
            for (auto [hash, cnt]: *lBucket) {
                if (rBucket->count(hash)) {
                    lMatched += cnt;
                }
                total += cnt;
            }

            if (!lMatched) {
                return std::numeric_limits<int>::max();
            }
            return std::max<int>(total / (step * lMatched), 1); // 1/(step * selectivity) is probability to has match in FK-PK join
        };

        ui64 result = 0;
        for (size_t i = 0; i < Buckets_; ++i) {
            auto& lBucket = lhsHist[i];
            auto& rBucket = rhsHist[i];
            if (lBucket.empty() || rBucket.empty()) {
                continue;
            }

            auto lTuples = lhsTuplesCount / lhsHist.size();
            auto rTuples = rhsTuplesCount / rhsHist.size();
            auto lV = estimateV(lBucket, lTuples);
            auto rV = estimateV(rBucket, rTuples);

            auto card = lTuples * rTuples / (std::max<ui64>(std::max<ui64>(lV, rV), 1U));
            auto ratio = std::min<ui64>(estimateSelectivity(&lBucket, &rBucket), 20); // assume that min selectivity is 5%, so 5% = 1/20

            result += card / ratio;
        }
        if (result == 0) {
            result = 1;
        }

        return result;
    }

private:
    ui64 Buckets_;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
