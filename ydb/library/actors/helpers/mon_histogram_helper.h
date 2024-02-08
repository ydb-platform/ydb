#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/cast.h>

namespace NActors {
    namespace NMon {
        class THistogramCounterHelper {
        public:
            THistogramCounterHelper()
                : FirstBucketVal(0)
                , BucketCount(0)
            {
            }

            THistogramCounterHelper(const THistogramCounterHelper&) = default;

            void Init(NMonitoring::TDynamicCounters* group, const TString& baseName, const TString& unit,
                ui64 firstBucket, ui64 bucketCnt, bool useSensorLabelName = true)
            {
                Y_ASSERT(FirstBucketVal == 0);
                Y_ASSERT(BucketCount == 0);

                FirstBucketVal = firstBucket;
                BucketCount = bucketCnt;
                BucketsHolder.reserve(BucketCount);
                Buckets.reserve(BucketCount);
                for (size_t i = 0; i < BucketCount; ++i) {
                    TString bucketName = GetBucketName(i) + " " + unit;
                    auto labelName = useSensorLabelName ? "sensor" : "name";
                    BucketsHolder.push_back(group->GetSubgroup(labelName, baseName)->GetNamedCounter("range", bucketName, true));
                    Buckets.push_back(BucketsHolder.back().Get());
                }
            }

            void Add(ui64 val) {
                Y_ASSERT(FirstBucketVal != 0);
                Y_ASSERT(BucketCount != 0);
                Y_ABORT_UNLESS(val <= (1ULL << 63ULL));
                size_t ind = 0;
                if (val > FirstBucketVal) {
                    ind = GetValueBitCount((2 * val - 1) / FirstBucketVal) - 1;
                    if (ind >= BucketCount) {
                        ind = BucketCount - 1;
                    }
                }
                Buckets[ind]->Inc();
            }

            ui64 GetBucketCount() const {
                return BucketCount;
            }

            ui64 GetBucketValue(size_t index) const {
                Y_ASSERT(index < BucketCount);
                return Buckets[index]->Val();
            }

            void SetBucketValue(ui64 index, ui64 value) {
                Y_ASSERT(index < BucketCount);
                *Buckets[index] = value;
            }

        private:
            TString GetBucketName(size_t ind) const {
                Y_ASSERT(FirstBucketVal != 0);
                Y_ASSERT(BucketCount != 0);
                Y_ASSERT(ind < BucketCount);
                if (ind + 1 < BucketCount) {
                    return ToString<ui64>(FirstBucketVal << ind);
                } else {
                    // Last slot is up to +INF
                    return "INF";
                }
            }

        private:
            ui64 FirstBucketVal;
            ui64 BucketCount;
            TVector<NMonitoring::TDynamicCounters::TCounterPtr> BucketsHolder;
            TVector<NMonitoring::TDeprecatedCounter*> Buckets;
        };

    }
}
