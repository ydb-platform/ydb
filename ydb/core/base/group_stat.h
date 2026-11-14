#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/string/printf.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>

namespace NKikimr {

    static constexpr size_t NumGroupStatBuckets = 100;

    extern std::array<TDuration, NumGroupStatBuckets + 1> BucketValues;

    // latency statistics for a group
    struct TLatencyHistogram {
        static constexpr TDuration HalfFadeoutInterval = TDuration::Minutes(5);

        std::array<ui32, NumGroupStatBuckets> Buckets;
        TInstant LastFadeoutTimestamp;

        static TString GetBucketName(size_t i) {
            return Sprintf("%s..%s", BucketValues[i].ToString().data(), BucketValues[i + 1].ToString().data());
        }

        static TDuration GetBucketValue(size_t i) {
            return BucketValues[i];
        }

        static size_t GetBucket(TDuration sample) {
            const size_t index = std::upper_bound(BucketValues.begin(), BucketValues.end(), sample) - BucketValues.begin() - 1;
            return std::min(index, NumGroupStatBuckets - 1);
        }

        TLatencyHistogram() {
            Buckets.fill(0);
        }

        TLatencyHistogram(const TLatencyHistogram& other) = default;
        TLatencyHistogram &operator=(const TLatencyHistogram& other) = default;

        void Update(TDuration sample, TInstant now) {
            Fadeout(now);
            const size_t bucket = GetBucket(sample);
            ++Buckets[bucket];
        }

        void Add(const TLatencyHistogram& other) {
            for (size_t i = 0; i < NumGroupStatBuckets; ++i) {
                Buckets[i] += other.Buckets[i];
            }
        }

        void Replace(const TLatencyHistogram& current, const TLatencyHistogram& previous) {
            for (size_t i = 0; i < NumGroupStatBuckets; ++i) {
                Y_DEBUG_ABORT_UNLESS(Buckets[i] + current.Buckets[i] >= previous.Buckets[i]);
                Buckets[i] += current.Buckets[i] - previous.Buckets[i];
            }
        }

        void Subtract(const TLatencyHistogram& other) {
            for (size_t i = 0; i < NumGroupStatBuckets; ++i) {
                Y_DEBUG_ABORT_UNLESS(Buckets[i] >= other.Buckets[i]);
                Buckets[i] -= other.Buckets[i];
            }
        }

        TMaybe<TDuration> Percentile(double p) const;

        bool Fadeout(TInstant now);

        template<typename TProto>
        void Serialize(TProto *pb) const {
            pb->ClearBuckets();
            for (ui32 value : Buckets) {
                pb->AddBuckets(value);
            }
        }

        template<typename TProto>
        bool Deserialize(const TProto& pb) {
            if (pb.BucketsSize() != NumGroupStatBuckets) {
                return false;
            }
            size_t i = 0;
            for (ui32 value : pb.GetBuckets()) {
                Buckets[i++] = value;
            }
            return true;
        }
    };

    class TGroupStat {
        TLatencyHistogram PutTabletLog;
        TLatencyHistogram PutUserData;
        TLatencyHistogram GetFast;

    public:
        enum class EKind {
            PUT_TABLET_LOG,
            PUT_USER_DATA,
            GET_FAST,
        };

    public:
        bool Fadeout(TInstant now);

        void Add(const TGroupStat& other) {
            PutTabletLog.Add(other.PutTabletLog);
            PutUserData.Add(other.PutUserData);
            GetFast.Add(other.GetFast);
        }

        void Replace(const TGroupStat& current, const TGroupStat& previous) {
            PutTabletLog.Replace(current.PutTabletLog, previous.PutTabletLog);
            PutUserData.Replace(current.PutUserData, previous.PutUserData);
            GetFast.Replace(current.GetFast, previous.GetFast);
        }

        void Subtract(const TGroupStat& other) {
            PutTabletLog.Subtract(other.PutTabletLog);
            PutUserData.Subtract(other.PutUserData);
            GetFast.Subtract(other.GetFast);
        }

        void Update(EKind kind, TDuration sample, TInstant now) {
            TLatencyHistogram& histogram = GetLatencyHistogram(kind);
            histogram.Update(sample, now);
        }

        template<typename TProto>
        void Serialize(TProto *pb) const {
            PutTabletLog.Serialize(pb->MutablePutTabletLog());
            PutUserData.Serialize(pb->MutablePutUserData());
            GetFast.Serialize(pb->MutableGetFast());
        }

        template<typename TProto>
        bool Deserialize(const TProto& pb) {
            return PutTabletLog.Deserialize(pb.GetPutTabletLog()) &&
                PutUserData.Deserialize(pb.GetPutUserData()) &&
                GetFast.Deserialize(pb.GetGetFast());
        }

        ui32 GetItem(EKind kind, size_t bucket) const {
            return GetLatencyHistogram(kind).Buckets[bucket];
        }

        TMaybe<TDuration> GetPercentile(EKind kind, double p) const {
            return GetLatencyHistogram(kind).Percentile(p);
        }

    private:
        const TLatencyHistogram& GetLatencyHistogram(EKind kind) const {
            return const_cast<TGroupStat&>(*this).GetLatencyHistogram(kind);
        }

        TLatencyHistogram& GetLatencyHistogram(EKind kind) {
            switch (kind) {
                case EKind::PUT_TABLET_LOG:
                    return PutTabletLog;
                case EKind::PUT_USER_DATA:
                    return PutUserData;
                case EKind::GET_FAST:
                    return GetFast;
            }
            Y_ABORT("unexpected TGroupStat::EKind value");
        }
    };

} // NKikimr
