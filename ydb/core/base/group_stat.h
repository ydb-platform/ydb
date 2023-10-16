#pragma once

#include "defs.h"

#include <util/datetime/base.h>

#include <array>
#include <cmath>

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

        TMaybe<TDuration> Percentile(double p) const {
            std::array<ui64, NumGroupStatBuckets + 1> accum;
            ui64 counter = 0;
            for (size_t i = 0;; ++i) {
                accum[i] = counter;
                if (i < NumGroupStatBuckets) {
                    counter += Buckets[i];
                } else {
                    break;
                }
            }
            if (!counter) {
                return {};
            }

            // calculate the quantile value
            const double quantile = counter * p;

            // find it in the buckets array
            size_t index = std::upper_bound(accum.begin(), accum.end(), quantile) - accum.begin();
            Y_ABORT_UNLESS(index > 0 && index <= NumGroupStatBuckets);

            const double w = (quantile - accum[index - 1]) / (accum[index] - accum[index - 1]);
            return TDuration::FromValue(GetBucketValue(index - 1).GetValue() * (1 - w) + GetBucketValue(index).GetValue() * w);
        }

        bool Fadeout(TInstant now) {
            if (LastFadeoutTimestamp != TInstant::Zero()) {
                const TDuration elapsed = now - LastFadeoutTimestamp;

                // number of full half-fadeout intervals
                const size_t shift = elapsed.GetValue() / HalfFadeoutInterval.GetValue();

                // number of ticks inside last full half-fadeout interval
                const ui64 subshift = elapsed.GetValue() % HalfFadeoutInterval.GetValue();

                // scale factors are calculated as 2**20 * pow(0.5, i / 256.0)
                static const ui64 factor[] = {
                    1048576, 1045740, 1042913, 1040093, 1037280, 1034476, 1031678, 1028889, 1026107, 1023332, 1020565,
                    1017806, 1015053, 1012309, 1009572, 1006842, 1004119, 1001404, 998696, 995996, 993303, 990617,
                    987939, 985267, 982603, 979946, 977296, 974654, 972019, 969390, 966769, 964155, 961548, 958948,
                    956355, 953769, 951190, 948618, 946053, 943495, 940944, 938400, 935862, 933332, 930808, 928291,
                    925781, 923278, 920781, 918292, 915809, 913332, 910863, 908400, 905944, 903494, 901051, 898615,
                    896185, 893762, 891345, 888935, 886531, 884134, 881743, 879359, 876981, 874610, 872245, 869887,
                    867535, 865189, 862849, 860516, 858189, 855869, 853555, 851247, 848945, 846650, 844360, 842077,
                    839800, 837529, 835265, 833006, 830754, 828508, 826267, 824033, 821805, 819583, 817367, 815157,
                    812953, 810754, 808562, 806376, 804195, 802021, 799852, 797690, 795533, 793382, 791236, 789097,
                    786963, 784835, 782713, 780597, 778486, 776381, 774282, 772188, 770100, 768018, 765941, 763870,
                    761805, 759745, 757690, 755642, 753598, 751561, 749529, 747502, 745481, 743465, 741455, 739450,
                    737450, 735456, 733468, 731484, 729507, 727534, 725567, 723605, 721648, 719697, 717751, 715810,
                    713875, 711944, 710019, 708100, 706185, 704275, 702371, 700472, 698578, 696689, 694805, 692926,
                    691053, 689184, 687321, 685462, 683609, 681760, 679917, 678078, 676245, 674416, 672593, 670774,
                    668960, 667152, 665348, 663549, 661754, 659965, 658181, 656401, 654626, 652856, 651091, 649330,
                    647574, 645823, 644077, 642336, 640599, 638867, 637139, 635416, 633698, 631985, 630276, 628572,
                    626872, 625177, 623487, 621801, 620119, 618443, 616770, 615103, 613439, 611781, 610126, 608477,
                    606831, 605191, 603554, 601922, 600295, 598671, 597053, 595438, 593828, 592223, 590621, 589024,
                    587432, 585843, 584259, 582679, 581104, 579532, 577965, 576403, 574844, 573290, 571740, 570194,
                    568652, 567114, 565581, 564052, 562526, 561005, 559488, 557976, 556467, 554962, 553462, 551965,
                    550473, 548984, 547500, 546019, 544543, 543070, 541602, 540138, 538677, 537221, 535768, 534319,
                    532874, 531434, 529997, 528564, 527134, 525709
                };

                // index into scale factor array
                const size_t numScaleFactors = sizeof(factor) / sizeof(factor[0]);
                size_t index = subshift * numScaleFactors / HalfFadeoutInterval.GetValue();

                ui32 numNonzeroItems = 0;

                if (shift + 20 < 64) {
                    for (ui32& item : Buckets) {
                        item = ui64(item) * factor[index] >> (shift + 20);
                        numNonzeroItems += !!item;
                    }
                } else {
                    std::fill(Buckets.begin(), Buckets.end(), 0);
                }

                // adjust timestamp to keep fading precision
                LastFadeoutTimestamp += TDuration::FromValue((numScaleFactors * shift + index) * HalfFadeoutInterval.GetValue() / numScaleFactors);

                return numNonzeroItems;
            } else {
                LastFadeoutTimestamp = now;
                for (ui32 value : Buckets) {
                    if (value) {
                        return true;
                    }
                }
                return false;
            }
        }

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
        bool Fadeout(TInstant now) {
            const bool x1 = PutTabletLog.Fadeout(now);
            const bool x2 = PutUserData.Fadeout(now);
            const bool x3 = GetFast.Fadeout(now);
            return x1 || x2 || x3;
        }

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
