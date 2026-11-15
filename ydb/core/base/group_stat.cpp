#include "group_stat.h"

namespace NKikimr {

    std::array<TDuration, NumGroupStatBuckets + 1> BucketValues{
        TDuration::Zero(),
        TDuration::MicroSeconds(250),
        TDuration::MicroSeconds(500),
        TDuration::MicroSeconds(750),
        TDuration::MilliSeconds(1),
        TDuration::MilliSeconds(2),
        TDuration::MilliSeconds(3),
        TDuration::MilliSeconds(4),
        TDuration::MilliSeconds(5),
        TDuration::MilliSeconds(6),
        TDuration::MilliSeconds(7),
        TDuration::MilliSeconds(8),
        TDuration::MilliSeconds(9),
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(12),
        TDuration::MilliSeconds(14),
        TDuration::MilliSeconds(16),
        TDuration::MilliSeconds(18),
        TDuration::MilliSeconds(20),
        TDuration::MilliSeconds(24),
        TDuration::MilliSeconds(28),
        TDuration::MilliSeconds(32),
        TDuration::MilliSeconds(36),
        TDuration::MilliSeconds(40),
        TDuration::MilliSeconds(45),
        TDuration::MilliSeconds(50),
        TDuration::MilliSeconds(55),
        TDuration::MilliSeconds(60),
        TDuration::MilliSeconds(65),
        TDuration::MilliSeconds(70),
        TDuration::MilliSeconds(75),
        TDuration::MilliSeconds(80),
        TDuration::MilliSeconds(85),
        TDuration::MilliSeconds(90),
        TDuration::MilliSeconds(100),
        TDuration::MilliSeconds(120),
        TDuration::MilliSeconds(140),
        TDuration::MilliSeconds(160),
        TDuration::MilliSeconds(180),
        TDuration::MilliSeconds(200),
        TDuration::MilliSeconds(220),
        TDuration::MilliSeconds(240),
        TDuration::MilliSeconds(260),
        TDuration::MilliSeconds(280),
        TDuration::MilliSeconds(300),
        TDuration::MilliSeconds(320),
        TDuration::MilliSeconds(340),
        TDuration::MilliSeconds(360),
        TDuration::MilliSeconds(380),
        TDuration::MilliSeconds(400),
        TDuration::MilliSeconds(450),
        TDuration::MilliSeconds(500),
        TDuration::MilliSeconds(550),
        TDuration::MilliSeconds(600),
        TDuration::MilliSeconds(650),
        TDuration::MilliSeconds(700),
        TDuration::MilliSeconds(750),
        TDuration::MilliSeconds(800),
        TDuration::MilliSeconds(850),
        TDuration::MilliSeconds(900),
        TDuration::MilliSeconds(950),
        TDuration::MilliSeconds(1000),
        TDuration::MilliSeconds(1100),
        TDuration::MilliSeconds(1200),
        TDuration::MilliSeconds(1300),
        TDuration::MilliSeconds(1400),
        TDuration::MilliSeconds(1500),
        TDuration::MilliSeconds(1600),
        TDuration::MilliSeconds(1700),
        TDuration::MilliSeconds(1800),
        TDuration::MilliSeconds(1900),
        TDuration::MilliSeconds(2000),
        TDuration::MilliSeconds(2250),
        TDuration::MilliSeconds(2500),
        TDuration::MilliSeconds(2750),
        TDuration::MilliSeconds(3000),
        TDuration::MilliSeconds(3250),
        TDuration::MilliSeconds(3500),
        TDuration::MilliSeconds(3750),
        TDuration::MilliSeconds(4000),
        TDuration::MilliSeconds(4250),
        TDuration::MilliSeconds(4500),
        TDuration::MilliSeconds(4750),
        TDuration::MilliSeconds(5000),
        TDuration::MilliSeconds(5250),
        TDuration::MilliSeconds(5500),
        TDuration::MilliSeconds(5750),
        TDuration::MilliSeconds(6000),
        TDuration::MilliSeconds(6250),
        TDuration::MilliSeconds(6500),
        TDuration::MilliSeconds(6750),
        TDuration::MilliSeconds(7000),
        TDuration::MilliSeconds(7250),
        TDuration::MilliSeconds(7500),
        TDuration::MilliSeconds(7750),
        TDuration::MilliSeconds(8000),
        TDuration::MilliSeconds(8250),
        TDuration::MilliSeconds(8500),
        TDuration::MilliSeconds(8750),
        TDuration::MilliSeconds(9000),
        TDuration::Seconds(10)
    };

} // NKikimr

namespace NKikimr {

TMaybe<TDuration> TLatencyHistogram::Percentile(double p) const {
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

bool TLatencyHistogram::Fadeout(TInstant now) {
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

bool TGroupStat::Fadeout(TInstant now) {
    const bool x1 = PutTabletLog.Fadeout(now);
    const bool x2 = PutUserData.Fadeout(now);
    const bool x3 = GetFast.Fadeout(now);
    return x1 || x2 || x3;
}



} // namespace NKikimr
