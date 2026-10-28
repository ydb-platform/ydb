#pragma once

#include <ydb/library/actors/metrics/lines/group_line_frontend.h>

namespace NActors::NHarmonizerMetrics {

    struct TFloatField {
        using TValueType = float;
        inline static constexpr std::array<TLineLabelView, 0> Labels = {};
    };

    struct TGlobal {
        static constexpr TStringBuf Name = "harmonizer.global";

        struct TAvgAwakeningTimeUs : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.avg_awakening_time_us";
        };
        struct TAvgWakingUpTimeUs : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.avg_waking_up_time_us";
        };
        struct TBudget : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.budget";
        };
        struct TSharedFreeCpu : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.shared_free_cpu";
        };

        using TFields = std::tuple<TAvgAwakeningTimeUs, TAvgWakingUpTimeUs, TBudget, TSharedFreeCpu>;
    };

    struct TPool {
        static constexpr TStringBuf Name = "harmonizer.pool.cpu";

        struct TAvgUsedCpu : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.pool.avg_used_cpu";
        };
        struct TAvgElapsedCpu : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.pool.avg_elapsed_cpu";
        };
        struct TPotentialMaxThreadCount : TFloatField {
            static constexpr TStringBuf Name = "harmonizer.pool.potential_max_thread_count";
        };

        using TFields = std::tuple<TAvgUsedCpu, TAvgElapsedCpu, TPotentialMaxThreadCount>;
    };

    using TDecimalStorage = TCompressedLineStorage<100'000, TDecimalEncoding<2>>;
    using TGlobalFrontend = TGroupLineFrontend<TGlobal, TDecimalStorage>;

} // namespace NActors::NHarmonizerMetrics
