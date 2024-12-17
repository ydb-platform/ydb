#pragma once

#include "defs.h"
#include "gen.h"

namespace NKikimr {
    namespace NSizeGenerator {
        struct TItem {
            ui32 Min;
            ui32 Max;

            TItem(const NKikimr::TEvLoadTestRequest::TSizeInfo& x)
                : Min(x.GetMin())
                , Max(x.GetMax())
            {
                Y_ABORT_UNLESS(x.HasMin() && x.HasMax());
            }

            ui32 Generate() const {
                ui32 range = Max - Min + 1;
                return Min + TAppData::RandomProvider->GenRand64() % range;
            }
        };
    } // NSizeGenerator

    class TSizeGenerator : public TGenerator<NSizeGenerator::TItem> {
    public:
        TSizeGenerator() = default;

        template<typename T>
        TSizeGenerator(const T& settings)
            : TGenerator<NSizeGenerator::TItem>(settings)
        {}

        ui32 GetMax() const {
            if (Items.empty())
                return 0U;
            auto it = Items.cbegin();
            auto max = it->second.Max;
            while (Items.cend() != ++it)
                max = std::max(max, it->second.Max);
            return max;
        }
    };
} // NKikimr
