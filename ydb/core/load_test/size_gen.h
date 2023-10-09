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

    using TSizeGenerator = TGenerator<NSizeGenerator::TItem>;
} // NKikimr
