#pragma once

#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TRow;

    struct TMixerOne {
        ui32 operator()(const TRow&) noexcept
        {
            return 0;
        }
    };

    struct TMixerRnd {
        TMixerRnd(ui32 num) : Num(num) { }

        ui32 operator()(const TRow&) noexcept
        {
            return Random.Uniform(Num);
        }

    private:
        const ui32 Num = 1;
        TMersenne<ui64> Random;
    };

    struct TMixerSeq {
        TMixerSeq(ui32 num, ui64 rows)
            : Num(num)
            , Base(rows / num)
            , Skip(rows % num)
        {

        }

        ui32 operator()(const TRow&) noexcept
        {
            if (Buck-- == 0) {
                ui64 one = (Skip && Skip > Random.Uniform(Num) ? 1 : 0);

                Buck = Base + one, Skip -= one, Hash++;
            }

            return Min(Hash, Num - 1);
        }

    private:
        const ui32 Num = 1;
        ui64 Base = Max<ui64>();
        ui64 Skip = 0;
        ui64 Buck = 0;
        ui32 Hash = Max<ui32>();
        TMersenne<ui64> Random;
    };
}
}
}
