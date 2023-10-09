#pragma once

#include "flat_sausage_misc.h"
#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NPageCollection {

    class TAlign {
    public:
        TAlign(TArrayRef<const ui64> steps) : Steps(steps) { }

        TBorder Lookup(ui64 offset, const ui64 sz) const noexcept
        {
            auto it = UpperBound(Steps.begin(), Steps.end(), offset);
            Y_ABORT_UNLESS(it != Steps.end(), "Lookup span is out of borders");

            const ui32 first = std::distance(Steps.begin(), it);
            const ui64 base = it == Steps.begin() ? 0 : *(it - 1);
            const ui64 szoffset = offset + sz;

            ui32 last = first;
            ui64 lastOffset = base;

            while (Steps[last] < szoffset) {
                lastOffset = Steps[last++];
                Y_ABORT_UNLESS(last < Steps.size(), "Lookup span is out of borders");
            }

            return
                {
                    sz,
                        { first, ui32(offset - base) },
                        { last, ui32(szoffset - lastOffset) }
                };
        }

        const TArrayRef<const ui64> Steps;
    };

}
}
