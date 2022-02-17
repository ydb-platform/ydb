#pragma once

#include "flat_row_nulls.h"
#include <util/generic/vector.h>
#include <array>

namespace NKikimr {
namespace NTable {

    struct TCelled {
        using TRaw = TArrayRef<const TRawTypeValue>;

        TCelled(TRaw key, const TKeyCellDefaults &keyDefaults, bool extend)
            : Size(extend ? keyDefaults->size() : Min(keyDefaults->size(), key.size()))
            , Large(Size > Small.size() ? Size : 0)
            , Cells(Large ? Large.begin() : Small.begin())
        {
            Y_VERIFY(key.size() <= keyDefaults->size(), "Key is tool large");

            for (ui32 it = 0; it < Size; it++) {
                if (it >= key.size()) {
                    Cells[it] = keyDefaults[it];
                } else if (key[it] && key[it].Type() != keyDefaults.Types[it].GetTypeId()) {
                    Y_FAIL("Key does not comply table schema");
                } else {
                    Cells[it] = TCell((char*)key[it].Data(), key[it].Size());
                }

                Bytes += Cells[it].Size();
            }
        }

        const TCell& operator[](size_t on) const noexcept
        {
            return Cells[on];
        }

        operator TArrayRef<const TCell>() const noexcept
        {
            return { Cells, Size };
        }

        const size_t Size = 0;
        size_t Bytes = 0;
        std::array<TCell, 16> Small;
        TVector<TCell> Large;
        TCell * const Cells = nullptr;
    };

}
}
