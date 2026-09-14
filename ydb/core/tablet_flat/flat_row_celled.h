#pragma once

#include "flat_row_nulls.h"
#include "util_fmt_abort.h"
#include <util/generic/vector.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {
namespace NTable {

    struct TCelled {
        using TRaw = TArrayRef<const TRawTypeValue>;

        TCelled(TRaw key, const TKeyCellDefaults &keyDefaults, bool extend)
            : Size(extend ? keyDefaults->size() : Min(keyDefaults->size(), key.size()))
            , Storage(Size)
            , Cells(Storage.data())
        {
            Y_ENSURE(key.size() <= keyDefaults->size(), "Key is too large");

            for (ui32 it = 0; it < Size; it++) {
                if (it >= key.size()) {
                    Cells[it] = keyDefaults[it];
                } else if (key[it] && key[it].Type() != keyDefaults.Types[it].GetTypeId()) {
                    Y_TABLET_ERROR("Key does not comply table schema");
                } else {
                    Cells[it] = TCell((char*)key[it].Data(), key[it].Size());
                }

                Bytes += Cells[it].Size();
            }
        }

        TCelled(TArrayRef<const TCell> key, const TKeyCellDefaults &keyDefaults, bool extend)
            : Size(extend ? keyDefaults->size() : Min(keyDefaults->size(), key.size()))
            , Storage(Size)
            , Cells(Storage.data())
        {
            Y_ENSURE(key.size() <= keyDefaults->size(), "Key is too large");

            for (ui32 it = 0; it < Size; it++) {
                if (it >= key.size()) {
                    Cells[it] = keyDefaults[it];
                } else {
                    Cells[it] = key[it];
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

        const size_t Size;
        size_t Bytes = 0;
        TStackVec<TCell, 16> Storage;
        TCell* const Cells;
    };

}
}
