#pragma once

#include "flat_row_eggs.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_id.h>

#include <atomic>

namespace NKikimr {
namespace NTable {
namespace NMem {

#pragma pack(push,4)

    struct TColumnUpdate {
        TColumnUpdate() : TColumnUpdate(Max<TTag>(), ECellOp::Empty, { }) { }

        TColumnUpdate(TTag tag, TCellOp op, const TCell& value)
            : Tag(tag)
            , Op((ui32)op.Raw())
            , Value(value)
        {

        }

        ui32 Tag:24;
        ui32 Op:8;
        TCell Value;
    };

#pragma pack(pop)

    struct TUpdate {
        TColumnUpdate* Ops() noexcept
        {
            return reinterpret_cast<TColumnUpdate*>(this + 1);
        }

        const TColumnUpdate* Ops() const noexcept
        {
            return reinterpret_cast<const TColumnUpdate*>(this + 1);
        }

        TArrayRef<const TColumnUpdate> operator*() const noexcept
        {
            return { Ops(), Items };
        }

        const TUpdate *Next;
        TRowVersion RowVersion;
        ui16 Items;
        ERowOp Rop;
    };

    struct TTreeKey {
        explicit TTreeKey(const TCell* keyCells)
            : KeyCells(keyCells)
        { }

        const TCell* KeyCells;
    };

    struct TTreeValue {
        explicit TTreeValue(const TUpdate* chain)
            : Chain(chain)
        { }

        const TUpdate* GetFirst() const {
            return Chain;
        }

        const TUpdate* Chain;
    };

    static_assert(sizeof(TColumnUpdate) == 16, "TColumnUpdate must be 16 bytes");
    static_assert(sizeof(TUpdate) == 32, "TUpdate must be 32 bytes");
}
}
}
