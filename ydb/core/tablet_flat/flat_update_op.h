#pragma once

#include "flat_row_eggs.h"
#include "flat_row_column.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_raw_type_value.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NTable {

    inline const char* EOpToStr(const ECellOp op) {
        switch (op) {
        case ECellOp::Empty:
            return "Empty";
        case ECellOp::Set:
            return "Set";
        case ECellOp::Null:
            return "Null";
        case ECellOp::Reset:
            return "Reset";
        default:
            return "!!unexpected op!!";
        }
    }

    struct TUpdateOp {
        TUpdateOp() = default;

        TUpdateOp(TTag tag, TCellOp op, TRawTypeValue value)
            : Tag(tag)
            , Op(op)
            , Value(value)
        {

        }

        TArrayRef<const char> AsRef() const noexcept
        {
            return { static_cast<const char*>(Value.Data()), Value.Size() };
        }

        TCell AsCell() const noexcept
        {
            return { static_cast<const char*>(Value.Data()), Value.Size() };
        }

        TCellOp NormalizedCellOp() const noexcept
        {
            return Value || Op != ECellOp::Set ? Op : TCellOp(ECellOp::Null);
        }

        TTag Tag = Max<TTag>();
        TCellOp Op = ECellOp::Empty;
        TRawTypeValue Value;
    };

}

}
