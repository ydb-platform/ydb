#pragma once

#include "flat_row_column.h"

#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NTable {

    struct TColumn {

        enum EDefaults {
            LeaderFamily = 0,
        };

        TColumn() = default;

        TColumn(const TString& name, TTag tag, NScheme::TTypeInfo type, bool notNull = false)
            : Id(tag)
            , PType(type)
            , Name(name)
            , NotNull(notNull)
        {

        }

        bool IsTheSame(const TColumn &col) const noexcept
        {
            return
                Id == col.Id
                && PType == col.PType
                && KeyOrder == col.KeyOrder
                && Name == col.Name
                && Family == col.Family
                && NotNull == col.NotNull;
        }

        void SetDefault(const TCell &null)
        {
            if (!null || TCell::CanInline(null.Size())) {
                Storage = { };
                Null = null;
            } else {
                Storage.assign(null.Data(), null.Size());
                Null = TCell(Storage.data(), Storage.size());
            }
        }

        NTable::TTag Id = Max<TTag>();
        NScheme::TTypeInfo PType;
        TString Name;
        ui32 Family = LeaderFamily;
        NTable::TPos KeyOrder = Max<TPos>();
        TCell Null;
        TString Storage;
        bool NotNull = false;
    };
}
}
