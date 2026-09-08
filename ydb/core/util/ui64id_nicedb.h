#pragma once

#include "ui64id.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NIceDb {

template <typename TColumnType, class TTag>
struct TConvertValue<TColumnType, TRawTypeValue, TUi64Id<TTag>> {
    using TSourceType = TUi64Id<TTag>;

    ui64 Store;
    TTypeValue Value;

    TConvertValue(const TSourceType& value)
        : Store(ui64(value))
        , Value(Store, TColumnType::ColumnType)
    {
        static_assert(TColumnType::ColumnType == NScheme::NTypeIds::Uint64, "use TUi64Id only with Uint64");
    }

    operator const TRawTypeValue&() const {
        return Value;
    }
};

template <typename TColumnType, class TTag>
struct TConvertValue<TColumnType, TUi64Id<TTag>, TRawTypeValue> {
    using TTargetType = TUi64Id<TTag>;

    TTypeValue Value;

    TConvertValue(const TRawTypeValue& value)
        : Value(value)
    {
        static_assert(TColumnType::ColumnType == NScheme::NTypeIds::Uint64, "use TUi64Id only with Uint64");
    }

    operator TTargetType() const {
        return TTargetType(ui64(Value));
    }
};

} // namespace NKikimr::NIceDb
