#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/system/types.h>
#include <util/system/type_name.h>
#include <util/stream/output.h>

namespace NKikimr {

template <class TTag, ui64 invalidValue>
class TUi64Id {
protected:
    ui64 Value;

public:
    using TSelf = TUi64Id<TTag, invalidValue>;

    constexpr TUi64Id()
        : Value(invalidValue)
    {
    }

    constexpr explicit TUi64Id(ui64 value)
        : Value(value)
    {
    }

    TUi64Id(const TSelf& value) = default;
    TUi64Id(TSelf&& value) = default;
    TSelf& operator =(const TSelf& value) = default;
    TSelf& operator =(TSelf&& value) = default;

    ui64 Hash() const {
        return Hash64to32(Value);
    }

    bool operator==(const TSelf& r) const {
        return Value == r.Value;
    }

    bool operator!=(const TSelf& r) const {
        return Value != r.Value;
    }

    bool operator<(const TSelf& r) const {
        return Value < r.Value;
    }

    bool operator>(const TSelf& r) const {
        return Value > r.Value;
    }

    bool operator<=(const TSelf& r) const {
        return Value <= r.Value;
    }

    bool operator>=(const TSelf& r) const {
        return Value >= r.Value;
    }

    explicit operator bool() const {
        return Value != invalidValue;
    }

    ui64 GetValue() const {
        return Value;
    }

    explicit operator ui64() const {
        return Value;
    }

    friend inline IOutputStream& operator<<(IOutputStream& out, const TSelf& id) {
        return out << id.Value;
    }
};

namespace NIceDb {

template <typename TColumnType, class TTag, ui64 invalidValue>
struct TConvertValue<TColumnType, TRawTypeValue, NKikimr::TUi64Id<TTag, invalidValue>> {
    typedef NKikimr::TUi64Id<TTag, invalidValue> TSourceType;

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

template <typename TColumnType, class TTag, ui64 invalidValue>
struct TConvertValue<TColumnType, NKikimr::TUi64Id<TTag, invalidValue>, TRawTypeValue> {
    typedef NKikimr::TUi64Id<TTag, invalidValue> TTargetType;

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

}
}

template <class TTag, ui64 invalidValue>
struct THash<NKikimr::TUi64Id<TTag, invalidValue>> {
    inline ui64 operator()(const NKikimr::TUi64Id<TTag, invalidValue> &x) const noexcept {
        return x.Hash();
    }
};


