#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/system/types.h>
#include <util/system/type_name.h>
#include <util/stream/output.h>

namespace NKikimr {

namespace NStrongTypeUtils {

enum class EConstructorType {
    Value,
    Deleted,
};

} // namespace NStrongTypeUtils

template <class TTag>
class TUi64Id {
protected:
    ui64 Value;

public:
    using TSelf = TUi64Id<TTag>;
    using TTraits = TTag;

    template<NStrongTypeUtils::EConstructorType ConstructorType = TTraits::ConstructorType, typename std::enable_if<ConstructorType == NStrongTypeUtils::EConstructorType::Value, int>::type = 0>
    constexpr TUi64Id()
        : Value(TTraits::DefaultValue)
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
        return Value != TTraits::InvalidValue;
    }

    ui64 GetValue() const {
        return Value;
    }

    void SetValue(ui64 value) {
        Value = value;
    }

    explicit operator ui64() const {
        return Value;
    }

    friend inline IOutputStream& operator<<(IOutputStream& out, const TSelf& id) {
        return out << id.Value;
    }
};

namespace NIceDb {

template <typename TColumnType, class TTag>
struct TConvertValue<TColumnType, TRawTypeValue, NKikimr::TUi64Id<TTag>> {
    typedef NKikimr::TUi64Id<TTag> TSourceType;

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
struct TConvertValue<TColumnType, NKikimr::TUi64Id<TTag>, TRawTypeValue> {
    typedef NKikimr::TUi64Id<TTag> TTargetType;

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

template <class TTag>
struct THash<NKikimr::TUi64Id<TTag>> {
    inline ui64 operator()(const NKikimr::TUi64Id<TTag> &x) const noexcept {
        return x.Hash();
    }
};


/**
 * Define strong ui64 alias with deleted default constructor
 */
#define STRONG_UI64_TYPE_DEF_NDC(T, InvalidVal) \
    struct T ## Tag \
    { \
        static constexpr ::NKikimr::NStrongTypeUtils::EConstructorType ConstructorType = ::NKikimr::NStrongTypeUtils::EConstructorType::Deleted; \
        static constexpr ui64 InvalidValue = InvalidVal; \
    }; \
    using T = ::NKikimr::TUi64Id<T##Tag>;

/**
 * Define strong ui64 alias with with default constructor
 * The values is set to @Val
 */
#define STRONG_UI64_TYPE_DEF_DV(T, Val, InvalidVal) \
    struct T ## Tag \
    { \
        static constexpr ::NKikimr::NStrongTypeUtils::EConstructorType ConstructorType = ::NKikimr::NStrongTypeUtils::EConstructorType::Value; \
        static constexpr ui64 DefaultValue = Val; \
        static constexpr ui64 InvalidValue = InvalidVal; \
    }; \
    using T = ::NKikimr::TUi64Id<T##Tag>;
