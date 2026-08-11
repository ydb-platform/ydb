#pragma once

#include <yql/essentials/minikql/udf_value_test_support/dynumber.h>
#include <yql/essentials/minikql/udf_value_test_support/singular_void.h>
#include <yql/essentials/minikql/udf_value_test_support/stream_view.h>
#include <yql/essentials/minikql/udf_value_test_support/struct_type.h>
#include <yql/essentials/minikql/udf_value_test_support/utf8.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <tuple>
#include <type_traits>
#include <variant>

namespace NYql::NUdf {

template <typename T>
struct TValueToStringConverter;

namespace NPrivate {

template <typename TElement, typename TRange>
TString RangeToString(const TRange& range) {
    TStringBuilder result;
    result << "[";
    bool first = true;
    for (const auto& item : range) {
        result << (first ? "" : ", ") << TValueToStringConverter<TElement>::Convert(item);
        first = false;
    }
    result << "]";
    return result;
}

} // namespace NPrivate

template <typename T>
struct TValueToStringConverter {
    static_assert(sizeof(T) == 0,
                  "TValueToStringConverter is not specialized for this type. "
                  "Add a specialization of TValueToStringConverter<T> to support it.");
};

template <typename T>
    requires(std::is_arithmetic_v<T> || std::is_same_v<T, TString> || std::is_same_v<T, TStringBuf>)
struct TValueToStringConverter<T> {
    static TString Convert(const T& value) {
        return TStringBuilder() << value;
    }
};

template <>
struct TValueToStringConverter<NYql::NDecimal::TInt128> {
    static TString Convert(const NYql::NDecimal::TInt128& value) {
        return NYql::NDecimal::ToString(value, NYql::NDecimal::MaxPrecision);
    }
};

template <>
struct TValueToStringConverter<TGUID> {
    static TString Convert(const TGUID& value) {
        return value.AsGuidString();
    }
};

template <>
struct TValueToStringConverter<NTest::TUtf8> {
    static TString Convert(const NTest::TUtf8& value) {
        return value.Value;
    }
};

template <>
struct TValueToStringConverter<NTest::TSingularVoid> {
    static TString Convert(const NTest::TSingularVoid&) {
        return TString("Void");
    }
};

template <>
struct TValueToStringConverter<NTest::TTestDyNumber> {
    static TString Convert(const NTest::TTestDyNumber& value) {
        return value.Value;
    }
};

template <typename T>
struct TValueToStringConverter<TMaybe<T>> {
    static TString Convert(const TMaybe<T>& value) {
        return value ? TStringBuilder() << "Just(" << TValueToStringConverter<T>::Convert(*value) << ")"
                     : TString("Nothing");
    }
};

template <typename T>
struct TValueToStringConverter<TVector<T>> {
    static TString Convert(const TVector<T>& value) {
        return NPrivate::RangeToString<T>(value);
    }
};

template <typename... Ts>
struct TValueToStringConverter<std::tuple<Ts...>> {
    static TString Convert(const std::tuple<Ts...>& value) {
        TStringBuilder result;
        result << "(";
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ((result << (Is == 0 ? "" : ", ") << TValueToStringConverter<Ts>::Convert(std::get<Is>(value))), ...);
        }(std::index_sequence_for<Ts...>{});
        result << ")";
        return result;
    }
};

template <typename... Ts>
struct TValueToStringConverter<std::variant<Ts...>> {
    static TString Convert(const std::variant<Ts...>& value) {
        if (value.valueless_by_exception()) {
            return "<valueless variant>";
        }
        const size_t index = value.index();
        return std::visit([index]<typename TAlt>(const TAlt& item) -> TString {
            return TStringBuilder() << "#" << index << "(" << TValueToStringConverter<TAlt>::Convert(item) << ")";
        }, value);
    }
};

template <typename T>
struct TValueToStringConverter<TUnboxedValueComparatorStreamView<T>> {
    static TString Convert(const TUnboxedValueComparatorStreamView<T>& value) {
        return NPrivate::RangeToString<T>(value.Data());
    }
};

template <typename... TMembers>
struct TValueToStringConverter<NTest::TStructType<TMembers...>> {
    static TString Convert(const NTest::TStructType<TMembers...>& value) {
        TStringBuilder result;
        result << "{";
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ((result << (Is == 0 ? "" : ", ")
                     << std::tuple_element_t<Is, std::tuple<TMembers...>>::MemberName()
                     << ": "
                     << TValueToStringConverter<typename std::tuple_element_t<Is, std::tuple<TMembers...>>::TValueType>::Convert(
                            std::get<Is>(value.Members).Value)),
             ...);
        }(std::index_sequence_for<TMembers...>{});
        result << "}";
        return result;
    }
};

} // namespace NYql::NUdf
