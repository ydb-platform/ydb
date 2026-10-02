#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/udf_value_test_support/dynumber.h>
#include <yql/essentials/minikql/udf_value_test_support/singular_void.h>
#include <yql/essentials/minikql/udf_value_test_support/stream_view.h>
#include <yql/essentials/minikql/udf_value_test_support/struct_type.h>
#include <yql/essentials/minikql/udf_value_test_support/test_types_equal_to.h>
#include <yql/essentials/minikql/udf_value_test_support/test_types_hash.h>
#include <yql/essentials/minikql/udf_value_test_support/test_types_string_converter.h>
#include <yql/essentials/minikql/udf_value_test_support/utf8.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/arrow/block_item.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/str_stl.h>
#include <util/string/builder.h>
#include <util/system/unaligned_mem.h>
#include <util/system/yassert.h>

#include <expected>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <variant>

namespace NYql::NUdf {

using TUnboxedValueComparatorResult = std::expected<void, TString>;

template <typename T>
concept CComparatorUtilsUdfValue =
    std::is_base_of_v<TUnboxedValuePod, std::remove_cvref_t<T>> ||
    std::same_as<std::remove_cvref_t<T>, TBlockItem>;

template <typename T, typename = void>
struct TUnboxedValueConverter {
    static_assert(sizeof(T) == 0,
                  "TUnboxedValueConverter is not specialized for this type. "
                  "Add a specialization of TUnboxedValueConverter<T> to support it.");
};

template <typename T>
using TConvertedValueType = decltype(TUnboxedValueConverter<T>::Convert(std::declval<const TUnboxedValuePod&>()));

template <typename T>
    requires(std::is_arithmetic_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128>)
struct TUnboxedValueConverter<T> {
    template <CComparatorUtilsUdfValue THolder>
    static T Convert(const THolder& value) {
        return value.template Get<T>();
    }
};

template <>
struct TUnboxedValueConverter<TStringBuf> {
    template <CComparatorUtilsUdfValue THolder>
    static TString Convert(const THolder& value) {
        return TString(value.AsStringRef());
    }
};

template <>
struct TUnboxedValueConverter<TString> {
    template <CComparatorUtilsUdfValue THolder>
    static TString Convert(const THolder& value) {
        return TUnboxedValueConverter<TStringBuf>::Convert(value);
    }
};

template <>
struct TUnboxedValueConverter<NTest::TUtf8> {
    template <CComparatorUtilsUdfValue THolder>
    static NTest::TUtf8 Convert(const THolder& value) {
        return NTest::TUtf8(TUnboxedValueConverter<TStringBuf>::Convert(value));
    }
};

template <>
struct TUnboxedValueConverter<NTest::TSingularVoid> {
    template <CComparatorUtilsUdfValue THolder>
    static NTest::TSingularVoid Convert(const THolder& value) {
        Y_ENSURE(value, "Expected a defined Void value");
        return NTest::TSingularVoid();
    }
};

template <typename T>
struct TUnboxedValueConverter<TMaybe<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TMaybe<TConvertedValueType<T>> Convert(const THolder& value) {
        if (!value) {
            return Nothing();
        }
        return TMaybe<TConvertedValueType<T>>(TUnboxedValueConverter<T>::Convert(value.GetOptionalValue()));
    }
};

template <typename... Ts>
struct TUnboxedValueConverter<std::tuple<Ts...>> {
    template <CComparatorUtilsUdfValue THolder>
    static std::tuple<TConvertedValueType<Ts>...> Convert(const THolder& value) {
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return std::tuple<TConvertedValueType<Ts>...>{TUnboxedValueConverter<Ts>::Convert(value.GetElement(Is))...};
        }(std::index_sequence_for<Ts...>{});
    }
};

template <typename T>
struct TUnboxedValueConverter<TVector<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TVector<TConvertedValueType<T>> Convert(const THolder& value) {
        TVector<TConvertedValueType<T>> result;
        auto it = value.GetListIterator();
        TUnboxedValue item;
        while (it.Next(item)) {
            result.push_back(TUnboxedValueConverter<T>::Convert(item));
        }
        // A list iterator must keep reporting exhausted once it has: call Next once more to
        // catch an iterator that resumes producing items right after it claimed to be done.
        Y_ENSURE(!it.Next(item), "List iterator produced more items after reporting exhausted");
        return result;
    }
};

template <typename T>
struct TUnboxedValueConverter<TUnboxedValueComparatorStreamView<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TVector<TConvertedValueType<T>> Convert(const THolder& value) {
        TVector<TConvertedValueType<T>> result;
        for (EFetchStatus status = EFetchStatus::Yield; status != EFetchStatus::Finish;) {
            TUnboxedValue item;
            status = value.Fetch(item);
            if (status == EFetchStatus::Ok) {
                result.push_back(TUnboxedValueConverter<T>::Convert(item));
            }
        }
        TUnboxedValue extra;
        for (EFetchStatus status = EFetchStatus::Yield; status != EFetchStatus::Finish;) {
            status = value.Fetch(extra);
            Y_ENSURE(status != EFetchStatus::Ok, "Stream produced more items after reporting Finish");
        }
        return result;
    }
};

template <typename... Ts>
struct TUnboxedValueConverter<std::variant<Ts...>> {
    template <CComparatorUtilsUdfValue THolder>
    static std::variant<TConvertedValueType<Ts>...> Convert(const THolder& value) {
        const ui32 index = value.GetVariantIndex();
        Y_ENSURE(index < sizeof...(Ts), "Variant index out of range: " << index);
        const auto item = value.GetVariantItem();
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            std::variant<TConvertedValueType<Ts>...> result;
            Y_UNUSED(((Is == index && (result = std::variant<TConvertedValueType<Ts>...>(std::in_place_index<Is>, TUnboxedValueConverter<Ts>::Convert(item)), true)) || ...));
            return result;
        }(std::index_sequence_for<Ts...>{});
    }
};

template <typename TMember>
struct TConvertedStructMember;

template <NTest::TStructMemberName Name, typename T>
struct TConvertedStructMember<NTest::TStructMember<Name, T>> {
    using TType = NTest::TStructMember<Name, TConvertedValueType<T>>;
};

template <typename... TMembers>
struct TUnboxedValueConverter<NTest::TStructType<TMembers...>> {
    template <CComparatorUtilsUdfValue THolder>
    static NTest::TStructType<typename TConvertedStructMember<TMembers>::TType...> Convert(const THolder& value) {
        return NTest::TStructType<typename TConvertedStructMember<TMembers>::TType...>{
            ConvertAll(value, std::index_sequence_for<TMembers...>{})};
    }

private:
    template <size_t OriginalIdx, CComparatorUtilsUdfValue THolder>
    static auto ConvertOne(const THolder& value) {
        using TMember = std::tuple_element_t<OriginalIdx, std::tuple<TMembers...>>;
        using TConvertedMember = typename TConvertedStructMember<TMember>::TType;
        constexpr size_t SortedIdx = NTest::TStructType<TMembers...>::OriginalIndexMapping[OriginalIdx];
        return TConvertedMember{TUnboxedValueConverter<typename TMember::TValueType>::Convert(value.GetElement(SortedIdx))};
    }

    template <CComparatorUtilsUdfValue THolder, size_t... OriginalIdx>
    static std::tuple<typename TConvertedStructMember<TMembers>::TType...> ConvertAll(const THolder& value, std::index_sequence<OriginalIdx...>) {
        return std::tuple<typename TConvertedStructMember<TMembers>::TType...>{ConvertOne<OriginalIdx>(value)...};
    }
};

template <>
struct TUnboxedValueConverter<TGUID> {
    template <CComparatorUtilsUdfValue THolder>
    static TGUID Convert(const THolder& value) {
        const auto ref = value.AsStringRef();
        Y_ENSURE(ref.Size() == sizeof(TGUID), "Unexpected Uuid size: " << ref.Size());
        return ReadUnaligned<TGUID>(ref.Data());
    }
};

template <>
struct TUnboxedValueConverter<NTest::TTestDyNumber> {
    template <CComparatorUtilsUdfValue THolder>
    static NTest::TTestDyNumber Convert(const THolder& value) {
        const auto decoded = NKikimr::NDyNumber::DyNumberToString(value.AsStringRef());
        Y_ENSURE(decoded, "Invalid DyNumber bytes");
        return NTest::TTestDyNumber(*decoded);
    }
};

template <typename TExpected, typename TConverted>
TUnboxedValueComparatorResult MakeUnboxedValueMismatch(const TExpected& expected, const TConverted& converted) {
    return std::unexpected(TStringBuilder()
                           << "Expected " << TValueToStringConverter<TExpected>::Convert(expected)
                           << " but got " << TValueToStringConverter<TConverted>::Convert(converted));
}

template <typename TExpectedElem, typename TConvertedElem>
TUnboxedValueComparatorResult CompareUnorderedVectors(
    TArrayRef<const TExpectedElem> expected,
    TArrayRef<const TConvertedElem> converted) {
    if (expected.size() != converted.size()) {
        return std::unexpected("Unordered vectors have different sizes");
    }

    THashMultiSet<TExpectedElem, TTestTypeHash<TExpectedElem>, TTestTypeEqualTo<TExpectedElem>> remaining(expected.begin(), expected.end());
    for (const auto& actual : converted) {
        const auto it = remaining.find(actual);
        if (it == remaining.end()) {
            return std::unexpected("Unordered vectors have different elements");
        }
        remaining.erase(it);
    }

    return {};
}

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult CompareValues(const THolder& value, const T& expected) {
    const auto converted = TUnboxedValueConverter<T>::Convert(value);
    if (TTestTypeEqualTo<T>{}(expected, converted)) {
        return {};
    }
    return MakeUnboxedValueMismatch(expected, converted);
}

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult CompareValuesUnordered(const THolder& value, const TVector<T>& expected) {
    const auto converted = TUnboxedValueConverter<TVector<T>>::Convert(value);
    const auto comparison = CompareUnorderedVectors(
        MakeArrayRef(expected),
        MakeArrayRef(converted));
    return comparison ? comparison : MakeUnboxedValueMismatch(expected, converted);
}

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult CompareValuesUnordered(
    const THolder& value,
    const TUnboxedValueComparatorStreamView<T>& expected) {
    const auto converted = TUnboxedValueConverter<TUnboxedValueComparatorStreamView<T>>::Convert(value);
    const auto comparison = CompareUnorderedVectors(
        expected.Data(),
        MakeArrayRef(converted));
    return comparison ? comparison : MakeUnboxedValueMismatch(expected, converted);
}

template <CComparatorUtilsUdfValue TValue, typename TExpected>
void AssertUnboxedValueElementEqual(const TValue& value, const TExpected& expected) {
    const auto r = CompareValues(value, expected);
    UNIT_ASSERT_C(r, r.error());
}

template <CComparatorUtilsUdfValue TValue, typename T>
void AssertUnboxedValueElementEqualUnordered(const TValue& value, const TVector<T>& expected) {
    const auto r = CompareValuesUnordered(value, expected);
    UNIT_ASSERT_C(r, r.error());
}

template <CComparatorUtilsUdfValue TValue, typename T>
void AssertUnboxedValueElementEqualUnordered(
    const TValue& value,
    const TUnboxedValueComparatorStreamView<T>& expected) {
    const auto r = CompareValuesUnordered(value, expected);
    UNIT_ASSERT_C(r, r.error());
}

} // namespace NYql::NUdf
