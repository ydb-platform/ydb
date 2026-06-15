#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_item.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/array_ref.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <expected>
#include <tuple>
#include <type_traits>
#include <variant>

namespace NYql::NUdf {
namespace NPrivate {

using TUnboxedValueComparatorResult = std::expected<void, TString>;

template <typename T>
concept CComparatorUtilsUdfValue =
    std::is_base_of_v<TUnboxedValuePod, std::remove_cvref_t<T>> ||
    std::same_as<std::remove_cvref_t<T>, TBlockItem>;

template <typename T>
class TUnboxedValueComparatorStreamView {
public:
    explicit TUnboxedValueComparatorStreamView(TArrayRef<const T> data)
        : Data_(data)
    {
    }

    TArrayRef<const T> Data() const {
        return Data_;
    }

private:
    TArrayRef<const T> Data_;
};

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult IsElementEqualImpl(const THolder& value, const T& expected);

template <typename T, typename = void>
struct TUnboxedValueComparator {
    static_assert(sizeof(T) == 0,
                  "TUnboxedValueComparator is not specialized for this type. "
                  "Add a specialization of TUnboxedValueComparator<T> to support it.");
};

template <typename T>
    requires(std::is_arithmetic_v<T> || std::is_same_v<T, NYql::NDecimal::TInt128> || std::is_same_v<T, bool>)
struct TUnboxedValueComparator<T> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const T& expected) {
        const T got = value.template Get<T>();
        if (got != expected) {
            return std::unexpected(TStringBuilder() << "Expected " << expected << " but got " << got);
        }
        return {};
    }
};

template <>
struct TUnboxedValueComparator<TString> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const TString& expected) {
        const TStringBuf got(value.AsStringRef());
        if (got != TStringBuf(expected)) {
            return std::unexpected(TStringBuilder() << "Expected string \"" << expected << "\" but got \"" << got << "\"");
        }
        return {};
    }
};

template <typename T>
struct TUnboxedValueComparator<TMaybe<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const TMaybe<T>& expected) {
        if (!expected.Defined()) {
            if (value) {
                return std::unexpected(TString("Expected Nothing but got a value"));
            }
            return {};
        }
        if (!value) {
            return std::unexpected(TString("Expected a value but got Nothing"));
        }
        auto r = IsElementEqualImpl(value.GetOptionalValue(), *expected);
        if (!r) {
            return std::unexpected(TStringBuilder() << "Optional inner value: " << r.error());
        }
        return {};
    }
};

template <typename... Ts>
struct TUnboxedValueComparator<std::tuple<Ts...>> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const std::tuple<Ts...>& expected) {
        return [&]<size_t... Is>(std::index_sequence<Is...>) -> TUnboxedValueComparatorResult {
            TUnboxedValueComparatorResult result{};
            size_t failedIdx = 0;
            bool ok = ((failedIdx = Is,
                        result = IsElementEqualImpl(value.GetElement(Is), std::get<Is>(expected)),
                        result.has_value()) &&
                       ...);
            if (!ok) {
                return std::unexpected(TStringBuilder() << "Tuple[" << failedIdx << "]: " << result.error());
            }
            return {};
        }(std::index_sequence_for<Ts...>{});
    }
};

template <typename T>
struct TUnboxedValueComparator<TVector<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const TVector<T>& expected) {
        auto it = value.GetListIterator();
        for (size_t i = 0; i < expected.size(); ++i) {
            TUnboxedValue item;
            if (!it.Next(item)) {
                return std::unexpected(TStringBuilder()
                                       << "List too short: expected " << expected.size()
                                       << " elements but ended at [" << i << "]");
            }
            auto r = IsElementEqualImpl(item, expected[i]);
            if (!r) {
                return std::unexpected(TStringBuilder() << "List[" << i << "]: " << r.error());
            }
        }
        TUnboxedValue extra;
        for (size_t i = 0; i < 2; ++i) {
            if (it.Next(extra)) {
                return std::unexpected(TStringBuilder()
                                       << "List too long: expected " << expected.size() << " elements but got more");
            }
        }
        return {};
    }
};

template <typename T>
struct TUnboxedValueComparator<TUnboxedValueComparatorStreamView<T>> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const TUnboxedValueComparatorStreamView<T>& expected) {
        const auto data = expected.Data();
        for (size_t i = 0; i < data.size(); ++i) {
            TUnboxedValue item;
            auto fetchStatus = EFetchStatus::Yield;
            while (fetchStatus == EFetchStatus::Yield) {
                fetchStatus = value.Fetch(item);
            }
            if (fetchStatus != EFetchStatus::Ok) {
                return std::unexpected(TStringBuilder()
                                       << "Stream ended early at [" << i << "], expected "
                                       << data.size() << " elements");
            }
            auto r = IsElementEqualImpl(item, data[i]);
            if (!r) {
                return std::unexpected(TStringBuilder() << "Stream[" << i << "]: " << r.error());
            }
        }
        TUnboxedValue extra;
        for (size_t i = 0; i < 2; ++i) {
            if (value.Fetch(extra) != EFetchStatus::Finish) {
                return std::unexpected(TStringBuilder()
                                       << "Stream has extra elements after " << data.size() << " expected");
            }
        }
        return {};
    }
};

template <typename... Ts>
struct TUnboxedValueComparator<std::variant<Ts...>> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const std::variant<Ts...>& expected) {
        if (value.GetVariantIndex() != expected.index()) {
            return std::unexpected(TStringBuilder()
                                   << "Variant index mismatch: expected " << expected.index()
                                   << " but got " << value.GetVariantIndex());
        }
        const auto item = value.GetVariantItem();
        TUnboxedValueComparatorResult result{};
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            Y_UNUSED(((Is == expected.index() &&
                       (result = IsElementEqualImpl(item, std::get<Is>(expected)), true)) ||
                      ...));
        }(std::index_sequence_for<Ts...>{});
        if (!result) {
            return std::unexpected(TStringBuilder()
                                   << "Variant[" << expected.index() << "]: " << result.error());
        }
        return {};
    }
};

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult IsElementEqualImpl(const THolder& value, const T& expected) {
    return NPrivate::TUnboxedValueComparator<T>::IsEqual(value, expected);
}

} // namespace NPrivate

using TUnboxedValueComparatorResult = NPrivate::TUnboxedValueComparatorResult;
template <typename T>
using TUnboxedValueComparatorStreamView = NPrivate::TUnboxedValueComparatorStreamView<T>;

template <typename T>
concept CComparatorUtilsUdfValue = (NPrivate::CComparatorUtilsUdfValue<T>);

template <CComparatorUtilsUdfValue THolder, typename T>
TUnboxedValueComparatorResult IsUnboxedValueElementEqual(const THolder& value, const T& expected) {
    return NPrivate::IsElementEqualImpl<THolder, T>(value, expected);
}

template <CComparatorUtilsUdfValue TValue, typename TExpected>
void AssertUnboxedValueElementEqual(const TValue& value, const TExpected& expected) {
    const auto r = IsUnboxedValueElementEqual(value, expected);
    UNIT_ASSERT_C(r, r.error());
}

} // namespace NYql::NUdf
