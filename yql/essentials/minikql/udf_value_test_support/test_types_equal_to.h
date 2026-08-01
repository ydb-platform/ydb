#pragma once

#include <yql/essentials/minikql/udf_value_test_support/dynumber.h>
#include <yql/essentials/minikql/udf_value_test_support/stream_view.h>
#include <yql/essentials/minikql/udf_value_test_support/struct_type.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/str_stl.h>

#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace NYql::NUdf {

template <typename T>
struct TTestTypeEqualTo: public TEqualTo<T> {
};

template <typename T>
struct TTestTypeEqualTo<TVector<T>> {
    template <typename TOther>
    bool operator()(const TVector<T>& a, const TOther& b) const {
        if (a.size() != b.size()) {
            return false;
        }
        for (size_t i = 0; i < a.size(); ++i) {
            if (!TTestTypeEqualTo<T>{}(a[i], b[i])) {
                return false;
            }
        }
        return true;
    }
    using is_transparent = void;
};

template <typename T>
struct TTestTypeEqualTo<TMaybe<T>> {
    template <typename TOther>
    bool operator()(const TMaybe<T>& a, const TOther& b) const {
        if (a.Defined() != static_cast<bool>(b)) {
            return false;
        }
        return !a.Defined() || TTestTypeEqualTo<T>{}(*a, *b);
    }
    using is_transparent = void;
};

template <typename... Ts>
struct TTestTypeEqualTo<std::tuple<Ts...>> {
    template <typename TOther>
    bool operator()(const std::tuple<Ts...>& a, const TOther& b) const {
        static_assert(std::tuple_size_v<TOther> == sizeof...(Ts),
                      "Tuple arity mismatch: elements of the probe beyond the expected arity would "
                      "be silently left out of the comparison.");
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return (... && TTestTypeEqualTo<Ts>{}(std::get<Is>(a), std::get<Is>(b)));
        }(std::index_sequence_for<Ts...>{});
    }
    using is_transparent = void;
};

template <typename... Ts>
struct TTestTypeEqualTo<std::variant<Ts...>> {
    template <typename TOther>
    bool operator()(const std::variant<Ts...>& a, const TOther& b) const {
        static_assert(std::variant_size_v<TOther> == sizeof...(Ts),
                      "Variant arity mismatch between the expected type and the probe.");
        Y_ENSURE(!a.valueless_by_exception() && !b.valueless_by_exception(),
                 "Cannot compare a valueless_by_exception variant");
        if (a.index() != b.index()) {
            return false;
        }
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return ((Is == a.index() && TTestTypeEqualTo<Ts>{}(std::get<Is>(a), std::get<Is>(b))) || ...);
        }(std::index_sequence_for<Ts...>{});
    }
    using is_transparent = void;
};

template <typename T>
struct TTestTypeEqualTo<NYql::NUdf::TUnboxedValueComparatorStreamView<T>> {
    template <typename TOther>
    bool operator()(const NYql::NUdf::TUnboxedValueComparatorStreamView<T>& a, const TOther& b) const {
        const auto data = a.Data();
        if (static_cast<size_t>(data.size()) != b.size()) {
            return false;
        }
        for (size_t i = 0; i < b.size(); ++i) {
            if (!TTestTypeEqualTo<T>{}(data[i], b[i])) {
                return false;
            }
        }
        return true;
    }
    using is_transparent = void;
};

template <typename... TMembers>
struct TTestTypeEqualTo<NTest::TStructType<TMembers...>> {
    template <typename TOther>
    bool operator()(const NTest::TStructType<TMembers...>& a, const TOther& b) const {
        static_assert(std::tuple_size_v<std::remove_cvref_t<decltype(b.Members)>> == sizeof...(TMembers),
                      "Struct member count mismatch: members of the probe beyond the expected count "
                      "would be silently left out of the comparison.");
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return (... && TTestTypeEqualTo<typename std::tuple_element_t<Is, std::tuple<TMembers...>>::TValueType>{}(
                               std::get<Is>(a.Members).Value, std::get<Is>(b.Members).Value));
        }(std::index_sequence_for<TMembers...>{});
    }
    using is_transparent = void;
};

template <>
struct TTestTypeEqualTo<NTest::TTestDyNumber> {
    bool operator()(const NTest::TTestDyNumber& lhs, const NTest::TTestDyNumber& rhs) const {
        const auto lhsBytes = NKikimr::NDyNumber::ParseDyNumberString(lhs.Value);
        const auto rhsBytes = NKikimr::NDyNumber::ParseDyNumberString(rhs.Value);
        Y_ENSURE(lhsBytes, "Invalid DyNumber string: " << lhs.Value);
        Y_ENSURE(rhsBytes, "Invalid DyNumber string: " << rhs.Value);
        return *lhsBytes == *rhsBytes;
    }
};

} // namespace NYql::NUdf
