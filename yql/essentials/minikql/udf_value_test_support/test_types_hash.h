#pragma once

#include <yql/essentials/minikql/udf_value_test_support/dynumber.h>
#include <yql/essentials/minikql/udf_value_test_support/singular_void.h>
#include <yql/essentials/minikql/udf_value_test_support/stream_view.h>
#include <yql/essentials/minikql/udf_value_test_support/struct_type.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/digest/numeric.h>
#include <util/digest/sequence.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/str_stl.h>
#include <util/system/compiler.h>
#include <util/system/unaligned_mem.h>

#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace NYql::NUdf {

template <typename T>
struct TTestTypeHash {
    template <typename TOther>
    size_t operator()(const TOther& value) const {
        return THash<TOther>{}(value);
    }
    using is_transparent = void;
};

template <typename T>
struct TTestTypeHash<TVector<T>> {
    template <typename TOther>
    size_t operator()(const TOther& value) const {
        return TRangeHash<TTestTypeHash<T>>{}(value);
    }
    using is_transparent = void;
};

template <typename T>
struct TTestTypeHash<TUnboxedValueComparatorStreamView<T>> {
    size_t operator()(const TUnboxedValueComparatorStreamView<T>& value) const {
        return TRangeHash<TTestTypeHash<T>>{}(value.Data());
    }
};

template <typename T>
struct TTestTypeHash<TMaybe<T>> {
    template <typename TOther>
    size_t operator()(const TOther& value) const {
        return value ? CombineHashes(size_t(1), TTestTypeHash<T>{}(*value)) : size_t(0);
    }
    using is_transparent = void;
};

template <typename... Ts>
struct TTestTypeHash<std::tuple<Ts...>> {
    template <typename TOther>
    size_t operator()(const TOther& value) const {
        static_assert(std::tuple_size_v<TOther> == sizeof...(Ts),
                      "Tuple arity mismatch: elements of the probe beyond the expected arity would "
                      "be silently left out of the hash.");
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            size_t result = 0;
            ((result = CombineHashes(result, TTestTypeHash<Ts>{}(std::get<Is>(value)))), ...);
            return result;
        }(std::index_sequence_for<Ts...>{});
    }
    using is_transparent = void;
};

template <typename... Ts>
struct TTestTypeHash<std::variant<Ts...>> {
    template <typename TOther>
    size_t operator()(const TOther& value) const {
        static_assert(std::variant_size_v<TOther> == sizeof...(Ts),
                      "Variant arity mismatch between the expected type and the probe.");
        Y_ENSURE(!value.valueless_by_exception(), "Cannot hash a valueless_by_exception variant");
        const size_t index = value.index();
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            size_t result = 0;
            Y_UNUSED(((Is == index && (result = CombineHashes(Is, TTestTypeHash<Ts>{}(std::get<Is>(value))), true)) || ...));
            return result;
        }(std::index_sequence_for<Ts...>{});
    }
    using is_transparent = void;
};

template <typename... TMembers>
struct TTestTypeHash<NTest::TStructType<TMembers...>> {
    template <typename... TOtherMembers>
    size_t operator()(const NTest::TStructType<TOtherMembers...>& value) const {
        static_assert(sizeof...(TMembers) == sizeof...(TOtherMembers), "Hashing structs with a different number of members");
        return [&]<size_t... SortedPos>(std::index_sequence<SortedPos...>) {
            size_t result = 0;
            ((result = CombineHashes(result, HashMemberAt<NTest::TStructType<TMembers...>::SortedIndexMapping[SortedPos], TOtherMembers...>(value))),
             ...);
            return result;
        }(std::index_sequence_for<TMembers...>{});
    }

private:
    template <size_t Is, typename... TOtherMembers>
    static size_t HashMemberAt(const NTest::TStructType<TOtherMembers...>& value) {
        using TMember = std::tuple_element_t<Is, std::tuple<TMembers...>>;
        constexpr size_t OtherIdx = NTest::TStructType<TOtherMembers...>::FindMemberIndexByName(TMember::MemberName());
        return TTestTypeHash<typename TMember::TValueType>{}(std::get<OtherIdx>(value.Members).Value);
    }
};

template <>
struct TTestTypeHash<NYql::NDecimal::TInt128> {
    size_t operator()(const NYql::NDecimal::TInt128& value) const {
        static_assert(sizeof(value) == 2 * sizeof(ui64));
        const auto* bytes = reinterpret_cast<const char*>(&value);
        return CombineHashes(THash<ui64>{}(ReadUnaligned<ui64>(bytes)),
                             THash<ui64>{}(ReadUnaligned<ui64>(bytes + sizeof(ui64))));
    }
};

template <>
struct TTestTypeHash<NTest::TTestDyNumber> {
    size_t operator()(const NTest::TTestDyNumber& value) const {
        const auto bytes = NKikimr::NDyNumber::ParseDyNumberString(value.Value);
        Y_ENSURE(bytes, "Invalid DyNumber string: " << value.Value);
        return TTestTypeHash<TString>{}(*bytes);
    }
};

template <>
struct TTestTypeHash<NTest::TSingularVoid> {
    size_t operator()(const NYql::NUdf::NTest::TSingularVoid&) const {
        return 0;
    }
};

} // namespace NYql::NUdf
