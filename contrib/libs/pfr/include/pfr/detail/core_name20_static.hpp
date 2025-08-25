// Copyright (c) 2023 Bela Schaum, X-Ryl669, Denis Mikhailov.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


// Initial implementation by Bela Schaum, https://github.com/schaumb
// The way to make it union and UB free by X-Ryl669, https://github.com/X-Ryl669
//

#ifndef PFR_DETAIL_CORE_NAME20_STATIC_HPP
#define PFR_DETAIL_CORE_NAME20_STATIC_HPP
#pragma once

#include <pfr/detail/config.hpp>

#include <pfr/detail/core.hpp>
#include <pfr/detail/fake_object.hpp>
#include <pfr/detail/fields_count.hpp>
#include <pfr/detail/for_each_field.hpp>
#include <pfr/detail/make_integer_sequence.hpp>
#include <pfr/detail/sequence_tuple.hpp>
#include <pfr/detail/stdarray.hpp>

#if !defined(PFR_INTERFACE_UNIT)
#include <type_traits>
#include <string_view>
#include <array>
#include <memory> // for std::addressof
#endif

namespace pfr { namespace detail {

struct core_name_skip {
    std::size_t size_at_begin;
    std::size_t size_at_end;
    bool is_backward;
    std::string_view until_runtime;

    consteval std::string_view apply(std::string_view sv) const noexcept {
        // We use std::min here to make the compiler diagnostic shorter and
        // cleaner in case of misconfigured PFR_CORE_NAME_PARSING
        sv.remove_prefix((std::min)(size_at_begin, sv.size()));
        sv.remove_suffix((std::min)(size_at_end, sv.size()));
        if (until_runtime.empty()) {
            return sv;
        }

        const auto found = is_backward ? sv.rfind(until_runtime)
                                       : sv.find(until_runtime);

        const auto cut_until = found + until_runtime.size();
        const auto safe_cut_until = (std::min)(cut_until, sv.size());
        return sv.substr(safe_cut_until);
    }
};

struct backward {
    explicit consteval backward(std::string_view value) noexcept
        : value(value)
    {}

    std::string_view value;
};

consteval core_name_skip make_core_name_skip(std::size_t size_at_begin,
                                             std::size_t size_at_end,
                                             std::string_view until_runtime) noexcept
{
    return core_name_skip{size_at_begin, size_at_end, false, until_runtime};
}

consteval core_name_skip make_core_name_skip(std::size_t size_at_begin,
                                             std::size_t size_at_end,
                                             backward until_runtime) noexcept
{
    return core_name_skip{size_at_begin, size_at_end, true, until_runtime.value};
}

// it might be compilation failed without this workaround sometimes
// See https://github.com/llvm/llvm-project/issues/41751 for details
template <class>
consteval std::string_view clang_workaround(std::string_view value) noexcept
{
    return value;
}

template <class MsvcWorkaround, auto ptr>
consteval auto name_of_field_impl() noexcept {
    // Some of the following compiler specific macro may be defined only
    // inside the function body:

#ifndef PFR_FUNCTION_SIGNATURE
#   if defined(__FUNCSIG__)
#       define PFR_FUNCTION_SIGNATURE __FUNCSIG__
#   elif defined(__PRETTY_FUNCTION__) || defined(__GNUC__) || defined(__clang__)
#       define PFR_FUNCTION_SIGNATURE __PRETTY_FUNCTION__
#   else
#       define PFR_FUNCTION_SIGNATURE ""
#   endif
#endif

    constexpr std::string_view sv = detail::clang_workaround<MsvcWorkaround>(PFR_FUNCTION_SIGNATURE);
    static_assert(!sv.empty(),
        "====================> Boost.PFR: Field reflection parser configured in a wrong way. "
        "Please define the PFR_FUNCTION_SIGNATURE to a compiler specific macro, "
        "that outputs the whole function signature including non-type template parameters."
    );

    constexpr auto skip = detail::make_core_name_skip PFR_CORE_NAME_PARSING;
    static_assert(skip.size_at_begin + skip.size_at_end + skip.until_runtime.size() < sv.size(),
        "====================> Boost.PFR: Field reflection parser configured in a wrong way. "
        "It attempts to skip more chars than available. "
        "Please define PFR_CORE_NAME_PARSING to correct values. See documentation section "
        "'Limitations and Configuration' for more information."
    );
    constexpr auto fn = skip.apply(sv);
    static_assert(
        !fn.empty(),
        "====================> Boost.PFR: Extraction of field name is misconfigured for your compiler. "
        "It skipped all the input, leaving the field name empty. "
        "Please define PFR_CORE_NAME_PARSING to correct values. See documentation section "
        "'Limitations and Configuration' for more information."
    );
    auto res = std::array<char, fn.size()+1>{};

    auto* out = res.data();
    for (auto x: fn) {
        *out = x;
        ++out;
    }

    return res;
}

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundefined-var-template"

// clang 16 and earlier don't support address of non-static member as template parameter
// but fortunately it's possible to use C++20 non-type template parameters in another way
// even in clang 16 and more older clangs
// all we need is to wrap pointer into 'clang_wrapper_t' and then pass it into template
template <class T>
struct clang_wrapper_t {
    T v;
};
template <class T>
clang_wrapper_t(T) -> clang_wrapper_t<T>;

template <class T>
constexpr auto make_clang_wrapper(const T& arg) noexcept {
    return clang_wrapper_t{arg};
}

#else

template <class T>
constexpr const T& make_clang_wrapper(const T& arg) noexcept {
    // It's everything OK with address of non-static member as template parameter support on this compiler
    // so we don't need a wrapper here, just pass the pointer into template
    return arg;
}

#endif

template <class MsvcWorkaround, auto ptr>
consteval auto name_of_field() noexcept {
    // Sanity check: known field name must match the deduced one
    static_assert(
        sizeof(MsvcWorkaround)  // do not trigger if `name_of_field()` is not used
        && std::string_view{
            detail::name_of_field_impl<
                core_name_skip, detail::make_clang_wrapper(std::addressof(
                    detail::fake_object<core_name_skip>().size_at_begin
                ))
            >().data()
        } == "size_at_begin",
        "====================> Boost.PFR: Extraction of field name is misconfigured for your compiler. "
        "It does not return the proper field name. "
        "Please define PFR_CORE_NAME_PARSING to correct values. See documentation section "
        "'Limitations and Configuration' for more information."
    );

    return detail::name_of_field_impl<MsvcWorkaround, ptr>();
}

// Storing part of a string literal into an array minimizes the binary size.
//
// Without passing 'T' into 'name_of_field' different fields from different structures might have the same name!
// See https://developercommunity.visualstudio.com/t/__FUNCSIG__-outputs-wrong-value-with-C/10458554 for details
template <class T, std::size_t I>
inline constexpr auto stored_name_of_field = detail::name_of_field<T,
    detail::make_clang_wrapper(std::addressof(detail::sequence_tuple::get<I>(
        detail::tie_as_tuple(detail::fake_object<T>())
    )))
>();

#ifdef __clang__
#pragma clang diagnostic pop
#endif

template <class T, std::size_t... I>
constexpr auto tie_as_names_tuple_impl(std::index_sequence<I...>) noexcept {
    return detail::sequence_tuple::make_sequence_tuple(std::string_view{stored_name_of_field<T, I>.data()}...);
}

template <class T, std::size_t I>
constexpr std::string_view get_name() noexcept {
    static_assert(
        !std::is_union<T>::value,
        "====================> Boost.PFR: For safety reasons it is forbidden to reflect unions. See `Reflection of unions` section in the docs for more info."
    );
    static_assert(
        !std::is_array<T>::value,
        "====================> Boost.PFR: It is impossible to extract name from old C array since it doesn't have named members"
    );
    static_assert(
        sizeof(T) && PFR_USE_CPP17,
        "====================> Boost.PFR: Extraction of field's names is allowed only when the PFR_USE_CPP17 macro enabled."
   );

   return stored_name_of_field<T, I>.data();
}

template <class T>
constexpr auto tie_as_names_tuple() noexcept {
    static_assert(
        !std::is_union<T>::value,
        "====================> Boost.PFR: For safety reasons it is forbidden to reflect unions. See `Reflection of unions` section in the docs for more info."
    );
    static_assert(
        !std::is_array<T>::value,
        "====================> Boost.PFR: It is impossible to extract name from old C array since it doesn't have named members"
    );
    static_assert(
        sizeof(T) && PFR_USE_CPP17,
        "====================> Boost.PFR: Extraction of field's names is allowed only when the PFR_USE_CPP17 macro enabled."
    );

    return detail::tie_as_names_tuple_impl<T>(detail::make_index_sequence<detail::fields_count<T>()>{});
}

template <class T, class F>
constexpr void for_each_field_with_name(T&& value, F&& func) {
    return pfr::detail::for_each_field(
        std::forward<T>(value),
        [f = std::forward<F>(func)](auto&& field, auto index) mutable {
            using IndexType = decltype(index);
            using FieldType = decltype(field);
            constexpr auto name = pfr::detail::get_name<std::remove_reference_t<T>, IndexType::value>();
            if constexpr (std::is_invocable_v<F, std::string_view, FieldType, IndexType>) {
                f(name, std::forward<FieldType>(field), index);
            } else {
                f(name, std::forward<FieldType>(field));
            }
        });
}

}} // namespace pfr::detail

#endif // PFR_DETAIL_CORE_NAME20_STATIC_HPP

