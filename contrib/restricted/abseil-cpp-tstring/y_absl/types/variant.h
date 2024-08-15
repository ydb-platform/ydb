// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// variant.h
// -----------------------------------------------------------------------------
//
// This header file defines an `y_absl::variant` type for holding a type-safe
// value of some prescribed set of types (noted as alternative types), and
// associated functions for managing variants.
//
// The `y_absl::variant` type is a form of type-safe union. An `y_absl::variant`
// should always hold a value of one of its alternative types (except in the
// "valueless by exception state" -- see below). A default-constructed
// `y_absl::variant` will hold the value of its first alternative type, provided
// it is default-constructible.
//
// In exceptional cases due to error, an `y_absl::variant` can hold no
// value (known as a "valueless by exception" state), though this is not the
// norm.
//
// As with `y_absl::optional`, an `y_absl::variant` -- when it holds a value --
// allocates a value of that type directly within the `variant` itself; it
// cannot hold a reference, array, or the type `void`; it can, however, hold a
// pointer to externally managed memory.
//
// `y_absl::variant` is a C++11 compatible version of the C++17 `std::variant`
// abstraction and is designed to be a drop-in replacement for code compliant
// with C++17.

#ifndef Y_ABSL_TYPES_VARIANT_H_
#define Y_ABSL_TYPES_VARIANT_H_

#include "y_absl/base/config.h"
#include "y_absl/utility/utility.h"

#ifdef Y_ABSL_USES_STD_VARIANT

#include <variant>  // IWYU pragma: export

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
using std::bad_variant_access;
using std::get;
using std::get_if;
using std::holds_alternative;
using std::monostate;
using std::variant;
using std::variant_alternative;
using std::variant_alternative_t;
using std::variant_npos;
using std::variant_size;
using std::variant_size_v;
using std::visit;
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#else  // Y_ABSL_USES_STD_VARIANT

#include <functional>
#include <new>
#include <type_traits>
#include <utility>

#include "y_absl/base/macros.h"
#include "y_absl/base/port.h"
#include "y_absl/meta/type_traits.h"
#include "y_absl/types/internal/variant.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN

// -----------------------------------------------------------------------------
// y_absl::variant
// -----------------------------------------------------------------------------
//
// An `y_absl::variant` type is a form of type-safe union. An `y_absl::variant` --
// except in exceptional cases -- always holds a value of one of its alternative
// types.
//
// Example:
//
//   // Construct a variant that holds either an integer or a TString and
//   // assign it to a TString.
//   y_absl::variant<int, TString> v = TString("abc");
//
//   // A default-constructed variant will hold a value-initialized value of
//   // the first alternative type.
//   auto a = y_absl::variant<int, TString>();   // Holds an int of value '0'.
//
//   // variants are assignable.
//
//   // copy assignment
//   auto v1 = y_absl::variant<int, TString>("abc");
//   auto v2 = y_absl::variant<int, TString>(10);
//   v2 = v1;  // copy assign
//
//   // move assignment
//   auto v1 = y_absl::variant<int, TString>("abc");
//   v1 = y_absl::variant<int, TString>(10);
//
//   // assignment through type conversion
//   a = 128;         // variant contains int
//   a = "128";       // variant contains TString
//
// An `y_absl::variant` holding a value of one of its alternative types `T` holds
// an allocation of `T` directly within the variant itself. An `y_absl::variant`
// is not allowed to allocate additional storage, such as dynamic memory, to
// allocate the contained value. The contained value shall be allocated in a
// region of the variant storage suitably aligned for all alternative types.
template <typename... Ts>
class variant;

// swap()
//
// Swaps two `y_absl::variant` values. This function is equivalent to `v.swap(w)`
// where `v` and `w` are `y_absl::variant` types.
//
// Note that this function requires all alternative types to be both swappable
// and move-constructible, because any two variants may refer to either the same
// type (in which case, they will be swapped) or to two different types (in
// which case the values will need to be moved).
//
template <
    typename... Ts,
    y_absl::enable_if_t<
        y_absl::conjunction<std::is_move_constructible<Ts>...,
                          type_traits_internal::IsSwappable<Ts>...>::value,
        int> = 0>
void swap(variant<Ts...>& v, variant<Ts...>& w) noexcept(noexcept(v.swap(w))) {
  v.swap(w);
}

// variant_size
//
// Returns the number of alternative types available for a given `y_absl::variant`
// type as a compile-time constant expression. As this is a class template, it
// is not generally useful for accessing the number of alternative types of
// any given `y_absl::variant` instance.
//
// Example:
//
//   auto a = y_absl::variant<int, TString>;
//   constexpr int num_types =
//       y_absl::variant_size<y_absl::variant<int, TString>>();
//
//   // You can also use the member constant `value`.
//   constexpr int num_types =
//       y_absl::variant_size<y_absl::variant<int, TString>>::value;
//
//   // `y_absl::variant_size` is more valuable for use in generic code:
//   template <typename Variant>
//   constexpr bool IsVariantMultivalue() {
//       return y_absl::variant_size<Variant>() > 1;
//   }
//
// Note that the set of cv-qualified specializations of `variant_size` are
// provided to ensure that those specializations compile (especially when passed
// within template logic).
template <class T>
struct variant_size;

template <class... Ts>
struct variant_size<variant<Ts...>>
    : std::integral_constant<std::size_t, sizeof...(Ts)> {};

// Specialization of `variant_size` for const qualified variants.
template <class T>
struct variant_size<const T> : variant_size<T>::type {};

// Specialization of `variant_size` for volatile qualified variants.
template <class T>
struct variant_size<volatile T> : variant_size<T>::type {};

// Specialization of `variant_size` for const volatile qualified variants.
template <class T>
struct variant_size<const volatile T> : variant_size<T>::type {};

// variant_alternative
//
// Returns the alternative type for a given `y_absl::variant` at the passed
// index value as a compile-time constant expression. As this is a class
// template resulting in a type, it is not useful for access of the run-time
// value of any given `y_absl::variant` variable.
//
// Example:
//
//   // The type of the 0th alternative is "int".
//   using alternative_type_0
//     = y_absl::variant_alternative<0, y_absl::variant<int, TString>>::type;
//
//   static_assert(std::is_same<alternative_type_0, int>::value, "");
//
//   // `y_absl::variant_alternative` is more valuable for use in generic code:
//   template <typename Variant>
//   constexpr bool IsFirstElementTrivial() {
//       return std::is_trivial_v<variant_alternative<0, Variant>::type>;
//   }
//
// Note that the set of cv-qualified specializations of `variant_alternative`
// are provided to ensure that those specializations compile (especially when
// passed within template logic).
template <std::size_t I, class T>
struct variant_alternative;

template <std::size_t I, class... Types>
struct variant_alternative<I, variant<Types...>> {
  using type =
      variant_internal::VariantAlternativeSfinaeT<I, variant<Types...>>;
};

// Specialization of `variant_alternative` for const qualified variants.
template <std::size_t I, class T>
struct variant_alternative<I, const T> {
  using type = const typename variant_alternative<I, T>::type;
};

// Specialization of `variant_alternative` for volatile qualified variants.
template <std::size_t I, class T>
struct variant_alternative<I, volatile T> {
  using type = volatile typename variant_alternative<I, T>::type;
};

// Specialization of `variant_alternative` for const volatile qualified
// variants.
template <std::size_t I, class T>
struct variant_alternative<I, const volatile T> {
  using type = const volatile typename variant_alternative<I, T>::type;
};

// Template type alias for variant_alternative<I, T>::type.
//
// Example:
//
//   using alternative_type_0
//     = y_absl::variant_alternative_t<0, y_absl::variant<int, TString>>;
//   static_assert(std::is_same<alternative_type_0, int>::value, "");
template <std::size_t I, class T>
using variant_alternative_t = typename variant_alternative<I, T>::type;

// holds_alternative()
//
// Checks whether the given variant currently holds a given alternative type,
// returning `true` if so.
//
// Example:
//
//   y_absl::variant<int, TString> foo = 42;
//   if (y_absl::holds_alternative<int>(foo)) {
//       std::cout << "The variant holds an integer";
//   }
template <class T, class... Types>
constexpr bool holds_alternative(const variant<Types...>& v) noexcept {
  static_assert(
      variant_internal::UnambiguousIndexOfImpl<variant<Types...>, T,
                                               0>::value != sizeof...(Types),
      "The type T must occur exactly once in Types...");
  return v.index() ==
         variant_internal::UnambiguousIndexOf<variant<Types...>, T>::value;
}

// get()
//
// Returns a reference to the value currently within a given variant, using
// either a unique alternative type amongst the variant's set of alternative
// types, or the variant's index value. Attempting to get a variant's value
// using a type that is not unique within the variant's set of alternative types
// is a compile-time error. If the index of the alternative being specified is
// different from the index of the alternative that is currently stored, throws
// `y_absl::bad_variant_access`.
//
// Example:
//
//   auto a = y_absl::variant<int, TString>;
//
//   // Get the value by type (if unique).
//   int i = y_absl::get<int>(a);
//
//   auto b = y_absl::variant<int, int>;
//
//   // Getting the value by a type that is not unique is ill-formed.
//   int j = y_absl::get<int>(b);     // Compile Error!
//
//   // Getting value by index not ambiguous and allowed.
//   int k = y_absl::get<1>(b);

// Overload for getting a variant's lvalue by type.
template <class T, class... Types>
constexpr T& get(variant<Types...>& v) {  // NOLINT
  return variant_internal::VariantCoreAccess::CheckedAccess<
      variant_internal::IndexOf<T, Types...>::value>(v);
}

// Overload for getting a variant's rvalue by type.
template <class T, class... Types>
constexpr T&& get(variant<Types...>&& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<
      variant_internal::IndexOf<T, Types...>::value>(std::move(v));
}

// Overload for getting a variant's const lvalue by type.
template <class T, class... Types>
constexpr const T& get(const variant<Types...>& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<
      variant_internal::IndexOf<T, Types...>::value>(v);
}

// Overload for getting a variant's const rvalue by type.
template <class T, class... Types>
constexpr const T&& get(const variant<Types...>&& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<
      variant_internal::IndexOf<T, Types...>::value>(std::move(v));
}

// Overload for getting a variant's lvalue by index.
template <std::size_t I, class... Types>
constexpr variant_alternative_t<I, variant<Types...>>& get(
    variant<Types...>& v) {  // NOLINT
  return variant_internal::VariantCoreAccess::CheckedAccess<I>(v);
}

// Overload for getting a variant's rvalue by index.
template <std::size_t I, class... Types>
constexpr variant_alternative_t<I, variant<Types...>>&& get(
    variant<Types...>&& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<I>(std::move(v));
}

// Overload for getting a variant's const lvalue by index.
template <std::size_t I, class... Types>
constexpr const variant_alternative_t<I, variant<Types...>>& get(
    const variant<Types...>& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<I>(v);
}

// Overload for getting a variant's const rvalue by index.
template <std::size_t I, class... Types>
constexpr const variant_alternative_t<I, variant<Types...>>&& get(
    const variant<Types...>&& v) {
  return variant_internal::VariantCoreAccess::CheckedAccess<I>(std::move(v));
}

// get_if()
//
// Returns a pointer to the value currently stored within a given variant, if
// present, using either a unique alternative type amongst the variant's set of
// alternative types, or the variant's index value. If such a value does not
// exist, returns `nullptr`.
//
// As with `get`, attempting to get a variant's value using a type that is not
// unique within the variant's set of alternative types is a compile-time error.

// Overload for getting a pointer to the value stored in the given variant by
// index.
template <std::size_t I, class... Types>
constexpr y_absl::add_pointer_t<variant_alternative_t<I, variant<Types...>>>
get_if(variant<Types...>* v) noexcept {
  return (v != nullptr && v->index() == I)
             ? std::addressof(
                   variant_internal::VariantCoreAccess::Access<I>(*v))
             : nullptr;
}

// Overload for getting a pointer to the const value stored in the given
// variant by index.
template <std::size_t I, class... Types>
constexpr y_absl::add_pointer_t<const variant_alternative_t<I, variant<Types...>>>
get_if(const variant<Types...>* v) noexcept {
  return (v != nullptr && v->index() == I)
             ? std::addressof(
                   variant_internal::VariantCoreAccess::Access<I>(*v))
             : nullptr;
}

// Overload for getting a pointer to the value stored in the given variant by
// type.
template <class T, class... Types>
constexpr y_absl::add_pointer_t<T> get_if(variant<Types...>* v) noexcept {
  return y_absl::get_if<variant_internal::IndexOf<T, Types...>::value>(v);
}

// Overload for getting a pointer to the const value stored in the given variant
// by type.
template <class T, class... Types>
constexpr y_absl::add_pointer_t<const T> get_if(
    const variant<Types...>* v) noexcept {
  return y_absl::get_if<variant_internal::IndexOf<T, Types...>::value>(v);
}

// visit()
//
// Calls a provided functor on a given set of variants. `y_absl::visit()` is
// commonly used to conditionally inspect the state of a given variant (or set
// of variants).
//
// The functor must return the same type when called with any of the variants'
// alternatives.
//
// Example:
//
//   // Define a visitor functor
//   struct GetVariant {
//       template<typename T>
//       void operator()(const T& i) const {
//         std::cout << "The variant's value is: " << i;
//       }
//   };
//
//   // Declare our variant, and call `y_absl::visit()` on it.
//   // Note that `GetVariant()` returns void in either case.
//   y_absl::variant<int, TString> foo = TString("foo");
//   GetVariant visitor;
//   y_absl::visit(visitor, foo);  // Prints `The variant's value is: foo'
template <typename Visitor, typename... Variants>
variant_internal::VisitResult<Visitor, Variants...> visit(Visitor&& vis,
                                                          Variants&&... vars) {
  return variant_internal::
      VisitIndices<variant_size<y_absl::decay_t<Variants> >::value...>::Run(
          variant_internal::PerformVisitation<Visitor, Variants...>{
              std::forward_as_tuple(std::forward<Variants>(vars)...),
              std::forward<Visitor>(vis)},
          vars.index()...);
}

// monostate
//
// The monostate class serves as a first alternative type for a variant for
// which the first variant type is otherwise not default-constructible.
struct monostate {};

// `y_absl::monostate` Relational Operators

constexpr bool operator<(monostate, monostate) noexcept { return false; }
constexpr bool operator>(monostate, monostate) noexcept { return false; }
constexpr bool operator<=(monostate, monostate) noexcept { return true; }
constexpr bool operator>=(monostate, monostate) noexcept { return true; }
constexpr bool operator==(monostate, monostate) noexcept { return true; }
constexpr bool operator!=(monostate, monostate) noexcept { return false; }


//------------------------------------------------------------------------------
// `y_absl::variant` Template Definition
//------------------------------------------------------------------------------
template <typename T0, typename... Tn>
class variant<T0, Tn...> : private variant_internal::VariantBase<T0, Tn...> {
  static_assert(y_absl::conjunction<std::is_object<T0>,
                                  std::is_object<Tn>...>::value,
                "Attempted to instantiate a variant containing a non-object "
                "type.");
  // Intentionally not qualifying `negation` with `y_absl::` to work around a bug
  // in MSVC 2015 with inline namespace and variadic template.
  static_assert(y_absl::conjunction<negation<std::is_array<T0> >,
                                  negation<std::is_array<Tn> >...>::value,
                "Attempted to instantiate a variant containing an array type.");
  static_assert(y_absl::conjunction<std::is_nothrow_destructible<T0>,
                                  std::is_nothrow_destructible<Tn>...>::value,
                "Attempted to instantiate a variant containing a non-nothrow "
                "destructible type.");

  friend struct variant_internal::VariantCoreAccess;

 private:
  using Base = variant_internal::VariantBase<T0, Tn...>;

 public:
  // Constructors

  // Constructs a variant holding a default-initialized value of the first
  // alternative type.
  constexpr variant() /*noexcept(see 111above)*/ = default;

  // Copy constructor, standard semantics
  variant(const variant& other) = default;

  // Move constructor, standard semantics
  variant(variant&& other) /*noexcept(see above)*/ = default;

  // Constructs a variant of an alternative type specified by overload
  // resolution of the provided forwarding arguments through
  // direct-initialization.
  //
  // Note: If the selected constructor is a constexpr constructor, this
  // constructor shall be a constexpr constructor.
  //
  // NOTE: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0608r1.html
  // has been voted passed the design phase in the C++ standard meeting in Mar
  // 2018. It will be implemented and integrated into `y_absl::variant`.
  template <
      class T,
      std::size_t I = std::enable_if<
          variant_internal::IsNeitherSelfNorInPlace<variant,
                                                    y_absl::decay_t<T> >::value,
          variant_internal::IndexOfConstructedType<variant, T> >::type::value,
      class Tj = y_absl::variant_alternative_t<I, variant>,
      y_absl::enable_if_t<std::is_constructible<Tj, T>::value>* = nullptr>
  constexpr variant(T&& t) noexcept(std::is_nothrow_constructible<Tj, T>::value)
      : Base(variant_internal::EmplaceTag<I>(), std::forward<T>(t)) {}

  // Constructs a variant of an alternative type from the arguments through
  // direct-initialization.
  //
  // Note: If the selected constructor is a constexpr constructor, this
  // constructor shall be a constexpr constructor.
  template <class T, class... Args,
            typename std::enable_if<std::is_constructible<
                variant_internal::UnambiguousTypeOfT<variant, T>,
                Args...>::value>::type* = nullptr>
  constexpr explicit variant(in_place_type_t<T>, Args&&... args)
      : Base(variant_internal::EmplaceTag<
                 variant_internal::UnambiguousIndexOf<variant, T>::value>(),
             std::forward<Args>(args)...) {}

  // Constructs a variant of an alternative type from an initializer list
  // and other arguments through direct-initialization.
  //
  // Note: If the selected constructor is a constexpr constructor, this
  // constructor shall be a constexpr constructor.
  template <class T, class U, class... Args,
            typename std::enable_if<std::is_constructible<
                variant_internal::UnambiguousTypeOfT<variant, T>,
                std::initializer_list<U>&, Args...>::value>::type* = nullptr>
  constexpr explicit variant(in_place_type_t<T>, std::initializer_list<U> il,
                             Args&&... args)
      : Base(variant_internal::EmplaceTag<
                 variant_internal::UnambiguousIndexOf<variant, T>::value>(),
             il, std::forward<Args>(args)...) {}

  // Constructs a variant of an alternative type from a provided index,
  // through value-initialization using the provided forwarded arguments.
  template <std::size_t I, class... Args,
            typename std::enable_if<std::is_constructible<
                variant_internal::VariantAlternativeSfinaeT<I, variant>,
                Args...>::value>::type* = nullptr>
  constexpr explicit variant(in_place_index_t<I>, Args&&... args)
      : Base(variant_internal::EmplaceTag<I>(), std::forward<Args>(args)...) {}

  // Constructs a variant of an alternative type from a provided index,
  // through value-initialization of an initializer list and the provided
  // forwarded arguments.
  template <std::size_t I, class U, class... Args,
            typename std::enable_if<std::is_constructible<
                variant_internal::VariantAlternativeSfinaeT<I, variant>,
                std::initializer_list<U>&, Args...>::value>::type* = nullptr>
  constexpr explicit variant(in_place_index_t<I>, std::initializer_list<U> il,
                             Args&&... args)
      : Base(variant_internal::EmplaceTag<I>(), il,
             std::forward<Args>(args)...) {}

  // Destructors

  // Destroys the variant's currently contained value, provided that
  // `y_absl::valueless_by_exception()` is false.
  ~variant() = default;

  // Assignment Operators

  // Copy assignment operator
  variant& operator=(const variant& other) = default;

  // Move assignment operator
  variant& operator=(variant&& other) /*noexcept(see above)*/ = default;

  // Converting assignment operator
  //
  // NOTE: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0608r1.html
  // has been voted passed the design phase in the C++ standard meeting in Mar
  // 2018. It will be implemented and integrated into `y_absl::variant`.
  template <
      class T,
      std::size_t I = std::enable_if<
          !std::is_same<y_absl::decay_t<T>, variant>::value,
          variant_internal::IndexOfConstructedType<variant, T>>::type::value,
      class Tj = y_absl::variant_alternative_t<I, variant>,
      typename std::enable_if<std::is_assignable<Tj&, T>::value &&
                              std::is_constructible<Tj, T>::value>::type* =
          nullptr>
  variant& operator=(T&& t) noexcept(
      std::is_nothrow_assignable<Tj&, T>::value&&
          std::is_nothrow_constructible<Tj, T>::value) {
    variant_internal::VisitIndices<sizeof...(Tn) + 1>::Run(
        variant_internal::VariantCoreAccess::MakeConversionAssignVisitor(
            this, std::forward<T>(t)),
        index());

    return *this;
  }


  // emplace() Functions

  // Constructs a value of the given alternative type T within the variant. The
  // existing value of the variant is destroyed first (provided that
  // `y_absl::valueless_by_exception()` is false). Requires that T is unambiguous
  // in the variant.
  //
  // Example:
  //
  //   y_absl::variant<std::vector<int>, int, TString> v;
  //   v.emplace<int>(99);
  //   v.emplace<TString>("abc");
  template <
      class T, class... Args,
      typename std::enable_if<std::is_constructible<
          y_absl::variant_alternative_t<
              variant_internal::UnambiguousIndexOf<variant, T>::value, variant>,
          Args...>::value>::type* = nullptr>
  T& emplace(Args&&... args) {
    return variant_internal::VariantCoreAccess::Replace<
        variant_internal::UnambiguousIndexOf<variant, T>::value>(
        this, std::forward<Args>(args)...);
  }

  // Constructs a value of the given alternative type T within the variant using
  // an initializer list. The existing value of the variant is destroyed first
  // (provided that `y_absl::valueless_by_exception()` is false). Requires that T
  // is unambiguous in the variant.
  //
  // Example:
  //
  //   y_absl::variant<std::vector<int>, int, TString> v;
  //   v.emplace<std::vector<int>>({0, 1, 2});
  template <
      class T, class U, class... Args,
      typename std::enable_if<std::is_constructible<
          y_absl::variant_alternative_t<
              variant_internal::UnambiguousIndexOf<variant, T>::value, variant>,
          std::initializer_list<U>&, Args...>::value>::type* = nullptr>
  T& emplace(std::initializer_list<U> il, Args&&... args) {
    return variant_internal::VariantCoreAccess::Replace<
        variant_internal::UnambiguousIndexOf<variant, T>::value>(
        this, il, std::forward<Args>(args)...);
  }

  // Destroys the current value of the variant (provided that
  // `y_absl::valueless_by_exception()` is false) and constructs a new value at
  // the given index.
  //
  // Example:
  //
  //   y_absl::variant<std::vector<int>, int, int> v;
  //   v.emplace<1>(99);
  //   v.emplace<2>(98);
  //   v.emplace<int>(99);  // Won't compile. 'int' isn't a unique type.
  template <std::size_t I, class... Args,
            typename std::enable_if<
                std::is_constructible<y_absl::variant_alternative_t<I, variant>,
                                      Args...>::value>::type* = nullptr>
  y_absl::variant_alternative_t<I, variant>& emplace(Args&&... args) {
    return variant_internal::VariantCoreAccess::Replace<I>(
        this, std::forward<Args>(args)...);
  }

  // Destroys the current value of the variant (provided that
  // `y_absl::valueless_by_exception()` is false) and constructs a new value at
  // the given index using an initializer list and the provided arguments.
  //
  // Example:
  //
  //   y_absl::variant<std::vector<int>, int, int> v;
  //   v.emplace<0>({0, 1, 2});
  template <std::size_t I, class U, class... Args,
            typename std::enable_if<std::is_constructible<
                y_absl::variant_alternative_t<I, variant>,
                std::initializer_list<U>&, Args...>::value>::type* = nullptr>
  y_absl::variant_alternative_t<I, variant>& emplace(std::initializer_list<U> il,
                                                   Args&&... args) {
    return variant_internal::VariantCoreAccess::Replace<I>(
        this, il, std::forward<Args>(args)...);
  }

  // variant::valueless_by_exception()
  //
  // Returns false if and only if the variant currently holds a valid value.
  constexpr bool valueless_by_exception() const noexcept {
    return this->index_ == y_absl::variant_npos;
  }

  // variant::index()
  //
  // Returns the index value of the variant's currently selected alternative
  // type.
  constexpr std::size_t index() const noexcept { return this->index_; }

  // variant::swap()
  //
  // Swaps the values of two variant objects.
  //
  void swap(variant& rhs) noexcept(
      y_absl::conjunction<
          std::is_nothrow_move_constructible<T0>,
          std::is_nothrow_move_constructible<Tn>...,
          type_traits_internal::IsNothrowSwappable<T0>,
          type_traits_internal::IsNothrowSwappable<Tn>...>::value) {
    return variant_internal::VisitIndices<sizeof...(Tn) + 1>::Run(
        variant_internal::Swap<T0, Tn...>{this, &rhs}, rhs.index());
  }
};

// We need a valid declaration of variant<> for SFINAE and overload resolution
// to work properly above, but we don't need a full declaration since this type
// will never be constructed. This declaration, though incomplete, suffices.
template <>
class variant<>;

//------------------------------------------------------------------------------
// Relational Operators
//------------------------------------------------------------------------------
//
// If neither operand is in the `variant::valueless_by_exception` state:
//
//   * If the index of both variants is the same, the relational operator
//     returns the result of the corresponding relational operator for the
//     corresponding alternative type.
//   * If the index of both variants is not the same, the relational operator
//     returns the result of that operation applied to the value of the left
//     operand's index and the value of the right operand's index.
//   * If at least one operand is in the valueless_by_exception state:
//     - A variant in the valueless_by_exception state is only considered equal
//       to another variant in the valueless_by_exception state.
//     - If exactly one operand is in the valueless_by_exception state, the
//       variant in the valueless_by_exception state is less than the variant
//       that is not in the valueless_by_exception state.
//
// Note: The value 1 is added to each index in the relational comparisons such
// that the index corresponding to the valueless_by_exception state wraps around
// to 0 (the lowest value for the index type), and the remaining indices stay in
// the same relative order.

// Equal-to operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveEqualT<Types...> operator==(
    const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() == b.index()) &&
         variant_internal::VisitIndices<sizeof...(Types)>::Run(
             variant_internal::EqualsOp<Types...>{&a, &b}, a.index());
}

// Not equal operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveNotEqualT<Types...> operator!=(
    const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() != b.index()) ||
         variant_internal::VisitIndices<sizeof...(Types)>::Run(
             variant_internal::NotEqualsOp<Types...>{&a, &b}, a.index());
}

// Less-than operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveLessThanT<Types...> operator<(
    const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() != b.index())
             ? (a.index() + 1) < (b.index() + 1)
             : variant_internal::VisitIndices<sizeof...(Types)>::Run(
                   variant_internal::LessThanOp<Types...>{&a, &b}, a.index());
}

// Greater-than operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveGreaterThanT<Types...> operator>(
    const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() != b.index())
             ? (a.index() + 1) > (b.index() + 1)
             : variant_internal::VisitIndices<sizeof...(Types)>::Run(
                   variant_internal::GreaterThanOp<Types...>{&a, &b},
                   a.index());
}

// Less-than or equal-to operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveLessThanOrEqualT<Types...> operator<=(
    const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() != b.index())
             ? (a.index() + 1) < (b.index() + 1)
             : variant_internal::VisitIndices<sizeof...(Types)>::Run(
                   variant_internal::LessThanOrEqualsOp<Types...>{&a, &b},
                   a.index());
}

// Greater-than or equal-to operator
template <typename... Types>
constexpr variant_internal::RequireAllHaveGreaterThanOrEqualT<Types...>
operator>=(const variant<Types...>& a, const variant<Types...>& b) {
  return (a.index() != b.index())
             ? (a.index() + 1) > (b.index() + 1)
             : variant_internal::VisitIndices<sizeof...(Types)>::Run(
                   variant_internal::GreaterThanOrEqualsOp<Types...>{&a, &b},
                   a.index());
}

Y_ABSL_NAMESPACE_END
}  // namespace y_absl

namespace std {

// hash()
template <>  // NOLINT
struct hash<y_absl::monostate> {
  std::size_t operator()(y_absl::monostate) const { return 0; }
};

template <class... T>  // NOLINT
struct hash<y_absl::variant<T...>>
    : y_absl::variant_internal::VariantHashBase<y_absl::variant<T...>, void,
                                              y_absl::remove_const_t<T>...> {};

}  // namespace std

#endif  // Y_ABSL_USES_STD_VARIANT

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace variant_internal {

// Helper visitor for converting a variant<Ts...>` into another type (mostly
// variant) that can be constructed from any type.
template <typename To>
struct ConversionVisitor {
  template <typename T>
  To operator()(T&& v) const {
    return To(std::forward<T>(v));
  }
};

}  // namespace variant_internal

// ConvertVariantTo()
//
// Helper functions to convert an `y_absl::variant` to a variant of another set of
// types, provided that the alternative type of the new variant type can be
// converted from any type in the source variant.
//
// Example:
//
//   y_absl::variant<name1, name2, float> InternalReq(const Req&);
//
//   // name1 and name2 are convertible to name
//   y_absl::variant<name, float> ExternalReq(const Req& req) {
//     return y_absl::ConvertVariantTo<y_absl::variant<name, float>>(
//              InternalReq(req));
//   }
template <typename To, typename Variant>
To ConvertVariantTo(Variant&& variant) {
  return y_absl::visit(variant_internal::ConversionVisitor<To>{},
                     std::forward<Variant>(variant));
}

Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_TYPES_VARIANT_H_
