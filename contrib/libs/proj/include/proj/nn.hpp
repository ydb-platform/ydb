#pragma once

/*
 * Copyright (c) 2015 Dropbox, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <cstdlib>
#include <functional>
#include <memory>
#include <type_traits>

namespace dropbox {
namespace oxygen {

// Marker type and value for use by nn below.
struct i_promise_i_checked_for_null_t {};
static constexpr i_promise_i_checked_for_null_t i_promise_i_checked_for_null{};

// Helper to get the type pointed to by a raw or smart pointer. This can be
// explicitly
// specialized if need be to provide compatibility with user-defined smart
// pointers.
namespace nn_detail {
template <typename T> struct element_type {
  using type = typename T::element_type;
};
template <typename Pointee> struct element_type<Pointee *> {
  using type = Pointee;
};
}

template <typename PtrType> class nn;

// Trait to check whether a given type is a non-nullable pointer
template <typename T> struct is_nn : public std::false_type {};
template <typename PtrType>
struct is_nn<nn<PtrType>> : public std::true_type {};

/* nn<PtrType>
 *
 * Wrapper around a pointer that is guaranteed to not be null. This works with
 * raw pointers
 * as well as any smart pointer: nn<int *>, nn<shared_ptr<DbxTable>>,
 * nn<unique_ptr<Foo>>,
 * etc. An nn<PtrType> can be used just like a PtrType.
 *
 * An nn<PtrType> can be constructed from another nn<PtrType>, if the underlying
 * type would
 * allow such construction. For example, nn<shared_ptr<PtrType>> can be copied
 * and moved, but
 * nn<unique_ptr<PtrType>> can only be moved; an nn<unique_ptr<PtrType>> can be
 * explicitly
 * (but not implicitly) created from an nn<PtrType*>; implicit upcasts are
 * allowed; and so on.
 *
 * Similarly, non-nullable pointers can be compared with regular or other
 * non-nullable
 * pointers, using the same rules as the underlying pointer types.
 *
 * This module also provides helpers for creating an nn<PtrType> from operations
 * that would
 * always return a non-null pointer: nn_make_unique, nn_make_shared,
 * nn_shared_from_this, and
 * nn_addr (a replacement for operator&).
 *
 * We abbreviate nn<unique_ptr> as nn_unique_ptr - it's a little more readable.
 * Likewise,
 * nn<shared_ptr> can be written as nn_shared_ptr.
 *
 * Finally, we define macros NN_CHECK_ASSERT and NN_CHECK_THROW, to convert a
 * nullable pointer
 * to a non-nullable pointer. At Dropbox, these use customized error-handling
 * infrastructure
 * and are in a separate file. We've included sample implementations here.
 */
template <typename PtrType> class nn {
public:
  static_assert(!is_nn<PtrType>::value, "nn<nn<T>> is disallowed");

  using element_type = typename nn_detail::element_type<PtrType>::type;

  // Pass through calls to operator* and operator-> transparently
  element_type &operator*() const { return *ptr; }
  element_type *operator->() const { return &*ptr; }

  // Expose the underlying PtrType
  operator const PtrType &() const & { return ptr; }
  operator PtrType &&() && { return std::move(ptr); }

  // Trying to use the assignment operator to assign a nn<PtrType> to a PtrType
  // using the
  // above conversion functions hits an ambiguous resolution bug in clang:
  // http://llvm.org/bugs/show_bug.cgi?id=18359
  // While that exists, we can use these as simple ways of accessing the
  // underlying type
  // (instead of workarounds calling the operators explicitly or adding a
  // constructor call).
  const PtrType &as_nullable() const & { return ptr; }
  PtrType &&as_nullable() && { return std::move(ptr); }

  // Can't convert to bool (that would be silly). The explicit delete results in
  // "value of type 'nn<...>' is not contextually convertible to 'bool'", rather
  // than
  // "no viable conversion", which is a bit more clear.
  operator bool() const = delete;

  // Explicitly deleted constructors. These help produce clearer error messages,
  // as trying
  // to use them will result in clang printing the whole line, including the
  // comment.
  nn(std::nullptr_t) = delete;            // nullptr is not allowed here
  nn &operator=(std::nullptr_t) = delete; // nullptr is not allowed here
  nn(PtrType) = delete;            // must use NN_CHECK_ASSERT or NN_CHECK_THROW
  nn &operator=(PtrType) = delete; // must use NN_CHECK_ASSERT or NN_CHECK_THROW
  //PROJ_DLL ~nn();

  // Semi-private constructor for use by NN_CHECK_ macros.
  explicit nn(i_promise_i_checked_for_null_t, const PtrType &arg) noexcept : ptr(arg) {
  }
  explicit nn(i_promise_i_checked_for_null_t, PtrType &&arg) noexcept
      : ptr(std::move(arg)) {
  }

  // Type-converting move and copy constructor. We have four separate cases
  // here, for
  // implicit and explicit move and copy.
  template <typename OtherType,
            typename std::enable_if<
                std::is_constructible<PtrType, OtherType>::value &&
                    !std::is_convertible<OtherType, PtrType>::value,
                int>::type = 0>
  explicit nn(const nn<OtherType> &other)
      : ptr(other.operator const OtherType &()) {}

  template <typename OtherType,
            typename std::enable_if<
                std::is_constructible<PtrType, OtherType>::value &&
                    !std::is_convertible<OtherType, PtrType>::value &&
                    !std::is_pointer<OtherType>::value,
                int>::type = 0>
  explicit nn(nn<OtherType> &&other)
      : ptr(std::move(other).operator OtherType &&()) {}

  template <typename OtherType,
            typename std::enable_if<
                std::is_convertible<OtherType, PtrType>::value, int>::type = 0>
  nn(const nn<OtherType> &other) : ptr(other.operator const OtherType &()) {}

  template <
      typename OtherType,
      typename std::enable_if<std::is_convertible<OtherType, PtrType>::value &&
                                  !std::is_pointer<OtherType>::value,
                              int>::type = 0>
  nn(nn<OtherType> &&other) : ptr(std::move(other).operator OtherType &&()) {}

  // A type-converting move and copy assignment operator aren't necessary;
  // writing
  // "base_ptr = derived_ptr;" will run the type-converting constructor followed
  // by the
  // implicit move assignment operator.

  // Two-argument constructor, designed for use with the shared_ptr aliasing
  // constructor.
  // This will not be instantiated if PtrType doesn't have a suitable
  // constructor.
  template <
      typename OtherType,
      typename std::enable_if<
          std::is_constructible<PtrType, OtherType, element_type *>::value,
          int>::type = 0>
  nn(const nn<OtherType> &ownership_ptr, nn<element_type *> target_ptr)
      : ptr(ownership_ptr.operator const OtherType &(), target_ptr) {}

  // Comparisons. Other comparisons are implemented in terms of these.
  template <typename L, typename R>
  friend bool operator==(const nn<L> &, const R &);
  template <typename L, typename R>
  friend bool operator==(const L &, const nn<R> &);
  template <typename L, typename R>
  friend bool operator==(const nn<L> &, const nn<R> &);

  template <typename L, typename R>
  friend bool operator<(const nn<L> &, const R &);
  template <typename L, typename R>
  friend bool operator<(const L &, const nn<R> &);
  template <typename L, typename R>
  friend bool operator<(const nn<L> &, const nn<R> &);

  // ostream operator
  template <typename T>
  friend std::ostream &operator<<(std::ostream &, const nn<T> &);

  template <typename T = PtrType> element_type *get() const {
    return ptr.get();
  }

private:
  // Backing pointer
  PtrType ptr;
};

// Base comparisons - these are friends of nn<PtrType>, so they can access .ptr
// directly.
template <typename L, typename R> bool operator==(const nn<L> &l, const R &r) {
  return l.ptr == r;
}
template <typename L, typename R> bool operator==(const L &l, const nn<R> &r) {
  return l == r.ptr;
}
template <typename L, typename R>
bool operator==(const nn<L> &l, const nn<R> &r) {
  return l.ptr == r.ptr;
}
template <typename L, typename R> bool operator<(const nn<L> &l, const R &r) {
  return l.ptr < r;
}
template <typename L, typename R> bool operator<(const L &l, const nn<R> &r) {
  return l < r.ptr;
}
template <typename L, typename R>
bool operator<(const nn<L> &l, const nn<R> &r) {
  return l.ptr < r.ptr;
}
template <typename T>
std::ostream &operator<<(std::ostream &os, const nn<T> &p) {
  return os << p.ptr;
}

#define NN_DERIVED_OPERATORS(op, base)                                         \
  template <typename L, typename R>                                            \
  bool operator op(const nn<L> &l, const R &r) {                               \
    return base;                                                               \
  }                                                                            \
  template <typename L, typename R>                                            \
  bool operator op(const L &l, const nn<R> &r) {                               \
    return base;                                                               \
  }                                                                            \
  template <typename L, typename R>                                            \
  bool operator op(const nn<L> &l, const nn<R> &r) {                           \
    return base;                                                               \
  }

NN_DERIVED_OPERATORS(>, r < l)
NN_DERIVED_OPERATORS(<=, !(l > r))
NN_DERIVED_OPERATORS(>=, !(l < r))
NN_DERIVED_OPERATORS(!=, !(l == r))

#undef NN_DERIVED_OPERATORS

// Convenience typedefs
template <typename T> using nn_unique_ptr = nn<std::unique_ptr<T>>;
template <typename T> using nn_shared_ptr = nn<std::shared_ptr<T>>;

template <typename T, typename... Args>
nn_unique_ptr<T> nn_make_unique(Args &&... args) {
  return nn_unique_ptr<T>(
      i_promise_i_checked_for_null,
      std::unique_ptr<T>(new T(std::forward<Args>(args)...)));
}

template <typename T, typename... Args>
nn_shared_ptr<T> nn_make_shared(Args &&... args) {
  return nn_shared_ptr<T>(i_promise_i_checked_for_null,
                          std::make_shared<T>(std::forward<Args>(args)...));
}

template <typename T>
class nn_enable_shared_from_this : public std::enable_shared_from_this<T> {
public:
  using std::enable_shared_from_this<T>::enable_shared_from_this;
  nn_shared_ptr<T> nn_shared_from_this() {
    return nn_shared_ptr<T>(i_promise_i_checked_for_null,
                            this->shared_from_this());
  }
  nn_shared_ptr<const T> nn_shared_from_this() const {
    return nn_shared_ptr<const T>(i_promise_i_checked_for_null,
                                  this->shared_from_this());
  }
};

template <typename T> nn<T *> nn_addr(T &object) {
  return nn<T *>(i_promise_i_checked_for_null, &object);
}

template <typename T> nn<const T *> nn_addr(const T &object) {
  return nn<const T *>(i_promise_i_checked_for_null, &object);
}

/* Non-nullable equivalents of shared_ptr's specialized casting functions.
 * These convert through a shared_ptr since nn<shared_ptr<T>> lacks the
 * ref-count-sharing cast
 * constructor, but thanks to moves there shouldn't be any significant extra
 * cost. */
template <typename T, typename U>
nn_shared_ptr<T> nn_static_pointer_cast(const nn_shared_ptr<U> &org_ptr) {
  auto raw_ptr =
      static_cast<typename nn_shared_ptr<T>::element_type *>(org_ptr.get());
  std::shared_ptr<T> nullable_ptr(org_ptr.as_nullable(), raw_ptr);
  return nn_shared_ptr<T>(i_promise_i_checked_for_null,
                          std::move(nullable_ptr));
}

template <typename T, typename U>
std::shared_ptr<T> nn_dynamic_pointer_cast(const nn_shared_ptr<U> &org_ptr) {
  auto raw_ptr =
      dynamic_cast<typename std::shared_ptr<T>::element_type *>(org_ptr.get());
  if (!raw_ptr) {
    return nullptr;
  } else {
    return std::shared_ptr<T>(org_ptr.as_nullable(), raw_ptr);
  }
}

template <typename T, typename U>
nn_shared_ptr<T> nn_const_pointer_cast(const nn_shared_ptr<U> &org_ptr) {
  auto raw_ptr =
      const_cast<typename nn_shared_ptr<T>::element_type *>(org_ptr.get());
  std::shared_ptr<T> nullable_ptr(org_ptr.as_nullable(), raw_ptr);
  return nn_shared_ptr<T>(i_promise_i_checked_for_null,
                          std::move(nullable_ptr));
}
}
} /* end namespace dropbox::oxygen */

namespace std {
template <typename T> struct hash<::dropbox::oxygen::nn<T>> {
  using argument_type = ::dropbox::oxygen::nn<T>;
  using result_type = size_t;
  result_type operator()(const argument_type &obj) const {
    return std::hash<T>{}(obj.as_nullable());
  }
};
}

/* These have to be macros because our internal versions invoke other macros
 * that use
 * __FILE__ and __LINE__, which we want to correctly point to the call site.
 * We're looking
 * forward to std::source_location :)
 *
 * The lambdas ensure that we only evaluate _e once.
 */
#include <stdexcept>

// NN_CHECK_ASSERT takes a pointer of type PT (e.g. raw pointer, std::shared_ptr
// or std::unique_ptr)
// and returns a non-nullable pointer of type nn<PT>.
// Triggers an assertion if expression evaluates to null.
#define NN_CHECK_ASSERT(_e)                                                    \
  (([&](typename std::remove_reference<decltype(_e)>::type p) {                \
    /* note: assert() alone is not sufficient here, because it might be        \
     * compiled out. */                                                        \
    assert(p &&#_e " must not be null");                                       \
    if (!p)                                                                    \
      std::abort();                                                            \
    return dropbox::oxygen::nn<                                                \
        typename std::remove_reference<decltype(p)>::type>(                    \
        dropbox::oxygen::i_promise_i_checked_for_null, std::move(p));          \
  })(_e))

// NN_CHECK_THROW takes a pointer of type PT (e.g. raw pointer, std::shared_ptr
// or std::unique_ptr)
// and returns a non-nullable pointer of type nn<PT>.
// Throws if expression evaluates to null.
#define NN_CHECK_THROW(_e)                                                     \
  (([&](typename std::remove_reference<decltype(_e)>::type p) {                \
    if (!p)                                                                    \
      throw std::runtime_error(#_e " must not be null");                       \
    return dropbox::oxygen::nn<                                                \
        typename std::remove_reference<decltype(p)>::type>(                    \
        dropbox::oxygen::i_promise_i_checked_for_null, std::move(p));          \
  })(_e))
