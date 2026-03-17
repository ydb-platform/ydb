// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_SPAN_H_
#define CORE_FXCRT_SPAN_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <iterator>
#include <type_traits>

#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/unowned_ptr_exclusion.h"

// SAFETY: TODO(crbug.com/pdfium/2085): this entire file is to be replaced
// with the fully annotated one that is being prepared in base/.

namespace pdfium {

constexpr size_t dynamic_extent = static_cast<size_t>(-1);

template <typename T>
using DefaultSpanInternalPtr = UNOWNED_PTR_EXCLUSION T*;

template <typename T,
          size_t Extent = dynamic_extent,
          typename InternalPtr = DefaultSpanInternalPtr<T>>
class span;

namespace internal {

template <typename T>
struct IsSpanImpl : std::false_type {};

template <typename T>
struct IsSpanImpl<span<T>> : std::true_type {};

template <typename T>
using IsSpan = IsSpanImpl<typename std::decay<T>::type>;

template <typename T>
struct IsStdArrayImpl : std::false_type {};

template <typename T, size_t N>
struct IsStdArrayImpl<std::array<T, N>> : std::true_type {};

template <typename T>
using IsStdArray = IsStdArrayImpl<typename std::decay<T>::type>;

template <typename From, typename To>
using IsLegalSpanConversion = std::is_convertible<From*, To*>;

template <typename Container, typename T>
using ContainerHasConvertibleData =
    IsLegalSpanConversion<typename std::remove_pointer<decltype(
                              std::declval<Container>().data())>::type,
                          T>;
template <typename Container>
using ContainerHasIntegralSize =
    std::is_integral<decltype(std::declval<Container>().size())>;

template <typename From, typename To>
using EnableIfLegalSpanConversion =
    typename std::enable_if<IsLegalSpanConversion<From, To>::value>::type;

// SFINAE check if Container can be converted to a span<T>. Note that the
// implementation details of this check differ slightly from the requirements in
// the working group proposal: in particular, the proposal also requires that
// the container conversion constructor participate in overload resolution only
// if two additional conditions are true:
//
//   1. Container implements operator[].
//   2. Container::value_type matches remove_const_t<element_type>.
//
// The requirements are relaxed slightly here: in particular, not requiring (2)
// means that an immutable span can be easily constructed from a mutable
// container.
template <typename Container, typename T>
using EnableIfSpanCompatibleContainer =
    typename std::enable_if<!internal::IsSpan<Container>::value &&
                            !internal::IsStdArray<Container>::value &&
                            ContainerHasConvertibleData<Container, T>::value &&
                            ContainerHasIntegralSize<Container>::value>::type;

template <typename Container, typename T>
using EnableIfConstSpanCompatibleContainer =
    typename std::enable_if<std::is_const<T>::value &&
                            !internal::IsSpan<Container>::value &&
                            !internal::IsStdArray<Container>::value &&
                            ContainerHasConvertibleData<Container, T>::value &&
                            ContainerHasIntegralSize<Container>::value>::type;

}  // namespace internal

// A span is a value type that represents an array of elements of type T. Since
// it only consists of a pointer to memory with an associated size, it is very
// light-weight. It is cheap to construct, copy, move and use spans, so that
// users are encouraged to use it as a pass-by-value parameter. A span does not
// own the underlying memory, so care must be taken to ensure that a span does
// not outlive the backing store.
//
// span is somewhat analogous to StringPiece, but with arbitrary element types,
// allowing mutation if T is non-const.
//
// span is implicitly convertible from C++ arrays, as well as most [1]
// container-like types that provide a data() and size() method (such as
// std::vector<T>). A mutable span<T> can also be implicitly converted to an
// immutable span<const T>.
//
// Consider using a span for functions that take a data pointer and size
// parameter: it allows the function to still act on an array-like type, while
// allowing the caller code to be a bit more concise.
//
// For read-only data access pass a span<const T>: the caller can supply either
// a span<const T> or a span<T>, while the callee will have a read-only view.
// For read-write access a mutable span<T> is required.
//
// Without span:
//   Read-Only:
//     // std::string HexEncode(const uint8_t* data, size_t size);
//     std::vector<uint8_t> data_buffer = GenerateData();
//     std::string r = HexEncode(data_buffer.data(), data_buffer.size());
//
//  Mutable:
//     // ssize_t SafeSNPrintf(char* buf, size_t N, const char* fmt, Args...);
//     char str_buffer[100];
//     SafeSNPrintf(str_buffer, sizeof(str_buffer), "Pi ~= %lf", 3.14);
//
// With span:
//   Read-Only:
//     // std::string HexEncode(base::span<const uint8_t> data);
//     std::vector<uint8_t> data_buffer = GenerateData();
//     std::string r = HexEncode(data_buffer);
//
//  Mutable:
//     // ssize_t SafeSNPrintf(base::span<char>, const char* fmt, Args...);
//     char str_buffer[100];
//     SafeSNPrintf(str_buffer, "Pi ~= %lf", 3.14);
//
// Spans with "const" and pointers
// -------------------------------
//
// Const and pointers can get confusing. Here are vectors of pointers and their
// corresponding spans (you can always make the span "more const" too):
//
//   const std::vector<int*>        =>  base::span<int* const>
//   std::vector<const int*>        =>  base::span<const int*>
//   const std::vector<const int*>  =>  base::span<const int* const>
//
// Differences from the working group proposal
// -------------------------------------------
//
// https://wg21.link/P0122 is the latest working group proposal, Chromium
// currently implements R6. The biggest difference is span does not support a
// static extent template parameter. Other differences are documented in
// subsections below.
//
// Differences in constants and types:
// - no element_type type alias
// - no index_type type alias
// - no different_type type alias
// - no extent constant
//
// Differences from [span.cons]:
// - no constructor from a pointer range
//
// Differences from [span.sub]:
// - using size_t instead of ptrdiff_t for indexing
//
// Differences from [span.obs]:
// - using size_t instead of ptrdiff_t to represent size()
//
// Differences from [span.elem]:
// - no operator ()()
// - using size_t instead of ptrdiff_t for indexing
//
// Additions beyond the C++ standard draft
// - as_chars() function.
// - as_writable_chars() function.
// - as_byte_span() function.
// - as_writable_byte_span() function.
// - span_from_ref() function.
// - byte_span_from_ref() function.

// [span], class template span
template <typename T, size_t Extent, typename InternalPtr>
class TRIVIAL_ABI GSL_POINTER span {
 public:
  using value_type = typename std::remove_cv<T>::type;
  using pointer = T*;
  using reference = T&;
  using iterator = T*;
  using const_iterator = const T*;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  // [span.cons], span constructors, copy, assignment, and destructor
  constexpr span() noexcept = default;

  UNSAFE_BUFFER_USAGE constexpr span(T* data, size_t size) noexcept
      : data_(data), size_(size) {
    DCHECK(data_ || size_ == 0);
  }

  // TODO(dcheng): Implement construction from a |begin| and |end| pointer.
  template <size_t N>
  constexpr span(T (&array)[N]) noexcept : span(array, N) {
    static_assert(Extent == dynamic_extent || Extent == N);
  }

  template <size_t N>
  constexpr span(std::array<T, N>& array) noexcept : span(array.data(), N) {
    static_assert(Extent == dynamic_extent || Extent == N);
  }

  template <size_t N>
  constexpr span(const std::array<std::remove_cv_t<T>, N>& array) noexcept
      : span(array.data(), N) {
    static_assert(Extent == dynamic_extent || Extent == N);
  }

  // Conversion from a container that provides |T* data()| and |integral_type
  // size()|. Note that |data()| may not return nullptr for some empty
  // containers, which can lead to container overflow errors when probing
  // raw ptrs.
#if defined(ADDRESS_SANITIZER) && defined(PDF_USE_PARTITION_ALLOC)
  template <typename Container,
            typename = internal::EnableIfSpanCompatibleContainer<Container, T>>
  constexpr span(Container& container)
      : span(container.size() ? container.data() : nullptr, container.size()) {}
#else
  template <typename Container,
            typename = internal::EnableIfSpanCompatibleContainer<Container, T>>
  constexpr span(Container& container)
      : span(container.data(), container.size()) {}
#endif

  template <
      typename Container,
      typename = internal::EnableIfConstSpanCompatibleContainer<Container, T>>
  span(const Container& container) : span(container.data(), container.size()) {}

  constexpr span(const span& other) noexcept = default;

  // Conversions from spans of compatible types: this allows a span<T> to be
  // seamlessly used as a span<const T>, but not the other way around.
  template <typename U,
            size_t M,
            typename R,
            typename = internal::EnableIfLegalSpanConversion<U, T>>
  constexpr span(const span<U, M, R>& other)
      : span(other.data(), other.size()) {}

  span& operator=(const span& other) noexcept {
    if (this != &other) {
      data_ = other.data_;
      size_ = other.size_;
    }
    return *this;
  }
  ~span() noexcept = default;

  // [span.sub], span subviews
  template <size_t Count>
  span first() const {
    // TODO(tsepez): The following check isn't yet good enough to replace
    // the runtime check since we are still allowing unchecked conversions
    // to arbitrary non-dynamic_extent spans.
    static_assert(Extent == dynamic_extent || Count <= Extent);
    return first(Count);
  }
  const span first(size_t count) const {
    CHECK(count <= size_);
    // SAFETY: CHECK() on line above.
    return UNSAFE_BUFFERS(span(static_cast<T*>(data_), count));
  }

  template <size_t Count>
  span last() const {
    // TODO(tsepez): The following check isn't yet good enough to replace
    // the runtime check since we are still allowing unchecked conversions
    // to arbitrary non-dynamic_extent spans.
    static_assert(Extent == dynamic_extent || Count <= Extent);
    return last(Count);
  }
  const span last(size_t count) const {
    CHECK(count <= size_);
    return UNSAFE_BUFFERS(
        span(static_cast<T*>(data_) + (size_ - count), count));
  }

  template <size_t Offset, size_t Count = dynamic_extent>
  span subspan() const {
    // TODO(tsepez): The following check isn't yet good enough to replace
    // the runtime check since we are still allowing unchecked conversions
    // to arbitrary non-dynamic_extent spans.
    static_assert(Extent == dynamic_extent || Count == dynamic_extent ||
                  Offset + Count <= Extent);
    return subspan(Offset, Count);
  }
  const span subspan(size_t pos, size_t count = dynamic_extent) const {
    CHECK(pos <= size_);
    CHECK(count == dynamic_extent || count <= size_ - pos);
    // SAFETY: CHECK()s on lines above.
    return UNSAFE_BUFFERS(span(static_cast<T*>(data_) + pos,
                               count == dynamic_extent ? size_ - pos : count));
  }

  // [span.obs], span observers
  constexpr size_t size() const noexcept { return size_; }
  constexpr size_t size_bytes() const noexcept { return size() * sizeof(T); }
  constexpr bool empty() const noexcept { return size_ == 0; }

  // [span.elem], span element access
  T& operator[](size_t index) const noexcept {
    CHECK(index < size_);
    return UNSAFE_BUFFERS(static_cast<T*>(data_)[index]);
  }

  constexpr T& front() const noexcept {
    CHECK(!empty());
    return *data();
  }

  constexpr T& back() const noexcept {
    CHECK(!empty());
    return UNSAFE_BUFFERS(*(data() + size() - 1));
  }

  constexpr T* data() const noexcept { return static_cast<T*>(data_); }

  // [span.iter], span iterator support
  constexpr iterator begin() const noexcept { return static_cast<T*>(data_); }
  constexpr iterator end() const noexcept {
    return UNSAFE_BUFFERS(begin() + size_);
  }

  constexpr const_iterator cbegin() const noexcept { return begin(); }
  constexpr const_iterator cend() const noexcept { return end(); }

  constexpr reverse_iterator rbegin() const noexcept {
    return reverse_iterator(end());
  }
  constexpr reverse_iterator rend() const noexcept {
    return reverse_iterator(begin());
  }

  constexpr const_reverse_iterator crbegin() const noexcept {
    return const_reverse_iterator(cend());
  }
  constexpr const_reverse_iterator crend() const noexcept {
    return const_reverse_iterator(cbegin());
  }

 private:
  template <typename U>
  friend constexpr span<U> make_span(U* data, size_t size) noexcept;

  InternalPtr data_ = nullptr;
  size_t size_ = 0;
};

// Type-deducing helpers for constructing a span.
template <typename T>
UNSAFE_BUFFER_USAGE constexpr span<T> make_span(T* data, size_t size) noexcept {
  return UNSAFE_BUFFERS(span<T>(data, size));
}

template <typename T, size_t N>
constexpr span<T> make_span(T (&array)[N]) noexcept {
  return span<T>(array);
}

template <typename T, size_t N>
constexpr span<T> make_span(std::array<T, N>& array) noexcept {
  return span<T>(array);
}

template <typename Container,
          typename T = typename Container::value_type,
          typename = internal::EnableIfSpanCompatibleContainer<Container, T>>
constexpr span<T> make_span(Container& container) {
  return span<T>(container);
}

template <
    typename Container,
    typename T = typename std::add_const<typename Container::value_type>::type,
    typename = internal::EnableIfConstSpanCompatibleContainer<Container, T>>
constexpr span<T> make_span(const Container& container) {
  return span<T>(container);
}

// [span.objectrep], views of object representation
template <typename T, size_t N, typename P>
span<const uint8_t> as_bytes(span<T, N, P> s) noexcept {
  // SAFETY: from size_bytes() method.
  return UNSAFE_BUFFERS(
      make_span(reinterpret_cast<const uint8_t*>(s.data()), s.size_bytes()));
}

template <typename T,
          size_t N,
          typename P,
          typename U = typename std::enable_if<!std::is_const<T>::value>::type>
span<uint8_t> as_writable_bytes(span<T, N, P> s) noexcept {
  // SAFETY: from size_bytes() method.
  return UNSAFE_BUFFERS(
      make_span(reinterpret_cast<uint8_t*>(s.data()), s.size_bytes()));
}

template <typename T, size_t N, typename P>
span<const char> as_chars(span<T, N, P> s) noexcept {
  // SAFETY: from size_bytes() method.
  return UNSAFE_BUFFERS(
      make_span(reinterpret_cast<const char*>(s.data()), s.size_bytes()));
}

template <typename T,
          size_t N,
          typename P,
          typename U = typename std::enable_if<!std::is_const<T>::value>::type>
span<char> as_writable_chars(span<T, N, P> s) noexcept {
  // SAFETY: from size_bytes() method.
  return UNSAFE_BUFFERS(
      make_span(reinterpret_cast<char*>(s.data()), s.size_bytes()));
}

// `span_from_ref` converts a reference to T into a span of length 1.  This is a
// non-std helper that is inspired by the `std::slice::from_ref()` function from
// Rust.
template <typename T>
static constexpr span<T> span_from_ref(T& single_object) noexcept {
  // SAFETY: single object passed by reference.
  return UNSAFE_BUFFERS(make_span<T>(&single_object, 1u));
}

// `byte_span_from_ref` converts a reference to T into a span of uint8_t of
// length sizeof(T).  This is a non-std helper that is a sugar for
// `as_writable_bytes(span_from_ref(x))`.
template <typename T>
static constexpr span<const uint8_t> byte_span_from_ref(
    const T& single_object) noexcept {
  return as_bytes(span_from_ref(single_object));
}
template <typename T>
static constexpr span<uint8_t> byte_span_from_ref(T& single_object) noexcept {
  return as_writable_bytes(span_from_ref(single_object));
}

// Convenience function for converting an object which is itself convertible
// to span into a span of bytes (i.e. span of const uint8_t). Typically used
// to convert std::string or string-objects holding chars, or std::vector
// or vector-like objects holding other scalar types, prior to passing them
// into an API that requires byte spans.
template <typename T>
span<const uint8_t> as_byte_span(const T& arg) {
  return as_bytes(make_span(arg));
}
template <typename T>
span<const uint8_t> as_byte_span(T&& arg) {
  return as_bytes(make_span(arg));
}

// Convenience function for converting an object which is itself convertible
// to span into a span of mutable bytes (i.e. span of uint8_t). Typically used
// to convert std::string or string-objects holding chars, or std::vector
// or vector-like objects holding other scalar types, prior to passing them
// into an API that requires mutable byte spans.
template <typename T>
constexpr span<uint8_t> as_writable_byte_span(T&& arg) {
  return as_writable_bytes(make_span(arg));
}

}  // namespace pdfium

#endif  // CORE_FXCRT_SPAN_H_
