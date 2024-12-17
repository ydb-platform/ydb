//
// Copyright 2017 The Abseil Authors.
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

#ifndef Y_ABSL_STRINGS_INTERNAL_RESIZE_UNINITIALIZED_H_
#define Y_ABSL_STRINGS_INTERNAL_RESIZE_UNINITIALIZED_H_

#include <algorithm>
#include <util/generic/string.h>
#include <type_traits>
#include <utility>

#include "y_absl/base/port.h"
#include "y_absl/meta/type_traits.h"  //  for void_t

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace strings_internal {

// In this type trait, we look for a __resize_default_init member function, and
// we use it if available, otherwise, we use resize. We provide HasMember to
// indicate whether __resize_default_init is present.
template <typename string_type, typename = void>
struct ResizeUninitializedTraits {
  using HasMember = std::false_type;
  static void Resize(string_type* s, size_t new_size) { s->resize(new_size); }
};

// __resize_default_init is provided by libc++ >= 8.0
template <typename string_type>
struct ResizeUninitializedTraits<
    string_type, y_absl::void_t<decltype(std::declval<string_type&>()
                                           .__resize_default_init(237))> > {
  using HasMember = std::true_type;
  static void Resize(string_type* s, size_t new_size) {
    s->__resize_default_init(new_size);
  }
};

template <typename string_type>
struct ResizeUninitializedTraits<
    string_type, y_absl::void_t<decltype(std::declval<string_type&>()
                                           .ReserveAndResize(237))> > {
  using HasMember = std::true_type;
  static void Resize(string_type* s, size_t new_size) {
    s->ReserveAndResize(new_size);
  }
};

// Returns true if the TString implementation supports a resize where
// the new characters added to the TString are left untouched.
//
// (A better name might be "STLStringSupportsUninitializedResize", alluding to
// the previous function.)
template <typename string_type>
inline constexpr bool STLStringSupportsNontrashingResize(string_type*) {
  return ResizeUninitializedTraits<string_type>::HasMember::value;
}

// Like str->resize(new_size), except any new characters added to "*str" as a
// result of resizing may be left uninitialized, rather than being filled with
// '0' bytes. Typically used when code is then going to overwrite the backing
// store of the TString with known data.
template <typename string_type, typename = void>
inline void STLStringResizeUninitialized(string_type* s, size_t new_size) {
  ResizeUninitializedTraits<string_type>::Resize(s, new_size);
}

// Used to ensure exponential growth so that the amortized complexity of
// increasing the string size by a small amount is O(1), in contrast to
// O(str->size()) in the case of precise growth.
template <typename string_type>
void STLStringReserveAmortized(string_type* s, size_t new_size) {
  const size_t cap = s->capacity();
  if (new_size > cap) {
    // Make sure to always grow by at least a factor of 2x.
    s->reserve((std::max)(new_size, 2 * cap));
  }
}

// In this type trait, we look for an __append_default_init member function, and
// we use it if available, otherwise, we use append.
template <typename string_type, typename = void>
struct AppendUninitializedTraits {
  static void Append(string_type* s, size_t n) {
    s->append(n, typename string_type::value_type());
  }
};

template <typename string_type>
struct AppendUninitializedTraits<
    string_type, y_absl::void_t<decltype(std::declval<string_type&>()
                                           .__append_default_init(237))> > {
  static void Append(string_type* s, size_t n) {
    s->__append_default_init(n);
  }
};

template <typename string_type>
struct AppendUninitializedTraits<
    string_type, y_absl::void_t<decltype(std::declval<string_type&>()
                                           .ReserveAndResize(237))> > {
  static void Append(string_type* s, size_t n) {
    s->ReserveAndResize(s->size() + n);
  }
};

// Like STLStringResizeUninitialized(str, new_size), except guaranteed to use
// exponential growth so that the amortized complexity of increasing the string
// size by a small amount is O(1), in contrast to O(str->size()) in the case of
// precise growth.
template <typename string_type>
void STLStringResizeUninitializedAmortized(string_type* s, size_t new_size) {
  const size_t size = s->size();
  if (new_size > size) {
    AppendUninitializedTraits<string_type>::Append(s, new_size - size);
  } else {
    s->erase(new_size);
  }
}

}  // namespace strings_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_STRINGS_INTERNAL_RESIZE_UNINITIALIZED_H_
