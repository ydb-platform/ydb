#pragma once

#include <algorithm>
#include <array>

#include <google/protobuf/repeated_field.h>

#include <util/generic/vector.h>

namespace NYq {

template<std::size_t K, typename T, std::size_t N>
auto CreateArray(const T(&list)[N]) -> std::array<T, K> {
    static_assert(N == K, "not valid array size");
    std::array<T, K> result;
    std::copy(std::begin(list), std::end(list), std::begin(result));
    return result;
}

template <class TElement>
TVector<TElement> VectorFromProto(const ::google::protobuf::RepeatedPtrField<TElement>& field) {
    return { field.begin(), field.end() };
}

}  // namespace NYq
