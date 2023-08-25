#pragma once

#include <algorithm>
#include <array>

#include <google/protobuf/repeated_field.h>

#include <library/cpp/iterator/mapped.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/join.h>

namespace NFq {

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

template <typename TIter, typename TFunc>
TString JoinMapRange(TString delim, const TIter beg, const TIter end, const TFunc func) {
    auto mappedBegin =
        MakeMappedIterator(beg, func);
    auto mappedEnd =
        MakeMappedIterator(end, func);
    return JoinRange(delim, mappedBegin, mappedEnd);
}

TString EscapeString(const TString& value,
                     const TString& enclosingSeq,
                     const TString& replaceWith);

TString EscapeString(const TString& value, char enclosingChar);

TString EncloseAndEscapeString(const TString& value, char enclosingChar);

TString EncloseAndEscapeString(const TString& value,
                               const TString& enclosingSeq,
                               const TString& replaceWith);

}  // namespace NFq
