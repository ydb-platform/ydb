#pragma once

#include "public.h"

#include "pull_parser.h"
#include "protobuf_interop.h"

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void SkipAttributes(TYsonPullParserCursor* cursor);
void MaybeSkipAttributes(TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename T, typename = void>
struct TIsPullParserDeserializable
    : public std::false_type
{ };

template <typename T>
struct TIsPullParserDeserializable<T, std::void_t<decltype(Deserialize(std::declval<T&>(), (NYson::TYsonPullParserCursor*)(nullptr)))>>
    : public std::true_type
{ };

template <typename T>
struct TRemoveConst
{
    using Type = T;
};

template <typename K, typename V>
struct TRemoveConst<std::pair<const K, V>>
{
    using Type = std::pair<K, V>;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename ...Ts>
constexpr bool ArePullParserDeserializable()
{
    return (... && NDetail::TIsPullParserDeserializable<Ts>::value);
}

////////////////////////////////////////////////////////////////////////////////

// integers
void Deserialize(signed char& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned char& value, TYsonPullParserCursor* cursor);
void Deserialize(short& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned short& value, TYsonPullParserCursor* cursor);
void Deserialize(int& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned& value, TYsonPullParserCursor* cursor);
void Deserialize(long& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned long& value, TYsonPullParserCursor* cursor);
void Deserialize(long long& value, TYsonPullParserCursor* cursor);
void Deserialize(unsigned long long& value, TYsonPullParserCursor* cursor);

// double
void Deserialize(double& value, TYsonPullParserCursor* cursor);

// std::string
void Deserialize(std::string& value, TYsonPullParserCursor* cursor);

// TString
void Deserialize(TString& value, TYsonPullParserCursor* cursor);

// bool
void Deserialize(bool& value, TYsonPullParserCursor* cursor);

// char
void Deserialize(char& value, TYsonPullParserCursor* cursor);

// TDuration
void Deserialize(TDuration& value, TYsonPullParserCursor* cursor);

// TInstant
void Deserialize(TInstant& value, TYsonPullParserCursor* cursor);

// TGuid.
void Deserialize(TGuid& value, TYsonPullParserCursor* cursor);

// std::vector.
template <class T, class A>
void Deserialize(
    std::vector<T, A>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

// std::deque
template <class T, class A>
void Deserialize(
    std::deque<T, A>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

// std::optional.
template <class T>
void Deserialize(
    std::optional<T>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

// Enum.
template <class T>
    requires TEnumTraits<T>::IsEnum
void Deserialize(T& value, TYsonPullParserCursor* cursor);

// TCompactVector.
template <class T, size_t N>
void Deserialize(
    TCompactVector<T, N>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

// std::pair.
template <class F, class S>
void Deserialize(
    std::pair<F, S>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<F, S>(), void*> = nullptr);

// std::array.
template <class T, size_t N>
void Deserialize(
    std::array<T, N>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

// std::tuple.
template <class... T>
void Deserialize(
    std::tuple<T...>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T...>(), void*> = nullptr);

// For any associative container.
template <template<typename...> class C, class... T, class K = typename C<T...>::key_type>
void Deserialize(
    C<T...>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<typename NDetail::TRemoveConst<typename C<T...>::value_type>::Type>(), void*> = nullptr);

template <class E, class T, E Min, E Max>
void Deserialize(
    TEnumIndexedArray<E, T, Min, Max>& vector,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*> = nullptr);

template <class T>
void Deserialize(TIntrusivePtr<T>& value, TYsonPullParserCursor* cursor);

template <class T>
void Deserialize(std::unique_ptr<T>& value, TYsonPullParserCursor* cursor);

template <class T, class TTag>
void Deserialize(TStrongTypedef<T, TTag>& value, TYsonPullParserCursor* cursor);

void DeserializeProtobufMessage(
    google::protobuf::Message& message,
    const NYson::TProtobufMessageType* type,
    NYson::TYsonPullParserCursor* cursor,
    const NYson::TProtobufWriterOptions& options = {});

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
void Deserialize(
    T& message,
    NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

template <typename TTo>
TTo ExtractTo(TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PULL_PARSER_DESERIALIZE_INL_H_
#include "pull_parser_deserialize-inl.h"
#undef PULL_PARSER_DESERIALIZE_INL_H_
