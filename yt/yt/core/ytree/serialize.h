#pragma once

#include "public.h"

#include <yt/yt/core/yson/producer.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/mpl.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::EYsonType GetYsonType(const T&);
NYson::EYsonType GetYsonType(const NYson::TYsonString& yson);
NYson::EYsonType GetYsonType(const NYson::TYsonStringBuf& yson);
NYson::EYsonType GetYsonType(const NYson::TYsonInput& input);
NYson::EYsonType GetYsonType(const NYson::TYsonProducer& producer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void WriteYson(
    IZeroCopyOutput* output,
    const T& value,
    NYson::EYsonType type,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary,
    int indent = 4);

template <class T>
void WriteYson(
    IZeroCopyOutput* output,
    const T& value,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

template <class T>
void WriteYson(
    const NYson::TYsonOutput& output,
    const T& value,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Serialize(T* value, NYson::IYsonConsumer* consumer);

template <class T>
void Serialize(const TIntrusivePtr<T>& value, NYson::IYsonConsumer* consumer);

// integers
void Serialize(signed char value, NYson::IYsonConsumer* consumer);
void Serialize(unsigned char value, NYson::IYsonConsumer* consumer);
#ifdef __cpp_char8_t
void Serialize(char8_t value, NYson::IYsonConsumer* consumer);
#endif
void Serialize(short value, NYson::IYsonConsumer* consumer);
void Serialize(unsigned short value, NYson::IYsonConsumer* consumer);
void Serialize(int value, NYson::IYsonConsumer* consumer);
void Serialize(unsigned value, NYson::IYsonConsumer* consumer);
void Serialize(long value, NYson::IYsonConsumer* consumer);
void Serialize(unsigned long value, NYson::IYsonConsumer* consumer);
void Serialize(long long value, NYson::IYsonConsumer* consumer);
void Serialize(unsigned long long value, NYson::IYsonConsumer* consumer);

// double
void Serialize(double value, NYson::IYsonConsumer* consumer);

// TString
void Serialize(const TString& value, NYson::IYsonConsumer* consumer);

// TStringBuf
void Serialize(TStringBuf value, NYson::IYsonConsumer* consumer);

// const char*
void Serialize(const char* value, NYson::IYsonConsumer* consumer);

// bool
void Serialize(bool value, NYson::IYsonConsumer* consumer);

// char
void Serialize(char value, NYson::IYsonConsumer* consumer);

// TDuration
void Serialize(TDuration value, NYson::IYsonConsumer* consumer);

// TInstant
void Serialize(TInstant value, NYson::IYsonConsumer* consumer);

// TGuid
void Serialize(TGuid value, NYson::IYsonConsumer* consumer);

// IInputStream
void Serialize(IInputStream& input, NYson::IYsonConsumer* consumer);

// Enums
template <class T>
    requires TEnumTraits<T>::IsEnum
void Serialize(T value, NYson::IYsonConsumer* consumer);

// std::optional
template <class T>
void Serialize(const std::optional<T>& value, NYson::IYsonConsumer* consumer);

// std::vector
template <class T, class A>
void Serialize(const std::vector<T, A>& value, NYson::IYsonConsumer* consumer);

// std::deque
template <class T, class A>
void Serialize(const std::deque<T, A>& value, NYson::IYsonConsumer* consumer);

// TCompactVector
template <class T, size_t N>
void Serialize(const TCompactVector<T, N>& value, NYson::IYsonConsumer* consumer);

// RepeatedPtrField
template <class T>
void Serialize(const google::protobuf::RepeatedPtrField<T>& items, NYson::IYsonConsumer* consumer);

// RepeatedField
template <class T>
void Serialize(const google::protobuf::RepeatedField<T>& items, NYson::IYsonConsumer* consumer);

// TErrorOr
template <class T>
void Serialize(const TErrorOr<T>& error, NYson::IYsonConsumer* consumer);

template <class F, class S>
void Serialize(const std::pair<F, S>& value, NYson::IYsonConsumer* consumer);

template <class T, size_t N>
void Serialize(const std::array<T, N>& value, NYson::IYsonConsumer* consumer);

template <class... T>
void Serialize(const std::tuple<T...>& value, NYson::IYsonConsumer* consumer);

// For any associative container.
template <template<typename...> class C, class... T, class K = typename C<T...>::key_type>
void Serialize(const C<T...>& value, NYson::IYsonConsumer* consumer);

// TEnumIndexedArray
template <class E, class T, E Min, E Max>
void Serialize(const TEnumIndexedArray<E, T, Min, Max>& value, NYson::IYsonConsumer* consumer);

// Subtypes of google::protobuf::Message
template <class T>
void Serialize(
    const T& message,
    NYson::IYsonConsumer* consumer,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type* = nullptr);

template <class T, class TTag>
void Serialize(const TStrongTypedef<T, TTag>& value, NYson::IYsonConsumer* consumer);

void Serialize(const NStatisticPath::TStatisticPath& path, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TIntrusivePtr<T>& value, INodePtr node);

template <class T>
void Deserialize(std::unique_ptr<T>& value, INodePtr node);

// integers
void Deserialize(signed char& value, INodePtr node);
void Deserialize(unsigned char& value, INodePtr node);
#ifdef __cpp_char8_t
void Deserialize(char8_t& value, INodePtr node);
#endif
void Deserialize(short& value, INodePtr node);
void Deserialize(unsigned short& value, INodePtr node);
void Deserialize(int& value, INodePtr node);
void Deserialize(unsigned& value, INodePtr node);
void Deserialize(long& value, INodePtr node);
void Deserialize(unsigned long& value, INodePtr node);
void Deserialize(long long& value, INodePtr node);
void Deserialize(unsigned long long& value, INodePtr node);

// double
void Deserialize(double& value, INodePtr node);

// TString
void Deserialize(TString& value, INodePtr node);

// bool
void Deserialize(bool& value, INodePtr node);

// char
void Deserialize(char& value, INodePtr node);

// TDuration
void Deserialize(TDuration& value, INodePtr node);

// TInstant
void Deserialize(TInstant& value, INodePtr node);

// TGuid
void Deserialize(TGuid& value, INodePtr node);

// Enums
template <class T>
    requires TEnumTraits<T>::IsEnum
void Deserialize(T& value, INodePtr node);

// std::optional
template <class T>
void Deserialize(std::optional<T>& value, INodePtr node);

// std::vector
template <class T, class A>
void Deserialize(std::vector<T, A>& value, INodePtr node);

// std::deque
template <class T, class A>
void Deserialize(std::deque<T, A>& value, INodePtr node);

// TCompactVector
template <class T, size_t N>
void Deserialize(TCompactVector<T, N>& value, INodePtr node);

// TErrorOr
template <class T>
void Deserialize(TErrorOr<T>& error, INodePtr node);

template <class T>
void Deserialize(TErrorOr<T>& error, NYson::TYsonPullParserCursor* cursor);

template <class F, class S>
void Deserialize(std::pair<F, S>& value, INodePtr node);

template <class T, size_t N>
void Deserialize(std::array<T, N>& value, INodePtr node);

template <class... T>
void Deserialize(std::tuple<T...>& value, INodePtr node);

// For any associative container.
template <template<typename...> class C, class... T, class K = typename C<T...>::key_type>
void Deserialize(C<T...>& value, INodePtr node);

// TEnumIndexedArray
template <class E, class T, E Min, E Max>
void Deserialize(TEnumIndexedArray<E, T, Min, Max>& vector, INodePtr node);

// Subtypes of google::protobuf::Message
template <class T>
    requires std::derived_from<T, google::protobuf::Message>
void Deserialize(
    T& message,
    const INodePtr& node);

template <class T, class TTag>
void Deserialize(TStrongTypedef<T, TTag>& value, INodePtr node);

void Deserialize(NStatisticPath::TStatisticPath& path, INodePtr node);

////////////////////////////////////////////////////////////////////////////////

template <class T, class... TExtraArgs>
concept CYsonSerializable = requires (const T& value, NYson::IYsonConsumer* consumer, TExtraArgs&&... args)
{
    { Serialize(value, consumer, std::forward<TExtraArgs>(args)...) } -> std::same_as<void>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
