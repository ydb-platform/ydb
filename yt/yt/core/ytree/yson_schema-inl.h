#ifndef YSON_SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_schema.h"
// For the sake of sane code completion.
#include "yson_schema.h"
#endif

#include "fluent.h"

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/misc/enum.h>

#include <optional>
#include <type_traits>

namespace NYT::NYTree::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CIsEnum = TEnumTraits<T>::IsEnum;

template <class T>
concept CIsProtobufMessage = std::derived_from<std::decay_t<T>, google::protobuf::Message>;

template <class T>
concept CIsNullable = std::is_same_v<T, std::unique_ptr<typename T::value_type>> ||
    std::is_same_v<T, std::shared_ptr<typename T::value_type>> ||
    std::is_same_v<T, std::optional<typename T::value_type>>;

template <class T>
concept CIsArray = std::ranges::range<T>;

template <class T>
concept CIsMapping = requires(T) {
    typename T::key_type;
    typename T::mapped_type;
};

template <class T>
concept CIsAssociativeArray = CIsArray<T> && CIsMapping<T>;

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_SCHEMA_FOR_SIMPLE_TYPE(type, name) \
inline void WriteSchema(type, NYson::IYsonConsumer* consumer) \
{ \
    BuildYsonFluently(consumer) \
        .Value(#name); \
}

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(bool, bool)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(char, int8)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(signed char, int8)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(unsigned char, uint8)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(short, int16)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(unsigned short, uint16)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(int, int32)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(unsigned, uint32)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(long, int64)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(unsigned long, uint64)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(long long, int64)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(unsigned long long, uint64)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(double, double)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(float, float)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TString, string)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TStringBuf, string)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TInstant, datetime)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TDuration, interval)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TGuid, guid)

#undef DEFINE_SCHEMA_FOR_SIMPLE_TYPE

template <CIsEnum T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("enum")
            .Item("enum_name").Value(TEnumTraits<T>::GetTypeName())
            .Item("values").DoListFor(
                TEnumTraits<T>::GetDomainNames(), [] (auto fluent, TStringBuf name) {
                    fluent.Item().Value(EncodeEnumValue(name));
                })
        .EndMap();
}

template <CYsonStructDerived T>
void WriteSchema(const NYT::TIntrusivePtr<T>& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .Item("item").Do([&] (auto fluent) {
                (value ? value : New<T>())->WriteSchema(fluent.GetConsumer());
            })
        .EndMap();
}

template <CYsonStructDerived T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    return value.WriteSchema(consumer);
}

template <CIsProtobufMessage T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer)
{
    return NYson::WriteSchema(NYson::ReflectProtobufMessageType<T>(), consumer);
}

template <CIsArray T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("list")
            .Item("item").Do([&] (auto fluent) {
                WriteSchema(
                    std::begin(value) != std::end(value)
                        ? *std::begin(value)
                        : std::decay_t<decltype(*std::begin(value))>{},
                    fluent.GetConsumer());
            })
        .EndMap();
}

template <CIsAssociativeArray T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("dict")
            .Item("key").Do([&] (auto fluent) {
                WriteSchema(value.empty() ? typename T::key_type{} : value.begin()->first, fluent.GetConsumer());
            })
            .Item("value").Do([&] (auto fluent) {
                WriteSchema(value.empty() ? typename T::mapped_type{} : value.begin()->second, fluent.GetConsumer());
            })
        .EndMap();
}

template <CIsNullable T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .Item("item").Do([&] (auto fluent) {
                WriteSchema(value ? *value : std::decay_t<decltype(*value)>{}, fluent.GetConsumer());
            })
        .EndMap();
}

template <class T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    auto node = ConvertToNode(value);
    BuildYsonFluently(consumer).Value(FormatEnum(node->GetType()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NPrivate
