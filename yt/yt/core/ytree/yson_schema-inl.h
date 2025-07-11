#ifndef YSON_SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_schema.h"
// For the sake of sane code completion.
#include "yson_schema.h"
#endif

#include "fluent.h"

#include "proto_yson_struct.h"

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/misc/enum.h>

#include <optional>
#include <type_traits>

namespace NYT::NYTree::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CEnum = TEnumTraits<T>::IsEnum;

template <class T>
concept CNullable = std::is_same_v<T, std::unique_ptr<typename T::value_type>> ||
    std::is_same_v<T, std::shared_ptr<typename T::value_type>> ||
    std::is_same_v<T, std::optional<typename T::value_type>>;

template <class T>
concept CTuple = requires {
    std::tuple_size<T>::value;
};

// To remove ambiguous behaviour for std::array.
// Array is handling as tuple
template <class T>
concept CArray = std::ranges::range<T> && !CTuple<T>;

template <class T>
concept CAssociativeArray = CArray<T> && NMpl::CMapping<T>;

template <class T>
concept CHasWriteSchema = requires (
    const T& parameter,
    NYson::IYsonConsumer* consumer)
{
    parameter.WriteSchema(consumer);
};

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
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(std::string, string)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(std::string_view, string)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TInstant, datetime)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TDuration, interval)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TGuid, guid)

#undef DEFINE_SCHEMA_FOR_SIMPLE_TYPE

template <CEnum T>
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

template <CHasWriteSchema T>
void WriteSchemaForNull(NYson::IYsonConsumer* consumer)
{
    if constexpr (std::is_same_v<T, TYsonStruct>) {
       // It is not allowed to instantiate object of type `TYsonStruct`.
       BuildYsonFluently(consumer)
            .BeginMap()
                .Item("type_name").Value("struct")
                .Item("members").BeginList().EndList()
            .EndMap();
    } else {
         New<T>()->WriteSchema(consumer);
    }
}

template <CHasWriteSchema T>
void WriteSchema(const NYT::TIntrusivePtr<T>& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .Item("item").Do([&] (auto fluent) {
                if (value) {
                    value->WriteSchema(fluent.GetConsumer());
                } else {
                    WriteSchemaForNull<T>(fluent.GetConsumer());
                }
            })
        .EndMap();
}

template <CHasWriteSchema T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    return value.WriteSchema(consumer);
}

template <CProtobufMessageAsYson T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer)
{
    return NYson::WriteSchema(NYson::ReflectProtobufMessageType<T>(), consumer);
}

template <CProtobufMessageAsString T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer)
{
    return WriteSchema(TStringBuf(), consumer);
}

template <CArray T>
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

template <CTuple T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("tuple")
            .Item("elements")
                .DoList([&] (auto fluent) {
                    std::apply(
                        [&] (auto&&... args) {
                            (fluent.Item()
                                .BeginMap()
                                    .Item("type").Do([&] (auto fluent) {
                                        WriteSchema(args, fluent.GetConsumer());
                                    })
                                .EndMap(), ...);
                        },
                        value);
                })
        .EndMap();
}

template <CAssociativeArray T>
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

template <CNullable T>
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
