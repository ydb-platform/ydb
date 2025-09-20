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
inline void WriteSchema(type, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/) \
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
void WriteSchema(const T&, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("tagged")
            .Item("tag").Value(Format("enum/%v", TEnumTraits<T>::GetTypeName()))
            .Item("item").Value("string")
            .Item("enum").DoListFor(
                TEnumTraits<T>::GetDomainNames(), [] (auto fluent, TStringBuf name) {
                    fluent.Item().Value(EncodeEnumValue(name));
                })
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
        .EndMap();
}

template <CHasWriteSchema T>
void WriteSchemaForNull(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    if constexpr (std::is_same_v<T, TYsonStruct>) {
       // It is not allowed to instantiate object of type `TYsonStruct`.
       BuildYsonFluently(consumer)
            .BeginMap()
                .Item("type_name").Value("struct")
                .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                    fluent.Item("cpp_type_name").Value(TypeName<T>());
                })
                .Item("members").BeginList().EndList()
            .EndMap();
    } else {
        New<T>()->WriteSchema(consumer, options);
    }
}

template <class T>
void WriteSchema(const NYT::TIntrusivePtr<T>& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<NYT::TIntrusivePtr<T>>());
            })
            .Item("item").Do([&] (auto fluent) {
                if constexpr (CHasWriteSchema<T>) {
                    if (value) {
                        value->WriteSchema(fluent.GetConsumer(), options);
                    } else {
                        WriteSchemaForNull<T>(fluent.GetConsumer(), options);
                    }
                } else {
                    if (value) {
                        auto node = ConvertToNode(value);
                        fluent.Value(FormatEnum(node->GetType()));
                    } else {
                        fluent.Value(FormatEnum(ENodeType::Entity));
                    }
                }
            })
        .EndMap();
}

template <CHasWriteSchema T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    return value.WriteSchema(consumer, options);
}

template <CProtobufMessageAsYson T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    return NYson::WriteSchema(NYson::ReflectProtobufMessageType<T>(), consumer, options);
}

template <CProtobufMessageAsString T>
void WriteSchema(const T&, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    return WriteSchema(TStringBuf(), consumer, options);
}

template <CArray T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("list")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("item").Do([&] (auto fluent) {
                WriteSchema(
                    std::begin(value) != std::end(value)
                        ? *std::begin(value)
                        : std::decay_t<decltype(*std::begin(value))>{},
                    fluent.GetConsumer(),
                    options);
            })
        .EndMap();
}

template <CTuple T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("tuple")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("elements")
                .DoList([&] (auto fluent) {
                    std::apply(
                        [&] (auto&&... args) {
                            (fluent.Item()
                                .BeginMap()
                                    .Item("type").Do([&] (auto fluent) {
                                        WriteSchema(args, fluent.GetConsumer(), options);
                                    })
                                .EndMap(), ...);
                        },
                        value);
                })
        .EndMap();
}

template <CAssociativeArray T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("dict")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("key").Do([&] (auto fluent) {
                WriteSchema(value.empty() ? typename T::key_type{} : value.begin()->first, fluent.GetConsumer(), options);
            })
            .Item("value").Do([&] (auto fluent) {
                WriteSchema(value.empty() ? typename T::mapped_type{} : value.begin()->second, fluent.GetConsumer(), options);
            })
        .EndMap();
}

template <CNullable T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("item").Do([&] (auto fluent) {
                WriteSchema(value ? *value : std::decay_t<decltype(*value)>{}, fluent.GetConsumer(), options);
            })
        .EndMap();
}

template <class T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    auto node = ConvertToNode(value);
    BuildYsonFluently(consumer).Value(FormatEnum(node->GetType()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NPrivate
