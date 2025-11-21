#ifndef YSON_SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_schema.h"
// For the sake of sane code completion.
#include "yson_schema.h"
#endif

#include "fluent.h"
#include "proto_yson_struct.h"
#include "yson_struct.h"

#include <yt/yt/core/misc/mpl.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/strong_typedef.h>

#include <optional>
#include <type_traits>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CEnum = TEnumTraits<T>::IsEnum;

template <class T>
concept CNullable = NMpl::IsSpecialization<T, std::unique_ptr> ||
    NMpl::IsSpecialization<T, std::shared_ptr> ||
    NMpl::IsSpecialization<T, std::optional> ||
    NMpl::IsSpecialization<T, NYT::TIntrusivePtr>;

template <class T>
concept CYsonStruct = std::derived_from<T, TYsonStruct>;

template <class T>
concept CYsonStructLite = std::derived_from<T, TYsonStructLite>;

template <class T>
concept CTuple = requires {
    std::tuple_size<T>::value;
} && !CYsonStruct<T> && !CYsonStructLite<T>;

template <class T>
concept CStringLike = std::is_same_v<std::decay_t<T>, std::string> ||
    std::is_same_v<std::decay_t<T>, std::string_view> ||
    std::is_same_v<std::decay_t<T>, TString> ||
    std::is_same_v<std::decay_t<T>, TStringBuf>;

// To remove ambiguous behaviour for std::array.
// std::array is handling as tuple
template <class T>
concept CList = std::ranges::range<T> && !CTuple<T> && !CStringLike<T> && !CYsonStruct<T> && !CYsonStructLite<T>;

template <class T>
concept CDict = CList<T> && NMpl::CMapping<T> && !CYsonStruct<T> && !CYsonStructLite<T>;

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_SCHEMA_FOR_SIMPLE_TYPE(type, name) \
template <> \
inline void WriteSchema<type>(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/) \
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

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(NYson::TYsonString, yson);

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TInstant, datetime)
DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TDuration, interval)

DEFINE_SCHEMA_FOR_SIMPLE_TYPE(TGuid, guid)

#undef DEFINE_SCHEMA_FOR_SIMPLE_TYPE

template <CStrongTypedef T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    WriteSchema<typename T::TUnderlying>(consumer, options);
}

template <CEnum T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
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

template <CStringLike T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    BuildYsonFluently(consumer)
        .Value("string");
}

template <CList T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("list")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("item").Do([&] (auto fluent) {
                WriteSchema<std::decay_t<decltype(*std::begin(T{}))>>(
                    fluent.GetConsumer(),
                    options);
            })
        .EndMap();
}

template <CTuple T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
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
                                        WriteSchema<std::decay_t<decltype(args)>>(fluent.GetConsumer(), options);
                                    })
                                .EndMap(), ...);
                        },
                        T{});
                })
        .EndMap();
}

template <CDict T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("dict")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("key").Do([&] (auto fluent) {
                WriteSchema<typename T::key_type>(fluent.GetConsumer(), options);
            })
            .Item("value").Do([&] (auto fluent) {
                WriteSchema<typename T::mapped_type>(fluent.GetConsumer(), options);
            })
        .EndMap();
}

template <CNullable T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("item").Do([&] (auto fluent) {
                WriteSchema<std::decay_t<decltype(*T())>>(fluent.GetConsumer(), options);
            })
        .EndMap();
}

template <CProtobufMessageAsYson T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    return NYson::WriteSchema(NYson::ReflectProtobufMessageType<T>(), consumer, options);
}

template <CProtobufMessageAsString T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    return WriteSchema<TString>(consumer, options);
}

template <CYsonStruct T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    New<T>()->WriteSchema(consumer, options);
}

template <CYsonStructLite T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    T().WriteSchema(consumer, options);
}

// Default implementation
template <class T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("type_name").Value("optional")
            .DoIf(options.AddCppTypeNames, [] (auto fluent) {
                fluent.Item("cpp_type_name").Value(TypeName<T>());
            })
            .Item("item").Value("yson")
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
