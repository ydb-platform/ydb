#pragma once

#include "public.h"

#include "protobuf_interop_options.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <variant>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! An opaque reflected counterpart of ::google::protobuf::Descriptor.
/*!
 *  Reflecting a descriptor takes the following options into account:
 *  NYT.NProto.NYson.field_name:      overrides the default name of field
 *  NYT.NProto.NYson.enum_value_name: overrides the default name of enum value
 */
class TProtobufMessageType;

//! An opaque reflected counterpart of ::google::protobuf::EnumDescriptor.
class TProtobufEnumType;

//! Reflects ::google::protobuf::Descriptor.
/*!
 *  The call caches its result in a static variable and is thus efficient.
 */
template <class T>
const TProtobufMessageType* ReflectProtobufMessageType();

//! Reflects ::google::protobuf::Descriptor.
/*!
 *  The call invokes the internal reflection registry and takes spinlocks.
 *  Should not be assumed to be efficient.
 */
const TProtobufMessageType* ReflectProtobufMessageType(const ::google::protobuf::Descriptor* descriptor);

//! Reflects ::google::protobuf::EnumDescriptor.
/*!
 *  The call invokes the internal reflection registry and takes spinlocks.
 *  Should not be assumed to be efficient.
 */
const TProtobufEnumType* ReflectProtobufEnumType(const ::google::protobuf::EnumDescriptor* descriptor);

//! Extracts the underlying ::google::protobuf::Descriptor from a reflected instance.
const ::google::protobuf::Descriptor* UnreflectProtobufMessageType(const TProtobufMessageType* type);

//! Extracts the underlying ::google::protobuf::EnumDescriptor from a reflected instance.
const ::google::protobuf::EnumDescriptor* UnreflectProtobufMessageType(const TProtobufEnumType* type);

////////////////////////////////////////////////////////////////////////////////

struct TProtobufMessageElement;
struct TProtobufScalarElement;
struct TProtobufAttributeDictionaryElement;
struct TProtobufRepeatedElement;
struct TProtobufMapElement;
struct TProtobufAnyElement;

using TProtobufElement = std::variant<
    std::unique_ptr<TProtobufMessageElement>,
    std::unique_ptr<TProtobufScalarElement>,
    std::unique_ptr<TProtobufAttributeDictionaryElement>,
    std::unique_ptr<TProtobufRepeatedElement>,
    std::unique_ptr<TProtobufMapElement>,
    std::unique_ptr<TProtobufAnyElement>
>;

struct TProtobufMessageElement
{
    const TProtobufMessageType* Type;
};

struct TProtobufScalarElement
{
    YT_DEFINE_STRONG_TYPEDEF(TType, int);
    TType Type;

    // Meaningful only when TYPE == TYPE_ENUM.
    EEnumYsonStorageType EnumStorageType;
};

struct TProtobufAttributeDictionaryElement
{
    // The actual message type containing attribute_dictionary extension.
    const TProtobufMessageType* Type;
};

struct TProtobufRepeatedElement
{
    TProtobufElement Element;
};

struct TProtobufMapElement
{
    TProtobufScalarElement KeyElement;
    TProtobufElement Element;
};

struct TProtobufAnyElement
{
};

struct TProtobufElementResolveResult
{
    TProtobufElement Element;
    TStringBuf HeadPath;
    TStringBuf TailPath;
};

//! Introspects a given #rootType and locates an element (represented
//! by TProtobufElement discriminated union) at a given #path.
//! Throws if some definite error occurs during resolve (i.e. a malformed
//! YPath or a reference to a non-existing field).
TProtobufElementResolveResult ResolveProtobufElementByYPath(
    const TProtobufMessageType* rootType,
    const NYPath::TYPathBuf path,
    const TResolveProtobufElementByYPathOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

constexpr int UnknownYsonFieldNumber = 3005;


//! Creates a YSON consumer that converts IYsonConsumer calls into
//! a byte sequence in protobuf wire format.
/*!
 *  The resulting sequence of bytes is actually fed into the output stream
 *  only at the very end since constructing it involves an additional pass
 *  to compute lengths of nested submessages.
 */
std::unique_ptr<IYsonConsumer> CreateProtobufWriter(
    ::google::protobuf::io::ZeroCopyOutputStream* outputStream,
    const TProtobufMessageType* rootType,
    TProtobufWriterOptions options = TProtobufWriterOptions());

////////////////////////////////////////////////////////////////////////////////

//! Parses a byte sequence and translates it into IYsonConsumer calls.
/*!
 *  IMPORTANT! Due to performance reasons the implementation currently assumes
 *  that the byte sequence obeys the following additional condition (not enforced
 *  by protobuf wire format as it is): for each repeated field, its occurrences
 *  are sequential. This property is always true for byte sequences produced
 *  from message classes.
 *
 *  In case you need to handle generic protobuf sequences, you should extend the
 *  code appropriately and provide a fallback flag (since zero-overhead support
 *  does not seem possible).
 */
void ParseProtobuf(
    IYsonConsumer* consumer,
    ::google::protobuf::io::ZeroCopyInputStream* inputStream,
    const TProtobufMessageType* rootType,
    const TProtobufParserOptions& options = TProtobufParserOptions());

//! Invokes #ParseProtobuf to write #message into #consumer.
void WriteProtobufMessage(
    IYsonConsumer* consumer,
    const ::google::protobuf::Message& message,
    const TProtobufParserOptions& options = TProtobufParserOptions());


//! Given a enum type T, tries to convert a string literal to T.
//! Returns null if the literal is not known.
template <class T>
std::optional<T> FindProtobufEnumValueByLiteral(
    const TProtobufEnumType* type,
    TStringBuf literal);

//! Given a enum type T, tries to convert a value of T to string literals.
//! Returns null if no literal is known for this value.
template <class T>
TStringBuf FindProtobufEnumLiteralByValue(
    const TProtobufEnumType* type,
    T value);

//! Converts a string or integral #node to enum underlying value type T.
//! Throws if #node is not of string or integral type or if #node corresponds to an unknown enum value.
template <class T>
T ConvertToProtobufEnumValue(
    const TProtobufEnumType* type,
    const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

//! This method is assumed to be called during static initialization only.
//! We defer running actions until static protobuf descriptors are ready.
//! Accessing type descriptors during static initialization phase may break
//! descriptors (at least under darwin).
void AddProtobufConverterRegisterAction(std::function<void()> action);

////////////////////////////////////////////////////////////////////////////////

struct TProtobufMessageConverter
{
    std::function<void(IYsonConsumer* consumer, const google::protobuf::Message* message)> Serializer;
    std::function<void(google::protobuf::Message* message, const NYTree::INodePtr& node)> Deserializer;
};

//! This method is called during static initialization and not assumed to be called during runtime.
void RegisterCustomProtobufConverter(
    const google::protobuf::Descriptor* descriptor,
    const TProtobufMessageConverter& converter);

#define REGISTER_INTERMEDIATE_PROTO_INTEROP_REPRESENTATION(ProtoType, Type)                                             \
    YT_ATTRIBUTE_USED static const void* PP_ANONYMOUS_VARIABLE(RegisterIntermediateProtoInteropRepresentation) = [] {   \
        NYT::NYson::AddProtobufConverterRegisterAction([] {                                                             \
            auto* descriptor = ProtoType::default_instance().GetDescriptor();                                           \
            NYT::NYson::TProtobufMessageConverter converter;                                                            \
            converter.Serializer = [] (NYT::NYson::IYsonConsumer* consumer, const google::protobuf::Message* message) { \
                const auto* typedMessage = dynamic_cast<const ProtoType*>(message);                                     \
                YT_VERIFY(typedMessage);                                                                                \
                Type value;                                                                                             \
                FromProto(&value, *typedMessage);                                                                       \
                Serialize(value, consumer);                                                                             \
            };                                                                                                          \
            converter.Deserializer = [] (google::protobuf::Message* message, const NYT::NYTree::INodePtr& node) {       \
                auto* typedMessage = dynamic_cast<ProtoType*>(message);                                                 \
                YT_VERIFY(typedMessage);                                                                                \
                Type value;                                                                                             \
                Deserialize(value, node);                                                                               \
                ToProto(typedMessage, value);                                                                           \
            };                                                                                                          \
            NYT::NYson::RegisterCustomProtobufConverter(descriptor, converter);                                         \
        });                                                                                                             \
        return nullptr;                                                                                                 \
    } ();

////////////////////////////////////////////////////////////////////////////////

struct TProtobufMessageBytesFieldConverter
{
    std::function<void(IYsonConsumer* consumer, TStringBuf bytes)> Serializer;
    std::function<void(TString* bytes, const NYTree::INodePtr& node)> Deserializer;
};

//! This method is called during static initialization and not assumed to be called during runtime.
void RegisterCustomProtobufBytesFieldConverter(
    const google::protobuf::Descriptor* descriptor,
    int fieldNumber,
    const TProtobufMessageBytesFieldConverter& converter);

#define REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(ProtoType, FieldNumber, Type)             \
    static const void* PP_ANONYMOUS_VARIABLE(RegisterIntermediateProtoInterpBytesFieldRepresentation) = [] {     \
        NYT::NYson::AddProtobufConverterRegisterAction([] {                                                      \
            const auto* descriptor = ProtoType::default_instance().GetDescriptor();                              \
            NYT::NYson::TProtobufMessageBytesFieldConverter converter;                                           \
            converter.Serializer = [] (NYT::NYson::IYsonConsumer* consumer, TStringBuf bytes) {                  \
                Type value;                                                                                      \
                FromBytes(&value, bytes);                                                                        \
                Serialize(value, consumer);                                                                      \
            };                                                                                                   \
            converter.Deserializer = [] (TString* bytes, const NYT::NYTree::INodePtr& node) {                    \
                Type value;                                                                                      \
                Deserialize(value, node);                                                                        \
                ToBytes(bytes, value);                                                                           \
            };                                                                                                   \
            NYT::NYson::RegisterCustomProtobufBytesFieldConverter(descriptor, FieldNumber, converter);           \
        });                                                                                                      \
        return nullptr;                                                                                          \
    } ();

////////////////////////////////////////////////////////////////////////////////

TString YsonStringToProto(
    const TYsonString& ysonString,
    const TProtobufMessageType* payloadType,
    EUnknownYsonFieldsMode unknownFieldsMode);

TString YsonStringToProto(
    const TYsonString& ysonString,
    const TProtobufMessageType* payloadType,
    TProtobufWriterOptions options);

////////////////////////////////////////////////////////////////////////////////

void SetProtobufInteropConfig(TProtobufInteropConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

//! Returns type v3 schema for protobuf message type.
//! Note: Recursive types (message has field with self type) are not supported.
void WriteSchema(const TProtobufMessageType* type, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PROTOBUF_INTEROP_INL_H_
#include "protobuf_interop-inl.h"
#undef PROTOBUF_INTEROP_INL_H_
