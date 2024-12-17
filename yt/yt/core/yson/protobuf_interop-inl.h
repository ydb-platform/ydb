#ifndef PROTOBUF_INTEROP_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_interop.h"
// For the sake of sane code completion.
#include "protobuf_interop.h"
#endif

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <class T>
const TProtobufMessageType* ReflectProtobufMessageType()
{
    static const auto* type = ReflectProtobufMessageType(T::default_instance().GetDescriptor());
    return type;
}

////////////////////////////////////////////////////////////////////////////////

template <class ProtoType, class Type, bool UseParseOptionsInSerialize>
static const void* DoRegisterIntermediateProtoInteropRepresentation() {
    NYT::NYson::AddProtobufConverterRegisterAction([] {
        auto* descriptor = ProtoType::default_instance().GetDescriptor();
        NYT::NYson::TProtobufMessageConverter converter;
        converter.Serializer = [] (
            NYT::NYson::IYsonConsumer* consumer,
            const google::protobuf::Message* message,
            const NYson::TProtobufParserOptions& parseOptions = {})
            {
                const auto* typedMessage = dynamic_cast<const ProtoType*>(message);
                YT_VERIFY(typedMessage);
                Type value;
                FromProto(&value, *typedMessage);
                if constexpr (UseParseOptionsInSerialize) {
                    Serialize(value, consumer, parseOptions);
                } else {
                    Serialize(value, consumer);
                }
            };
        converter.Deserializer = [] (google::protobuf::Message* message, const NYT::NYTree::INodePtr& node) {
            auto* typedMessage = dynamic_cast<ProtoType*>(message);
            YT_VERIFY(typedMessage);
            Type value;
            Deserialize(value, node);
            ToProto(typedMessage, value);
        };
        NYT::NYson::RegisterCustomProtobufConverter(descriptor, converter);
    });
    return nullptr;
};

////////////////////////////////////////////////////////////////////////////////

std::optional<int> FindProtobufEnumValueByLiteralUntyped(
    const TProtobufEnumType* type,
    TStringBuf literal);
TStringBuf FindProtobufEnumLiteralByValueUntyped(
    const TProtobufEnumType* type,
    int value);
int ConvertToProtobufEnumValueUntyped(
    const TProtobufEnumType* type,
    const NYTree::INodePtr& node);

template <class T>
std::optional<T> FindProtobufEnumValueByLiteral(
    const TProtobufEnumType* type,
    TStringBuf literal)
{
    auto untyped = FindProtobufEnumValueByLiteralUntyped(type, literal);
    return untyped ? static_cast<T>(*untyped) : std::optional<T>();
}

template <class T>
TStringBuf FindProtobufEnumLiteralByValue(
    const TProtobufEnumType* type,
    T value)
{
    return FindProtobufEnumLiteralByValueUntyped(type, static_cast<int>(value));
}

template <class T>
T ConvertToProtobufEnumValue(
    const TProtobufEnumType* type,
    const NYTree::INodePtr& node)
{
    return static_cast<T>(ConvertToProtobufEnumValueUntyped(type, node));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
