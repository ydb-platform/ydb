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
