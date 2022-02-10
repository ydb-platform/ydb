#include "consumer.h"

#include <library/cpp/yson_pull/detail/macros.h>

using namespace NYsonPull;

void IConsumer::OnScalar(const TScalar& value) {
    switch (value.Type()) {
        case EScalarType::Entity:
            OnEntity();
            break;

        case EScalarType::Boolean:
            OnScalarBoolean(value.AsBoolean());
            break;

        case EScalarType::Int64:
            OnScalarInt64(value.AsInt64());
            break;

        case EScalarType::UInt64:
            OnScalarUInt64(value.AsUInt64());
            break;

        case EScalarType::Float64:
            OnScalarFloat64(value.AsFloat64());
            break;

        case EScalarType::String:
            OnScalarString(value.AsString());
            break;

        default:
            Y_UNREACHABLE();
    }
}

void IConsumer::OnEvent(const TEvent& value) {
    switch (value.Type()) {
        case EEventType::BeginStream:
            OnBeginStream();
            break;

        case EEventType::EndStream:
            OnEndStream();
            break;

        case EEventType::BeginList:
            OnBeginList();
            break;

        case EEventType::EndList:
            OnEndList();
            break;

        case EEventType::BeginMap:
            OnBeginMap();
            break;

        case EEventType::Key:
            OnKey(value.AsString());
            break;

        case EEventType::EndMap:
            OnEndMap();
            break;

        case EEventType::BeginAttributes:
            OnBeginAttributes();
            break;

        case EEventType::EndAttributes:
            OnEndAttributes();
            break;

        case EEventType::Scalar:
            OnScalar(value.AsScalar());
            break;

        default:
            Y_UNREACHABLE();
    }
}
