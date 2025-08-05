#include "pull_parser_deserialize.h"

#include "protobuf_interop.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/yson/consumer.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// integers
template <typename T>
void DeserializeInteger(T& value, TYsonPullParserCursor* cursor, TStringBuf typeName)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::Int64Value:
            value = CheckedIntegralCast<T>((*cursor)->UncheckedAsInt64());
            cursor->Next();
            break;
        case EYsonItemType::Uint64Value:
            value = CheckedIntegralCast<T>((*cursor)->UncheckedAsUint64());
            cursor->Next();
            break;
        case EYsonItemType::BeginAttributes:
            SkipAttributes(cursor);
            DeserializeInteger(value, cursor, typeName);
            break;
        default:
            ThrowUnexpectedYsonTokenException(
                typeName,
                *cursor,
                {EYsonItemType::Int64Value, EYsonItemType::Uint64Value});
    }
}

#define DESERIALIZE(type) \
    void Deserialize(type& value, TYsonPullParserCursor* cursor) \
    { \
        DeserializeInteger(value, cursor, TStringBuf(#type)); \
    }

DESERIALIZE(signed char)
DESERIALIZE(short)
DESERIALIZE(int)
DESERIALIZE(long)
DESERIALIZE(long long)
DESERIALIZE(unsigned char)
DESERIALIZE(unsigned short)
DESERIALIZE(unsigned)
DESERIALIZE(unsigned long)
DESERIALIZE(unsigned long long)

#undef DESERIALIZE

// double
void Deserialize(double& value, TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::Int64Value:
            // Allow integers to be deserialized into doubles.
            value = (*cursor)->UncheckedAsInt64();
            cursor->Next();
            break;
        case EYsonItemType::Uint64Value:
            value = (*cursor)->UncheckedAsUint64();
            cursor->Next();
            break;
        case EYsonItemType::DoubleValue:
            value = (*cursor)->UncheckedAsDouble();
            cursor->Next();
            break;
        case EYsonItemType::BeginAttributes:
            SkipAttributes(cursor);
            Deserialize(value, cursor);
            break;
        default:
            ThrowUnexpectedYsonTokenException(
                "double",
                *cursor,
                {EYsonItemType::Int64Value, EYsonItemType::Uint64Value, EYsonItemType::DoubleValue});
    }
}

// std::string
void Deserialize(std::string& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("string", *cursor, EYsonItemType::StringValue);
    value = (*cursor)->UncheckedAsString();
    cursor->Next();
}

// TString
void Deserialize(TString& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("string", *cursor, EYsonItemType::StringValue);
    value = (*cursor)->UncheckedAsString();
    cursor->Next();
}

// bool
void Deserialize(bool& value, TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::BooleanValue:
            value = (*cursor)->UncheckedAsBoolean();
            cursor->Next();
            break;
        case EYsonItemType::StringValue:
            value = ParseBool(TString((*cursor)->UncheckedAsString()));
            cursor->Next();
            break;
        case EYsonItemType::Int64Value: {
            auto intValue = (*cursor)->UncheckedAsInt64();
            if (intValue != 0 && intValue != 1) {
                THROW_ERROR_EXCEPTION("Expected 0 or 1 but found %v", intValue);
            }
            value = static_cast<bool>(intValue);
            cursor->Next();
            break;
        }
        case EYsonItemType::Uint64Value: {
            auto uintValue = (*cursor)->UncheckedAsUint64();
            if (uintValue != 0 && uintValue != 1) {
                THROW_ERROR_EXCEPTION("Expected 0 or 1 but found %v", uintValue);
            }
            value = static_cast<bool>(uintValue);
            cursor->Next();
            break;
        }
        case EYsonItemType::BeginAttributes:
            SkipAttributes(cursor);
            Deserialize(value, cursor);
            break;
        default:
            ThrowUnexpectedYsonTokenException(
                "bool",
                *cursor,
                {EYsonItemType::BooleanValue, EYsonItemType::StringValue});
    }
}

// char
void Deserialize(char& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("char", *cursor, EYsonItemType::StringValue);
    auto stringValue = (*cursor)->UncheckedAsString();
    if (stringValue.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected string of length 1 but found of length %v", stringValue.size());
    }
    value = stringValue[0];
    cursor->Next();
}

// TDuration
void Deserialize(TDuration& value, TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::Int64Value:
            value = TDuration::MilliSeconds((*cursor)->UncheckedAsInt64());
            cursor->Next();
            break;

        case EYsonItemType::Uint64Value:
            value = TDuration::MilliSeconds((*cursor)->UncheckedAsUint64());
            cursor->Next();
            break;

        case EYsonItemType::StringValue:
            value = TDuration::Parse((*cursor)->UncheckedAsString());
            cursor->Next();
            break;

        case EYsonItemType::DoubleValue: {
            auto ms = (*cursor)->UncheckedAsDouble();
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Duration cannot be negative");
            }
            value = TDuration::MicroSeconds(static_cast<ui64>(ms * 1000.0));
            cursor->Next();
            break;
        }

        case EYsonItemType::BeginAttributes:
            SkipAttributes(cursor);
            Deserialize(value, cursor);
            break;


        default:
            ThrowUnexpectedYsonTokenException(
                "TDuration",
                *cursor,
                {EYsonItemType::Int64Value, EYsonItemType::Uint64Value});
    }
}

// TInstant.
void Deserialize(TInstant& value, TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::Int64Value:
            value = TInstant::MilliSeconds((*cursor)->UncheckedAsInt64());
            cursor->Next();
            break;

        case EYsonItemType::Uint64Value:
            value = TInstant::MilliSeconds((*cursor)->UncheckedAsUint64());
            cursor->Next();
            break;

        case EYsonItemType::StringValue:
            value = TInstant::ParseIso8601((*cursor)->UncheckedAsString());
            cursor->Next();
            break;

        case EYsonItemType::DoubleValue: {
            auto ms = (*cursor)->UncheckedAsDouble();
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Duration cannot be negative");
            }
            value = TInstant::MicroSeconds(static_cast<ui64>(ms * 1000.0));
            cursor->Next();
            break;
        }

        case EYsonItemType::BeginAttributes:
            SkipAttributes(cursor);
            Deserialize(value, cursor);
            break;

        default:
            ThrowUnexpectedYsonTokenException(
                "TInstant",
                *cursor,
                {EYsonItemType::Int64Value, EYsonItemType::Uint64Value, EYsonItemType::StringValue});
    }
}

// TGuid.
void Deserialize(TGuid& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    EnsureYsonToken("GUID", *cursor, EYsonItemType::StringValue);
    value = TGuid::FromString((*cursor)->UncheckedAsString());
    cursor->Next();
}

void DeserializeProtobufMessage(
    google::protobuf::Message& message,
    const TProtobufMessageType* type,
    NYson::TYsonPullParserCursor* cursor,
    const NYson::TProtobufWriterOptions& options)
{
    TProtobufString wireBytes;
    google::protobuf::io::StringOutputStream outputStream(&wireBytes);
    auto protobufWriter = CreateProtobufWriter(&outputStream, type, options);

    cursor->TransferComplexValue(protobufWriter.get());

    if (!message.ParseFromArray(wireBytes.data(), wireBytes.size())) {
        THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
            message.GetTypeName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
