#include "serialize.h"

#include "tree_visitor.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/cast.h>

#include <library/cpp/yt/memory/blob.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NYTree {

using namespace NYson;
using namespace google::protobuf;
using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

// Unversioned values use microsecond precision for TInstant values,
// while YSON deserializes values with millisecond precision.
// It can lead to unexpected results when converting TInstant -> TUnversionedValue -> INodePtr -> TInstant.
// These boundaries allow to correctly deserialize microsecond values back to TInstant.
//
// log2(timeEpoch("2100-01-01") * 10**3) < 42.
// log2(timeEpoch("1970-03-01") * 10**6) > 42.
// log2(timeEpoch("2100-01-01") * 10**6) < 52.
// log2(timeEpoch("1970-03-01") * 10**9) > 52.
// log2(timeEpoch("2100-01-01") * 10**9) < 62.
static constexpr ui64 MicrosecondLowerWidthBoundary = 42;
static constexpr ui64 MicrosecondUpperWidthBoundary = 52;
static constexpr ui64 NanosecondUpperWidthBoundary = 62;
static constexpr ui64 UnixTimeMicrosecondLowerBoundary = 1ull << MicrosecondLowerWidthBoundary;
static constexpr ui64 UnixTimeMicrosecondUpperBoundary = 1ull << MicrosecondUpperWidthBoundary;
static constexpr ui64 UnixTimeNanosecondUpperBoundary = 1ull << NanosecondUpperWidthBoundary;

TInstant ConvertRawValueToUnixTime(ui64 value)
{
    if (value < UnixTimeMicrosecondLowerBoundary) {
        return TInstant::MilliSeconds(value);
    } else if (value < UnixTimeMicrosecondUpperBoundary) {
        return TInstant::MicroSeconds(value);
    } else if (value < UnixTimeNanosecondUpperBoundary) {
        return TInstant::MicroSeconds(value / 1'000);
    } else {
        THROW_ERROR_EXCEPTION("Uint64 value does not represent valid UNIX time")
            << TErrorAttribute("uint64_value", value);
    }
}

////////////////////////////////////////////////////////////////////////////////

EYsonType GetYsonType(const TYsonString& yson)
{
    return yson.GetType();
}

EYsonType GetYsonType(const TYsonStringBuf& yson)
{
    return yson.GetType();
}

EYsonType GetYsonType(const TYsonInput& input)
{
    return input.GetType();
}

EYsonType GetYsonType(const TYsonProducer& producer)
{
    return producer.GetType();
}

////////////////////////////////////////////////////////////////////////////////

// signed integers
#define SERIALIZE(type) \
    void Serialize(type value, IYsonConsumer* consumer) \
    { \
        consumer->OnInt64Scalar(CheckedIntegralCast<i64>(value)); \
    }

SERIALIZE(signed char)
SERIALIZE(short)
SERIALIZE(int)
SERIALIZE(long)
SERIALIZE(long long)


#undef SERIALIZE


// unsigned integers
#define SERIALIZE(type) \
    void Serialize(type value, IYsonConsumer* consumer) \
    { \
        consumer->OnUint64Scalar(CheckedIntegralCast<ui64>(value)); \
    }

SERIALIZE(unsigned char)
#ifdef __cpp_char8_t
SERIALIZE(char8_t)
#endif
SERIALIZE(unsigned short)
SERIALIZE(unsigned)
SERIALIZE(unsigned long)
SERIALIZE(unsigned long long)

#undef SERIALIZE

// double
void Serialize(double value, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

// std::string
void Serialize(const std::string& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// TString
void Serialize(const TString& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// TStringBuf
void Serialize(TStringBuf value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

// const char*
void Serialize(const char* value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(TStringBuf(value));
}

// bool
void Serialize(bool value, IYsonConsumer* consumer)
{
    consumer->OnBooleanScalar(value);
}

// char
void Serialize(char value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(TStringBuf(&value, 1));
}

// TDuration
void Serialize(TDuration value, IYsonConsumer* consumer)
{
    consumer->OnInt64Scalar(value.MilliSeconds());
}

// TInstant
void Serialize(TInstant value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

// TGuid
void Serialize(TGuid value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

// IInputStream
void Serialize(IInputStream& input, IYsonConsumer* consumer)
{
    Serialize(TYsonInput(&input), consumer);
}

// TStatisticPath.
void Serialize(const NStatisticPath::TStatisticPath& path, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(path.Path());
}

// Subtypes of google::protobuf::Message
void SerializeProtobufMessage(
    const Message& message,
    const TProtobufMessageType* type,
    NYson::IYsonConsumer* consumer)
{
    auto byteSize = message.ByteSizeLong();
    struct TProtobufToYsonTag { };
    TBlob wireBytes(GetRefCountedTypeCookie<TProtobufToYsonTag>(), byteSize, false);
    YT_VERIFY(message.SerializePartialToArray(wireBytes.Begin(), byteSize));
    ArrayInputStream inputStream(wireBytes.Begin(), byteSize);
    ParseProtobuf(consumer, &inputStream, type);
}

////////////////////////////////////////////////////////////////////////////////

// integers
#define DESERIALIZE(type) \
    void Deserialize(type& value, INodePtr node) \
    { \
        if (node->GetType() == ENodeType::Int64) { \
            value = CheckedIntegralCast<type>(node->AsInt64()->GetValue()); \
        } else if (node->GetType() == ENodeType::Uint64) { \
            value = CheckedIntegralCast<type>(node->AsUint64()->GetValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("\"" #type "\" cannot be parsed from not integer") \
                << TErrorAttribute("value_type", node->GetType()); \
        } \
    }

DESERIALIZE(signed char)
DESERIALIZE(short)
DESERIALIZE(int)
DESERIALIZE(long)
DESERIALIZE(long long)
DESERIALIZE(unsigned char)
#ifdef __cpp_char8_t
DESERIALIZE(char8_t)
#endif
DESERIALIZE(unsigned short)
DESERIALIZE(unsigned)
DESERIALIZE(unsigned long)
DESERIALIZE(unsigned long long)

#undef DESERIALIZE

// double
void Deserialize(double& value, INodePtr node)
{
    // Allow integer nodes to be serialized into doubles.
    if (node->GetType() == ENodeType::Int64) {
        value = node->AsInt64()->GetValue();
    } else if (node->GetType() == ENodeType::Uint64) {
        value = node->AsUint64()->GetValue();
    } else {
        value = node->AsDouble()->GetValue();
    }
}

// std::string
void Deserialize(std::string& value, INodePtr node)
{
    value = node->AsString()->GetValue();
}

// TString
void Deserialize(TString& value, INodePtr node)
{
    value = node->AsString()->GetValue();
}

// bool
void Deserialize(bool& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Boolean) {
        value = node->AsBoolean()->GetValue();
    } else if (node->GetType() == ENodeType::Int64) {
        auto intValue = node->AsInt64()->GetValue();
        if (intValue != 0 && intValue != 1) {
            THROW_ERROR_EXCEPTION("Bool cannot be parsed from integer other than 0 or 1")
                << TErrorAttribute("integer_value", intValue);
        }
        value = static_cast<bool>(intValue);
    } else if (node->GetType() == ENodeType::Uint64) {
        auto uintValue = node->AsUint64()->GetValue();
        if (uintValue != 0 && uintValue != 1) {
            THROW_ERROR_EXCEPTION("Bool cannot be parsed from integer other than 0 or 1")
                << TErrorAttribute("integer_value", uintValue);
        }
        value = static_cast<bool>(uintValue);
    } else {
        auto stringValue = node->AsString()->GetValue();
        value = ParseBool(stringValue);
    }
}

// char
void Deserialize(char& value, INodePtr node)
{
    TString stringValue = node->AsString()->GetValue();
    if (stringValue.size() != 1) {
        THROW_ERROR_EXCEPTION("Char cannot be parsed from string whose length is not equal to 1")
            << TErrorAttribute("string_length", stringValue.size());
    }
    value = stringValue[0];
}

// TDuration
void Deserialize(TDuration& value, INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::Int64: {
            auto ms = node->AsInt64()->GetValue();
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Duration value cannot be negative")
                    << TErrorAttribute("duration_value", ms);
            }
            value = TDuration::MilliSeconds(static_cast<ui64>(ms));
            break;
        }

        case ENodeType::Uint64: {
            value = TDuration::MilliSeconds(node->AsUint64()->GetValue());
            break;
        }

        case ENodeType::Double: {
            auto ms = node->AsDouble()->GetValue();
            THROW_ERROR_EXCEPTION_IF(!std::isfinite(ms), "Duration must be finite");
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Duration value cannot be negative")
                    << TErrorAttribute("duration_value", ms);
            }
            value = TDuration::MilliSeconds(ms);
            break;
        }

        case ENodeType::String:
            value = TDuration::Parse(node->AsString()->GetValue());
            break;

        default:
            THROW_ERROR_EXCEPTION("Duration cannot be parsed from value of type other than Int64, Uint64, Double or String")
                << TErrorAttribute("value_type", node->GetType());
    }
}

// TInstant
void Deserialize(TInstant& value, INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::Int64: {
            auto ms = CheckedIntegralCast<ui64>(node->AsInt64()->GetValue());
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Instant value cannot be negative")
                    << TErrorAttribute("instant_value", ms);
            }
            value = ConvertRawValueToUnixTime(ms);
            break;
        }

        case ENodeType::Uint64: {
            auto ms = node->AsUint64()->GetValue();
            value = ConvertRawValueToUnixTime(ms);
            break;
        }

        case ENodeType::Double: {
            auto ms = node->AsDouble()->GetValue();
            THROW_ERROR_EXCEPTION_IF(!std::isfinite(ms), "Instant must be finite");
            if (ms < 0) {
                THROW_ERROR_EXCEPTION("Instant value cannot be negative")
                    << TErrorAttribute("instant_value", ms);
            }
            value = ConvertRawValueToUnixTime(ms);
            break;
        }

        case ENodeType::String:
            value = TInstant::ParseIso8601(node->AsString()->GetValue());
            break;

        default:
            THROW_ERROR_EXCEPTION("Instant cannot be parsed from value of type other than Int64, Uint64, Double or String")
                << TErrorAttribute("value_type", node->GetType());
    }
}

// TGuid
void Deserialize(TGuid& value, INodePtr node)
{
    value = TGuid::FromString(node->AsString()->GetValue());
}

// TStatisticPath.
void Deserialize(NStatisticPath::TStatisticPath& value, INodePtr node)
{
    const TString& path = node->AsString()->GetValue();

    // Try to parse slashed paths.
    if (!path.empty() && path.StartsWith('/')) {
        value = NStatisticPath::SlashedStatisticPath(path).ValueOrThrow();
    } else {
        value = NStatisticPath::ParseStatisticPath(path).ValueOrThrow();
    }
}

// Subtypes of google::protobuf::Message
void DeserializeProtobufMessage(
    Message& message,
    const TProtobufMessageType* type,
    const INodePtr& node,
    const NYson::TProtobufWriterOptions& options)
{
    TProtobufString wireBytes;
    StringOutputStream outputStream(&wireBytes);
    auto protobufWriter = CreateProtobufWriter(&outputStream, type, options);
    VisitTree(node, protobufWriter.get(), true);
    if (!message.ParseFromArray(wireBytes.data(), wireBytes.size())) {
        THROW_ERROR_EXCEPTION("Error parsing protobuf message from wire bytes")
            << TErrorAttribute("protobuf_type", message.GetTypeName());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
