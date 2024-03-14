#include "message_format.h"

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NRpc {

using namespace NYson;
using namespace NJson;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

struct IMessageFormat
{
    virtual ~IMessageFormat() = default;

    virtual TSharedRef ConvertFrom(
        const TSharedRef& message,
        const NYson::TProtobufMessageType* messageType,
        const TYsonString& formatOptionsYson) = 0;
    virtual TSharedRef ConvertTo(
        const TSharedRef& message,
        const NYson::TProtobufMessageType* messageType,
        const TYsonString& formatOptionsYson) = 0;
};

////////////////////////////////////////////////////////////////////////////////

THashMap<EMessageFormat, IMessageFormat*>& GetMessageFormatRegistry()
{
    static THashMap<EMessageFormat, IMessageFormat*> Registry;
    return Registry;
}

IMessageFormat* GetMessageFormatOrThrow(EMessageFormat format)
{
    const auto& registry = GetMessageFormatRegistry();
    auto it = registry.find(format);
    if (it == registry.end()) {
        THROW_ERROR_EXCEPTION("Unsupported message format %Qlv",
            format);
    }
    return it->second;
}

void RegisterCustomMessageFormat(EMessageFormat format, IMessageFormat* formatHandler)
{
    YT_VERIFY(!GetMessageFormatRegistry()[format]);
    GetMessageFormatRegistry()[format] = formatHandler;
}

namespace {

class TYsonMessageFormat
    : public IMessageFormat
{
public:
    TYsonMessageFormat()
    {
        RegisterCustomMessageFormat(EMessageFormat::Yson, this);
    }

    TSharedRef ConvertFrom(const TSharedRef& message, const NYson::TProtobufMessageType* messageType, const TYsonString& /*formatOptionsYson*/) override
    {
        TString protoBuffer;
        {
            google::protobuf::io::StringOutputStream output(&protoBuffer);
            auto converter = CreateProtobufWriter(&output, messageType);
            // NB: formatOptionsYson is ignored, since YSON parser has no user-defined options.
            ParseYsonStringBuffer(TStringBuf(message.Begin(), message.End()), EYsonType::Node, converter.get());
        }
        return TSharedRef::FromString(protoBuffer);
    }

    TSharedRef ConvertTo(const TSharedRef& message, const NYson::TProtobufMessageType* messageType, const TYsonString& /*formatOptionsYson*/) override
    {
        google::protobuf::io::ArrayInputStream stream(message.Begin(), message.Size());
        TString ysonBuffer;
        {
            TStringOutput output(ysonBuffer);
            // TODO(ignat): refactor TYsonFormatConfig, move it closer to YSON.
            TYsonWriter writer{&output, EYsonFormat::Text};
            ParseProtobuf(&writer, &stream, messageType);
        }
        return TSharedRef::FromString(ysonBuffer);
    }
} YsonFormat;

class TJsonMessageFormat
    : public IMessageFormat
{
public:
    TJsonMessageFormat()
    {
        RegisterCustomMessageFormat(EMessageFormat::Json, this);
    }

    TSharedRef ConvertFrom(const TSharedRef& message, const NYson::TProtobufMessageType* messageType, const TYsonString& formatOptionsYson) override
    {
        TString protoBuffer;
        {
            google::protobuf::io::StringOutputStream output(&protoBuffer);
            auto converter = CreateProtobufWriter(&output, messageType);
            TMemoryInput input{message.Begin(), message.Size()};
            auto formatConfig = New<TJsonFormatConfig>();
            if (formatOptionsYson) {
                formatConfig->Load(NYTree::ConvertToNode(formatOptionsYson));
            }
            ParseJson(&input, converter.get(), formatConfig);
        }
        return TSharedRef::FromString(protoBuffer);
    }

    TSharedRef ConvertTo(const TSharedRef& message, const NYson::TProtobufMessageType* messageType, const TYsonString& formatOptionsYson) override
    {
        google::protobuf::io::ArrayInputStream stream(message.Begin(), message.Size());
        TString ysonBuffer;
        {
            TStringOutput output(ysonBuffer);
            auto formatConfig = New<TJsonFormatConfig>();
            if (formatOptionsYson) {
                formatConfig->Load(NYTree::ConvertToNode(formatOptionsYson));
            }
            auto writer = CreateJsonConsumer(&output, EYsonType::Node, formatConfig);
            ParseProtobuf(writer.get(), &stream, messageType);
            writer->Flush();
        }
        return TSharedRef::FromString(ysonBuffer);
    }
} JsonFormat;

} // namespace

TSharedRef ConvertMessageToFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const TProtobufMessageType* messageType,
    const TYsonString& formatOptionsYson)
{
    return GetMessageFormatOrThrow(format)->ConvertTo(message, messageType, formatOptionsYson);
}

// COMPAT(danilalexeev): legacy RPC codecs
TSharedRef ConvertMessageFromFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const TProtobufMessageType* messageType,
    const TYsonString& formatOptionsYson,
    bool enveloped)
{
    auto result = GetMessageFormatOrThrow(format)->ConvertFrom(
        enveloped ? PopEnvelope(message) : message,
        messageType,
        formatOptionsYson);
    return enveloped ? PushEnvelope(result) : result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
