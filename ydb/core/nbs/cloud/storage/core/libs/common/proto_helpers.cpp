#include "proto_helpers.h"

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/messagext.h>
#include <google/protobuf/text_format.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNullErrorCollector: public google::protobuf::io::ErrorCollector
{
    void AddError(
        int line,
        int column,
        const google::protobuf::string& message) override
    {
        Y_UNUSED(line);
        Y_UNUSED(column);
        Y_UNUSED(message);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TryParseProtoTextFromStringWithoutError(
    const TString& text,
    google::protobuf::Message& dst)
{
    TStringInput in(text);
    NProtoBuf::io::TCopyingInputStreamAdaptor adaptor(&in);
    NProtoBuf::TextFormat::Parser parser;

    TNullErrorCollector nullErrorCollector;
    parser.RecordErrorsTo(&nullErrorCollector);

    if (!parser.Parse(&adaptor, &dst)) {
        // remove everything that may have been read
        dst.Clear();
        return false;
    }

    return true;
}

bool TryParseProtoTextFromString(
    const TString& text,
    google::protobuf::Message& dst)
{
    TStringInput in(text);
    return TryParseFromTextFormat(in, dst);
}

bool TryParseProtoTextFromFile(
    const TString& fileName,
    google::protobuf::Message& dst)
{
    if (!TFsPath(fileName).Exists()) {
        return false;
    }

    TFileInput in(fileName);
    return TryParseFromTextFormat(in, dst);
}

void ParseProtoTextFromString(
    const TString& text,
    google::protobuf::Message& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst);
}

void ParseProtoTextFromFile(
    const TString& fileName,
    google::protobuf::Message& dst)
{
    TFileInput in(fileName);
    ParseFromTextFormat(in, dst);
}

void ParseProtoTextFromStringRobust(
    const TString& text,
    google::protobuf::Message& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst, EParseFromTextFormatOption::AllowUnknownField);
}

void ParseProtoTextFromFileRobust(
    const TString& fileName,
    google::protobuf::Message& dst)
{
    TFileInput in(fileName);
    ParseFromTextFormat(in, dst, EParseFromTextFormatOption::AllowUnknownField);
}

TString ProtoToText(const google::protobuf::Message& proto)
{
    TString s;
    Y_ENSURE(NProtoBuf::TextFormat::PrintToString(proto, &s));
    return s;
}

bool HasField(
    const google::protobuf::Message& message,
    const TProtoStringType& fieldName)
{
    const auto* descriptor = message.GetDescriptor();
    if (!descriptor) {
        return false;
    }

    const auto* field = descriptor->FindFieldByName(fieldName);
    const auto* reflection = message.GetReflection();
    if (!field || !reflection) {
        return false;
    }
    if (field->is_repeated()) {
        return reflection->FieldSize(message, field) != 0;
    }
    return reflection->HasField(message, field);
}

}   // namespace NYdb::NBS
