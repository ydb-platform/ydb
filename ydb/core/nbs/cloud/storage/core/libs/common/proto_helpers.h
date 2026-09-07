#pragma once

#include "public.h"

#include <util/generic/string.h>

#include <google/protobuf/message.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

bool TryParseProtoTextFromStringWithoutError(
    const TString& text,
    google::protobuf::Message& dst);

bool TryParseProtoTextFromString(
    const TString& text,
    google::protobuf::Message& dst);

bool TryParseProtoTextFromFile(
    const TString& fileName,
    google::protobuf::Message& dst);

void ParseProtoTextFromString(
    const TString& text,
    google::protobuf::Message& dst);

void ParseProtoTextFromFile(
    const TString& fileName,
    google::protobuf::Message& dst);

void ParseProtoTextFromStringRobust(
    const TString& text,
    google::protobuf::Message& dst);

void ParseProtoTextFromFileRobust(
    const TString& fileName,
    google::protobuf::Message& dst);

TString ProtoToText(const google::protobuf::Message& proto);

bool HasField(
    const google::protobuf::Message& message,
    const TProtoStringType& fieldName);

}   // namespace NYdb::NBS
