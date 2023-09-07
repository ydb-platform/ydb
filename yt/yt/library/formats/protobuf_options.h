#pragma once

#include <yt/yt/client/formats/config.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <google/protobuf/message.h>

#include <util/generic/maybe.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

enum class ESpecialProtobufType
{
    EnumInt       /* "enum_int" */,
    EnumString    /* "enum_string" */,
    Any           /* "any" */,
    OtherColumns  /* "other_columns" */,
};

enum class EProtobufSerializationMode
{
    Protobuf,
    Yt,
    Embedded,
};

enum class EProtobufListMode
{
    Optional,
    Required,
};

enum class EProtobufMapMode
{
    ListOfStructsLegacy,
    ListOfStructs,
    Dict,
    OptionalDict,
};

enum class EProtobufFieldSortOrder
{
    AsInProtoFile,
    ByFieldNumber,
};

enum class EProtobufOneofMode
{
    SeparateFields,
    Variant,
};

struct TProtobufOneofOptions
{
    EProtobufOneofMode Mode = EProtobufOneofMode::Variant;
    TString VariantFieldName;
};

struct TProtobufFieldOptions
{
    TMaybe<ESpecialProtobufType> Type;
    EProtobufSerializationMode SerializationMode = EProtobufSerializationMode::Protobuf;
    EProtobufListMode ListMode = EProtobufListMode::Required;
    EProtobufMapMode MapMode = EProtobufMapMode::ListOfStructsLegacy;
    EProtobufEnumWritingMode EnumWritingMode = EProtobufEnumWritingMode::CheckValues;
};

struct TProtobufMessageOptions
{
    EProtobufFieldSortOrder FieldSortOrder = EProtobufFieldSortOrder::ByFieldNumber;
};

TProtobufFieldOptions GetDefaultFieldOptions(
    const ::google::protobuf::Descriptor* descriptor,
    TProtobufFieldOptions defaultFieldOptions = {});

TProtobufOneofOptions GetDefaultOneofOptions(
    const ::google::protobuf::Descriptor* descriptor);

TProtobufFieldOptions GetFieldOptions(
    const ::google::protobuf::FieldDescriptor* fieldDescriptor,
    const TMaybe<TProtobufFieldOptions>& defaultFieldOptions = {});

TProtobufOneofOptions GetOneofOptions(
    const ::google::protobuf::OneofDescriptor* oneofDescriptor,
    const TMaybe<TProtobufOneofOptions>& defaultOneofOptions = {});

TString GetColumnName(const ::google::protobuf::FieldDescriptor* field);

void ValidateProtobufType(
    const ::google::protobuf::FieldDescriptor* fieldDescriptor,
    ESpecialProtobufType protobufType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
