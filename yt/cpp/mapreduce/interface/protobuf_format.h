#pragma once

#include "common.h"

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <util/generic/maybe.h>

#include <google/protobuf/message.h>

/// @cond Doxygen_Suppress
namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

enum class EProtobufType
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

enum class EProtobufEnumWritingMode
{
    SkipUnknownValues,
    CheckValues,
};

struct TProtobufOneofOptions
{
    EProtobufOneofMode Mode = EProtobufOneofMode::Variant;
    TString VariantFieldName;
};

struct TProtobufFieldOptions
{
    TMaybe<EProtobufType> Type;
    EProtobufSerializationMode SerializationMode = EProtobufSerializationMode::Protobuf;
    EProtobufListMode ListMode = EProtobufListMode::Required;
    EProtobufMapMode MapMode = EProtobufMapMode::ListOfStructsLegacy;
};

struct TProtobufMessageOptions
{
    EProtobufFieldSortOrder FieldSortOrder = EProtobufFieldSortOrder::ByFieldNumber;
};

TString GetColumnName(const ::google::protobuf::FieldDescriptor& field);

TProtobufFieldOptions GetFieldOptions(
    const ::google::protobuf::FieldDescriptor* fieldDescriptor,
    const TMaybe<TProtobufFieldOptions>& defaultFieldOptions = {});

TProtobufOneofOptions GetOneofOptions(
    const ::google::protobuf::OneofDescriptor* oneofDescriptor,
    const TMaybe<TProtobufOneofOptions>& defaultOneofOptions = {});

TProtobufMessageOptions GetMessageOptions(const ::google::protobuf::Descriptor* descriptor);

TMaybe<TVector<TString>> InferColumnFilter(const ::google::protobuf::Descriptor& descriptor);

TNode MakeProtoFormatConfigWithTables(const TVector<const ::google::protobuf::Descriptor*>& descriptors);
TNode MakeProtoFormatConfigWithDescriptors(const TVector<const ::google::protobuf::Descriptor*>& descriptors);

TTableSchema CreateTableSchemaImpl(
    const ::google::protobuf::Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
/// @endcond
