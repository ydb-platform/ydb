#include "format.h"
#include "protobuf_format.h"

#include "errors.h"

#include <google/protobuf/descriptor.h>

namespace NYT {

TTableSchema CreateTableSchema(
    const ::google::protobuf::Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension)
{
    return NDetail::CreateTableSchemaImpl(messageDescriptor, keepFieldsWithoutExtension);
}

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(const TNode& config)
    : Config(config)
{ }


TFormat TFormat::Protobuf(
    const TVector<const ::google::protobuf::Descriptor*>& descriptors,
    bool withDescriptors)
{
    if (withDescriptors) {
        return TFormat(NDetail::MakeProtoFormatConfigWithDescriptors(descriptors));
    } else {
        return TFormat(NDetail::MakeProtoFormatConfigWithTables(descriptors));
    }
}

TFormat TFormat::YsonText()
{
    TNode config("yson");
    config.Attributes()("format", "text");
    return TFormat(config);
}

TFormat TFormat::YsonBinary()
{
    TNode config("yson");
    config.Attributes()("format", "binary");
    return TFormat(config);
}

TFormat TFormat::YaMRLenval()
{
    TNode config("yamr");
    config.Attributes()("lenval", true)("has_subkey", true);
    return TFormat(config);
}

TFormat TFormat::Json()
{
    return TFormat(TNode("json"));
}

TFormat TFormat::Dsv()
{
    return TFormat(TNode("dsv"));
}

bool TFormat::IsTextYson() const
{
    if (!Config.IsString() || Config.AsString() != "yson") {
        return false;
    }
    if (!Config.HasAttributes()) {
        return false;
    }
    const auto& attributes = Config.GetAttributes();
    if (!attributes.HasKey("format") || attributes["format"] != TNode("text")) {
        return false;
    }
    return true;
}

bool TFormat::IsProtobuf() const
{
    return Config.IsString() && Config.AsString() == "protobuf";
}

bool TFormat::IsYamredDsv() const
{
    return Config.IsString() && Config.AsString() == "yamred_dsv";
}

static TString FormatName(const TFormat& format)
{
    if (!format.Config.IsString()) {
        Y_ABORT_UNLESS(format.Config.IsUndefined());
        return "<undefined>";
    }
    return format.Config.AsString();
}

TYamredDsvAttributes TFormat::GetYamredDsvAttributes() const
{
    if (!IsYamredDsv()) {
        ythrow TApiUsageError() << "Cannot get yamred_dsv attributes for " << FormatName(*this) << " format";
    }
    TYamredDsvAttributes attributes;

    const auto& nodeAttributes = Config.GetAttributes();
    {
        const auto& keyColumns = nodeAttributes["key_column_names"];
        if (!keyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: key_column_names is of non-list type: " << keyColumns.GetType();
        }
        for (auto& column : keyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: key_column_names: " << column.GetType();
            }
            attributes.KeyColumnNames.push_back(column.AsString());
        }
    }

    if (nodeAttributes.HasKey("subkey_column_names")) {
        const auto& subkeyColumns = nodeAttributes["subkey_column_names"];
        if (!subkeyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: subkey_column_names is not a list: " << subkeyColumns.GetType();
        }
        for (const auto& column : subkeyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: non-string inside subkey_key_column_names: " << column.GetType();
            }
            attributes.SubkeyColumnNames.push_back(column.AsString());
        }
    }

    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
