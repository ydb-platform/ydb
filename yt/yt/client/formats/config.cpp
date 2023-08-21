#include "config.h"

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

void TControlAttributesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_key_switch", &TThis::EnableKeySwitch)
        .Default(false);

    registrar.Parameter("enable_end_of_stream", &TThis::EnableEndOfStream)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TYsonFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("format", &TThis::Format)
        .Default(NYson::EYsonFormat::Binary);
    registrar.Parameter("complex_type_mode", &TThis::ComplexTypeMode)
        .Default(EComplexTypeMode::Named);
    registrar.Parameter("string_keyed_dict_mode", &TThis::StringKeyedDictMode)
        .Default(EDictMode::Positional);
    registrar.Parameter("decimal_mode", &TThis::DecimalMode)
        .Default(EDecimalMode::Binary);
    registrar.Parameter("time_mode", &TThis::TimeMode)
        .Default(ETimeMode::Binary);
    registrar.Parameter("uuid_mode", &TThis::UuidMode)
        .Default(EUuidMode::Binary);
    registrar.Parameter("skip_null_values", &TThis::SkipNullValues)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TYamrFormatConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("has_subkey", &TThis::HasSubkey)
        .Default(false);
    registrar.Parameter("key", &TThis::Key)
        .Default("key");
    registrar.Parameter("subkey", &TThis::Subkey)
        .Default("subkey");
    registrar.Parameter("value", &TThis::Value)
        .Default("value");
    registrar.BaseClassParameter("lenval", &TThis::Lenval)
        .Default(false);
    registrar.BaseClassParameter("fs", &TThis::FieldSeparator)
        .Default('\t');
    registrar.BaseClassParameter("rs", &TThis::RecordSeparator)
        .Default('\n');
    registrar.BaseClassParameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);
    registrar.BaseClassParameter("enable_escaping", &TThis::EnableEscaping)
        .Default(false);
    registrar.BaseClassParameter("escaping_symbol", &TThis::EscapingSymbol)
        .Default('\\');
    registrar.BaseClassParameter("enable_eom", &TThis::EnableEom)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        if (config->EnableEom && !config->Lenval) {
            THROW_ERROR_EXCEPTION("EOM marker is not supported in YAMR text mode");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDsvFormatConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("record_separator", &TThis::RecordSeparator)
        .Default('\n');
    registrar.BaseClassParameter("key_value_separator", &TThis::KeyValueSeparator)
        .Default('=');
    registrar.BaseClassParameter("field_separator", &TThis::FieldSeparator)
        .Default('\t');
    registrar.BaseClassParameter("line_prefix", &TThis::LinePrefix)
        .Default();
    registrar.BaseClassParameter("enable_escaping", &TThis::EnableEscaping)
        .Default(true);
    registrar.BaseClassParameter("escaping_symbol", &TThis::EscapingSymbol)
        .Default('\\');
    registrar.BaseClassParameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);
    registrar.Parameter("table_index_column", &TThis::TableIndexColumn)
        .Default("@table_index")
        .NonEmpty();
    registrar.Parameter("skip_unsupported_types", &TThis::SkipUnsupportedTypes)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TYamredDsvFormatConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("record_separator", &TThis::RecordSeparator)
        .Default('\n');
    registrar.BaseClassParameter("key_value_separator", &TThis::KeyValueSeparator)
        .Default('=');
    registrar.BaseClassParameter("field_separator", &TThis::FieldSeparator)
        .Default('\t');
    registrar.BaseClassParameter("line_prefix", &TThis::LinePrefix)
        .Default();
    registrar.BaseClassParameter("enable_escaping", &TThis::EnableEscaping)
        .Default(true);
    registrar.BaseClassParameter("escaping_symbol", &TThis::EscapingSymbol)
        .Default('\\');
    registrar.BaseClassParameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);
    registrar.BaseClassParameter("has_subkey", &TThis::HasSubkey)
        .Default(false);
    registrar.BaseClassParameter("lenval", &TThis::Lenval)
        .Default(false);
    registrar.Parameter("key_column_names", &TThis::KeyColumnNames);
    registrar.Parameter("subkey_column_names", &TThis::SubkeyColumnNames)
        .Default();
    registrar.Parameter("yamr_keys_separator", &TThis::YamrKeysSeparator)
        .Default(' ');
    registrar.BaseClassParameter("enable_eom", &TThis::EnableEom)
        .Default(false);
    registrar.Parameter("skip_unsupported_types_in_value", &TThis::SkipUnsupportedTypesInValue)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        if (config->EnableEom && !config->Lenval) {
            THROW_ERROR_EXCEPTION("EOM marker is not supported in YAMR text mode");
        }
    });

    registrar.Postprocessor([] (TThis* config) {
        THashSet<TString> names;

        for (const auto& name : config->KeyColumnNames) {
            if (!names.insert(name).second) {
                THROW_ERROR_EXCEPTION("Duplicate column %Qv found in \"key_column_names\"",
                    name);
            }
        }

        for (const auto& name : config->SubkeyColumnNames) {
            if (!names.insert(name).second) {
                THROW_ERROR_EXCEPTION("Duplicate column %Qv found in \"subkey_column_names\"",
                    name);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& TSchemafulDsvFormatConfig::GetColumnsOrThrow() const
{
    if (!Columns) {
        THROW_ERROR_EXCEPTION("Missing \"columns\" attribute in schemaful DSV format");
    }
    return *Columns;
}

void TSchemafulDsvFormatConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("record_separator", &TThis::RecordSeparator)
        .Default('\n');
    registrar.BaseClassParameter("field_separator", &TThis::FieldSeparator)
        .Default('\t');

    registrar.BaseClassParameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);

    registrar.BaseClassParameter("enable_escaping", &TThis::EnableEscaping)
        .Default(true);
    registrar.BaseClassParameter("escaping_symbol", &TThis::EscapingSymbol)
        .Default('\\');

    registrar.Parameter("columns", &TThis::Columns)
        .Default();

    registrar.Parameter("missing_value_mode", &TThis::MissingValueMode)
        .Default(EMissingSchemafulDsvValueMode::Fail);

    registrar.Parameter("missing_value_sentinel", &TThis::MissingValueSentinel)
        .Default("");

    registrar.Parameter("enable_column_names_header", &TThis::EnableColumnNamesHeader)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Columns) {
            THashSet<TString> names;
            for (const auto& name : *config->Columns) {
                if (!names.insert(name).second) {
                    THROW_ERROR_EXCEPTION("Duplicate column name %Qv in schemaful DSV configuration",
                        name);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TProtobufTypeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("proto_type", &TThis::ProtoType);
    registrar.Parameter("fields", &TThis::Fields)
        .Default();
    registrar.Parameter("enumeration_name", &TThis::EnumerationName)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TProtobufColumnConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .NonEmpty();
    registrar.Parameter("field_number", &TThis::FieldNumber)
        .Optional();
    registrar.Parameter("repeated", &TThis::Repeated)
        .Default(false);
    registrar.Parameter("packed", &TThis::Packed)
        .Default(false);

    registrar.Parameter("type", &TThis::Type)
        .Default();

    registrar.Parameter("proto_type", &TThis::ProtoType)
        .Default();
    registrar.Parameter("fields", &TThis::Fields)
        .Default();
    registrar.Parameter("enumeration_name", &TThis::EnumerationName)
        .Default();

    registrar.Parameter("enum_writing_mode", &TThis::EnumWritingMode)
        .Default(EProtobufEnumWritingMode::CheckValues);

    registrar.Postprocessor([] (TThis* config) {
        config->CustomPostprocess();
    });
}

void TProtobufColumnConfig::CustomPostprocess()
{
    if (Packed && !Repeated) {
        THROW_ERROR_EXCEPTION("Field %Qv is marked \"packed\" but is not marked \"repeated\"",
            Name);
    }

    if (!Type) {
        Type = New<TProtobufTypeConfig>();
        if (!ProtoType) {
            THROW_ERROR_EXCEPTION("One of \"type\" and \"proto_type\" must be specified");
        }
        Type->ProtoType = *ProtoType;
        Type->Fields = std::move(Fields);
        Type->EnumerationName = EnumerationName;
    }

    if (!FieldNumber && Type->ProtoType != EProtobufType::Oneof) {
        THROW_ERROR_EXCEPTION("\"field_number\" is required for type %Qlv",
            Type->ProtoType);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TProtobufTableConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("columns", &TThis::Columns);

    registrar.Postprocessor([] (TThis* config) {
        bool hasOtherColumns = false;
        for (const auto& column: config->Columns) {
            if (column->ProtoType == EProtobufType::OtherColumns) {
                if (hasOtherColumns) {
                    THROW_ERROR_EXCEPTION("Multiple \"other_columns\" in protobuf config are not allowed");
                }
                hasOtherColumns = true;
            }
        }
    });
}

void TProtobufFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_descriptor_set", &TThis::FileDescriptorSet)
        .Default();
    registrar.Parameter("file_indices", &TThis::FileIndices)
        .Default();
    registrar.Parameter("message_indices", &TThis::MessageIndices)
        .Default();
    registrar.Parameter("nested_messages_mode", &TThis::NestedMessagesMode)
        .Default(ENestedMessagesMode::Protobuf);
    registrar.Parameter("enums_as_strings", &TThis::EnumsAsStrings)
        .Default();

    registrar.Parameter("tables", &TThis::Tables)
        .Default();
    registrar.Parameter("enumerations", &TThis::Enumerations)
        .Default();

    registrar.Parameter("file_descriptor_set_text", &TThis::FileDescriptorSetText)
        .Default();
    registrar.Parameter("type_names", &TThis::TypeNames)
        .Default();

    registrar.Parameter("complex_type_mode", &TThis::ComplexTypeMode)
        .Default(EComplexTypeMode::Named);
    registrar.Parameter("decimal_mode", &TThis::DecimalMode)
        .Default(EDecimalMode::Binary);
    registrar.Parameter("time_mode", &TThis::TimeMode)
        .Default(ETimeMode::Binary);
    registrar.Parameter("uuid_mode", &TThis::UuidMode)
        .Default(EUuidMode::Binary);
}

////////////////////////////////////////////////////////////////////////////////

void TWebJsonFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_selected_column_count", &TThis::MaxSelectedColumnCount)
        .Default(50)
        .GreaterThanOrEqual(0);
    registrar.Parameter("field_weight_limit", &TThis::FieldWeightLimit)
        .Default(1_KB)
        .GreaterThanOrEqual(0);
    registrar.Parameter("string_weight_limit", &TThis::StringWeightLimit)
        .Default(200)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_all_column_names_count", &TThis::MaxAllColumnNamesCount)
        .Default(2000)
        .GreaterThanOrEqual(0);
    registrar.Parameter("column_names", &TThis::ColumnNames)
        .Default();
    registrar.Parameter("value_format", &TThis::ValueFormat)
        .Default(EWebJsonValueFormat::Schemaless);
}

////////////////////////////////////////////////////////////////////////////////

void TSkiffFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("skiff_schema_registry", &TThis::SkiffSchemaRegistry)
        .Default();
    registrar.Parameter("table_skiff_schemas", &TThis::TableSkiffSchemas);

    registrar.Parameter("override_intermediate_table_schema", &TThis::OverrideIntermediateTableSchema)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
