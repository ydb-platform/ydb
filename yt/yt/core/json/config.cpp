#include "config.h"

namespace NYT::NJson {

////////////////////////////////////////////////////////////////////////////////

void TJsonFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("format", &TThis::Format)
        .Default(EJsonFormat::Text);
    registrar.Parameter("attributes_mode", &TThis::AttributesMode)
        .Default(EJsonAttributesMode::OnDemand);
    registrar.Parameter("plain", &TThis::Plain)
        .Default(false);
    registrar.Parameter("encode_utf8", &TThis::EncodeUtf8)
        .Default(true);

    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default(256_MB);
    registrar.Parameter("nesting_level_limit", &TThis::NestingLevelLimit)
        .GreaterThan(0)
        // JSON representation of YSON with attributes involves additional nesting levels.
        .Default(NYson::DefaultYsonParserNestingLevelLimit * 2);
    registrar.Parameter("string_length_limit", &TThis::StringLengthLimit)
        .GreaterThan(0)
        .Default();

    registrar.Parameter("stringify", &TThis::Stringify)
        .Default(false);
    registrar.Parameter("annotate_with_types", &TThis::AnnotateWithTypes)
        .Default(false);

    registrar.Parameter("support_infinity", &TThis::SupportInfinity)
        .Default(false);
    registrar.Parameter("stringify_nan_and_infinity", &TThis::StringifyNanAndInfinity)
        .Default(false);

    registrar.Parameter("buffer_size", &TThis::BufferSize)
        .GreaterThan(0)
        .Default(16_KBs);

    registrar.Parameter("skip_null_values", &TThis::SkipNullValues)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->SupportInfinity && config->StringifyNanAndInfinity) {
            THROW_ERROR_EXCEPTION("\"support_infinity\" and \"stringify_nan_and_infinity\" "
                "cannot be specified simultaneously");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TWebJsonFormatConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("format", &TThis::Format)
        .Default(EJsonFormat::Text);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
