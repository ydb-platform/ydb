#include "config.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void TProtobufInteropConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_enum_yson_storage_type", &TThis::DefaultEnumYsonStorageType)
        .Default(EEnumYsonStorageType::String);
    registrar.Parameter("utf8_check", &TThis::Utf8Check)
        .Default(EUtf8Check::ThrowOnFail);
    registrar.Parameter("force_snake_case_names", &TThis::ForceSnakeCaseNames)
        .Default(false);
    registrar.Parameter("force_enum_string_type", &TThis::ForceEnumStringType)
        .Default(false);
    registrar.Parameter("allow_yson_list_from_map", &TThis::AllowYsonListFromMap)
        .Default(false);
}

TProtobufInteropConfigPtr TProtobufInteropConfig::ApplyDynamic(
    const TProtobufInteropDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    NYTree::UpdateYsonStructField(mergedConfig->Utf8Check, dynamicConfig->Utf8Check);
    NYTree::UpdateYsonStructField(mergedConfig->AllowYsonListFromMap, dynamicConfig->AllowYsonListFromMap);
    mergedConfig->Postprocess();
    return mergedConfig;
}

void TProtobufInteropDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("utf8_check", &TThis::Utf8Check)
        .Optional();
    registrar.Parameter("allow_yson_list_from_map", &TThis::AllowYsonListFromMap)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
