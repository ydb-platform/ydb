#include "config.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void TProtobufInteropConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_enum_yson_storage_type", &TThis::DefaultEnumYsonStorageType)
        .Default(EEnumYsonStorageType::String);
    registrar.Parameter("utf8_check", &TThis::Utf8Check)
        .Default(EUtf8Check::Disable);
}

TProtobufInteropConfigPtr TProtobufInteropConfig::ApplyDynamic(
    const TProtobufInteropDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    NYTree::UpdateYsonStructField(mergedConfig->Utf8Check, dynamicConfig->Utf8Check);
    mergedConfig->Postprocess();
    return mergedConfig;
}

void TProtobufInteropDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("utf8_check", &TThis::Utf8Check)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
