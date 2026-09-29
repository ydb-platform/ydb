#pragma once

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <util/generic/string.h>

namespace NKikimr::NKqp {

inline constexpr TStringBuf DateTime2ModuleName = "DateTime2";
inline constexpr TStringBuf WriteOffsetWithColonAvailableSinceSetting = "MakeWriteOffsetWithColonAvailableSince";
inline constexpr TStringBuf WriteOffsetWithColonAvailableSinceValue = "2025.01";

inline bool HasWriteOffsetWithColonSetting(const NYql::NProto::TRuntimeSettings& proto) {
    for (const auto& udf : proto.GetUdfSettings()) {
        if (udf.GetModule() != DateTime2ModuleName) {
            continue;
        }
        for (const auto& setting : udf.GetRuntimeSettings()) {
            if (setting.GetName() == WriteOffsetWithColonAvailableSinceSetting) {
                return true;
            }
        }
    }
    return false;
}

inline void EnsureKqpDefaultRuntimeSettings(NYql::NProto::TRuntimeSettings& proto) {
    if (HasWriteOffsetWithColonSetting(proto)) {
        return;
    }
    auto* udf = proto.AddUdfSettings();
    udf->SetModule(TString(DateTime2ModuleName));
    auto* setting = udf->AddRuntimeSettings();
    setting->SetName(TString(WriteOffsetWithColonAvailableSinceSetting));
    setting->SetValue(TString(WriteOffsetWithColonAvailableSinceValue));
}

inline NYql::TRuntimeSettings::TConstPtr WithKqpDefaultRuntimeSettings(const NYql::TRuntimeSettings::TConstPtr& settings) {
    if (settings && !settings->GetUdfSetting(DateTime2ModuleName, WriteOffsetWithColonAvailableSinceSetting).empty()) {
        return settings;
    }
    auto updated = NYql::MakeRuntimeSettingsMutable();
    if (settings) {
        for (const auto& [module, moduleSettings] : settings->GetUdfSettings()) {
            for (const auto& [name, value] : moduleSettings) {
                updated->SetUdfSetting(module, name, value);
            }
        }
    }
    updated->SetUdfSetting(
        TString(DateTime2ModuleName),
        TString(WriteOffsetWithColonAvailableSinceSetting),
        TString(WriteOffsetWithColonAvailableSinceValue));
    return updated;
}

} // namespace NKikimr::NKqp
