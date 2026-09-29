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

inline void SetKqpDefaultRuntimeSetting(NYql::TRuntimeSettings& settings) {
    settings.SetUdfSetting(
        TString(DateTime2ModuleName),
        TString(WriteOffsetWithColonAvailableSinceSetting),
        TString(WriteOffsetWithColonAvailableSinceValue));
}

inline NYql::TRuntimeSettings::TConstPtr MakeKqpDefaultRuntimeSettings() {
    auto settings = NYql::MakeRuntimeSettingsMutable();
    SetKqpDefaultRuntimeSetting(*settings);
    return settings;
}

inline NYql::TRuntimeSettings::TConstPtr EnsureKqpDefaultRuntimeSettings(const NYql::TRuntimeSettings::TConstPtr& settings) {
    if (settings && !settings->GetUdfSetting(DateTime2ModuleName, WriteOffsetWithColonAvailableSinceSetting).empty()) {
        return settings;
    }
    auto updated = settings ? NYql::MakeRuntimeSettingsMutable(*settings) : NYql::MakeRuntimeSettingsMutable();
    SetKqpDefaultRuntimeSetting(*updated);
    return updated;
}

} // namespace NKikimr::NKqp
