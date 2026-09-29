#pragma once

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <util/generic/string.h>

namespace NKikimr::NKqp {

// DateTime2::Format grows WriteOffsetWithColon at langver 2025.05. YDB stays on
// 2025.01, and stable-26-3 pins the same threshold, so mixed clusters disagree
// on the optional-arg count unless KQP forces the threshold down. KIKIMR-26019.
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

// Returns settings that declare the KQP default. Copies UDF settings already present.
// Host settings that were explicitly overridden are not copied: call this on the
// context created by TTypeAnnotationContext, before anything else mutates them.
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
