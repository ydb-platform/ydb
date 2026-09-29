#pragma once

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <util/generic/string.h>

namespace NKikimr::NKqp {

inline constexpr TStringBuf DateTime2ModuleName = "DateTime2";
inline constexpr TStringBuf WriteOffsetWithColonAvailableSinceSetting = "MakeWriteOffsetWithColonAvailableSince";
inline constexpr TStringBuf WriteOffsetWithColonAvailableSinceValue = "2025.01";

inline bool HasWriteOffsetWithColonSetting(const NYql::NProto::TRuntimeSettings& proto) {
    const NYql::NProto::TRuntimeSetting* found = nullptr;
    for (const auto& udf : proto.GetUdfSettings()) {
        if (udf.GetModule() != DateTime2ModuleName) {
            continue;
        }
        for (const auto& setting : udf.GetRuntimeSettings()) {
            if (setting.GetName() == WriteOffsetWithColonAvailableSinceSetting) {
                found = &setting;
            }
        }
    }
    return found && !found->GetValue().empty();
}

inline void EnsureKqpDefaultRuntimeSettings(NYql::NProto::TRuntimeSettings& proto) {
    NYql::NProto::TUdfSettings* dateTime2Settings = nullptr;
    NYql::NProto::TRuntimeSetting* writeOffsetWithColonSetting = nullptr;
    for (auto& udf : *proto.MutableUdfSettings()) {
        if (udf.GetModule() != DateTime2ModuleName) {
            continue;
        }
        dateTime2Settings = &udf;
        for (auto& setting : *udf.MutableRuntimeSettings()) {
            if (setting.GetName() == WriteOffsetWithColonAvailableSinceSetting) {
                writeOffsetWithColonSetting = &setting;
            }
        }
    }

    if (writeOffsetWithColonSetting) {
        if (writeOffsetWithColonSetting->GetValue().empty()) {
            writeOffsetWithColonSetting->SetValue(TString(WriteOffsetWithColonAvailableSinceValue));
        }
        return;
    }

    if (!dateTime2Settings) {
        dateTime2Settings = proto.AddUdfSettings();
        dateTime2Settings->SetModule(TString(DateTime2ModuleName));
    }
    auto* setting = dateTime2Settings->AddRuntimeSettings();
    setting->SetName(TString(WriteOffsetWithColonAvailableSinceSetting));
    setting->SetValue(TString(WriteOffsetWithColonAvailableSinceValue));
}

inline NYql::TRuntimeSettings::TConstPtr MakeKqpDefaultRuntimeSettings() {
    auto settings = NYql::MakeRuntimeSettingsMutable();
    settings->SetUdfSetting(
        TString(DateTime2ModuleName),
        TString(WriteOffsetWithColonAvailableSinceSetting),
        TString(WriteOffsetWithColonAvailableSinceValue));
    return settings;
}

} // namespace NKikimr::NKqp
