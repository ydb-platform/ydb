#include "kqp_runtime_settings.h"

#include <util/generic/string.h>

namespace NKikimr::NKqp {

TKqpRuntimeSettings::TKqpRuntimeSettings() {
    auto settings = NYql::MakeRuntimeSettingsMutable();
    // Keep DateTime2.Format compatible with 26-3 nodes that use this threshold unconditionally.
    // Remove the override after 26-3 leaves supported mixed-version clusters (KIKIMR-26019).
    settings->SetUdfSetting(
        TString("DateTime2"),
        TString("MakeWriteOffsetWithColonAvailableSince"),
        TString("2025.01"));
    Settings_ = settings;
}

const NYql::TRuntimeSettings::TConstPtr& TKqpRuntimeSettings::Get() const {
    return Settings_;
}

void TKqpRuntimeSettings::ApplyTo(NYql::NProto::TRuntimeSettings& proto) const {
    for (const auto& [module, moduleSettings] : Settings_->GetUdfSettings()) {
        NYql::NProto::TUdfSettings* targetModule = nullptr;

        for (const auto& [name, defaultValue] : moduleSettings) {
            NYql::NProto::TRuntimeSetting* targetSetting = nullptr;
            for (auto& udf : *proto.MutableUdfSettings()) {
                if (udf.GetModule() != module) {
                    continue;
                }
                targetModule = &udf;
                for (auto& setting : *udf.MutableRuntimeSettings()) {
                    if (setting.GetName() == name) {
                        targetSetting = &setting;
                    }
                }
            }

            if (targetSetting) {
                if (targetSetting->GetValue().empty()) {
                    targetSetting->SetValue(defaultValue);
                }
                continue;
            }

            if (!targetModule) {
                targetModule = proto.AddUdfSettings();
                targetModule->SetModule(module);
            }
            auto* setting = targetModule->AddRuntimeSettings();
            setting->SetName(name);
            setting->SetValue(defaultValue);
        }
    }
}

} // namespace NKikimr::NKqp
