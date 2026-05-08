#pragma once

#include <yql/essentials/providers/common/config/yql_setting.h>

#include <util/generic/ptr.h>

namespace NYql {

template <typename TType>
class TRuntimeSetting {
public:
    using TConfSetting = NYql::NCommon::TConfSetting<TType, NYql::NCommon::EConfSettingType::Static>;

    explicit TRuntimeSetting(const TType& value)
        : DefaultValue_(value)
    {
    }

    TType Get() const {
        return Setting_.Get().GetOrElse(DefaultValue_);
    }

    void Set(const TType& value) {
        Setting_ = value;
    }

private:
    friend class TRuntimeSettingsConfiguration;

    TConfSetting Setting_;
    TType DefaultValue_;
};

struct TRuntimeSettings {
    using TPtr = TSharedPtr<TRuntimeSettings, TAtomicCounter>;
    using TConstPtr = TSharedPtr<const TRuntimeSettings, TAtomicCounter>;

    TRuntimeSettings();
    virtual ~TRuntimeSettings();

    // =============================== Host settings ===============================
    TRuntimeSetting<bool> DatumValidation{false};
    // Noop feature.
    // Used for testing only.
    TRuntimeSetting<bool> TestHostSetting{false};
    // =============================== Host settings end ===========================
    using TUdfSettings = THashMap<TString, TString>;

    TStringBuf GetUdfSetting(TStringBuf module, TStringBuf settingName) const {
        const auto moduleIt = ModuleToSettings_.find(module);
        if (moduleIt == ModuleToSettings_.end()) {
            return TStringBuf{};
        }
        const auto settingIt = moduleIt->second.find(settingName);
        if (settingIt == moduleIt->second.end()) {
            return TStringBuf{};
        }
        return settingIt->second;
    }

    void SetUdfSetting(const TString& module, const TString& settingName, const TString& value) {
        ModuleToSettings_[module][settingName] = value;
    }

    const THashMap<TString, TUdfSettings>& GetUdfSettings() const {
        return ModuleToSettings_;
    }

private:
    THashMap<TString, TUdfSettings> ModuleToSettings_;
};

TRuntimeSettings::TConstPtr MakeRuntimeSettings(auto&&... args) {
    return MakeShared<const TRuntimeSettings, TAtomicCounter>(std::forward<decltype(args)>(args)...);
}

TRuntimeSettings::TPtr MakeRuntimeSettingsMutable(auto&&... args) {
    return MakeShared<TRuntimeSettings, TAtomicCounter>(std::forward<decltype(args)>(args)...);
}

} // namespace NYql
