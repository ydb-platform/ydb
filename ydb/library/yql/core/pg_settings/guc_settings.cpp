#include "guc_settings.h"
#include <library/cpp/json/json_reader.h>

TGUCSettings::TGUCSettings(const TString &serialized) {
    if (!serialized.Empty()) {
        NJson::TJsonValue gucJson;
        Y_ENSURE(NJson::ReadJsonTree(serialized, &gucJson), "Error parsing GUCSettings");
        this->ImportFromJson(gucJson);
    }
}


void TGUCSettings::Setup(const std::unordered_map<std::string, std::string>& runtimeSettings) {
    RollbackSettings_ = runtimeSettings;
    RollBack();
}

std::optional<std::string> TGUCSettings::Get(const std::string& key) const {
    auto it = Settings_.find(key);
    if (it == Settings_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void TGUCSettings::Set(const std::string& key, const std::string& val, bool isLocal) {
    Settings_[key] = val;
    if (!isLocal) {
        SessionSettings_[key] = val;
    }
}

void TGUCSettings::Commit() {
    RollbackSettings_ = SessionSettings_;
}

void TGUCSettings::RollBack() {
    Settings_ = SessionSettings_ = RollbackSettings_;
}

void TGUCSettings::ExportToJson(NJson::TJsonValue& value) const {
    NJson::TJsonValue settings(NJson::JSON_MAP);
    for (const auto& setting : Settings_) {
        settings[setting.first] = setting.second;
    }
    NJson::TJsonValue rollbackSettings(NJson::JSON_MAP);
    for (const auto& setting : RollbackSettings_) {
        rollbackSettings[setting.first] = setting.second;
    }
    NJson::TJsonValue sessionSettings(NJson::JSON_MAP);
    for (const auto& setting : SessionSettings_) {
        sessionSettings[setting.first] = setting.second;
    }
    NJson::TJsonValue gucSettings(NJson::JSON_MAP);
    gucSettings.InsertValue("settings", std::move(settings));
    gucSettings.InsertValue("rollback_settings", std::move(rollbackSettings));
    gucSettings.InsertValue("session_settings", std::move(sessionSettings));
    value.InsertValue("guc_settings", std::move(gucSettings));
}

void TGUCSettings::ImportFromJson(const NJson::TJsonValue& value)
{
    Settings_.clear();
    RollbackSettings_.clear();
    SessionSettings_.clear();
    if (value.Has("guc_settings")) {
        auto gucSettings = value["guc_settings"];
        if (gucSettings.Has("settings")) {
            for (const auto& [settingName, settingValue] : gucSettings["settings"].GetMapSafe()) {
                Settings_[settingName] = settingValue.GetStringSafe();
            }
        }
        if (gucSettings.Has("rollback_settings")) {
            for (const auto& [settingName, settingValue] : gucSettings["rollback_settings"].GetMapSafe()) {
                RollbackSettings_[settingName] = settingValue.GetStringSafe();
            }
        }
        if (gucSettings.Has("session_settings")) {
            for (const auto& [settingName, settingValue] : gucSettings["session_settings"].GetMapSafe()) {
                SessionSettings_[settingName] = settingValue.GetStringSafe();
            }
        }
    }
}

TString TGUCSettings::SerializeToString() const {
    NJson::TJsonValue gucJson;
    this->ExportToJson(gucJson);
    return WriteJson(gucJson);
}

bool TGUCSettings::operator==(const TGUCSettings& other) const {
    return Settings_ == other.Settings_ &&
        RollbackSettings_ == other.RollbackSettings_ &&
        SessionSettings_ == other.SessionSettings_;
}

template <>
struct THash<std::pair<std::string, std::string>> {
    inline size_t operator()(const std::pair<std::string, std::string>& value) const {
        size_t result = 0;
        result = CombineHashes(THash<std::string>()(value.first), result);
        result = CombineHashes(THash<std::string>()(value.second), result);
        return result;
    }
};

template <>
struct THash<std::unordered_map<std::string, std::string>> {
    inline size_t operator()(const std::unordered_map<std::string, std::string>& values) const {
        size_t result = 0;
        for (const auto& value : values) {
            result = CombineHashes(THash<std::pair<std::string, std::string>>()(value), result);
        }
        return result;
    }
};

size_t TGUCSettings::GetHash() const noexcept {
    size_t hash = 0;
    hash = CombineHashes(THash<decltype(Settings_)>()(Settings_), hash);
    hash = CombineHashes(THash<decltype(RollbackSettings_)>()(RollbackSettings_), hash);
    hash = CombineHashes(THash<decltype(SessionSettings_)>()(SessionSettings_), hash);
    return  hash;
}
