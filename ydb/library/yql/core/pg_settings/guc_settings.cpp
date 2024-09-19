#include "guc_settings.h"


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

void TGUCSettings::ExportToProto(NKikimrKqp::TEvStartKqpTasksRequest::TGUCSettings& value) const {
    for (const auto& setting : Settings_) {
        value.MutableSettings()->insert({setting.first.c_str(), setting.second.c_str()});
    }
    for (const auto& setting : RollbackSettings_) {
        value.MutableRollbackSettings()->insert({setting.first.c_str(), setting.second.c_str()});
    }
    for (const auto& setting : SessionSettings_) {
        value.MutableSessionSettings()->insert({setting.first.c_str(), setting.second.c_str()});
    }
}

void TGUCSettings::ImportFromProto(const NKikimrKqp::TEvStartKqpTasksRequest::TGUCSettings& value)
{
    for (const auto& [settingName, settingValue] : value.GetSettings()) {
        Settings_[settingName] = settingValue;
    }
    for (const auto& [settingName, settingValue] : value.GetRollbackSettings()) {
        RollbackSettings_[settingName] = settingValue;
    }
    for (const auto& [settingName, settingValue] : value.GetSessionSettings()) {
         SessionSettings_[settingName] = settingValue;
    }
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
