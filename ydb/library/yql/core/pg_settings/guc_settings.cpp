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

bool TGUCSettings::operator==(const TGUCSettings& other) const {
    return Settings_ == other.Settings_ &&
        RollbackSettings_ == other.RollbackSettings_ &&
        SessionSettings_ == other.SessionSettings_;
}

std::unordered_map<std::string, std::string> TGUCSettings::GetSettings() const {
    return Settings_;
}

std::unordered_map<std::string, std::string> TGUCSettings::GetRollbackSettings() const {
    return RollbackSettings_;
}

std::unordered_map<std::string, std::string> TGUCSettings::GetSessionSettings() const {
    return SessionSettings_;
}
