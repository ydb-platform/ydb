#include "guc_settings.h"

void TGUCSettings::RollBack() {
    SessionSettings_ = RollbackSettings_;
    Settings_ = SessionSettings_;
}

std::optional<std::string> TGUCSettings::Get(const std::string& key) const {
    auto it = Settings_.find(key);
    if (it == Settings_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void TGUCSettings::Set(const std::string& key, const std::string& val, bool local) {
    Settings_[key] = val;
    if (!local) {
        SessionSettings_[key] = val;
    }
}

void TGUCSettings::Commit() {
    RollbackSettings_ = SessionSettings_;
}
