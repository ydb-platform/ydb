#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <memory>

class TGUCSettings {
public:
    using TPtr = std::shared_ptr<TGUCSettings>;
    void Setup(const std::unordered_map<std::string, std::string>& runtimeSettings);
    std::optional<std::string> Get(const std::string&) const;
    void Set(const std::string&, const std::string&, bool isLocal = false);
    void Commit();
    void RollBack();
    std::unordered_map<std::string, std::string> GetSettings() const;
    std::unordered_map<std::string, std::string> GetRollbackSettings() const;
    std::unordered_map<std::string, std::string> GetSessionSettings() const;

    bool operator==(const TGUCSettings& other) const;
private:
    std::unordered_map<std::string, std::string> Settings_;
    std::unordered_map<std::string, std::string> RollbackSettings_;
    std::unordered_map<std::string, std::string> SessionSettings_;
};
