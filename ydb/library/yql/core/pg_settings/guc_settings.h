#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <memory>

class TGUCSettings {
public:
    using TPtr = std::shared_ptr<TGUCSettings>;
    void RollBack();
    void Set(const std::string&, const std::string&, bool local = false);
    std::optional<std::string> Get(const std::string&) const;
    void Commit();
private:
    std::unordered_map<std::string, std::string> Settings_;
    std::unordered_map<std::string, std::string> RollbackSettings_;
    std::unordered_map<std::string, std::string> SessionSettings_;
};
