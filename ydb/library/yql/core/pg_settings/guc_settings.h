#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <memory>

#include <util/generic/hash.h>

class TGUCSettings {
public:
    using TPtr = std::shared_ptr<TGUCSettings>;
    void Setup(const std::unordered_map<std::string, std::string>& runtimeSettings);
    std::optional<std::string> Get(const std::string&) const;
    void Set(const std::string&, const std::string&, bool isLocal = false);
    void Commit();
    void RollBack();

    size_t GetHash() const noexcept {
        auto tuple = std::make_tuple(
            Settings_.size(),
            RollbackSettings_.size(),
            SessionSettings_.size());
        return THash<decltype(tuple)>()(tuple);
    }

    bool operator==(const TGUCSettings& other) const;
private:
    std::unordered_map<std::string, std::string> Settings_;
    std::unordered_map<std::string, std::string> RollbackSettings_;
    std::unordered_map<std::string, std::string> SessionSettings_;
};
