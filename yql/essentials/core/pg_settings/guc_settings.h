#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <memory>

#include <util/generic/hash.h>

#include <library/cpp/json/json_writer.h>

class TGUCSettings {
public:
    TGUCSettings() = default;
    TGUCSettings(const TString& serialized);

    using TPtr = std::shared_ptr<TGUCSettings>;
    void Setup(const std::unordered_map<std::string, std::string>& runtimeSettings);
    std::optional<std::string> Get(const std::string&) const;
    void Set(const std::string&, const std::string&, bool isLocal = false);
    void Commit();
    void RollBack();
    void ExportToJson(NJson::TJsonValue& value) const;
    void ImportFromJson(const NJson::TJsonValue& value);
    TString SerializeToString() const;

    size_t GetHash() const noexcept;
    bool operator==(const TGUCSettings& other) const;
private:
    std::unordered_map<std::string, std::string> Settings_;
    std::unordered_map<std::string, std::string> RollbackSettings_;
    std::unordered_map<std::string, std::string> SessionSettings_;
};
