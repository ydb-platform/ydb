#pragma once

#include <util/generic/fwd.h>
#include <util/folder/path.h>

#include <yaml-cpp/node/node.h>

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class TConfigurationManager;

// Wrapper around YAML::Node that tracks modifications and triggers config save
class TConfigNode {
public:
    TConfigNode() = default;
    TConfigNode(YAML::Node node, TConfigurationManager* manager, const TString& path);

    // Check if node exists and is valid
    bool IsDefined() const;
    bool IsNull() const;
    bool IsScalar() const;
    bool IsSequence() const;
    bool IsMap() const;

    // Get child node by key (for maps) - supports composite keys like "a.b.c"
    TConfigNode operator[](const TString& key);
    TConfigNode operator[](const char* key);

    // Get child node by index (for sequences)
    TConfigNode operator[](size_t index);

    // Get typed values with defaults
    template<typename T>
    T As(const T& defaultValue = T{}) const {
        if (!IsDefined() || IsNull()) {
            return defaultValue;
        }
        try {
            return Node.as<T>(defaultValue);
        } catch (...) {
            return defaultValue;
        }
    }

    // Specialization for TString
    TString AsString(const TString& defaultValue = "") const;
    bool AsBool(bool defaultValue = false) const;
    int AsInt(int defaultValue = 0) const;
    double AsDouble(double defaultValue = 0.0) const;

    // Set value - triggers config save
    template<typename T>
    void Set(const T& value) {
        Node = value;
        NotifyModified();
    }

    // Set from string
    void SetString(const TString& value);
    void SetBool(bool value);
    void SetInt(int value);

    // Remove this node from parent
    void Remove();

    // Get underlying YAML node (for advanced usage)
    YAML::Node& GetYamlNode() { return Node; }
    const YAML::Node& GetYamlNode() const { return Node; }

    // Get path in config
    const TString& GetPath() const { return Path; }

private:
    void NotifyModified();
    void SetValueInternal(const YAML::Node& value);
    TConfigNode GetChildBySimpleKey(const TString& key);

private:
    YAML::Node Node;
    TConfigurationManager* Manager = nullptr;
    TString Path;
};

// Global CLI configuration manager
// Provides generic key-value access to ~/.config/ydb/config.yaml
class TConfigurationManager {
public:
    using TPtr = std::shared_ptr<TConfigurationManager>;

    TConfigurationManager();
    ~TConfigurationManager();

    // Get config node by key path (supports composite keys like "interactive.enable_hints")
    TConfigNode operator[](const TString& key);
    TConfigNode operator[](const char* key);

    // Get root node
    TConfigNode Root();

    // Force save config to file
    void Save();

    // Reload config from file
    void Reload();

    // Config paths
    static TFsPath GetConfigDir();
    static TFsPath GetConfigFilePath();

private:
    friend class TConfigNode;

    void Load();
    void MarkModified();

private:
    YAML::Node Config;
    bool Modified = false;
};

// Global configuration instance (initializes on first call)
TConfigurationManager::TPtr GetGlobalConfig();

} // namespace NYdb::NConsoleClient
