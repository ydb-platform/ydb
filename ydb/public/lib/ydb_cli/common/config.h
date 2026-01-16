#pragma once

#include <util/generic/fwd.h>
#include <util/folder/path.h>

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class TConfigurationManager;

// Wrapper around YAML::Node that tracks modifications and triggers config save
// Uses pimpl to hide YAML dependency from header
class TConfigNode {
public:
    TConfigNode();
    ~TConfigNode();
    TConfigNode(const TConfigNode& other);
    TConfigNode(TConfigNode&& other) noexcept;
    TConfigNode& operator=(const TConfigNode& other);
    TConfigNode& operator=(TConfigNode&& other) noexcept;

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
    TString AsString(const TString& defaultValue = "") const;
    bool AsBool(bool defaultValue = false) const;
    int AsInt(int defaultValue = 0) const;
    double AsDouble(double defaultValue = 0.0) const;

    // Set value - triggers config save
    void SetString(const TString& value);
    void SetBool(bool value);
    void SetInt(int value);

    // Remove this node from parent
    void Remove();

    // Get path in config
    const TString& GetPath() const { return Path; }

private:
    friend class TConfigurationManager;

    struct TImpl;
    std::unique_ptr<TImpl> Impl;
    TConfigurationManager* Manager = nullptr;
    TString Path;

    void NotifyModified();
    void SetValueInternal(const TString& value);
    void SetValueInternal(bool value);
    void SetValueInternal(int value);
    TConfigNode GetChildBySimpleKey(const TString& key);
};

// Global CLI configuration manager
// Provides generic key-value access to ~/.config/ydb/config.yaml
class TConfigurationManager {
public:
    using TPtr = std::shared_ptr<TConfigurationManager>;

    // Default constructor - uses ~/.config/ydb/config.yaml
    TConfigurationManager();

    // Constructor with custom config file path (useful for testing)
    explicit TConfigurationManager(const TFsPath& configPath);

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

    // Get current config file path
    TFsPath GetPath() const { return ConfigPath; }

private:
    friend class TConfigNode;

    struct TImpl;
    std::unique_ptr<TImpl> Impl;
    TFsPath ConfigPath;
    bool Modified = false;

    void Load();
    void MarkModified();
};

// Global configuration instance (initializes on first call)
TConfigurationManager::TPtr GetGlobalConfig();

} // namespace NYdb::NConsoleClient
