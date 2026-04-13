#include "config.h"
#include "log.h"

#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

#include <util/folder/dirut.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/file.h>

namespace NYdb::NConsoleClient {

// Pimpl implementation structures
struct TConfigNode::TImpl {
    YAML::Node Node;

    TImpl() = default;
    explicit TImpl(YAML::Node node) : Node(std::move(node)) {}
};

struct TConfigurationManager::TImpl {
    YAML::Node Config;
};

namespace {

constexpr int DIR_MODE_PRIVATE = S_IRUSR | S_IWUSR | S_IXUSR; // rwx------

[[maybe_unused]] TString NodeTypeStr(const YAML::Node& node) {
    if (!node.IsDefined()) return "undefined";
    if (node.IsNull()) return "null";
    if (node.IsScalar()) {
        try {
            return TStringBuilder() << "scalar(\"" << node.as<std::string>() << "\")";
        } catch (...) {
            return "scalar(<error>)";
        }
    }
    if (node.IsSequence()) return "sequence";
    if (node.IsMap()) {
        TStringBuilder sb;
        sb << "map{";
        bool first = true;
        for (const auto& kv : node) {
            if (!first) sb << ", ";
            first = false;
            try {
                sb << kv.first.as<std::string>();
            } catch (...) {
                sb << "?";
            }
        }
        sb << "}";
        return sb;
    }
    return "unknown";
}

// Recursively set value at path, handling yaml-cpp quirks by cloning at each level
void SetValueAtPath(YAML::Node& parent, const TVector<TString>& parts, size_t index, const YAML::Node& value) {
    if (index >= parts.size()) {
        return;
    }

    const std::string key = std::string(parts[index]);

    if (index == parts.size() - 1) {
        // Last element - set the value
        parent[key] = value;
    } else {
        // Intermediate element - get/create subtree
        YAML::Node subtree;
        if (parent[key].IsMap()) {
            subtree = YAML::Clone(parent[key]);
        } else {
            subtree = YAML::Node(YAML::NodeType::Map);
        }

        // Recursively set value in subtree
        SetValueAtPath(subtree, parts, index + 1, value);

        // Assign back to parent
        parent[key] = subtree;
    }
}

// Recursively remove value at path
void RemoveValueAtPath(YAML::Node& parent, const TVector<TString>& parts, size_t index) {
    if (index >= parts.size() || !parent.IsMap()) {
        return;
    }

    const std::string key = std::string(parts[index]);

    if (index == parts.size() - 1) {
        // Last element - remove it
        parent.remove(key);
    } else {
        // Intermediate element - navigate deeper
        if (parent[key].IsMap()) {
            YAML::Node subtree = YAML::Clone(parent[key]);
            RemoveValueAtPath(subtree, parts, index + 1);
            parent[key] = subtree;
        }
    }
}

// Safe key lookup that doesn't corrupt yaml-cpp internal state
// yaml-cpp has a bug where accessing nodes (even existing ones) can corrupt internal structure
// This function iterates to find the key and returns a Clone to prevent corruption
YAML::Node SafeGetChild(const YAML::Node& node, const std::string& key) {
    if (!node.IsMap()) {
        return YAML::Node();
    }
    for (auto it = node.begin(); it != node.end(); ++it) {
        try {
            if (it->first.as<std::string>() == key) {
                // Return a clone to prevent yaml-cpp from corrupting the original structure
                return YAML::Clone(it->second);
            }
        } catch (...) {
            continue;
        }
    }
    return YAML::Node();
}

void EnsureDir(const TFsPath& path, int mode) {
    if (path.Exists()) {
        return;
    }
#if defined(_win32_)
    Y_UNUSED(mode);
    path.MkDirs();
#else
    if (mode > 0) {
        path.MkDirs(mode);
    } else {
        path.MkDirs();
    }
#endif
}

// Global instance
static TConfigurationManager::TPtr GlobalConfig;

} // anonymous namespace

// TConfigNode implementation
TConfigNode::TConfigNode()
    : Impl(std::make_unique<TImpl>())
{}

TConfigNode::~TConfigNode() = default;

TConfigNode::TConfigNode(const TConfigNode& other)
    : Impl(std::make_unique<TImpl>(YAML::Clone(other.Impl->Node)))
    , Manager(other.Manager)
    , Path(other.Path)
{}

TConfigNode::TConfigNode(TConfigNode&& other) noexcept = default;

TConfigNode& TConfigNode::operator=(const TConfigNode& other) {
    if (this != &other) {
        Impl = std::make_unique<TImpl>(YAML::Clone(other.Impl->Node));
        Manager = other.Manager;
        Path = other.Path;
    }
    return *this;
}

TConfigNode& TConfigNode::operator=(TConfigNode&& other) noexcept = default;

bool TConfigNode::IsDefined() const {
    return Impl->Node.IsDefined();
}

bool TConfigNode::IsNull() const {
    return !Impl->Node.IsDefined() || Impl->Node.IsNull();
}

bool TConfigNode::IsScalar() const {
    return Impl->Node.IsDefined() && Impl->Node.IsScalar();
}

bool TConfigNode::IsSequence() const {
    return Impl->Node.IsDefined() && Impl->Node.IsSequence();
}

bool TConfigNode::IsMap() const {
    return Impl->Node.IsDefined() && Impl->Node.IsMap();
}

TConfigNode TConfigNode::GetChildBySimpleKey(const TString& key) {
    TString childPath = Path.empty() ? key : (Path + "." + key);

    GetGlobalLogger().Debug() << "GetChildBySimpleKey: path=\"" << Path << "\", key=\"" << key
               << "\", Node=" << NodeTypeStr(Impl->Node);

    // Check if trying to access child of a scalar
    if (Impl->Node.IsDefined() && !Impl->Node.IsNull() && Impl->Node.IsScalar()) {
        GetGlobalLogger().Debug() << "GetChildBySimpleKey: Trying to access child of scalar";
        throw std::runtime_error(TStringBuilder() << "operator[] call on a scalar (key: \"" << key << "\")");
    }

    // Use safe lookup to avoid yaml-cpp corruption bug
    YAML::Node child = SafeGetChild(Impl->Node, std::string(key));
    GetGlobalLogger().Debug() << "GetChildBySimpleKey result: childPath=\"" << childPath << "\", child=" << NodeTypeStr(child);

    TConfigNode result;
    result.Impl->Node = std::move(child);
    result.Manager = Manager;
    result.Path = childPath;
    return result;
}

TConfigNode TConfigNode::operator[](const TString& key) {
    // Support composite keys like "a.b.c"
    TVector<TString> parts;
    Split(key, ".", parts);

    if (parts.empty()) {
        return TConfigNode();
    }

    TConfigNode current = GetChildBySimpleKey(parts[0]);
    for (size_t i = 1; i < parts.size(); ++i) {
        current = current.GetChildBySimpleKey(parts[i]);
    }

    return current;
}

TConfigNode TConfigNode::operator[](const char* key) {
    return (*this)[TString(key)];
}

TConfigNode TConfigNode::operator[](size_t index) {
    TString childPath = Path + "[" + ToString(index) + "]";
    TConfigNode result;
    result.Impl->Node = Impl->Node[index];
    result.Manager = Manager;
    result.Path = childPath;
    return result;
}

TString TConfigNode::AsString(const TString& defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Impl->Node.as<TString>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

bool TConfigNode::AsBool(bool defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Impl->Node.as<bool>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

int TConfigNode::AsInt(int defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Impl->Node.as<int>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

double TConfigNode::AsDouble(double defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Impl->Node.as<double>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

void TConfigNode::SetValueInternal(const TString& value) {
    GetGlobalLogger().Debug() << "SetValueInternal(string): Path=\"" << Path << "\"";

    if (!Manager || Path.empty()) {
        Impl->Node = YAML::Node(std::string(value));
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Impl->Config;

    GetGlobalLogger().Debug() << "SetValueInternal: parts.size=" << parts.size() << ", root=" << NodeTypeStr(root);

    // Ensure root is a map
    if (!root.IsDefined() || root.IsNull()) {
        root = YAML::Node(YAML::NodeType::Map);
        GetGlobalLogger().Debug() << "SetValueInternal: created root as map";
    }

    // Use recursive function to handle any nesting depth
    SetValueAtPath(root, parts, 0, YAML::Node(std::string(value)));
    GetGlobalLogger().Debug() << "SetValueInternal: after set, root=" << NodeTypeStr(root);

    Manager->MarkModified();
}

void TConfigNode::SetValueInternal(bool value) {
    GetGlobalLogger().Debug() << "SetValueInternal(bool): Path=\"" << Path << "\", value=" << (value ? "true" : "false");

    if (!Manager || Path.empty()) {
        Impl->Node = YAML::Node(value);
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Impl->Config;

    // Ensure root is a map
    if (!root.IsDefined() || root.IsNull()) {
        root = YAML::Node(YAML::NodeType::Map);
    }

    SetValueAtPath(root, parts, 0, YAML::Node(value));
    Manager->MarkModified();
}

void TConfigNode::SetValueInternal(int value) {
    GetGlobalLogger().Debug() << "SetValueInternal(int): Path=\"" << Path << "\"";

    if (!Manager || Path.empty()) {
        Impl->Node = YAML::Node(value);
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Impl->Config;

    // Ensure root is a map
    if (!root.IsDefined() || root.IsNull()) {
        root = YAML::Node(YAML::NodeType::Map);
    }

    SetValueAtPath(root, parts, 0, YAML::Node(value));
    Manager->MarkModified();
}

void TConfigNode::SetString(const TString& value) {
    SetValueInternal(value);
}

void TConfigNode::SetBool(bool value) {
    SetValueInternal(value);
}

void TConfigNode::SetInt(int value) {
    SetValueInternal(value);
}

void TConfigNode::Remove() {
    if (!Manager || Path.empty()) {
        Impl->Node = YAML::Node();
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Impl->Config;

    // Use recursive function to handle any nesting depth
    RemoveValueAtPath(root, parts, 0);

    Impl->Node = YAML::Node();
    Manager->MarkModified();
}

void TConfigNode::NotifyModified() {
    if (Manager) {
        Manager->MarkModified();
    }
}

// TConfigurationManager implementation
TConfigurationManager::TConfigurationManager()
    : Impl(std::make_unique<TImpl>())
    , ConfigPath(GetConfigFilePath())
{
    GetGlobalLogger().Debug() << "TConfigurationManager::ctor";
    Load();
}

TConfigurationManager::TConfigurationManager(const TFsPath& configPath)
    : Impl(std::make_unique<TImpl>())
    , ConfigPath(configPath)
{
    GetGlobalLogger().Debug() << "TConfigurationManager::ctor with path=" << configPath.GetPath();
    Load();
}

TConfigurationManager::~TConfigurationManager() {
    if (Modified) {
        Save();
    }
}

TFsPath TConfigurationManager::GetConfigDir() {
    TFsPath homeDir(GetHomeDir());
    return homeDir / ".config" / "ydb";
}

TFsPath TConfigurationManager::GetConfigFilePath() {
    return GetConfigDir() / "config.yaml";
}

void TConfigurationManager::Load() {
    try {
        GetGlobalLogger().Debug() << "Load: path=" << ConfigPath.GetPath() << ", exists=" << ConfigPath.Exists();
        if (ConfigPath.Exists()) {
            Impl->Config = YAML::LoadFile(ConfigPath.GetPath());
            GetGlobalLogger().Debug() << "Load: loaded Config=" << NodeTypeStr(Impl->Config);
        } else {
            Impl->Config = YAML::Node(YAML::NodeType::Map);
            GetGlobalLogger().Debug() << "Load: created empty map";
        }
    } catch (const std::exception& e) {
        GetGlobalLogger().Warning() << "Failed to load CLI config: " << e.what();
        Impl->Config = YAML::Node(YAML::NodeType::Map);
    }
    Modified = false;
}

void TConfigurationManager::Save() {
    try {
        TFsPath configDir = ConfigPath.Parent();
        EnsureDir(configDir, DIR_MODE_PRIVATE);

        GetGlobalLogger().Debug() << "Save: before emit, Config=" << NodeTypeStr(Impl->Config);

        YAML::Emitter out;
        out.SetMapFormat(YAML::Block);
        out.SetSeqFormat(YAML::Block);
        out << Impl->Config;

        TString content = out.c_str();
        GetGlobalLogger().Debug() << "Save: emitter output:\n" << content;

        {
            TFileOutput file(TFile(ConfigPath, CreateAlways | WrOnly | AWUser | ARUser));
            file << content;
            file.Finish();
        }

        Modified = false;

        // Reload config from file to ensure in-memory state is consistent
        GetGlobalLogger().Debug() << "Save: reloading from file";
        Impl->Config = YAML::LoadFile(ConfigPath.GetPath());
        GetGlobalLogger().Debug() << "Save: after reload, Config=" << NodeTypeStr(Impl->Config);
    } catch (const std::exception& e) {
        GetGlobalLogger().Warning() << "Failed to save CLI config: " << e.what();
    }
}

void TConfigurationManager::Reload() {
    Load();
}

TConfigNode TConfigurationManager::operator[](const TString& key) {
    GetGlobalLogger().Debug() << "operator[]: key=\"" << key << "\"";
    return Root()[key];
}

TConfigNode TConfigurationManager::operator[](const char* key) {
    return (*this)[TString(key)];
}

TConfigNode TConfigurationManager::Root() {
    GetGlobalLogger().Debug() << "Root: Config=" << NodeTypeStr(Impl->Config);
    TConfigNode result;
    result.Impl->Node = Impl->Config;
    result.Manager = this;
    result.Path = "";
    return result;
}

void TConfigurationManager::MarkModified() {
    GetGlobalLogger().Debug() << "MarkModified: saving";
    Modified = true;
    Save();
}

// Global config functions
TConfigurationManager::TPtr GetGlobalConfig() {
    if (!GlobalConfig) {
        GetGlobalLogger().Debug() << "GetGlobalConfig: creating new instance";
        GlobalConfig = std::make_shared<TConfigurationManager>();
    }
    return GlobalConfig;
}

} // namespace NYdb::NConsoleClient
