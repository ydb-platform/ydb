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
TConfigNode::TConfigNode(YAML::Node node, TConfigurationManager* manager, const TString& path)
    : Node(node)
    , Manager(manager)
    , Path(path)
{}

bool TConfigNode::IsDefined() const {
    return Node.IsDefined();
}

bool TConfigNode::IsNull() const {
    return !Node.IsDefined() || Node.IsNull();
}

bool TConfigNode::IsScalar() const {
    return Node.IsDefined() && Node.IsScalar();
}

bool TConfigNode::IsSequence() const {
    return Node.IsDefined() && Node.IsSequence();
}

bool TConfigNode::IsMap() const {
    return Node.IsDefined() && Node.IsMap();
}

TConfigNode TConfigNode::GetChildBySimpleKey(const TString& key) {
    TString childPath = Path.empty() ? key : (Path + "." + key);

    GetGlobalLogger().Debug() << "GetChildBySimpleKey: path=\"" << Path << "\", key=\"" << key
               << "\", Node=" << NodeTypeStr(Node);

    // Check if trying to access child of a scalar
    if (Node.IsDefined() && !Node.IsNull() && Node.IsScalar()) {
        GetGlobalLogger().Debug() << "GetChildBySimpleKey: Trying to access child of scalar";
        throw std::runtime_error(TStringBuilder() << "operator[] call on a scalar (key: \"" << key << "\")");
    }

    YAML::Node child = Node[std::string(key)];
    GetGlobalLogger().Debug() << "GetChildBySimpleKey result: childPath=\"" << childPath << "\", child=" << NodeTypeStr(child);

    return TConfigNode(child, Manager, childPath);
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
    return TConfigNode(Node[index], Manager, childPath);
}

TString TConfigNode::AsString(const TString& defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Node.as<TString>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

bool TConfigNode::AsBool(bool defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Node.as<bool>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

int TConfigNode::AsInt(int defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Node.as<int>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

double TConfigNode::AsDouble(double defaultValue) const {
    if (!IsDefined() || IsNull()) {
        return defaultValue;
    }
    try {
        return Node.as<double>(defaultValue);
    } catch (...) {
        return defaultValue;
    }
}

void TConfigNode::SetValueInternal(const YAML::Node& value) {
    GetGlobalLogger().Debug() << "SetValueInternal: Path=\"" << Path << "\"";

    if (!Manager || Path.empty()) {
        Node = YAML::Clone(value);
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Config;

    GetGlobalLogger().Debug() << "SetValueInternal: parts.size=" << parts.size() << ", root=" << NodeTypeStr(root);

    // Ensure root is a map
    if (!root.IsDefined() || root.IsNull()) {
        root = YAML::Node(YAML::NodeType::Map);
        GetGlobalLogger().Debug() << "SetValueInternal: created root as map";
    }

    // Build complete subtree and assign to avoid yaml-cpp chained access issues
    // yaml-cpp has bugs with chained access like root["a"]["b"] = value
    switch (parts.size()) {
        case 1:
            GetGlobalLogger().Debug() << "SetValueInternal: setting root[" << parts[0] << "]";
            root[std::string(parts[0])] = value;
            GetGlobalLogger().Debug() << "SetValueInternal: after set, root=" << NodeTypeStr(root);
            break;
        case 2: {
            GetGlobalLogger().Debug() << "SetValueInternal: building subtree for " << parts[0] << "." << parts[1];
            // Get existing subtree or create new one
            YAML::Node subtree;
            if (root[std::string(parts[0])].IsMap()) {
                // Clone existing map to preserve other keys
                subtree = YAML::Clone(root[std::string(parts[0])]);
                GetGlobalLogger().Debug() << "SetValueInternal: cloned existing map";
            } else {
                subtree = YAML::Node(YAML::NodeType::Map);
                GetGlobalLogger().Debug() << "SetValueInternal: created new map";
            }
            // Set the value in subtree
            subtree[std::string(parts[1])] = value;
            // Assign entire subtree to root
            root[std::string(parts[0])] = subtree;
            GetGlobalLogger().Debug() << "SetValueInternal: after set, root=" << NodeTypeStr(root);
            GetGlobalLogger().Debug() << "SetValueInternal: after set, root[" << parts[0] << "]=" << NodeTypeStr(root[std::string(parts[0])]);
            break;
        }
        case 3: {
            GetGlobalLogger().Debug() << "SetValueInternal: building subtree for " << parts[0] << "." << parts[1] << "." << parts[2];
            // Level 1
            YAML::Node level1;
            if (root[std::string(parts[0])].IsMap()) {
                level1 = YAML::Clone(root[std::string(parts[0])]);
            } else {
                level1 = YAML::Node(YAML::NodeType::Map);
            }
            // Level 2
            YAML::Node level2;
            if (level1[std::string(parts[1])].IsMap()) {
                level2 = YAML::Clone(level1[std::string(parts[1])]);
            } else {
                level2 = YAML::Node(YAML::NodeType::Map);
            }
            // Set value
            level2[std::string(parts[2])] = value;
            level1[std::string(parts[1])] = level2;
            root[std::string(parts[0])] = level1;
            GetGlobalLogger().Debug() << "SetValueInternal: after set, root=" << NodeTypeStr(root);
            break;
        }
        default:
            GetGlobalLogger().Warning() << "Path too deep (max 3 levels): " << Path;
            return;
    }

    Manager->MarkModified();
}

void TConfigNode::SetString(const TString& value) {
    SetValueInternal(YAML::Node(std::string(value)));
}

void TConfigNode::SetBool(bool value) {
    GetGlobalLogger().Debug() << "SetBool: value=" << (value ? "true" : "false");
    SetValueInternal(YAML::Node(value));
}

void TConfigNode::SetInt(int value) {
    SetValueInternal(YAML::Node(value));
}

void TConfigNode::Remove() {
    if (!Manager || Path.empty()) {
        Node = YAML::Node();
        NotifyModified();
        return;
    }

    TVector<TString> parts;
    Split(Path, ".", parts);

    if (parts.empty()) {
        return;
    }

    YAML::Node& root = Manager->Config;

    // Navigate to parent and remove the key
    if (parts.size() == 1) {
        if (root.IsMap()) {
            root.remove(std::string(parts[0]));
        }
    } else if (parts.size() == 2) {
        if (root[std::string(parts[0])].IsMap()) {
            root[std::string(parts[0])].remove(std::string(parts[1]));
        }
    } else if (parts.size() == 3) {
        if (root[std::string(parts[0])][std::string(parts[1])].IsMap()) {
            root[std::string(parts[0])][std::string(parts[1])].remove(std::string(parts[2]));
        }
    }

    Node = YAML::Node();
    Manager->MarkModified();
}

void TConfigNode::NotifyModified() {
    if (Manager) {
        Manager->MarkModified();
    }
}

// TConfigurationManager implementation
TConfigurationManager::TConfigurationManager() {
    GetGlobalLogger().Debug() << "TConfigurationManager::ctor";
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
        TFsPath configPath = GetConfigFilePath();
        GetGlobalLogger().Debug() << "Load: path=" << configPath.GetPath() << ", exists=" << configPath.Exists();
        if (configPath.Exists()) {
            Config = YAML::LoadFile(configPath.GetPath());
            GetGlobalLogger().Debug() << "Load: loaded Config=" << NodeTypeStr(Config);
        } else {
            Config = YAML::Node(YAML::NodeType::Map);
            GetGlobalLogger().Debug() << "Load: created empty map";
        }
    } catch (const std::exception& e) {
        GetGlobalLogger().Warning() << "Failed to load CLI config: " << e.what();
        Config = YAML::Node(YAML::NodeType::Map);
    }
    Modified = false;
}

void TConfigurationManager::Save() {
    try {
        TFsPath configDir = GetConfigDir();
        EnsureDir(configDir, DIR_MODE_PRIVATE);

        TFsPath configPath = GetConfigFilePath();

        GetGlobalLogger().Debug() << "Save: before emit, Config=" << NodeTypeStr(Config);

        YAML::Emitter out;
        out.SetMapFormat(YAML::Block);
        out.SetSeqFormat(YAML::Block);
        out << Config;

        TString content = out.c_str();
        GetGlobalLogger().Debug() << "Save: emitter output:\n" << content;

        {
            TFileOutput file(TFile(configPath, CreateAlways | WrOnly | AWUser | ARUser));
            file << content;
            file.Finish();
        }

        Modified = false;

        // Reload config from file to ensure in-memory state is consistent
        GetGlobalLogger().Debug() << "Save: reloading from file";
        Config = YAML::LoadFile(configPath.GetPath());
        GetGlobalLogger().Debug() << "Save: after reload, Config=" << NodeTypeStr(Config);
    } catch (const std::exception& e) {
        GetGlobalLogger().Warning() << "Failed to save CLI config: " << e.what();
    }
}

void TConfigurationManager::Reload() {
    Load();
}

TConfigNode TConfigurationManager::operator[](const TString& key) {
    GetGlobalLogger().Debug() << "operator[]: key=\"" << key << "\"";
    // Reload from file before each access to work around yaml-cpp corruption bug
    // when accessing non-existent keys
    TFsPath configPath = GetConfigFilePath();
    if (configPath.Exists()) {
        try {
            Config = YAML::LoadFile(configPath.GetPath());
            GetGlobalLogger().Debug() << "operator[]: reloaded Config=" << NodeTypeStr(Config);
        } catch (...) {
            // Keep current Config if reload fails
        }
    }
    return Root()[key];
}

TConfigNode TConfigurationManager::operator[](const char* key) {
    return (*this)[TString(key)];
}

TConfigNode TConfigurationManager::Root() {
    GetGlobalLogger().Debug() << "Root: Config=" << NodeTypeStr(Config);
    return TConfigNode(Config, this, "");
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
