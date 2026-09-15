#include "interactive_settings.h"

#include <ydb/public/lib/ydb_cli/common/config.h>

#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/ascii.h>
#include <util/system/file.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <algorithm>
#include <unordered_set>

namespace NYdb::NConsoleClient {

namespace {

constexpr int DIR_MODE_PRIVATE = S_IRUSR | S_IWUSR | S_IXUSR; // rwx------

// Map of named colors to replxx values (case-insensitive)
static const std::unordered_map<TString, TColor> NamedColors = {
    {"black", TColor::BLACK},
    {"red", TColor::RED},
    {"green", TColor::GREEN},
    {"brown", TColor::BROWN},
    {"blue", TColor::BLUE},
    {"magenta", TColor::MAGENTA},
    {"cyan", TColor::CYAN},
    {"lightgray", TColor::LIGHTGRAY},
    {"gray", TColor::GRAY},
    {"brightred", TColor::BRIGHTRED},
    {"brightgreen", TColor::BRIGHTGREEN},
    {"yellow", TColor::YELLOW},
    {"brightblue", TColor::BRIGHTBLUE},
    {"brightmagenta", TColor::BRIGHTMAGENTA},
    {"brightcyan", TColor::BRIGHTCYAN},
    {"white", TColor::WHITE},
    {"default", TColor::DEFAULT},
};

std::optional<TColor> NameToColor(const TString& name) {
    TString lowerName = to_lower(name);
    auto it = NamedColors.find(lowerName);
    if (it != NamedColors.end()) {
        return it->second;
    }
    return std::nullopt;
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

// Parse hex color "#RRGGBB" and quantize to rgb666
// Accepts both uppercase (#AABBCC) and lowercase (#aabbcc)
std::optional<TColor> ParseHexColor(const TString& hex) {
    if (hex.size() != 7 || hex[0] != '#') {
        return std::nullopt;
    }

    // Validate all characters are hex digits
    for (size_t i = 1; i < 7; ++i) {
        char c = hex[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return std::nullopt;
        }
    }

    try {
        int r = std::stoi(std::string(hex.substr(1, 2)), nullptr, 16);
        int g = std::stoi(std::string(hex.substr(3, 2)), nullptr, 16);
        int b = std::stoi(std::string(hex.substr(5, 2)), nullptr, 16);

        // Quantize 0-255 to 0-5 (for 216-color terminal palette)
        // Each component is scaled from 0-255 to 0-5
        int r6 = (r * 5 + 127) / 255;
        int g6 = (g * 5 + 127) / 255;
        int b6 = (b * 5 + 127) / 255;

        return replxx::color::rgb666(r6, g6, b6);
    } catch (...) {
        return std::nullopt;
    }
}

// Parse a simple color value (not fg/bg object)
// Supported formats:
//   - String: color name (blue, red, default, etc.) or "#RRGGBB"
//   - Integer: direct replxx::Color value
//   - Array [r, g, b]: rgb666 components (0-5 each)
std::optional<TColor> ParseSimpleColor(const YAML::Node& node) {
    if (!node.IsDefined() || node.IsNull()) {
        return std::nullopt;
    }

    // Array [r, g, b] -> rgb666
    if (node.IsSequence() && node.size() == 3) {
        try {
            int r = node[0].as<int>();
            int g = node[1].as<int>();
            int b = node[2].as<int>();
            if (r >= 0 && r <= 5 && g >= 0 && g <= 5 && b >= 0 && b <= 5) {
                return replxx::color::rgb666(r, g, b);
            }
        } catch (...) {
        }
        Cerr << "Warning: Invalid rgb666 array, using default" << Endl;
        return TColor::DEFAULT;
    }

    if (node.IsScalar()) {
        TString value = node.as<TString>("");
        if (value.empty()) {
            return std::nullopt;
        }

        // Named color
        if (auto color = NameToColor(value)) {
            return color;
        }

        // Hex color "#RRGGBB"
        if (value.StartsWith("#")) {
            if (auto color = ParseHexColor(value)) {
                return color;
            }
            Cerr << "Warning: Invalid hex color \"" << value << "\" (expected format: #RRGGBB with hex digits), using default" << Endl;
            return TColor::DEFAULT;
        }

        // Direct integer value
        try {
            int intValue = std::stoi(std::string(value));
            return static_cast<TColor>(intValue);
        } catch (...) {
        }

        Cerr << "Warning: Unknown color: " << value << ", using default" << Endl;
        return TColor::DEFAULT;
    }

    // Legacy format: {rgb666: [r, g, b]}
    if (node.IsMap() && node["rgb666"]) {
        auto rgb666Node = node["rgb666"];
        if (rgb666Node.IsSequence() && rgb666Node.size() == 3) {
            try {
                int r = rgb666Node[0].as<int>();
                int g = rgb666Node[1].as<int>();
                int b = rgb666Node[2].as<int>();
                if (r >= 0 && r <= 5 && g >= 0 && g <= 5 && b >= 0 && b <= 5) {
                    return replxx::color::rgb666(r, g, b);
                }
            } catch (...) {
            }
        }
        return TColor::DEFAULT;
    }

    return std::nullopt;
}

// Parse color specification from YAML node
// Supported formats:
//   - Simple value (string, array, int) -> foreground only
//   - Object {fg: ..., bg: ...} -> foreground + optional background
std::optional<TColorSpec> ParseColorNode(const YAML::Node& node) {
    if (!node.IsDefined() || node.IsNull()) {
        return std::nullopt;
    }

    // Object with fg/bg keys
    if (node.IsMap() && (node["fg"] || node["bg"])) {
        TColorSpec spec;

        if (auto fgNode = node["fg"]) {
            if (auto fg = ParseSimpleColor(fgNode)) {
                spec.fg = *fg;
            }
        }

        if (auto bgNode = node["bg"]) {
            if (auto bg = ParseSimpleColor(bgNode)) {
                spec.bg = *bg;
            }
        }

        return spec;
    }

    // Simple color value -> foreground only
    if (auto color = ParseSimpleColor(node)) {
        return TColorSpec(*color);
    }

    return std::nullopt;
}

std::optional<TColorSchema> ParseSchemaNode(const YAML::Node& schemaNode) {
    if (!schemaNode.IsDefined() || !schemaNode.IsMap()) {
        return std::nullopt;
    }

    TColorSchema schema;

    auto getColorSpec = [&schemaNode](const char* key) -> TColorSpec {
        if (auto spec = ParseColorNode(schemaNode[key])) {
            return *spec;
        }
        return TColorSpec(TColor::DEFAULT);
    };

    schema.keyword = getColorSpec("keyword");
    schema.operation = getColorSpec("operation");
    schema.string = getColorSpec("string");
    schema.number = getColorSpec("number");
    schema.comment = getColorSpec("comment");
    schema.unknown = getColorSpec("unknown");

    if (auto identNode = schemaNode["identifier"]) {
        if (identNode.IsMap()) {
            if (auto c = ParseColorNode(identNode["function"])) {
                schema.identifier.function = *c;
            }
            if (auto c = ParseColorNode(identNode["type"])) {
                schema.identifier.type = *c;
            }
            if (auto c = ParseColorNode(identNode["variable"])) {
                schema.identifier.variable = *c;
            }
            if (auto c = ParseColorNode(identNode["quoted"])) {
                schema.identifier.quoted = *c;
            }
        }
    }

    return schema;
}

YAML::Node SimpleColorToYamlNode(TColor color) {
    int colorValue = static_cast<int>(color);

    // Check named colors first
    for (const auto& [name, val] : NamedColors) {
        if (val == color) {
            return YAML::Node(std::string(name));
        }
    }

    // RGB666 colors (16-231) -> compact array [r, g, b]
    if (colorValue >= 16 && colorValue <= 231) {
        int idx = colorValue - 16;
        int b = idx % 6;
        int g = (idx / 6) % 6;
        int r = idx / 36;

        YAML::Node rgbNode(YAML::NodeType::Sequence);
        rgbNode.SetStyle(YAML::EmitterStyle::Flow);  // Output as [r, g, b] not multi-line
        rgbNode.push_back(r);
        rgbNode.push_back(g);
        rgbNode.push_back(b);
        return rgbNode;
    }

    // Fallback to integer
    return YAML::Node(colorValue);
}

YAML::Node ColorSpecToYamlNode(const TColorSpec& spec) {
    // If no background, use simple format
    if (!spec.bg) {
        return SimpleColorToYamlNode(spec.fg);
    }

    // With background, use {fg: ..., bg: ...} format
    YAML::Node node;
    node.SetStyle(YAML::EmitterStyle::Flow);  // Compact: {fg: blue, bg: [1, 1, 1]}
    node["fg"] = SimpleColorToYamlNode(spec.fg);
    node["bg"] = SimpleColorToYamlNode(*spec.bg);
    return node;
}

YAML::Node SchemaToYamlNode(const TColorSchema& schema) {
    YAML::Node schemaNode;

    schemaNode["keyword"] = ColorSpecToYamlNode(schema.keyword);
    schemaNode["operation"] = ColorSpecToYamlNode(schema.operation);

    YAML::Node identNode;
    identNode["function"] = ColorSpecToYamlNode(schema.identifier.function);
    identNode["type"] = ColorSpecToYamlNode(schema.identifier.type);
    identNode["variable"] = ColorSpecToYamlNode(schema.identifier.variable);
    identNode["quoted"] = ColorSpecToYamlNode(schema.identifier.quoted);
    schemaNode["identifier"] = identNode;

    schemaNode["string"] = ColorSpecToYamlNode(schema.string);
    schemaNode["number"] = ColorSpecToYamlNode(schema.number);
    schemaNode["comment"] = ColorSpecToYamlNode(schema.comment);
    schemaNode["unknown"] = ColorSpecToYamlNode(schema.unknown);

    return schemaNode;
}

} // anonymous namespace

// Hints
bool TInteractiveSettings::IsHintsEnabled() {
    return (*GetGlobalConfig())["interactive.enable_hints"].AsBool(true);
}

void TInteractiveSettings::SetHintsEnabled(bool enabled) {
    (*GetGlobalConfig())["interactive.enable_hints"].SetBool(enabled);
}

// Colors mode
EGlobalColorsMode TInteractiveSettings::GetColorsMode() {
    TString modeStr = (*GetGlobalConfig())["colors.enable"].AsString("auto");
    return ColorsModeFromString(modeStr).value_or(EGlobalColorsMode::Auto);
}

void TInteractiveSettings::SetColorsMode(EGlobalColorsMode mode) {
    (*GetGlobalConfig())["colors.enable"].SetString(ColorsModeToString(mode));
}

// Theme management
TString TInteractiveSettings::GetCurrentThemeName() {
    TString theme = (*GetGlobalConfig())["colors.theme"].AsString("");
    // Empty means default theme
    return theme.empty() ? GetDefaultThemeName() : theme;
}

TColorSchema TInteractiveSettings::GetCurrentColorSchema() {
    // If colors are disabled globally, return no-color schema
    if (GetColorsMode() == EGlobalColorsMode::Never) {
        return GetColorSchema("no-color");
    }

    if (auto theme = GetThemeByName(GetCurrentThemeName())) {
        return theme->Schema;
    }
    // Fallback to default
    return GetColorSchema("");
}

bool TInteractiveSettings::SetTheme(const TString& themeId) {
    auto themes = ListThemes();
    for (const auto& theme : themes) {
        if (theme.Id == themeId) {
            (*GetGlobalConfig())["colors.theme"].SetString(themeId);
            return true;
        }
    }
    return false;
}

std::vector<TThemeInfo> TInteractiveSettings::ListThemes() {
    std::vector<TThemeInfo> allThemes = GetBuiltInThemes();

    // Get built-in IDs for conflict detection
    std::unordered_set<TString> builtInIds;
    for (const auto& theme : allThemes) {
        builtInIds.insert(theme.Id);
    }

    // Load user themes, skip those conflicting with built-in
    auto userThemes = LoadUserThemes();
    for (auto& userTheme : userThemes) {
        if (builtInIds.count(userTheme.Id)) {
            Cerr << "Warning: User theme \"" << userTheme.FilePath 
                 << "\" has ID \"" << userTheme.Id 
                 << "\" which conflicts with built-in theme. Skipping." << Endl;
            continue;
        }
        allThemes.push_back(std::move(userTheme));
    }

    // Sort alphabetically by Name, then by Variant (Dark before Light)
    std::sort(allThemes.begin(), allThemes.end(), [](const TThemeInfo& a, const TThemeInfo& b) {
        if (a.Name != b.Name) {
            return a.Name < b.Name;
        }
        // If same name, Dark before Light before Unspecified
        return static_cast<int>(a.Variant) > static_cast<int>(b.Variant);
    });

    return allThemes;
}

std::optional<TThemeInfo> TInteractiveSettings::GetThemeByName(const TString& name) {
    auto themes = ListThemes();

    // Empty name means default theme
    TString lookupId = name.empty() ? TString(GetDefaultThemeName()) : name;

    for (const auto& theme : themes) {
        if (theme.Id == lookupId) {
            return theme;
        }
    }
    return std::nullopt;
}

bool TInteractiveSettings::CloneTheme(const TString& sourceName, const TString& newName, const TString& fileName) {
    auto sourceTheme = GetThemeByName(sourceName);
    if (!sourceTheme) {
        return false;
    }

    TString actualFileName = fileName;
    if (!actualFileName.EndsWith(".yaml")) {
        actualFileName += ".yaml";
    }

    // Id is the file name without extension
    TString id = actualFileName;
    if (id.EndsWith(".yaml")) {
        id = id.substr(0, id.size() - 5);
    }

    TThemeInfo newTheme;
    newTheme.Id = id;
    newTheme.Name = newName;
    newTheme.Schema = sourceTheme->Schema;
    newTheme.Variant = sourceTheme->Variant;  // Copy variant from source
    newTheme.IsBuiltIn = false;

    TFsPath themePath = GetThemesDir() / actualFileName;
    newTheme.FileName = actualFileName;
    newTheme.FilePath = themePath.GetPath();

    return SaveThemeToFile(newTheme, themePath);
}

TFsPath TInteractiveSettings::GetThemesDir() {
    return TConfigurationManager::GetConfigDir() / "themes";
}

std::vector<TThemeInfo> TInteractiveSettings::GetBuiltInThemes() {
    std::vector<TThemeInfo> themes;

    const TString defaultThemeName = GetDefaultThemeName();

    // Helper to add built-in theme with all metadata
    auto addBuiltIn = [&themes, &defaultThemeName](
        const TString& id,           // Internal ID for schema lookup
        const TString& displayName,  // User-friendly name
        const TString& fileName,     // Suggested file name
        EThemeVariant variant
    ) {
        TThemeInfo theme;
        theme.Id = id;
        theme.Name = displayName;
        theme.FileName = fileName;
        theme.IsBuiltIn = true;
        theme.IsDefault = (id == defaultThemeName);
        theme.Variant = variant;
        theme.Schema = GetColorSchema(id);
        themes.push_back(std::move(theme));
    };

    // Built-in themes (will be sorted alphabetically by ListThemes)
    addBuiltIn("default", "Default", "default.yaml", EThemeVariant::Unspecified);
    addBuiltIn("dracula-bg", "Dracula", "dracula-bg.yaml", EThemeVariant::Dark);
    addBuiltIn("gruvbox-dark", "Gruvbox", "gruvbox-dark.yaml", EThemeVariant::Dark);
    addBuiltIn("gruvbox-light", "Gruvbox", "gruvbox-light.yaml", EThemeVariant::Light);
    addBuiltIn("monaco-dark", "Monaco", "monaco-dark.yaml", EThemeVariant::Dark);
    addBuiltIn("monaco-light", "Monaco", "monaco-light.yaml", EThemeVariant::Light);
    addBuiltIn("no-color", "No Color", "no-color.yaml", EThemeVariant::Unspecified);
    addBuiltIn("nord", "Nord", "nord.yaml", EThemeVariant::Dark);
    addBuiltIn("paper-bg", "Paper", "paper-bg.yaml", EThemeVariant::Light);
    addBuiltIn("solarized-dark", "Solarized", "solarized-dark.yaml", EThemeVariant::Dark);
    addBuiltIn("solarized-light", "Solarized", "solarized-light.yaml", EThemeVariant::Light);

    return themes;
}

std::vector<TThemeInfo> TInteractiveSettings::LoadUserThemes() {
    std::vector<TThemeInfo> themes;

    try {
        TFsPath themesDir = GetThemesDir();
        if (!themesDir.Exists()) {
            return themes;
        }

        TVector<TFsPath> files;
        themesDir.List(files);

        for (const auto& file : files) {
            if (file.GetExtension() == "yaml" || file.GetExtension() == "yml") {
                if (auto theme = LoadThemeFromFile(file)) {
                    themes.push_back(std::move(*theme));
                }
            }
        }
    } catch (const std::exception& e) {
        Cerr << "Warning: Failed to load user themes: " << e.what() << Endl;
    }

    return themes;
}

std::optional<TThemeInfo> TInteractiveSettings::LoadThemeFromFile(const TFsPath& path) {
    try {
        YAML::Node yaml = YAML::LoadFile(path.GetPath());

        TThemeInfo theme;
        theme.FileName = path.GetName();
        theme.FilePath = path.GetPath();
        theme.IsBuiltIn = false;

        // ID is the file name without extension
        TString baseName = path.GetName();
        if (baseName.EndsWith(".yaml")) {
            theme.Id = baseName.substr(0, baseName.size() - 5);
        } else if (baseName.EndsWith(".yml")) {
            theme.Id = baseName.substr(0, baseName.size() - 4);
        } else {
            theme.Id = baseName;
        }

        // Display name from YAML or fallback to ID
        if (auto nameNode = yaml["name"]) {
            theme.Name = nameNode.as<TString>("");
        }
        if (theme.Name.empty()) {
            theme.Name = theme.Id;
        }

        // Parse variant (dark/light)
        if (auto variantNode = yaml["variant"]) {
            TString variantStr = variantNode.as<TString>("");
            if (variantStr == "dark") {
                theme.Variant = EThemeVariant::Dark;
            } else if (variantStr == "light") {
                theme.Variant = EThemeVariant::Light;
            }
        }

        if (auto schemaNode = yaml["schema"]) {
            if (auto schema = ParseSchemaNode(schemaNode)) {
                theme.Schema = *schema;
            } else {
                theme.Schema = GetColorSchema("");
            }
        } else {
            theme.Schema = GetColorSchema("");
        }

        return theme;
    } catch (const std::exception& e) {
        Cerr << "Warning: Failed to load theme from " << path.GetPath() << ": " << e.what() << Endl;
        return std::nullopt;
    }
}

// Header comment for theme files
constexpr const char* ThemeFileHeader = R"(# YDB CLI Color Theme
#
# Color formats:
#   - Named:   black, red, green, brown, blue, magenta, cyan, lightgray,
#              gray, brightred, brightgreen, yellow, brightblue,
#              brightmagenta, brightcyan, white, default
#   - RGB:     [r, g, b] where r, g, b are 0-5 (terminal 216-color palette)
#   - Hex:     "#RRGGBB" - converted to nearest RGB[0-5] value
#              Example: #9e7cb0 -> [3, 2, 3] (purple-ish)
#   - Integer: direct terminal color code (0-255)
#
# Schema fields:
#   keyword, operation, string, number, comment, unknown
#   identifier.function, identifier.type, identifier.variable, identifier.quoted
#

)";

bool TInteractiveSettings::SaveThemeToFile(const TThemeInfo& theme, const TFsPath& path) {
    try {
        TFsPath themesDir = GetThemesDir();
        EnsureDir(themesDir, DIR_MODE_PRIVATE);

        YAML::Node yaml;
        yaml["name"] = std::string(theme.Name);

        // Save variant if specified
        if (theme.Variant == EThemeVariant::Dark) {
            yaml["variant"] = "dark";
        } else if (theme.Variant == EThemeVariant::Light) {
            yaml["variant"] = "light";
        }

        yaml["schema"] = SchemaToYamlNode(theme.Schema);

        YAML::Emitter out;
        out.SetMapFormat(YAML::Block);
        // Don't set SetSeqFormat - let individual nodes keep their Flow style for [r, g, b]
        out << yaml;

        TFileOutput file(TFile(path, CreateAlways | WrOnly | AWUser | ARUser));
        file << ThemeFileHeader;
        file << out.c_str();
        file << "\n";

        return true;
    } catch (const std::exception& e) {
        Cerr << "Warning: Failed to save theme to " << path.GetPath() << ": " << e.what() << Endl;
        return false;
    }
}

// Helper functions
TString ColorsModeToString(EGlobalColorsMode mode) {
    switch (mode) {
        case EGlobalColorsMode::Auto:
            return "auto";
        case EGlobalColorsMode::Never:
            return "never";
        case EGlobalColorsMode::Always:
            return "always";
    }
    return "auto";
}

std::optional<EGlobalColorsMode> ColorsModeFromString(const TString& str) {
    TString lower = to_lower(str);
    if (lower == "auto") {
        return EGlobalColorsMode::Auto;
    }
    if (lower == "never") {
        return EGlobalColorsMode::Never;
    }
    if (lower == "always") {
        return EGlobalColorsMode::Always;
    }
    return std::nullopt;
}

} // namespace NYdb::NConsoleClient
