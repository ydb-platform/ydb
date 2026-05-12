#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <util/generic/fwd.h>
#include <util/folder/path.h>

#include <optional>
#include <vector>

namespace NYdb::NConsoleClient {

// Theme variant (dark/light)
enum class EThemeVariant {
    Unspecified,
    Dark,
    Light
};

// Theme information
struct TThemeInfo {
    TString Id;             // Unique identifier (e.g., "monaco-dark", or file name without extension for user themes)
    TString Name;           // Display name (e.g., "Monaco Dark")
    TString FileName;       // File name (e.g., "monaco-dark.yaml")
    TString FilePath;       // Full path to file (empty for built-in)
    bool IsBuiltIn = false; // True for built-in themes
    bool IsDefault = false; // True for the default theme
    EThemeVariant Variant = EThemeVariant::Unspecified;

    TColorSchema Schema;
};

// Interactive mode settings
// Works with global config for persistence and provides domain-specific API
class TInteractiveSettings {
public:
    // Hints
    static bool IsHintsEnabled();
    static void SetHintsEnabled(bool enabled);

    // Colors mode
    static EGlobalColorsMode GetColorsMode();
    static void SetColorsMode(EGlobalColorsMode mode);

    // Theme management
    static TString GetCurrentThemeName();
    static TColorSchema GetCurrentColorSchema();
    static bool SetTheme(const TString& themeName);

    // List all available themes (built-in + user)
    static std::vector<TThemeInfo> ListThemes();

    // Get specific theme by name
    static std::optional<TThemeInfo> GetThemeByName(const TString& name);

    // Clone a theme to user themes directory
    static bool CloneTheme(const TString& sourceName, const TString& newName, const TString& fileName);

    // Themes directory path
    static TFsPath GetThemesDir();

private:
    // Built-in themes
    static std::vector<TThemeInfo> GetBuiltInThemes();

    // Load user themes from disk
    static std::vector<TThemeInfo> LoadUserThemes();

    // Load single theme from file
    static std::optional<TThemeInfo> LoadThemeFromFile(const TFsPath& path);

    // Save theme to file
    static bool SaveThemeToFile(const TThemeInfo& theme, const TFsPath& path);
};

// Helper to convert EGlobalColorsMode to/from string
TString ColorsModeToString(EGlobalColorsMode mode);
std::optional<EGlobalColorsMode> ColorsModeFromString(const TString& str);

} // namespace NYdb::NConsoleClient

