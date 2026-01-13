#include "config_ui.h"
#include "interactive_settings.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>

#include <util/charset/utf8.h>

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/string/builder.h>
#include <util/string/strip.h>

#include <chrono>
#include <thread>

#if defined(_unix_)
#include <termios.h>
#include <unistd.h>
#elif defined(_win_)
#include <windows.h>
#endif

namespace NYdb::NConsoleClient {

namespace {

// Convert replxx color to FTXUI color
ftxui::Color ToFtxuiColor(replxx::Replxx::Color rColor) {
    const int colorIndex = static_cast<int>(rColor);
    if (rColor == replxx::Replxx::Color::DEFAULT || colorIndex < 0) {
        return ftxui::Color::Default;
    }
    return ftxui::Color::Palette256(static_cast<uint8_t>(colorIndex));
}

// Get the comment color from a schema as FTXUI color
ftxui::Color GetCommentColor(const TColorSchema& schema) {
    return ToFtxuiColor(schema.comment.fg);
}

// Sample YQL query for theme preview
constexpr const char* SampleQuery = R"(-- Sample YQL query
$from = CAST("2024-01-01T00:00:00Z" AS Timestamp); $min_total = CAST(10u AS Uint32);
$stats = (SELECT u.id AS `user_id`, u.name, COUNT(*) AS `total`
  FROM users AS u
  WHERE u.created_at >= $from AND u.status IN ("active","pending")
  GROUP BY u.id, u.name);
SELECT `user_id`, name, `total`
FROM $stats
WHERE `total` >= $min_total
ORDER BY `total` DESC
LIMIT 100;)";

// Create syntax-highlighted preview element for a theme
ftxui::Element CreateThemePreview(const TColorSchema& schema) {
    auto highlighter = MakeYQLHighlighter(schema);

    TString query(SampleQuery);
    TColors colors(GetNumberOfUTF8Chars(query), replxx::Replxx::Color::DEFAULT);

    highlighter->Apply(query, colors);

    return PrintYqlHighlightFtxuiColors(query, colors);
}

void FlushStdinLocal() {
#if defined(_unix_)
    struct termios t;
    if (tcgetattr(STDIN_FILENO, &t) == 0) {
        struct termios raw = t;
        raw.c_lflag &= ~ECHO;
        tcsetattr(STDIN_FILENO, TCSANOW, &raw);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        tcflush(STDIN_FILENO, TCIFLUSH);

        tcsetattr(STDIN_FILENO, TCSANOW, &t);
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        tcflush(STDIN_FILENO, TCIFLUSH);
    }
#elif defined(_win_)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    FlushConsoleInputBuffer(GetStdHandle(STD_INPUT_HANDLE));
#endif
}

// Common theme selector with preview
// Returns selected theme index, or nullopt if cancelled
// currentThemeId - if not empty, marks that theme with "(Current)"
std::optional<size_t> RunThemeSelector(
    const TString& title,
    const std::vector<TThemeInfo>& themes,
    int initialSelected,
    const TString& currentThemeId = "")
{
    if (themes.empty()) {
        return std::nullopt;
    }

    int selected = initialSelected;
    std::optional<size_t> result;

    try {
        auto screen = ftxui::ScreenInteractive::FitComponent();
        screen.TrackMouse(false);
        auto exit = screen.ExitLoopClosure();

        // Simple labels for Menu (we'll override rendering in transform)
        std::vector<std::string> themeLabels;
        for (const auto& theme : themes) {
            themeLabels.push_back(std::string(theme.Name));
        }

        ftxui::MenuOption menuOption;
        menuOption.on_enter = [&]() {
            result = selected;
            exit();
        };
        menuOption.focused_entry = selected;

        // Custom transform to render each entry with styled suffix
        menuOption.entries_option.transform = [&themes, &currentThemeId](const ftxui::EntryState& state) {
            const size_t idx = static_cast<size_t>(state.index);
            const auto& theme = themes[idx];

            // Use dim styling only when colors are enabled
            bool useDim = GetGlobalColorsMode() != EGlobalColorsMode::Never;

            // Build entry: Name + suffix parts
            std::vector<ftxui::Element> parts;
            parts.push_back(ftxui::text(std::string(theme.Name)));

            // Add [Dark] or [Light]
            if (theme.Variant == EThemeVariant::Dark) {
                auto suffix = ftxui::text(" [Dark]");
                parts.push_back(useDim ? suffix | ftxui::dim : suffix);
            } else if (theme.Variant == EThemeVariant::Light) {
                auto suffix = ftxui::text(" [Light]");
                parts.push_back(useDim ? suffix | ftxui::dim : suffix);
            }

            // Add (current) if this is the current theme
            if (!currentThemeId.empty() && theme.Id == currentThemeId) {
                auto suffix = ftxui::text(" (current)");
                parts.push_back(useDim ? suffix | ftxui::dim : suffix);
            }

            auto element = ftxui::hbox(parts);

            // Use state.active for highlighting (tracks our selected variable)
            // This ensures wrap-around navigation works correctly
            if (state.active) {
                element = element | ftxui::inverted;
                element = ftxui::hbox({ftxui::text("> "), element});
            } else {
                element = ftxui::hbox({ftxui::text("  "), element});
            }
            return element;
        };

        auto menu = ftxui::Menu(&themeLabels, &selected, menuOption);
        int numThemes = static_cast<int>(themes.size());

        auto component = ftxui::CatchEvent(menu, [&](const ftxui::Event& event) {
            if (event == ftxui::Event::Escape || event == ftxui::Event::CtrlC) {
                exit();
                return true;
            }
            // Wrap-around navigation
            if (event == ftxui::Event::ArrowDown || event == ftxui::Event::Character('j')) {
                if (selected == numThemes - 1) {
                    selected = 0;
                    return true;
                }
            }
            if (event == ftxui::Event::ArrowUp || event == ftxui::Event::Character('k')) {
                if (selected == 0) {
                    selected = numThemes - 1;
                    return true;
                }
            }
            return false;
        });

        auto renderer = ftxui::Renderer(component, [&]() {
            ftxui::Element preview;
            ftxui::Color borderColor = ftxui::Color::Default;

            if (selected >= 0 && static_cast<size_t>(selected) < themes.size()) {
                preview = CreateThemePreview(themes[selected].Schema);
                // Use comment color from selected theme for borders
                borderColor = GetCommentColor(themes[selected].Schema);
            } else {
                preview = ftxui::text("No preview available");
            }

            auto menuElement = menu->Render();

            // Left panel: theme list in bordered box with comment color border
            auto leftPanel = ftxui::vbox({
                ftxui::text(" Themes ") | ftxui::bold | ftxui::center,
                ftxui::separator() | ftxui::color(borderColor),
                menuElement | ftxui::frame,
            }) | ftxui::borderStyled(borderColor) | ftxui::size(ftxui::WIDTH, ftxui::EQUAL, 40);

            // Right panel: preview in bordered box with comment color border
            auto rightPanel = ftxui::vbox({
                ftxui::text(" Preview ") | ftxui::bold | ftxui::center,
                ftxui::separator() | ftxui::color(borderColor),
                preview,
            }) | ftxui::borderStyled(borderColor) | ftxui::flex;

            // Spacer between panels (3 spaces)
            auto spacer = ftxui::text("     ");

            // Inner content with horizontal padding
            auto innerContent = ftxui::hbox({
                ftxui::text(" "),  // Left padding
                leftPanel,
                spacer,
                rightPanel | ftxui::vcenter,
                ftxui::text(" "),  // Right padding
            });

            // Outer box with title and navigation hints, with comment color border
            return ftxui::vbox({
                ftxui::text(std::string(title)) | ftxui::bold,
                ftxui::separator() | ftxui::color(borderColor),
                innerContent,
                ftxui::separator() | ftxui::color(borderColor),
                ftxui::text("Use arrows ↑ and ↓ to choose, Enter to confirm, Esc to cancel"),
            }) | ftxui::borderStyled(borderColor) | ftxui::center;
        });

        screen.Loop(renderer);
    } catch (const std::exception& e) {
        Cerr << "FTXUI theme selector failed: " << e.what() << Endl;
        return std::nullopt;
    }

    FlushStdinLocal();
    return result;
}

} // anonymous namespace

bool TConfigUI::Run(const TContext& ctx) {
    Cout << Endl;

    // Set border color based on current theme (respects colors mode via GetCurrentColorSchema)
    auto updateBorderColor = []() {
        auto schema = TInteractiveSettings::GetCurrentColorSchema();
        SetFtxuiBorderColor(GetCommentColor(schema));
    };
    updateBorderColor();

    for (bool exit = false; !exit;) {
        std::vector<TMenuEntry> options;

        // Enable/Disable hints toggle (use \t for dimmed suffix)
        bool hintsEnabled = TInteractiveSettings::IsHintsEnabled();
        options.emplace_back(
            TStringBuilder() << "Enable/Disable hints\t" << (hintsEnabled ? "ON" : "OFF"),
            [&ctx]() {
                RunEnableHints(ctx);
            }
        );

        // Enable/Disable colors (second position as requested)
        EGlobalColorsMode colorsMode = TInteractiveSettings::GetColorsMode();
        options.emplace_back(
            TStringBuilder() << "Enable/Disable colors\t" << ColorsModeToString(colorsMode),
            [&ctx, &updateBorderColor]() {
                RunColorsMode(ctx);
                updateBorderColor();  // Update border color after colors mode change
            }
        );

        // Choose color theme
        TString currentTheme = TInteractiveSettings::GetCurrentThemeName();
        options.emplace_back(
            TStringBuilder() << "Choose color theme\t" << currentTheme,
            [&ctx, &updateBorderColor]() {
                RunChooseTheme(ctx);
                updateBorderColor();  // Update border color after theme change
            }
        );

        // Clone color theme
        options.emplace_back(
            "Clone color theme...",
            [&ctx]() {
                RunCloneTheme(ctx);
            }
        );

        if (!RunFtxuiMenuWithActions("Interactive Mode Settings:", options)) {
            exit = true;
            Cout << Endl;
        }
    }

    // Reset border color to default when exiting
    SetFtxuiBorderColor(ftxui::Color::Default);

    return true;
}

bool TConfigUI::RunEnableHints(const TContext& ctx) {
    std::vector<TString> options = {"Enable hints", "Disable hints"};

    // Select current state: 0 = enabled, 1 = disabled
    size_t initialSelected = TInteractiveSettings::IsHintsEnabled() ? 0 : 1;

    auto selected = RunFtxuiMenu("Enable hints (inline suggestions while typing):", options, initialSelected);
    if (!selected) {
        return false;
    }

    bool newValue = (*selected == 0);
    if (newValue != TInteractiveSettings::IsHintsEnabled()) {
        TInteractiveSettings::SetHintsEnabled(newValue);
        if (ctx.LineReader) {
            ctx.LineReader->SetHintsEnabled(newValue);
        }
        Cout << Endl << "Hints " << (newValue ? "enabled" : "disabled") << "." << Endl << Endl;
    }

    return true;
}

bool TConfigUI::RunChooseTheme(const TContext& ctx) {
    auto themes = TInteractiveSettings::ListThemes();
    if (themes.empty()) {
        Cout << "No themes available." << Endl;
        return false;
    }

    TString currentThemeId = TInteractiveSettings::GetCurrentThemeName();  // Returns theme ID

    // Find current theme index by Id
    int initialSelected = 0;
    for (size_t i = 0; i < themes.size(); ++i) {
        if (themes[i].Id == currentThemeId) {
            initialSelected = static_cast<int>(i);
            break;
        }
    }

    auto result = RunThemeSelector("Choose color theme:", themes, initialSelected, currentThemeId);

    if (result && *result < themes.size()) {
        const auto& selectedTheme = themes[*result];
        if (selectedTheme.Id != currentThemeId) {
            TInteractiveSettings::SetTheme(selectedTheme.Id);
            if (ctx.LineReader) {
                ctx.LineReader->SetColorSchema(selectedTheme.Schema);
            }

            // Build display name with variant
            TStringBuilder displayName;
            displayName << selectedTheme.Name;
            if (selectedTheme.Variant == EThemeVariant::Dark) {
                displayName << " [Dark]";
            } else if (selectedTheme.Variant == EThemeVariant::Light) {
                displayName << " [Light]";
            }

            Cout << Endl << "Theme changed to: " << selectedTheme.Id << " (" << displayName << ")" << Endl << Endl;
        }
        return true;
    }

    return false;
}

bool TConfigUI::RunCloneTheme(const TContext& ctx) {
    auto themes = TInteractiveSettings::ListThemes();
    if (themes.empty()) {
        Cout << "No themes available to clone." << Endl;
        return false;
    }

    auto sourceIdx = RunThemeSelector("Select theme to clone:", themes, 0);

    if (!sourceIdx || *sourceIdx >= themes.size()) {
        return false;
    }

    const auto& sourceTheme = themes[*sourceIdx];
    TString defaultName = TStringBuilder() << sourceTheme.Name << " (copy)";

    // Get new theme name - pre-fill with default value
    auto newName = RunFtxuiInput(
        "Enter name for cloned theme:",
        defaultName,
        [&themes](const TString& input, TString& error) {
            TString name = Strip(input);
            if (name.empty()) {
                error = "Theme name cannot be empty";
                return false;
            }
            for (const auto& theme : themes) {
                if (theme.Name == name) {
                    error = TStringBuilder() << "Theme \"" << name << "\" already exists";
                    return false;
                }
            }
            return true;
        }
    );

    if (!newName) {
        return false;
    }

    TString themeName = Strip(*newName);

    // Compute default file name:
    // If theme name wasn't changed, use source_id + "_copy"
    // Otherwise, use the entered theme name with special chars replaced
    TString suggestedFileName;
    if (themeName == defaultName) {
        // User didn't change the name, use source id + _copy
        suggestedFileName = sourceTheme.Id + "_copy";
    } else {
        // User changed the name, base file name on new name
        suggestedFileName = themeName;
        // Replace spaces and special chars with underscores for file name
        for (size_t i = 0; i < suggestedFileName.size(); ++i) {
            char c = suggestedFileName[i];
            if (c == ' ' || c == '(' || c == ')' || c == '/' || c == '\\') {
                suggestedFileName[i] = '_';
            }
        }
    }

    // Get file name with retry loop for existing files
    TString fileNameStr;
    while (true) {
        auto fileName = RunFtxuiInputWithSuffix(
            "Enter file name:",
            suggestedFileName,
            ".yaml",
            [](const TString& input, TString& error) {
                TString name = Strip(input);
                if (name.empty()) {
                    error = "File name cannot be empty";
                    return false;
                }
                return true;  // Don't check for existence here, we'll handle it separately
            }
        );

        if (!fileName) {
            return false;
        }

        fileNameStr = Strip(*fileName) + ".yaml";
        TFsPath path = TInteractiveSettings::GetThemesDir() / fileNameStr;

        if (path.Exists()) {
            Cout << Endl << "File \"" << fileNameStr << "\" already exists." << Endl;
            if (AskYesNoFtxui("Overwrite existing file?", false)) {
                break;  // User confirmed overwrite
            }
            // User declined, let them enter a new name
            suggestedFileName = Strip(*fileName);  // Keep their input for next iteration
            continue;
        }

        break;  // File doesn't exist, proceed
    }

    // Clone the theme
    if (TInteractiveSettings::CloneTheme(sourceTheme.Id, themeName, fileNameStr)) {
        TFsPath themePath = TInteractiveSettings::GetThemesDir() / fileNameStr;
        auto& colors = AutoColors(Cout);
        Cout << Endl << "Theme \"" << themeName << "\" created." << Endl;
        Cout << "You can edit file " << colors.BoldColor() << themePath << colors.OldColor() << " to customize colors." << Endl << Endl;

        // Theme ID is the file name without .yaml extension
        TString themeId = fileNameStr;
        if (themeId.EndsWith(".yaml")) {
            themeId = themeId.substr(0, themeId.size() - 5);
        }

        // Ask if user wants to switch to the cloned theme
        if (AskYesNoFtxui("Switch to the cloned theme?", true)) {
            TInteractiveSettings::SetTheme(themeId);
            if (auto theme = TInteractiveSettings::GetThemeByName(themeId)) {
                if (ctx.LineReader) {
                    ctx.LineReader->SetColorSchema(theme->Schema);
                }
            }
            Cout << Endl << "Switched to theme: " << themeName << Endl;
        }

        Cout << Endl;
        return true;
    } else {
        Cerr << "Failed to clone theme." << Endl;
        return false;
    }
}

bool TConfigUI::RunColorsMode(const TContext& ctx) {
    std::vector<TString> options = {
        "auto - default behavior (respect NO_COLOR environment variable and TTY)",
        "never - completely disable colors",
        "always - enable colors (ignore NO_COLOR environment variable)"
    };

    EGlobalColorsMode currentMode = TInteractiveSettings::GetColorsMode();

    // Select current mode: Auto=0, Never=1, Always=2
    size_t initialSelected = static_cast<size_t>(currentMode);

    auto selected = RunFtxuiMenu("Choose colors mode:", options, initialSelected);
    if (!selected) {
        return false;
    }

    EGlobalColorsMode newMode = static_cast<EGlobalColorsMode>(*selected);
    if (newMode != currentMode) {
        TInteractiveSettings::SetColorsMode(newMode);

        // Apply globally
        SetGlobalColorsMode(newMode);

        // Apply to line reader
        if (ctx.LineReader) {
            // Color schema: GetCurrentColorSchema respects the global colors mode
            ctx.LineReader->SetColorSchema(TInteractiveSettings::GetCurrentColorSchema());

            // Prompt: always use AutoColors which respects the global mode
            auto& colors = AutoColors(Cout);
            ctx.LineReader->SetPrompt(TStringBuilder() << colors.Green() << "YQL" << colors.OldColor() << "> ");
        }

        // Update border color for menus (GetCurrentColorSchema respects colors mode)
        auto schema = TInteractiveSettings::GetCurrentColorSchema();
        SetFtxuiBorderColor(GetCommentColor(schema));

        Cout << Endl << "Colors mode set to: " << ColorsModeToString(newMode) << Endl << Endl;
    }

    return true;
}

} // namespace NYdb::NConsoleClient
