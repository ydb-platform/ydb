#include "schema.h"

#include <util/stream/output.h>

#include <unordered_map>
#include <functional>

namespace NYdb::NConsoleClient {

namespace {

    using replxx::color::rgb666;

    // Helper to create TColorSpec with background
    TColorSpec WithBg(TColor fg, TColor bg) {
        return TColorSpec(fg, bg);
    }

    TColorSchema MakeNoColor() {
        return {
            .keyword = TColor::DEFAULT,
            .operation = TColor::DEFAULT,
            .identifier = {
                .function = TColor::DEFAULT,
                .type = TColor::DEFAULT,
                .variable = TColor::DEFAULT,
                .quoted = TColor::DEFAULT,
            },
            .string = TColor::DEFAULT,
            .number = TColor::DEFAULT,
            .comment = TColor::DEFAULT,
            .unknown = TColor::DEFAULT,
        };
    }

    // Default theme - balanced colors for both dark and light terminals
    TColorSchema MakeDefault() {
        return {
            .keyword = rgb666(1, 2, 5),
            .operation = rgb666(2, 2, 2),
            .identifier = {
                .function = rgb666(3, 2, 3),
                .type = rgb666(2, 3, 2),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 3, 3),
            },
            .string = rgb666(4, 2, 1),
            .number = rgb666(1, 3, 1),
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeMonacoDark() {
        return {
            .keyword = TColor::BLUE,
            .operation = rgb666(3, 3, 3),
            .identifier = {
                .function = rgb666(4, 1, 5),
                .type = rgb666(2, 3, 2),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 3, 3),
            },
            .string = rgb666(3, 0, 0),
            .number = TColor::BRIGHTGREEN,
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeMonacoLight() {
        return {
            .keyword = TColor::BLUE,
            .operation = rgb666(2, 2, 2),
            .identifier = {
                .function = rgb666(3, 0, 4),
                .type = rgb666(1, 2, 1),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(0, 2, 2),
            },
            .string = rgb666(3, 0, 0),
            .number = TColor::GREEN,
            .comment = rgb666(1, 1, 1),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeSolarizedDark() {
        return {
            .keyword = rgb666(1, 3, 4),
            .operation = rgb666(3, 3, 3),
            .identifier = {
                .function = rgb666(2, 2, 4),
                .type = rgb666(3, 3, 0),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 3, 3),
            },
            .string = rgb666(4, 1, 0),
            .number = rgb666(4, 2, 0),
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeSolarizedLight() {
        return {
            .keyword = rgb666(1, 3, 4),
            .operation = rgb666(2, 2, 2),
            .identifier = {
                .function = rgb666(2, 2, 4),
                .type = rgb666(3, 3, 0),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(0, 2, 2),
            },
            .string = rgb666(4, 1, 0),
            .number = rgb666(4, 2, 0),
            .comment = rgb666(1, 1, 1),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeGruvboxDark() {
        return {
            .keyword = rgb666(5, 3, 0),
            .operation = rgb666(3, 3, 3),
            .identifier = {
                .function = rgb666(4, 1, 5),
                .type = rgb666(2, 4, 2),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 4, 4),
            },
            .string = rgb666(2, 4, 2),
            .number = rgb666(5, 2, 0),
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeGruvboxLight() {
        return {
            .keyword = rgb666(4, 2, 0),
            .operation = rgb666(2, 2, 2),
            .identifier = {
                .function = rgb666(3, 0, 3),
                .type = rgb666(1, 3, 1),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(0, 3, 3),
            },
            .string = rgb666(1, 3, 1),
            .number = rgb666(4, 1, 0),
            .comment = rgb666(1, 1, 1),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema MakeNord() {
        return {
            .keyword = rgb666(2, 3, 5),
            .operation = rgb666(3, 3, 3),
            .identifier = {
                .function = rgb666(4, 2, 5),
                .type = rgb666(2, 4, 3),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 4, 4),
            },
            .string = rgb666(2, 4, 3),
            .number = rgb666(3, 4, 5),
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    // Debug theme - not exposed in UI, but kept for development/testing
    [[maybe_unused]] TColorSchema MakeDebug() {
        return {
            .keyword = TColor::BLUE,
            .operation = TColor::GRAY,
            .identifier = {
                .function = TColor::MAGENTA,
                .type = TColor::YELLOW,
                .variable = TColor::RED,
                .quoted = TColor::CYAN,
            },
            .string = TColor::GREEN,
            .number = TColor::BRIGHTGREEN,
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    // Themes with explicit background colors
    TColorSchema MakeDraculaBg() {
        // Background: #282A36 -> quantized to rgb666(1, 1, 1)
        TColor bg = rgb666(1, 1, 1);
        return {
            .keyword = WithBg(rgb666(2, 3, 5), bg),
            .operation = WithBg(rgb666(3, 3, 3), bg),
            .identifier = {
                .function = WithBg(rgb666(4, 1, 5), bg),
                .type = WithBg(rgb666(2, 4, 3), bg),
                .variable = WithBg(TColor::DEFAULT, bg),
                .quoted = WithBg(rgb666(1, 4, 4), bg),
            },
            .string = WithBg(rgb666(2, 4, 2), bg),
            .number = WithBg(TColor::BRIGHTGREEN, bg),
            .comment = WithBg(rgb666(2, 2, 2), bg),
            .unknown = WithBg(TColor::DEFAULT, bg),
        };
    }

    TColorSchema MakePaperBg() {
        // Background: #FAFAFA -> quantized to rgb666(5, 5, 5)
        TColor bg = rgb666(5, 5, 5);
        return {
            .keyword = WithBg(TColor::BLUE, bg),
            .operation = WithBg(rgb666(2, 2, 2), bg),
            .identifier = {
                .function = WithBg(TColor::MAGENTA, bg),
                .type = WithBg(rgb666(1, 3, 1), bg),
                .variable = WithBg(TColor::DEFAULT, bg),
                .quoted = WithBg(TColor::CYAN, bg),
            },
            .string = WithBg(rgb666(3, 0, 0), bg),
            .number = WithBg(TColor::BROWN, bg),
            .comment = WithBg(rgb666(1, 1, 1), bg),
            .unknown = WithBg(TColor::DEFAULT, bg),
        };
    }

    // Built-in themes registry
    using TSchemaFactory = std::function<TColorSchema()>;

    const std::unordered_map<std::string, TSchemaFactory>& GetBuiltInSchemas() {
        static const std::unordered_map<std::string, TSchemaFactory> schemas = {
            {"default", MakeDefault},
            {"no-color", MakeNoColor},
            {"monaco-dark", MakeMonacoDark},
            {"monaco-light", MakeMonacoLight},
            {"solarized-dark", MakeSolarizedDark},
            {"solarized-light", MakeSolarizedLight},
            {"gruvbox-dark", MakeGruvboxDark},
            {"gruvbox-light", MakeGruvboxLight},
            {"nord", MakeNord},
            {"dracula-bg", MakeDraculaBg},
            {"paper-bg", MakePaperBg},
        };
        return schemas;
    }

    // Default theme name - change this to change the default
    constexpr const char* DefaultThemeName = "default";

} // namespace

const char* GetDefaultThemeName() {
    return DefaultThemeName;
}

TColorSchema GetColorSchema(const std::string& name) {
    const auto& schemas = GetBuiltInSchemas();

    // Empty name means default theme
    std::string lookupName = name.empty() ? GetDefaultThemeName() : name;

    auto it = schemas.find(lookupName);
    if (it != schemas.end()) {
        return it->second();
    }

    // Unknown theme - fallback to default
    it = schemas.find(GetDefaultThemeName());
    if (it != schemas.end()) {
        return it->second();
    }

    // Should never happen if DefaultThemeName is valid
    return MakeNoColor();
}

} // namespace NYdb::NConsoleClient
