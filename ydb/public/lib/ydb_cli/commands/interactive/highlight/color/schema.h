#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

#include <optional>

namespace NYdb::NConsoleClient {

    using TColor = replxx::Replxx::Color;

    // Color specification with optional background
    struct TColorSpec {
        TColor fg = TColor::DEFAULT;
        std::optional<TColor> bg;

        TColorSpec() = default;
        TColorSpec(TColor foreground) : fg(foreground) {}
        TColorSpec(TColor foreground, TColor background) : fg(foreground), bg(background) {}

        // Implicit conversion from TColor for backward compatibility
        operator TColor() const { return fg; }
    };

    struct TColorSchema {
        TColorSpec keyword;
        TColorSpec operation;
        struct {
            TColorSpec function;
            TColorSpec type;
            TColorSpec variable;
            TColorSpec quoted;
        } identifier;
        TColorSpec string;
        TColorSpec number;
        TColorSpec comment;
        TColorSpec unknown;
    };

    TColorSchema GetColorSchema(const std::string& name = "");

    // Returns the name of the default color theme
    const char* GetDefaultThemeName();

} // namespace NYdb::NConsoleClient
