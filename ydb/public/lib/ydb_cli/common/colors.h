#pragma once

#include <library/cpp/colorizer/colors.h>

namespace NYdb::NConsoleClient {

// Global colors mode
enum class EGlobalColorsMode {
    Auto,    // Default behavior (respects NO_COLOR, TTY detection)
    Never,   // Completely disable colors
    Always   // Enable colors, ignore NO_COLOR
};

// Get the current global colors mode
EGlobalColorsMode GetGlobalColorsMode();

// Set the global colors mode
void SetGlobalColorsMode(EGlobalColorsMode mode);

// Get colors instance respecting global mode
NColorizer::TColors& AutoColors(IOutputStream& out);

} // namespace NYdb::NConsoleClient