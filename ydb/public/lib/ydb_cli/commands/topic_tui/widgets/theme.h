#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>

#include <util/system/types.h>

namespace NYdb::NConsoleClient::NTheme {

// =============================================================================
// Colors
// =============================================================================

// Selection and highlighting
inline const ftxui::Color HighlightBg = ftxui::Color::RGB(40, 60, 100);
inline const ftxui::Color HoverBg = ftxui::Color::RGB(50, 50, 55);  // Subtle hover
inline const ftxui::Color HeaderBg = ftxui::Color::GrayDark;

// Text colors
inline const ftxui::Color ErrorText = ftxui::Color::Red;
inline const ftxui::Color SuccessText = ftxui::Color::Green;
inline const ftxui::Color WarningText = ftxui::Color::Yellow;
inline const ftxui::Color AccentText = ftxui::Color::Cyan;
inline const ftxui::Color MutedText = ftxui::Color::GrayLight;

// Status colors for lag/metrics
inline const ftxui::Color LagCritical = ftxui::Color::Red;
inline const ftxui::Color LagWarning = ftxui::Color::Yellow;
inline const ftxui::Color LagNormal = ftxui::Color::Green;

// =============================================================================
// Standard Column Widths
// =============================================================================

// Identifiers
constexpr int ColPartitionId = 6;
constexpr int ColNodeId = 6;

// Offsets and counts
constexpr int ColOffset = 14;
constexpr int ColCount = 12;
constexpr int ColLag = 11;

// Sizes and rates
constexpr int ColBytes = 10;
constexpr int ColBytesPerMin = 12;

// Durations
constexpr int ColDuration = 12;
constexpr int ColDurationShort = 11;

// Strings
constexpr int ColSessionId = 14;
constexpr int ColReaderName = 14;
constexpr int ColConsumerName = 25;
constexpr int ColSparkline = 17;  // For sparkline columns

// Form layout
constexpr int FormLabelWidth = 22;
constexpr int FormInputWidth = 50;
constexpr int FormWidth = 75;

// =============================================================================
// Spinner Animation
// =============================================================================

inline const std::vector<std::string> SpinnerFrames = {
    "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"
};

inline ftxui::Element RenderSpinner(int frame, const std::string& message) {
    using namespace ftxui;
    std::string spinChar = SpinnerFrames[frame % SpinnerFrames.size()];
    return hbox({
        text(spinChar) | color(AccentText),
        text(" " + message) | dim
    });
}

// =============================================================================
// Common Element Decorators
// =============================================================================

// Form section header (e.g., "Basic Settings")
inline ftxui::Element SectionHeader(const std::string& title) {
    using namespace ftxui;
    return text(" " + title) | bold | color(AccentText);
}

// Form footer with action hints
inline ftxui::Element FormFooter(bool canSubmit = true) {
    using namespace ftxui;
    auto submitText = canSubmit 
        ? (text(" [Enter] Submit ") | color(SuccessText))
        : (text(" [Enter] Submit ") | dim);
    return hbox({
        filler(),
        submitText,
        text(" [Esc] Cancel ") | dim,
        filler()
    });
}

// Confirmation dialog footer
inline ftxui::Element ConfirmFooter(bool canConfirm, const std::string& confirmLabel = "Confirm") {
    using namespace ftxui;
    auto confirmText = canConfirm 
        ? (text(" [Enter] " + confirmLabel + " ") | color(WarningText) | bold)
        : (text(" [Enter] " + confirmLabel + " ") | dim);
    return hbox({
        filler(),
        confirmText,
        text(" [Esc] Cancel ") | dim,
        filler()
    });
}

// Danger confirmation footer (for delete operations)
inline ftxui::Element DangerFooter(bool canConfirm) {
    using namespace ftxui;
    auto confirmText = canConfirm 
        ? (text(" [Enter] Delete ") | color(ErrorText) | bold)
        : (text(" [Enter] Delete ") | dim);
    return hbox({
        filler(),
        confirmText,
        text(" [Esc] Cancel ") | dim,
        filler()
    });
}

// =============================================================================
// Lag Coloring Helper
// =============================================================================

inline ftxui::Color GetLagColor(ui64 lag) {
    if (lag > 1000) return LagCritical;
    if (lag > 100) return LagWarning;
    return LagNormal;
}

} // namespace NYdb::NConsoleClient::NTheme
