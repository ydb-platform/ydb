#include "progress_bar.h"
#include "interactive.h"
#include "print_utils.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/screen.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NYdb {
namespace NConsoleClient {

// ============================================================================
// TTextProgressBarRenderer - for non-interactive terminals
// ============================================================================

TTextProgressBarRenderer::TTextProgressBarRenderer(size_t updateIntervalMs)
    : UpdateIntervalMs(updateIntervalMs)
{
}

TTextProgressBarRenderer::~TTextProgressBarRenderer() {
    if (!Finished) {
        Cerr << Endl;
    }
}

void TTextProgressBarRenderer::Render(double percent, const TString& leftText, const TString& rightText) {
    TInstant now = TInstant::Now();
    TDuration elapsed = now - LastRenderTime;

    // Always render at 100% (final state), otherwise throttle
    bool isFinal = (percent >= 100.0);
    bool intervalPassed = (elapsed.MilliSeconds() >= UpdateIntervalMs);

    if (!isFinal && !intervalPassed) {
        return;
    }

    // Don't render final state twice
    if (isFinal && RenderedFinal) {
        return;
    }

    LastRenderTime = now;
    if (isFinal) {
        RenderedFinal = true;
    }

    TStringBuilder output;
    output << leftText;
    if (rightText) {
        output << " " << rightText;
    }
    Cerr << output << Endl;
}

void TTextProgressBarRenderer::Finish() {
    Finished = true;
}

void TTextProgressBarRenderer::Clear() {
    // Nothing to clear in text mode
}

// ============================================================================
// TFancyProgressBarRenderer - for interactive terminals using ftxui
// ============================================================================

TFancyProgressBarRenderer::TFancyProgressBarRenderer() {
}

TFancyProgressBarRenderer::~TFancyProgressBarRenderer() {
    if (!Finished) {
        Cerr << Endl;
    }
}

void TFancyProgressBarRenderer::Render(double percent, const TString& leftText, const TString& rightText) {
    // Don't render after Finish() was called to avoid duplicate lines
    if (Finished) {
        return;
    }

    TInstant now = TInstant::Now();
    TDuration elapsed = now - LastRenderTime;

    // Throttle rendering to ~30 FPS
    if (elapsed.MilliSeconds() < 33 && percent < 100.0) {
        return;
    }
    LastRenderTime = now;

    Percent = percent;
    LeftText = leftText;
    RightText = rightText;

    // Update cached text less frequently to make it readable
    TDuration textElapsed = now - LastTextUpdateTime;
    if (textElapsed.MilliSeconds() >= TextUpdateIntervalMs || CachedRightText.empty() || percent >= 100.0) {
        CachedRightText = rightText;
        LastTextUpdateTime = now;
    }

    DoRender();
}

void TFancyProgressBarRenderer::DoRender() {
    using namespace ftxui;

    int termWidth = Terminal::Size().dimx;
    if (termWidth <= 0) {
        termWidth = 80;
    }

    // Colors: always green
    Color gaugeColor = Color::Green;
    Color bgColor = Color::Grey23;  // Dark gray background for unfilled part

    auto left = text(std::string(LeftText)) | bold;

    // Reserve fixed width for right text (metadata)
    // This prevents the progress bar from jumping when ETA disappears
    // Format: "15.2 MiB/s | 102.4 MiB / 102.4 MiB | ETA: 59m 59s" = ~48 chars max
    const int reservedRightWidth = 48;
    int actualRightWidth = static_cast<int>(CachedRightText.size());

    // Pad right text to reserved width (left-aligned within reserved space)
    std::string paddedRight = std::string(CachedRightText);
    if (actualRightWidth < reservedRightWidth) {
        paddedRight.append(reservedRightWidth - actualRightWidth, ' ');
    }

    auto right = text(paddedRight) | dim;

    // Calculate gauge width
    // Format: "100% │████████████████████│ metadata..."
    int leftWidth = static_cast<int>(LeftText.size());
    int rightWidth = Max(actualRightWidth, reservedRightWidth);
    int bordersAndSpaces = 4; // " │" + "│ "
    int gaugeMinWidth = 15;

    int gaugeWidth = gaugeMinWidth;
    int availableForGauge = termWidth - leftWidth - rightWidth - bordersAndSpaces;
    if (availableForGauge > gaugeMinWidth) {
        gaugeWidth = availableForGauge;
    }

    // Build progress bar with visible borders and background
    auto gauge_element = gauge(static_cast<float>(Percent / 100.0))
        | color(gaugeColor)
        | bgcolor(bgColor)
        | size(WIDTH, EQUAL, gaugeWidth);

    auto document = hbox({
        left,
        text(" "),
        text("▕") | bold,
        gauge_element,
        text("▏") | bold,
        text(" "),
        right,
    });

    auto screen = Screen::Create(Dimension::Full(), Dimension::Fit(document));
    ftxui::Render(screen, document);

    // Move cursor to beginning of line and print
    Cerr << "\r";
    screen.Print();
    Cerr.Flush();
}

void TFancyProgressBarRenderer::Finish() {
    if (!Finished) {
        Finished = true;
        Cerr << Endl;
    }
}

void TFancyProgressBarRenderer::Clear() {
    // Clear the current line
    Cerr << "\r\033[K";
    Cerr.Flush();
}

// ============================================================================
// Factory function
// ============================================================================

std::unique_ptr<IProgressBarRenderer> CreateProgressBarRenderer(size_t updateIntervalMs) {
    std::optional<size_t> termWidth = GetErrTerminalWidth();
    if (termWidth) {
        return std::make_unique<TFancyProgressBarRenderer>();
    } else {
        return std::make_unique<TTextProgressBarRenderer>(updateIntervalMs);
    }
}

// ============================================================================
// TProgressBar - simple progress bar
// ============================================================================

TProgressBar::TProgressBar(size_t capacity, size_t parts)
    : Capacity(capacity)
    , Parts(parts)
    , Renderer(CreateProgressBarRenderer())
{
}

TProgressBar::~TProgressBar() {
    if (!Finished && Renderer) {
        Renderer->Finish();
    }
}

void TProgressBar::SetProgress(size_t progress) {
    CurProgress = Min(progress, Capacity);
    Render();
}

void TProgressBar::AddProgress(size_t value) {
    SetProgress(CurProgress + value);
}

void TProgressBar::Render(bool force) {
    if (Capacity == 0 || !Renderer) {
        return;
    }

    // Check if we need to render (for text mode with parts)
    bool shouldRender = force;
    if (Parts > 0) {
        size_t partSize = Max<size_t>(Capacity / Parts, 1);
        size_t currentPart = CurProgress / partSize;
        if (currentPart != LastRenderedPart) {
            LastRenderedPart = currentPart;
            shouldRender = true;
        }
    } else {
        shouldRender = true;
    }

    if (!shouldRender && CurProgress < Capacity) {
        return;
    }

    double percent = static_cast<double>(CurProgress) * 100.0 / static_cast<double>(Capacity);
    TString leftText = Sprintf("%3.0f%%", percent);
    TString rightText = TStringBuilder() << CurProgress << " / " << Capacity;

    Renderer->Render(percent, leftText, rightText);

    if (CurProgress >= Capacity) {
        Finished = true;
        Renderer->Finish();
    }
}

// ============================================================================
// TBytesProgressBar - progress bar for byte transfers
// ============================================================================

TBytesProgressBar::TBytesProgressBar(ui64 totalBytes)
    : TotalBytes(totalBytes)
    , Renderer(CreateProgressBarRenderer())
    , StartTime(TInstant::Now())
{
}

TBytesProgressBar::~TBytesProgressBar() {
    if (!Finished && Renderer) {
        Renderer->Finish();
    }
}

void TBytesProgressBar::SetProgress(ui64 downloadedBytes) {
    DownloadedBytes = downloadedBytes;
    if (TotalBytes > 0) {
        DownloadedBytes = Min(DownloadedBytes, TotalBytes);
    }
    Render();
}

void TBytesProgressBar::AddProgress(ui64 bytes) {
    SetProgress(DownloadedBytes + bytes);
}

void TBytesProgressBar::SetTotal(ui64 totalBytes) {
    TotalBytes = totalBytes;
}

void TBytesProgressBar::Render() {
    if (!Renderer) {
        return;
    }

    TInstant now = TInstant::Now();
    TDuration elapsed = now - StartTime;

    // Calculate speed using floating point for accuracy
    double bytesPerSecond = 0;
    double elapsedSeconds = elapsed.SecondsFloat();
    if (elapsedSeconds > 0) {
        bytesPerSecond = static_cast<double>(DownloadedBytes) / elapsedSeconds;
    }

    // Build info string
    TStringBuilder rightText;
    rightText << FormatSpeed(bytesPerSecond);

    if (TotalBytes > 0) {
        rightText << " | " << FormatBytes(DownloadedBytes) << " / " << FormatBytes(TotalBytes);

        // Calculate ETA
        if (bytesPerSecond > 0 && DownloadedBytes < TotalBytes) {
            ui64 remaining = TotalBytes - DownloadedBytes;
            TDuration eta = TDuration::Seconds(remaining / bytesPerSecond);
            rightText << " | ETA: " << FormatEta(eta);
        }
    } else {
        rightText << " | " << FormatBytes(DownloadedBytes);
    }

    // Calculate percent
    double percent = 0;
    TString leftText;
    if (TotalBytes > 0) {
        percent = static_cast<double>(DownloadedBytes) * 100.0 / static_cast<double>(TotalBytes);
        leftText = Sprintf("%3.0f%%", percent);
    } else {
        // Unknown total - show indeterminate progress
        percent = 50.0; // Show half-filled gauge
        leftText = "  ?%";
    }

    Renderer->Render(percent, leftText, rightText);

    if (TotalBytes > 0 && DownloadedBytes >= TotalBytes) {
        Finished = true;
        Renderer->Finish();
    }
}

// ============================================================================
// TTextDualProgressBarRenderer - for non-interactive terminals
// ============================================================================

TTextDualProgressBarRenderer::TTextDualProgressBarRenderer(size_t updateIntervalMs)
    : UpdateIntervalMs(updateIntervalMs)
{
}

TTextDualProgressBarRenderer::~TTextDualProgressBarRenderer() {
    if (!Finished) {
        Cerr << Endl;
    }
}

void TTextDualProgressBarRenderer::Render(double primaryPercent, double secondaryPercent,
                                          const TString& leftText, const TString& rightText) {
    TInstant now = TInstant::Now();
    TDuration elapsed = now - LastRenderTime;

    // Always render at 100% (final state), otherwise throttle
    bool isFinal = (primaryPercent >= 100.0);
    bool intervalPassed = (elapsed.MilliSeconds() >= UpdateIntervalMs);

    if (!isFinal && !intervalPassed) {
        return;
    }

    // Don't render final state twice
    if (isFinal && RenderedFinal) {
        return;
    }

    LastRenderTime = now;
    if (isFinal) {
        RenderedFinal = true;
    }

    TStringBuilder output;
    output << leftText;
    // In text mode, also show buffered progress if significantly ahead of primary
    double inProgressPercent = secondaryPercent - primaryPercent;
    if (inProgressPercent >= 1.0) {
        output << " (+ " << Sprintf("%.0f%%", inProgressPercent) << " in progress)";
    }
    if (rightText) {
        output << " " << rightText;
    }
    Cerr << output << Endl;
}

void TTextDualProgressBarRenderer::Finish() {
    Finished = true;
}

void TTextDualProgressBarRenderer::Clear() {
    // Nothing to clear in text mode
}

// ============================================================================
// TFancyDualProgressBarRenderer - for interactive terminals using ftxui
// ============================================================================

TFancyDualProgressBarRenderer::TFancyDualProgressBarRenderer() {
}

TFancyDualProgressBarRenderer::~TFancyDualProgressBarRenderer() {
    if (!Finished) {
        Cerr << Endl;
    }
}

void TFancyDualProgressBarRenderer::Render(double primaryPercent, double secondaryPercent,
                                           const TString& leftText, const TString& rightText) {
    // Don't render after Finish() was called to avoid duplicate lines
    if (Finished) {
        return;
    }

    TInstant now = TInstant::Now();
    TDuration elapsed = now - LastRenderTime;

    // Throttle rendering to ~30 FPS
    if (elapsed.MilliSeconds() < 33 && primaryPercent < 100.0) {
        return;
    }
    LastRenderTime = now;

    PrimaryPercent = primaryPercent;
    SecondaryPercent = secondaryPercent;
    LeftText = leftText;
    RightText = rightText;

    // Update cached text less frequently to make it readable
    TDuration textElapsed = now - LastTextUpdateTime;
    if (textElapsed.MilliSeconds() >= TextUpdateIntervalMs || CachedRightText.empty() || primaryPercent >= 100.0) {
        CachedRightText = rightText;
        LastTextUpdateTime = now;
    }

    DoRender();
}

void TFancyDualProgressBarRenderer::DoRender() {
    using namespace ftxui;

    int termWidth = Terminal::Size().dimx;
    if (termWidth <= 0) {
        termWidth = 80;
    }

    // Colors
    Color primaryColor = Color::Green;      // Confirmed progress
    // Buffered progress: subtle gray, visible on both dark and light backgrounds
    // Grey50 (RGB ~128,128,128) is a good middle ground
    Color secondaryColor = Color::Grey50;
    Color bgColor = Color::Grey23;          // Background (dark)

    auto left = text(std::string(LeftText)) | bold;

    // Reserve fixed width for right text (metadata)
    // Format: "15.2 MiB/s | 102.4 MiB / 102.4 MiB | ETA: 59m 59s" = ~48 chars max
    const int reservedRightWidth = 48;
    int actualRightWidth = static_cast<int>(CachedRightText.size());

    std::string paddedRight = std::string(CachedRightText);
    if (actualRightWidth < reservedRightWidth) {
        paddedRight.append(reservedRightWidth - actualRightWidth, ' ');
    }

    auto right = text(paddedRight) | dim;

    // Calculate gauge width
    int leftWidth = static_cast<int>(LeftText.size());
    int rightWidth = Max(actualRightWidth, reservedRightWidth);
    int bordersAndSpaces = 4;
    int gaugeMinWidth = 15;

    int gaugeWidth = gaugeMinWidth;
    int availableForGauge = termWidth - leftWidth - rightWidth - bordersAndSpaces;
    if (availableForGauge > gaugeMinWidth) {
        gaugeWidth = availableForGauge;
    }

    // Build dual progress bar manually
    // We'll use character-based approach for dual progress
    float primaryRatio = static_cast<float>(PrimaryPercent / 100.0);
    float secondaryRatio = static_cast<float>(SecondaryPercent / 100.0);

    primaryRatio = std::max(0.0f, std::min(1.0f, primaryRatio));
    secondaryRatio = std::max(0.0f, std::min(1.0f, secondaryRatio));

    int primaryChars = static_cast<int>(primaryRatio * gaugeWidth);
    int secondaryChars = static_cast<int>(secondaryRatio * gaugeWidth);
    int emptyChars = gaugeWidth - secondaryChars;

    // Ensure secondary >= primary (secondary is "ahead" of primary)
    secondaryChars = std::max(secondaryChars, primaryChars);
    int bufferChars = secondaryChars - primaryChars;

    // Check if we need a separator (when both primary and buffer exist)
    bool needsSeparator = (bufferChars > 0 && primaryChars > 0);

    // Build the gauge as text elements with centered labels
    // [green filled with "completed"][separator][gray buffer with "in progress"][dark empty]

    // Helper to center text in a string of given width
    auto centerText = [](const std::string& label, int width) -> std::string {
        if (width <= 0) return "";
        if (static_cast<int>(label.size()) > width) return std::string(width, ' ');
        int padding = (width - static_cast<int>(label.size())) / 2;
        std::string result(padding, ' ');
        result += label;
        result.append(width - result.size(), ' ');
        return result;
    };

    // For drawing, account for separator (it takes 1 char from primary space)
    int primaryDrawChars = needsSeparator ? primaryChars - 1 : primaryChars;
    if (primaryDrawChars < 0) primaryDrawChars = 0;

    // Create strings with centered labels if they fit
    // Center based on full primary width for stable positioning, then trim if needed
    std::string primaryStr;
    if (primaryChars >= static_cast<int>(CompletedLabel.size()) + 2) {
        std::string centered = centerText(CompletedLabel, primaryChars);
        // Trim to actual draw width (removes last char when separator present)
        primaryStr = centered.substr(0, primaryDrawChars);
    } else {
        primaryStr = std::string(primaryDrawChars, ' ');
    }
    std::string bufferStr = (bufferChars >= static_cast<int>(InProgressLabel.size()) + 2)
        ? centerText(InProgressLabel, bufferChars)
        : std::string(bufferChars, ' ');
    std::string emptyStr(emptyChars, ' ');

    Elements gauge_parts;
    if (primaryChars > 0) {
        gauge_parts.push_back(text(primaryStr) | bgcolor(primaryColor) | color(Color::Black));
    }
    // Add thin separator on green background (▕ = right eighth block, visually marks the end of confirmed progress)
    if (needsSeparator) {
        gauge_parts.push_back(text("▕") | bold | bgcolor(primaryColor));
    }
    if (bufferChars > 0) {
        gauge_parts.push_back(text(bufferStr) | bgcolor(secondaryColor) | color(Color::White));
    }
    if (emptyChars > 0) {
        gauge_parts.push_back(text(emptyStr) | bgcolor(bgColor));
    }

    auto gauge_element = hbox(gauge_parts);

    auto document = hbox({
        left,
        text(" "),
        text("▕") | bold,
        gauge_element,
        text("▏") | bold,
        text(" "),
        right,
    });

    auto screen = Screen::Create(Dimension::Full(), Dimension::Fit(document));
    ftxui::Render(screen, document);

    // Move cursor to beginning of line and print
    Cerr << "\r";
    screen.Print();
    Cerr.Flush();
}

void TFancyDualProgressBarRenderer::Finish() {
    if (!Finished) {
        Finished = true;
        Cerr << Endl;
    }
}

void TFancyDualProgressBarRenderer::Clear() {
    Cerr << "\r\033[K";
    Cerr.Flush();
}

// ============================================================================
// Factory function for dual progress bar
// ============================================================================

std::unique_ptr<IDualProgressBarRenderer> CreateDualProgressBarRenderer(size_t updateIntervalMs) {
    std::optional<size_t> termWidth = GetErrTerminalWidth();
    if (termWidth) {
        return std::make_unique<TFancyDualProgressBarRenderer>();
    } else {
        return std::make_unique<TTextDualProgressBarRenderer>(updateIntervalMs);
    }
}

// ============================================================================
// TDualBytesProgressBar - dual progress bar for import operations
// ============================================================================

TDualBytesProgressBar::TDualBytesProgressBar(ui64 totalBytes)
    : TotalBytes(totalBytes)
    , Renderer(CreateDualProgressBarRenderer())
    , StartTime(TInstant::Now())
{
}

TDualBytesProgressBar::~TDualBytesProgressBar() {
    if (!Finished && Renderer) {
        Renderer->Finish();
    }
}

void TDualBytesProgressBar::SetBufferedProgress(ui64 bufferedBytes) {
    BufferedBytes = bufferedBytes;
    if (TotalBytes > 0) {
        BufferedBytes = Min(BufferedBytes, TotalBytes);
    }
    Render();
}

void TDualBytesProgressBar::SetConfirmedProgress(ui64 confirmedBytes) {
    ConfirmedBytes = confirmedBytes;
    if (TotalBytes > 0) {
        ConfirmedBytes = Min(ConfirmedBytes, TotalBytes);
    }
    Render();
}

void TDualBytesProgressBar::SetTotal(ui64 totalBytes) {
    TotalBytes = totalBytes;
}

void TDualBytesProgressBar::Render() {
    if (!Renderer) {
        return;
    }

    TInstant now = TInstant::Now();
    TDuration elapsed = now - StartTime;

    // Calculate speed based on confirmed bytes (real progress) using floating point for accuracy
    double bytesPerSecond = 0;
    double elapsedSeconds = elapsed.SecondsFloat();
    if (elapsedSeconds > 0) {
        bytesPerSecond = static_cast<double>(ConfirmedBytes) / elapsedSeconds;
    }

    // Build info string
    TStringBuilder rightText;
    rightText << FormatSpeed(bytesPerSecond);

    if (TotalBytes > 0) {
        rightText << " | " << FormatBytes(ConfirmedBytes) << " / " << FormatBytes(TotalBytes);

        // Calculate ETA based on confirmed bytes
        if (bytesPerSecond > 0 && ConfirmedBytes < TotalBytes) {
            ui64 remaining = TotalBytes - ConfirmedBytes;
            TDuration eta = TDuration::Seconds(remaining / bytesPerSecond);
            rightText << " | ETA: " << FormatEta(eta);
        }
    } else {
        rightText << " | " << FormatBytes(ConfirmedBytes);
    }

    // Calculate percentages
    double primaryPercent = 0;   // Confirmed (green)
    double secondaryPercent = 0; // Buffered (grey)
    TString leftText;

    if (TotalBytes > 0) {
        primaryPercent = static_cast<double>(ConfirmedBytes) * 100.0 / static_cast<double>(TotalBytes);
        secondaryPercent = static_cast<double>(BufferedBytes) * 100.0 / static_cast<double>(TotalBytes);
        // Show primary (confirmed) percent as main percentage
        leftText = Sprintf("%3.0f%%", primaryPercent);
    } else {
        primaryPercent = 50.0;
        secondaryPercent = 50.0;
        leftText = "  ?%";
    }

    Renderer->Render(primaryPercent, secondaryPercent, leftText, rightText);

    if (TotalBytes > 0 && ConfirmedBytes >= TotalBytes) {
        Finished = true;
        Renderer->Finish();
    }
}

} // namespace NConsoleClient
} // namespace NYdb
