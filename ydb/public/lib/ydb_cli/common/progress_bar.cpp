#include "progress_bar.h"
#include "interactive.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/screen.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NYdb {
namespace NConsoleClient {

// ============================================================================
// Helper functions
// ============================================================================

TString FormatBytes(ui64 bytes) {
    const char* suffixes[] = {"B", "KB", "MB", "GB", "TB"};
    int suffixIndex = 0;
    double size = static_cast<double>(bytes);

    while (size >= 1024.0 && suffixIndex < 4) {
        size /= 1024.0;
        suffixIndex++;
    }

    if (suffixIndex == 0) {
        return Sprintf("%.0f %s", size, suffixes[suffixIndex]);
    }
    return Sprintf("%.1f %s", size, suffixes[suffixIndex]);
}

TString FormatSpeed(ui64 bytesPerSecond) {
    return FormatBytes(bytesPerSecond) + "/s";
}

TString FormatEta(TDuration duration) {
    ui64 totalSeconds = duration.Seconds();
    if (totalSeconds == 0) {
        return "<1s";
    }

    ui64 hours = totalSeconds / 3600;
    ui64 minutes = (totalSeconds % 3600) / 60;
    ui64 seconds = totalSeconds % 60;

    TStringBuilder result;
    if (hours > 0) {
        result << hours << "h ";
    }
    if (minutes > 0 || hours > 0) {
        result << minutes << "m ";
    }
    result << seconds << "s";

    return result;
}

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
    DoRender();
}

void TFancyProgressBarRenderer::DoRender() {
    using namespace ftxui;

    int termWidth = Terminal::Size().dimx;
    if (termWidth <= 0) {
        termWidth = 80;
    }

    // Colors
    Color gaugeColor = (Percent >= 100.0) ? Color::Green : Color::Cyan;
    Color bgColor = Color::Grey23;  // Dark gray background for unfilled part

    auto left = text(std::string(LeftText)) | bold;

    // Reserve fixed width for right text (metadata)
    // This prevents the progress bar from jumping when ETA disappears
    // Format: "15.2 MB/s | 102.4 MB / 102.4 MB | ETA: 59m 59s" = ~45 chars max
    const int reservedRightWidth = 45;
    int actualRightWidth = static_cast<int>(RightText.size());

    // Pad right text to reserved width (left-aligned within reserved space)
    std::string paddedRight = std::string(RightText);
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

    // Calculate speed
    ui64 bytesPerSecond = 0;
    if (elapsed.Seconds() > 0) {
        bytesPerSecond = DownloadedBytes / elapsed.Seconds();
    } else if (elapsed.MilliSeconds() > 0) {
        bytesPerSecond = DownloadedBytes * 1000 / elapsed.MilliSeconds();
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

} // namespace NConsoleClient
} // namespace NYdb
