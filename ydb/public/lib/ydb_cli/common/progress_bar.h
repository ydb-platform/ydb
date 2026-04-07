#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <ydb/library/accessor/accessor.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

namespace NYdb {
namespace NConsoleClient {

// Default update interval for non-interactive progress bars (in milliseconds)
constexpr size_t DefaultProgressUpdateIntervalMs = 10000;

// Base progress bar renderer interface
class IProgressBarRenderer {
public:
    virtual ~IProgressBarRenderer() = default;

    // Render progress bar
    // percent: 0-100
    // leftText: text to show on the left (e.g., "45%")
    // rightText: text to show on the right (e.g., "45/100" or "15.2 MB/s | 45.3 MB / 102.5 MB | ETA: 2m 15s")
    virtual void Render(double percent, const TString& leftText, const TString& rightText) = 0;

    // Called when progress is complete
    virtual void Finish() = 0;

    // Called to clear/reset the progress bar
    virtual void Clear() = 0;
};

// Creates appropriate renderer based on terminal capabilities
// If stderr is a terminal - returns ftxui-based renderer
// Otherwise - returns text-based renderer that prints periodically
std::unique_ptr<IProgressBarRenderer> CreateProgressBarRenderer(size_t updateIntervalMs = DefaultProgressUpdateIntervalMs);

// Text renderer that prints progress periodically (for non-interactive mode)
class TTextProgressBarRenderer : public IProgressBarRenderer {
public:
    explicit TTextProgressBarRenderer(size_t updateIntervalMs);
    ~TTextProgressBarRenderer() override;

    void Render(double percent, const TString& leftText, const TString& rightText) override;
    void Finish() override;
    void Clear() override;

private:
    size_t UpdateIntervalMs;
    TInstant LastRenderTime;
    bool Finished = false;
    bool RenderedFinal = false;
};

// Interactive renderer using ftxui with colors and gauge
class TFancyProgressBarRenderer : public IProgressBarRenderer {
public:
    TFancyProgressBarRenderer();
    ~TFancyProgressBarRenderer() override;

    void Render(double percent, const TString& leftText, const TString& rightText) override;
    void Finish() override;
    void Clear() override;

private:
    void DoRender();

    double Percent = 0;
    TString LeftText;
    TString RightText;
    TString CachedRightText;  // Cached text for display (updated less frequently)
    TInstant LastRenderTime;
    TInstant LastTextUpdateTime;
    bool Finished = false;

    static constexpr size_t TextUpdateIntervalMs = 500;  // Update text ~2 times per second
};

// Simple progress bar: shows current/total with percentage
// Compatible with existing TProgressBar API
class TProgressBar {
public:
    // capacity: total number of items
    // parts: if non-zero, in non-interactive mode prints only when crossing part boundaries
    explicit TProgressBar(size_t capacity, size_t parts = 0);
    ~TProgressBar();

    void SetProgress(size_t progress);
    void AddProgress(size_t value);

    YDB_READONLY(size_t, Capacity, 0);
    YDB_READONLY(size_t, CurProgress, 0);
    YDB_READONLY(size_t, Parts, 0);

private:
    void Render(bool force = false);

    std::unique_ptr<IProgressBarRenderer> Renderer;
    size_t LastRenderedPart = 0;
    bool Finished = false;
};

// Progress bar for byte transfers: shows speed and ETA
class TBytesProgressBar {
public:
    // totalBytes: total size in bytes (0 if unknown)
    explicit TBytesProgressBar(ui64 totalBytes = 0);
    ~TBytesProgressBar();

    void SetProgress(ui64 downloadedBytes);
    void AddProgress(ui64 bytes);

    // Update total if it wasn't known at construction time
    void SetTotal(ui64 totalBytes);

    YDB_READONLY(ui64, TotalBytes, 0);
    YDB_READONLY(ui64, DownloadedBytes, 0);

private:
    void Render();

    std::unique_ptr<IProgressBarRenderer> Renderer;
    TInstant StartTime;
    TInstant LastRenderTime;
    bool Finished = false;
};

// ============================================================================
// Dual progress bar for import operations
// Shows two progress indicators:
// - Primary (green): confirmed/committed progress (e.g., successfully imported bytes)
// - Secondary (grey): buffered/read progress (e.g., bytes read from file)
// ============================================================================

// Renderer interface for dual progress bar
class IDualProgressBarRenderer {
public:
    virtual ~IDualProgressBarRenderer() = default;

    // Render dual progress bar
    // primaryPercent: confirmed progress (0-100), shown in green
    // secondaryPercent: buffered progress (0-100), shown in grey
    // leftText: text to show on the left (e.g., "45%")
    // rightText: text to show on the right (e.g., speed, ETA, etc.)
    virtual void Render(double primaryPercent, double secondaryPercent,
                        const TString& leftText, const TString& rightText) = 0;

    virtual void Finish() = 0;
    virtual void Clear() = 0;
};

// Factory function for dual renderer
std::unique_ptr<IDualProgressBarRenderer> CreateDualProgressBarRenderer(size_t updateIntervalMs = DefaultProgressUpdateIntervalMs);

// Text renderer for dual progress (non-interactive)
class TTextDualProgressBarRenderer : public IDualProgressBarRenderer {
public:
    explicit TTextDualProgressBarRenderer(size_t updateIntervalMs);
    ~TTextDualProgressBarRenderer() override;

    void Render(double primaryPercent, double secondaryPercent,
                const TString& leftText, const TString& rightText) override;
    void Finish() override;
    void Clear() override;

private:
    size_t UpdateIntervalMs;
    TInstant LastRenderTime;
    bool Finished = false;
    bool RenderedFinal = false;
};

// Interactive renderer for dual progress using ftxui
class TFancyDualProgressBarRenderer : public IDualProgressBarRenderer {
public:
    TFancyDualProgressBarRenderer();
    ~TFancyDualProgressBarRenderer() override;

    void Render(double primaryPercent, double secondaryPercent,
                const TString& leftText, const TString& rightText) override;
    void Finish() override;
    void Clear() override;

private:
    void DoRender();

    double PrimaryPercent = 0;
    double SecondaryPercent = 0;
    TString LeftText;
    TString RightText;
    TString CachedRightText;  // Cached text for display (updated less frequently)
    TInstant LastRenderTime;
    TInstant LastTextUpdateTime;
    bool Finished = false;

    static constexpr size_t TextUpdateIntervalMs = 500;  // Update text ~2 times per second

    // Labels for progress bar sections (initialized once per object)
    const std::string CompletedLabel = "completed";
    const std::string InProgressLabel = "in progress";
};

// Dual progress bar for import operations with byte tracking
// Shows confirmed (imported) vs buffered (read) progress
class TDualBytesProgressBar {
public:
    // totalBytes: total size in bytes (0 if unknown)
    explicit TDualBytesProgressBar(ui64 totalBytes = 0);
    ~TDualBytesProgressBar();

    // Update buffered (read from file) progress - shown in grey
    void SetBufferedProgress(ui64 bufferedBytes);

    // Update confirmed (imported) progress - shown in green
    // This is the "real" progress that determines the percentage shown
    void SetConfirmedProgress(ui64 confirmedBytes);

    // Update total if it wasn't known at construction time
    void SetTotal(ui64 totalBytes);

    YDB_READONLY(ui64, TotalBytes, 0);
    YDB_READONLY(ui64, BufferedBytes, 0);
    YDB_READONLY(ui64, ConfirmedBytes, 0);

private:
    void Render();

    std::unique_ptr<IDualProgressBarRenderer> Renderer;
    TInstant StartTime;
    TInstant LastRenderTime;
    bool Finished = false;
};

} // namespace NConsoleClient
} // namespace NYdb
