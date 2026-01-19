#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <ydb/library/accessor/accessor.h>

#include <cstddef>
#include <memory>
#include <optional>

namespace NYdb {
namespace NConsoleClient {

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
std::unique_ptr<IProgressBarRenderer> CreateProgressBarRenderer(size_t updateIntervalMs = 10000);

// Text renderer that prints progress periodically (for non-interactive mode)
class TTextProgressBarRenderer : public IProgressBarRenderer {
public:
    explicit TTextProgressBarRenderer(size_t updateIntervalMs = 10000);
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
    TInstant LastRenderTime;
    bool Finished = false;
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

// Helper functions for formatting
TString FormatBytes(ui64 bytes);
TString FormatSpeed(ui64 bytesPerSecond);
TString FormatEta(TDuration duration);

} // namespace NConsoleClient
} // namespace NYdb
