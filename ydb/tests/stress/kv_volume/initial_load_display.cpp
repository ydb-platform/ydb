#include "initial_load_display.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/stream/output.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <iomanip>
#include <iterator>
#include <sstream>
#include <string>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#endif

namespace NKvVolumeStress {

using namespace ftxui;

constexpr auto DisplaySleepStep = std::chrono::milliseconds(20);

bool IsInteractiveTerminal() {
#ifdef _win_
    return _isatty(_fileno(stdout)) != 0 && _isatty(_fileno(stderr)) != 0;
#else
    return isatty(STDOUT_FILENO) != 0 && isatty(STDERR_FILENO) != 0;
#endif
}

double CalcRate(ui64 current, ui64 previous, double elapsedSeconds) {
    if (elapsedSeconds <= 0.0 || current < previous) {
        return 0.0;
    }
    return static_cast<double>(current - previous) / elapsedSeconds;
}

std::string FormatBytes(ui64 bytes) {
    static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};

    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < std::size(units)) {
        value /= 1024.0;
        ++unit;
    }

    std::stringstream ss;
    if (unit == 0) {
        ss << static_cast<ui64>(value) << " " << units[unit];
    } else {
        ss << std::fixed << std::setprecision(value >= 100.0 ? 0 : 1) << value << " " << units[unit];
    }
    return ss.str();
}

std::string FormatRate(double bytesPerSecond) {
    if (bytesPerSecond <= 0.0) {
        return "0 B/s";
    }
    return FormatBytes(static_cast<ui64>(bytesPerSecond)) + "/s";
}

std::string FormatDuration(double elapsedSeconds) {
    const ui64 seconds = static_cast<ui64>(std::max(0.0, std::floor(elapsedSeconds)));
    std::stringstream ss;
    ss << seconds / 60 << ":" << std::setfill('0') << std::setw(2) << (seconds % 60);
    return ss.str();
}

class TInitialLoadDisplayController::TTui {
public:
    explicit TTui(std::shared_ptr<TInitialLoadDisplayData> data)
        : Screen_(ScreenInteractive::Fullscreen())
        , DataToDisplay_(std::move(data))
    {
        Thread_ = std::thread([this] {
            Screen_.Loop(BuildComponent());
        });
    }

    ~TTui() {
        Screen_.Exit();
        if (Thread_.joinable()) {
            Thread_.join();
        }
    }

    void Update(std::shared_ptr<TInitialLoadDisplayData> data) {
        std::atomic_store(&DataToDisplay_, std::move(data));
        Screen_.PostEvent(Event::Custom);
    }

private:
    Element BuildMainPart() {
        std::shared_ptr<TInitialLoadDisplayData> data = std::atomic_load(&DataToDisplay_);
        const double progress = std::clamp(data->ProgressPercent / 100.0, 0.0, 1.0);

        std::stringstream bytesSs;
        bytesSs << "processed: " << FormatBytes(data->ProcessedBytes)
                << " / " << FormatBytes(data->TotalBytes)
                << "   remaining: " << FormatBytes(data->RemainingBytes);

        std::stringstream commandsSs;
        commandsSs << "commands: " << data->ProcessedCommands << "/" << data->TotalCommands
                   << "   failed: " << data->FailedCommands
                   << "   errors: " << data->TotalErrors;

        std::stringstream rateSs;
        rateSs << "throughput(cur): " << FormatRate(data->ProcessedBytesPerSecond)
               << "   throughput(ok): " << FormatRate(data->SuccessfulBytesPerSecond);

        return window(text("kv_volume initial data load"),
            vbox({
                text("elapsed: " + FormatDuration(data->ElapsedSeconds)),
                text(bytesSs.str()) | bold,
                text(commandsSs.str()),
                text(rateSs.str()),
                hbox({
                    text("Progress: ["),
                    gauge(static_cast<float>(progress)) | size(WIDTH, EQUAL, 24),
                    text("] " + std::to_string(static_cast<int>(data->ProgressPercent)) + "%"),
                }),
            }));
    }

    Component BuildComponent() {
        return Renderer([this] {
            return BuildMainPart();
        });
    }

private:
    ScreenInteractive Screen_;
    std::thread Thread_;
    std::shared_ptr<TInitialLoadDisplayData> DataToDisplay_;
};

TInitialLoadDisplayController::TInitialLoadDisplayController(
    const TInitialLoadProgress& progress,
    const TRunStats& stats,
    bool noTui,
    bool verbose)
    : Progress_(progress)
    , Stats_(stats)
    , NoTui_(noTui)
    , Verbose_(verbose)
{
}

TInitialLoadDisplayController::~TInitialLoadDisplayController() {
    Stop();
}

TInitialLoadDisplayController::EDisplayMode TInitialLoadDisplayController::DetectMode() const {
    if (NoTui_ || Verbose_) {
        return EDisplayMode::Text;
    }

    if (IsInteractiveTerminal()) {
        return EDisplayMode::Tui;
    }

    return EDisplayMode::Text;
}

void TInitialLoadDisplayController::Start() {
    if (Thread_.joinable()) {
        return;
    }

    StartTs_ = std::chrono::steady_clock::now();
    PrevTs_ = StartTs_;
    PrevSnapshot_ = Progress_.Snapshot();
    HasPrevSnapshot_ = true;

    Mode_ = DetectMode();
    if (Mode_ == EDisplayMode::None) {
        return;
    }

    if (Mode_ == EDisplayMode::Tui) {
        Tui_ = std::make_unique<TTui>(BuildDisplayData(StartTs_));
    }

    StopRequested_.store(false, std::memory_order_relaxed);
    Thread_ = std::thread([this] {
        Loop();
    });
}

void TInitialLoadDisplayController::Stop() {
    StopRequested_.store(true, std::memory_order_relaxed);
    if (Thread_.joinable()) {
        Thread_.join();
    }
    Tui_.reset();
}

void TInitialLoadDisplayController::Loop() {
    while (!StopRequested_.load(std::memory_order_relaxed)) {
        const auto now = std::chrono::steady_clock::now();
        std::shared_ptr<TInitialLoadDisplayData> data = BuildDisplayData(now);

        switch (Mode_) {
            case EDisplayMode::Text:
                PrintText(*data);
                break;
            case EDisplayMode::Tui:
                if (Tui_) {
                    Tui_->Update(std::move(data));
                }
                break;
            case EDisplayMode::None:
                break;
        }

        for (int i = 0; i < 10 && !StopRequested_.load(std::memory_order_relaxed); ++i) {
            std::this_thread::sleep_for(DisplaySleepStep);
        }
    }
}

std::shared_ptr<TInitialLoadDisplayData> TInitialLoadDisplayController::BuildDisplayData(
    std::chrono::steady_clock::time_point now)
{
    const TInitialLoadProgressSnapshot snapshot = Progress_.Snapshot();
    const double elapsedSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(now - StartTs_).count();
    const double intervalSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(now - PrevTs_).count();

    const ui64 remainingBytes = snapshot.TotalBytes > snapshot.ProcessedBytes
        ? snapshot.TotalBytes - snapshot.ProcessedBytes
        : 0;

    const double processedRate = HasPrevSnapshot_
        ? CalcRate(snapshot.ProcessedBytes, PrevSnapshot_.ProcessedBytes, intervalSeconds)
        : 0.0;
    const double successfulRate = HasPrevSnapshot_
        ? CalcRate(snapshot.SuccessfulBytes, PrevSnapshot_.SuccessfulBytes, intervalSeconds)
        : 0.0;

    auto data = std::make_shared<TInitialLoadDisplayData>();
    data->TotalBytes = snapshot.TotalBytes;
    data->ProcessedBytes = snapshot.ProcessedBytes;
    data->SuccessfulBytes = snapshot.SuccessfulBytes;
    data->RemainingBytes = remainingBytes;
    data->TotalCommands = snapshot.TotalCommands;
    data->ProcessedCommands = snapshot.ProcessedCommands;
    data->FailedCommands = snapshot.FailedCommands;
    data->TotalErrors = Stats_.GetTotalErrors();
    data->ProgressPercent = snapshot.TotalBytes > 0
        ? std::min(100.0, static_cast<double>(snapshot.ProcessedBytes) * 100.0 / snapshot.TotalBytes)
        : 100.0;
    data->ProcessedBytesPerSecond = processedRate;
    data->SuccessfulBytesPerSecond = successfulRate;
    data->ElapsedSeconds = elapsedSeconds;

    PrevSnapshot_ = snapshot;
    PrevTs_ = now;
    HasPrevSnapshot_ = true;

    return data;
}

void TInitialLoadDisplayController::PrintText(const TInitialLoadDisplayData& data) const {
    std::stringstream line;
    line << "[kv_volume][initial] "
         << std::fixed << std::setprecision(1) << data.ProgressPercent << "%"
         << " processed=" << FormatBytes(data.ProcessedBytes)
         << "/" << FormatBytes(data.TotalBytes)
         << " remaining=" << FormatBytes(data.RemainingBytes)
         << " throughput(cur)=" << FormatRate(data.ProcessedBytesPerSecond)
         << " commands=" << data.ProcessedCommands << "/" << data.TotalCommands
         << " failed=" << data.FailedCommands
         << " errors=" << data.TotalErrors;

    Cerr << line.str() << Endl;
}

} // namespace NKvVolumeStress
