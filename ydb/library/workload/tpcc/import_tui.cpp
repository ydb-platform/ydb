#include "import_tui.h"

#include "log_backend.h"
#include "runner.h"
#include "logs_scroller.h"
#include "util.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>

#include <thread>
#include <sstream>

using namespace ftxui;

namespace NYdb::NTPCC {

TImportTui::TImportTui(std::shared_ptr<TLog>& log, const TRunConfig& runConfig, TLogBackendWithCapture& logBacked, const TImportDisplayData& data)
    : Log(log)
    , Config(runConfig)
    , LogBackend(logBacked)
    , DataToDisplay(data)
    , Screen(ScreenInteractive::Fullscreen())
{
    TuiThread = std::thread([&] {
        Screen.Loop(BuildComponent());

        // ftxui catches signals and breaks the loop above, but
        // we have to let know the rest of app
        GetGlobalInterruptSource().request_stop();
    });
}

TImportTui::~TImportTui() {
    Screen.Exit();
    if (TuiThread.joinable()) {
        TuiThread.join();
    }
}

void TImportTui::Update(const TImportDisplayData& data) {
    DataToDisplay = data;
    Screen.PostEvent(Event::Custom);
}

Element TImportTui::BuildUpperPart() {
    // our header with main information

    std::stringstream headerSs;
    headerSs << "TPC-C Import: " << Config.WarehouseCount << " warehouses, "
                << Config.LoadThreadCount << " threads   Estimated size: "
                << GetFormattedSize(DataToDisplay.ImportState.ApproximateDataSize);

    std::stringstream progressSs;
    progressSs << std::fixed << std::setprecision(1) << DataToDisplay.StatusData.PercentLoaded << "% ("
                << GetFormattedSize(DataToDisplay.StatusData.CurrentDataSizeLoaded) << ")";

    std::stringstream speedSs;
    speedSs << std::fixed << std::setprecision(1)
            << "Speed: " << DataToDisplay.StatusData.InstantSpeedMiBs << " MiB/s   "
            << "Avg: " << DataToDisplay.StatusData.AvgSpeedMiBs << " MiB/s   "
            << "Elapsed: " << DataToDisplay.StatusData.ElapsedMinutes << ":"
            << std::setfill('0') << std::setw(2) << DataToDisplay.StatusData.ElapsedSeconds << "   "
            << "ETA: " << DataToDisplay.StatusData.EstimatedTimeLeftMinutes << ":"
            << std::setfill('0') << std::setw(2) << DataToDisplay.StatusData.EstimatedTimeLeftSeconds;

    // Calculate progress ratio for gauge
    float progressRatio = static_cast<float>(DataToDisplay.StatusData.PercentLoaded / 100.0);

    // Left side: Import details
    auto importDetails = vbox({
        text(headerSs.str()),
        hbox({
            text("Progress: "),
            gauge(progressRatio) | flex,
            text("  " + progressSs.str())
        }),
        text(speedSs.str())
    });

    auto topRow = window(text("TPC-C data upload"), hbox({
        importDetails
    }));

    // Index progress section (always shown)

    Elements indexElements;
    TString indexText;
    if (DataToDisplay.ImportState.IndexBuildStates.empty()) {
        indexText = "Index Creation Didn't Start";
    } else {
        indexText = "Index Creation";
    }

    if (DataToDisplay.ImportState.IndexBuildStates.empty()) {
        // Index building not started yet, need to leave enough space
        for (size_t i = 0; i < INDEX_COUNT; ++i) {
            float indexRatio = static_cast<float>(0.0);

            std::stringstream indexSs;
            indexSs << std::fixed << std::setprecision(1) << 0.0 << "%";

            indexElements.push_back(
                hbox({
                    text("  [ index " + std::to_string(i + 1) + " ] "),
                    gauge(indexRatio) | flex,
                    text(" " + indexSs.str())
                })
            );
        }
    } else {
        // Show progress for each index
        for (size_t i = 0; i < DataToDisplay.ImportState.IndexBuildStates.size(); ++i) {
            const auto& indexState = DataToDisplay.ImportState.IndexBuildStates[i];
            float indexRatio = static_cast<float>(indexState.Progress / 100.0);

            std::stringstream indexSs;
            indexSs << std::fixed << std::setprecision(1) << indexState.Progress << "%";

            indexElements.push_back(
                hbox({
                    text("  [ index " + std::to_string(i + 1) + " ] "),
                    gauge(indexRatio) | flex,
                    text(" " + indexSs.str())
                })
            );
        }
    }

    auto indicesRow = window(text(indexText), vbox(indexElements));

    auto layout = vbox({
        topRow,
        indicesRow,
    });

    return layout;
}

Component TImportTui::BuildComponent() {
    try {
        // Main layout
        return Container::Vertical({
            Renderer([=]{ return BuildUpperPart(); }),
            LogsScroller(LogBackend),
        });
    } catch (const std::exception& ex) {
        LOG_E("Exception in TUI: " << ex.what());
        RequestStop();
        return Renderer([] { return filler(); });
    }
}

} // namespace NYdb::NTPCC
