#include "runner_tui.h"

#include "log_backend.h"
#include "runner.h"
#include "scroller.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>

using namespace ftxui;

namespace NYdb::NTPCC {

TRunnerTui::TRunnerTui(TLogBackendWithCapture& logBacked, std::shared_ptr<TRunDisplayData> data)
    : LogBackend(logBacked)
    , DataToDisplay(std::move(data))
    , Screen(ScreenInteractive::Fullscreen())
{
    TuiThread = std::thread([&] {
        Screen.Loop(BuildComponent());

        // ftxui catches signals and breaks the loop above, but
        // we have to let know the rest of app
        GetGlobalInterruptSource().request_stop();
    });
}

TRunnerTui::~TRunnerTui() {
    Screen.Exit();
    if (TuiThread.joinable()) {
        TuiThread.join();
    }
}

void TRunnerTui::Update(std::shared_ptr<TRunDisplayData> data) {
    std::atomic_store(&DataToDisplay, data);
    Screen.PostEvent(Event::Custom);
}

Element TRunnerTui::BuildUpperPart() {
    // Get window width to determine which columns to show
    constexpr int MIN_WINDOW_WIDTH_FOR_EXTENDED_COLUMNS = 140;

    std::shared_ptr<TRunDisplayData> data = std::atomic_load(&DataToDisplay);

    auto screen = Screen::Create(Dimension::Full(), Dimension::Full());
    const int windowWidth = screen.dimx();
    const bool showExtendedColumns = windowWidth >= MIN_WINDOW_WIDTH_FOR_EXTENDED_COLUMNS;

    // Left side of header: runner info, efficiency, phase, progress

    std::stringstream headerSs;
    headerSs << "Result preview: " << data->StatusData.Phase;

    std::stringstream metricsSs;
    metricsSs << "Efficiency: " << std::setw(3) << std::fixed << std::setprecision(1)
        << data->StatusData.Efficiency << "%   "
        << "tpmC: " << std::fixed << std::setprecision(0) << data->StatusData.Tpmc;

    std::stringstream timingSs;
    timingSs << data->StatusData.ElapsedMinutesTotal << ":"
             << std::setfill('0') << std::setw(2) << data->StatusData.ElapsedSecondsTotal << " elapsed"
             << ", "
             << data->StatusData.RemainingMinutesTotal << ":" << std::setfill('0') << std::setw(2)
             << data->StatusData.RemainingSecondsTotal << " remaining";

    // Calculate progress ratio for gauge
    float progressRatio = static_cast<float>(data->StatusData.ProgressPercentTotal / 100.0);
    constexpr int progressBarWidth = 15;

    auto topLeftMainInfo = vbox({
        text(metricsSs.str()) | bold,
        text(timingSs.str()),
        hbox({
            text("Progress: ["),
            gauge(progressRatio) | size(WIDTH, EQUAL, progressBarWidth),
            text("] " + std::to_string(static_cast<int>(data->StatusData.ProgressPercentTotal)) + "%")
        })
    });

    // Right side of header: Transaction statistics table (without header)

    Elements txRows;
    // Add header row for transaction table
    txRows.push_back(hbox({
        text("Transaction") | size(WIDTH, EQUAL, 12),
        text("p50, ms") | align_right | size(WIDTH, EQUAL, 9),
        text("p90, ms") | align_right | size(WIDTH, EQUAL, 9),
        text("p99, ms") | align_right | size(WIDTH, EQUAL, 9)
    }));

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        const auto& totalForType = data->Statistics.TotalTerminalStats.GetStats(type);

        std::stringstream p50Ss, p90Ss, p99Ss;
        p50Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(50);
        p90Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(90);
        p99Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(99);

        txRows.push_back(hbox({
            text(std::string(ToString(type))) | size(WIDTH, EQUAL, 12),
            text(p50Ss.str()) | align_right | size(WIDTH, EQUAL, 9),
            text(p90Ss.str()) | align_right | size(WIDTH, EQUAL, 9),
            text(p99Ss.str()) | align_right | size(WIDTH, EQUAL, 9)
        }));
    }
    auto topRightTransactionStats = vbox(txRows);

    auto topSection = window(text(headerSs.str()), hbox({
        topLeftMainInfo | flex,
        separator(),
        topRightTransactionStats | flex
    }));

    // Per-thread statistics in two columns with header

    Elements leftThreadElements, rightThreadElements;
    size_t threadCount = data->Statistics.StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    // Create header row elements
    Elements headerLeftColumn;
    headerLeftColumn.push_back(text("Thr") | size(WIDTH, EQUAL, 4));
    headerLeftColumn.push_back(text("Load") | center | size(WIDTH, EQUAL, 24));
    headerLeftColumn.push_back(text("QPS") | align_right | size(WIDTH, EQUAL, 8));
    if (showExtendedColumns) {
        headerLeftColumn.push_back(text("Queue") | align_right | size(WIDTH, EQUAL, 10));
        headerLeftColumn.push_back(text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20));
    }

    Elements headerRightColumn;
    headerRightColumn.push_back(text("Thr") | size(WIDTH, EQUAL, 4));
    headerRightColumn.push_back(text("Load") | center | size(WIDTH, EQUAL, 24));
    headerRightColumn.push_back(text("QPS") | align_right | size(WIDTH, EQUAL, 8));
    if (showExtendedColumns) {
        headerRightColumn.push_back(text("Queue") | align_right | size(WIDTH, EQUAL, 10));
        headerRightColumn.push_back(text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20));
    }

    auto headerLeft = hbox(headerLeftColumn);
    auto headerRight = hbox(headerRightColumn);

    leftThreadElements.push_back(headerLeft);
    rightThreadElements.push_back(headerRight);

    for (size_t i = 0; i < threadCount; ++i) {
        const auto& stats = data->Statistics.StatVec[i];
        double load = stats.TotalTime != 0 ? stats.ExecutingTime / stats.TotalTime : 0;

        // Create custom progress bar with individual "|" characters
        constexpr int barWidth = 10;
        int filledBars = static_cast<int>(load * barWidth);
        std::string barContent(filledBars, '|');
        barContent += std::string(barWidth - filledBars, ' ');

        auto loadBar = text(barContent);
        Color loadColor;
        if (load < 0.6) {
            loadBar = loadBar | color(Color::Green);
            loadColor = Color::Green;
        } else if (load < 0.8) {
            loadBar = loadBar | color(Color::Yellow);
            loadColor = Color::Yellow;
        } else {
            loadBar = loadBar | color(Color::Red);
            loadColor = Color::Red;
        }

        std::stringstream loadPercentSs, qpsSs, queueSizeSs, queueP90Ss;
        loadPercentSs << std::fixed << std::setprecision(1) << std::setw(4) << std::right << (load * 100) << "%";
        qpsSs << std::fixed << std::setprecision(0) << std::setw(8) << std::right << stats.QueriesPerSecond;
        queueSizeSs << stats.TaskThreadStats->InternalTasksWaitingInflight;
        queueP90Ss << std::fixed << std::setprecision(1) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);

        // Create thread line elements
        Elements threadLineElements;
        threadLineElements.push_back(text(std::to_string(i + 1)) | size(WIDTH, EQUAL, 4));
        threadLineElements.push_back(hbox({
            text("["),
            loadBar | size(WIDTH, EQUAL, 10),
            text("] "),
            text(loadPercentSs.str()) | color(loadColor)
        }) | size(WIDTH, EQUAL, 24));
        threadLineElements.push_back(text(qpsSs.str()) | align_right | size(WIDTH, EQUAL, 8));
        if (showExtendedColumns) {
            threadLineElements.push_back(text(queueSizeSs.str()) | align_right | size(WIDTH, EQUAL, 10));
            threadLineElements.push_back(text(queueP90Ss.str()) | align_right | size(WIDTH, EQUAL, 20));
        }

        auto threadLine = hbox(threadLineElements);

        if (i < halfCount) {
            leftThreadElements.push_back(threadLine);
        } else {
            rightThreadElements.push_back(threadLine);
        }
    }

    // Pad the shorter column with empty lines
    while (leftThreadElements.size() < rightThreadElements.size()) {
        leftThreadElements.push_back(text(""));
    }
    while (rightThreadElements.size() < leftThreadElements.size()) {
        rightThreadElements.push_back(text(""));
    }

    auto threadSection = window(text("TPC-C client state"), hbox({
        vbox(leftThreadElements) | flex,
        separator(),
        vbox(rightThreadElements) | flex
    }));

    return vbox({
        topSection,
        threadSection,
    });
}

Component TRunnerTui::BuildComponent() {
    // Logs section

    auto scrollableLogs = Scroller(Renderer([&] {
        Elements logElements;

        LogBackend.GetLogLines([&](ELogPriority, const std::string& line) {
            logElements.push_back(paragraph(line));
        });

        auto logsContent = vbox(logElements) | flex;
        return logsContent;
    }), "Logs");

    // Main layout

    return Container::Vertical({
        Renderer([=]{ return BuildUpperPart(); }),
        scrollableLogs,
    });
}

} // namespace NYdb::NTPCC
