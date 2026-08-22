#include "run_tui.h"

#include "scroller.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

#include <util/string/cast.h>

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace NKvVolumeStress {

using namespace ftxui;

namespace {

double ToMiBPerSecond(double bytesPerSecond) {
    return bytesPerSecond / (1024.0 * 1024.0);
}

} // namespace

TRunTui::TRunTui(std::shared_ptr<TRunDisplayData> data)
    : Screen_(ScreenInteractive::Fullscreen())
    , DataToDisplay_(std::move(data))
{
    Thread_ = std::thread([this] {
        Screen_.Loop(BuildComponent());
    });
}

TRunTui::~TRunTui() {
    Screen_.Exit();
    if (Thread_.joinable()) {
        Thread_.join();
    }
}

void TRunTui::Update(std::shared_ptr<TRunDisplayData> data) {
    std::atomic_store(&DataToDisplay_, std::move(data));
    Screen_.PostEvent(Event::Custom);
}

Element TRunTui::BuildHeaderPart() {
    std::shared_ptr<TRunDisplayData> data = std::atomic_load(&DataToDisplay_);

    const double progress = std::clamp(data->ProgressPercent / 100.0, 0.0, 1.0);

    std::stringstream timeSs;
    timeSs << data->ElapsedSeconds / 60 << ":" << std::setfill('0') << std::setw(2) << (data->ElapsedSeconds % 60)
           << " elapsed, "
           << data->RemainingSeconds / 60 << ":" << std::setfill('0') << std::setw(2) << (data->RemainingSeconds % 60)
           << " remaining";

    std::stringstream metricsSs;
    metricsSs << "total ops: " << data->TotalActions
              << "   ops/s(cur): " << std::fixed << std::setprecision(2) << data->ActionsPerSecond
              << "   ops/s(avg): " << std::fixed << std::setprecision(1) << data->AverageActionsPerSecond
              << "   errors: " << data->TotalErrors;

    std::stringstream latencySs;
    if (data->Latency.Samples == 0) {
        latencySs << "latency(ms): n/a";
    } else {
        latencySs << "latency(ms):"
                  << " p50=" << data->Latency.P50Ms
                  << " p90=" << data->Latency.P90Ms
                  << " p99=" << data->Latency.P99Ms
                  << " p100=" << data->Latency.P100Ms
                  << "  samples=" << data->Latency.Samples;
    }

    return window(text("kv_volume workload"),
        vbox({
            text(timeSs.str()),
            text(metricsSs.str()) | bold,
            text(latencySs.str()),
            hbox({
                text("Progress: ["),
                gauge(static_cast<float>(progress)) | size(WIDTH, EQUAL, 20),
                text("] " + std::to_string(static_cast<int>(data->ProgressPercent)) + "%"),
            }),
        }));
}

Element TRunTui::BuildActionsPart() {
    std::shared_ptr<TRunDisplayData> data = std::atomic_load(&DataToDisplay_);

    Elements rows;
    rows.push_back(hbox({
        text("Action") | size(WIDTH, EQUAL, 16),
        text("Total") | align_right | size(WIDTH, EQUAL, 9),
        text("ops/s") | align_right | size(WIDTH, EQUAL, 8),
        text("Err") | align_right | size(WIDTH, EQUAL, 6),
        text("p50") | align_right | size(WIDTH, EQUAL, 6),
        text("p90") | align_right | size(WIDTH, EQUAL, 6),
        text("p99") | align_right | size(WIDTH, EQUAL, 6),
        text("p100") | align_right | size(WIDTH, EQUAL, 7),
        text("rMB/s") | align_right | size(WIDTH, EQUAL, 8),
        text("wMB/s") | align_right | size(WIDTH, EQUAL, 8),
    }));

    if (data->Actions.empty()) {
        rows.push_back(text("no actions yet"));
    } else {
        for (const auto& action : data->Actions) {
            std::stringstream rateSs;
            rateSs << std::fixed << std::setprecision(1) << action.RunsPerSecond;
            const TString p50 = action.Latency.Samples > 0 ? ToString(action.Latency.P50Ms) : "-";
            const TString p90 = action.Latency.Samples > 0 ? ToString(action.Latency.P90Ms) : "-";
            const TString p99 = action.Latency.Samples > 0 ? ToString(action.Latency.P99Ms) : "-";
            const TString p100 = action.Latency.Samples > 0 ? ToString(action.Latency.P100Ms) : "-";
            std::stringstream readBwSs;
            readBwSs << std::fixed << std::setprecision(1) << ToMiBPerSecond(action.ReadBytesPerSecond);
            std::stringstream writeBwSs;
            writeBwSs << std::fixed << std::setprecision(1) << ToMiBPerSecond(action.WriteBytesPerSecond);

            rows.push_back(hbox({
                text(action.Name) | size(WIDTH, EQUAL, 16),
                text(ToString(action.TotalRuns)) | align_right | size(WIDTH, EQUAL, 9),
                text(rateSs.str()) | align_right | size(WIDTH, EQUAL, 8),
                text(ToString(action.ErrorCount)) | align_right | size(WIDTH, EQUAL, 6),
                text(p50) | align_right | size(WIDTH, EQUAL, 6),
                text(p90) | align_right | size(WIDTH, EQUAL, 6),
                text(p99) | align_right | size(WIDTH, EQUAL, 6),
                text(p100) | align_right | size(WIDTH, EQUAL, 7),
                text(readBwSs.str()) | align_right | size(WIDTH, EQUAL, 8),
                text(writeBwSs.str()) | align_right | size(WIDTH, EQUAL, 8),
            }));
        }
    }

    return vbox(rows);
}

Element TRunTui::BuildWorkersPart() {
    std::shared_ptr<TRunDisplayData> data = std::atomic_load(&DataToDisplay_);

    Elements rows;
    if (data->WorkerLoad.WorkersTotal == 0) {
        rows.push_back(text("Worker load: n/a"));
        return vbox(rows);
    }

    std::stringstream summarySs;
    summarySs << "Workers busy: " << data->WorkerLoad.BusyWorkers << "/" << data->WorkerLoad.WorkersTotal
              << " (" << std::fixed << std::setprecision(1) << data->WorkerLoad.BusyPercent << "%)"
              << "   Active actions: " << data->WorkerLoad.ActiveActionsTotal << "/" << data->WorkerLoad.CapacityTotal
              << " (" << std::fixed << std::setprecision(1) << data->WorkerLoad.UtilizationPercent << "%)";
    rows.push_back(text(summarySs.str()) | bold);

    rows.push_back(hbox({
        text("Worker") | size(WIDTH, EQUAL, 10),
        text("Active") | align_right | size(WIDTH, EQUAL, 10),
        text("Cap") | align_right | size(WIDTH, EQUAL, 10),
        text("Util%") | align_right | size(WIDTH, EQUAL, 10),
    }));

    for (const auto& row : data->WorkerLoad.Workers) {
        std::stringstream utilSs;
        utilSs << std::fixed << std::setprecision(1) << row.UtilizationPercent;

        rows.push_back(hbox({
            text(ToString(row.WorkerId)) | size(WIDTH, EQUAL, 10),
            text(ToString(row.ActiveActions)) | align_right | size(WIDTH, EQUAL, 10),
            text(ToString(row.Capacity)) | align_right | size(WIDTH, EQUAL, 10),
            text(utilSs.str()) | align_right | size(WIDTH, EQUAL, 10),
        }));
    }

    if (data->WorkerLoad.Workers.empty()) {
        rows.push_back(text("no workers"));
    }

    return vbox(rows);
}

Component TRunTui::BuildComponent() {
    Component header = Renderer([this] {
        return BuildHeaderPart();
    });

    Component actions = Scroller(Renderer([this] {
        return BuildActionsPart();
    }), "Actions");

    Component workers = Scroller(Renderer([this] {
        return BuildWorkersPart();
    }), "Workers");

    auto container = Container::Vertical({header, actions, workers});

    return Renderer(container, [=] {
        const int termHeight = Terminal::Size().dimy;
        Element headerElement = header->Render();
        headerElement->ComputeRequirement();
        const int headerHeight = headerElement->requirement().min_y;

        if (termHeight <= headerHeight + 6) {
            return vbox({headerElement});
        }

        Element actionsElement = actions->Render() | size(HEIGHT, GREATER_THAN, 8) | flex;
        Element workersElement = workers->Render() | size(HEIGHT, GREATER_THAN, 8) | flex;

        return vbox({
            headerElement,
            actionsElement,
            workersElement,
        });
    });
}

} // namespace NKvVolumeStress
