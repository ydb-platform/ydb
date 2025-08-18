#pragma once

#include "runner_display_data.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

namespace NYdb::NTPCC {

class TLogBackendWithCapture;

class TRunnerTui {
public:
    TRunnerTui(std::shared_ptr<TLog>& log, TLogBackendWithCapture& logBacked, std::shared_ptr<TRunDisplayData> data);
    ~TRunnerTui();

    void Update(std::shared_ptr<TRunDisplayData> data);

private:
    ftxui::Element BuildPreviewPart();
    ftxui::Element BuildThreadStatsPart();

    ftxui::Component BuildComponent();

private:
    std::shared_ptr<TLog> Log;
    TLogBackendWithCapture& LogBackend;
    std::shared_ptr<TRunDisplayData> DataToDisplay;
    ftxui::ScreenInteractive Screen;
    std::thread TuiThread;
};

} // namespace NYdb::NTPCC
