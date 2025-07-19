#pragma once

#include "runner_display_data.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

namespace NYdb::NTPCC {

class TLogBackendWithCapture;

class TRunnerTui {
public:
    TRunnerTui(TLogBackendWithCapture& logBacked, std::shared_ptr<TRunDisplayData> data);
    ~TRunnerTui();

    void Update(std::shared_ptr<TRunDisplayData> data);

private:
    ftxui::Element BuildUpperPart(); // everything except bottom with logs
    ftxui::Component BuildComponent();

private:
    TLogBackendWithCapture& LogBackend;
    std::shared_ptr<TRunDisplayData> DataToDisplay;
    ftxui::ScreenInteractive Screen;
    std::thread TuiThread;
};

} // namespace NYdb::NTPCC
