#pragma once

#include "import_display_data.h"
#include "log.h"
#include "runner.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

namespace NYdb::NTPCC {

class TLogBackendWithCapture;

class TImportTui {
public:
    TImportTui(std::shared_ptr<TLog>& log, const TRunConfig& runConfig, TLogBackendWithCapture& logBacked, const TImportDisplayData& data);
    ~TImportTui();

    void Update(const TImportDisplayData& data);

private:
    ftxui::Element BuildUpperPart(); // everything except bottom with logs
    ftxui::Component BuildComponent();

private:
    std::shared_ptr<TLog> Log;
    const TRunConfig Config;
    TLogBackendWithCapture& LogBackend;
    TImportDisplayData DataToDisplay;
    ftxui::ScreenInteractive Screen;
    std::thread TuiThread;
};

} // namespace NYdb::NTPCC
