#pragma once

#include "runner_display_data.h"
#include "tui_base.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

namespace NYdb::NTPCC {

class TLogBackendWithCapture;

class TRunnerTui : public TuiBase {
public:
    TRunnerTui(std::shared_ptr<TLog>& log, TLogBackendWithCapture& logBacked, std::shared_ptr<TRunDisplayData> data);

    void Update(std::shared_ptr<TRunDisplayData> data);

private:
    ftxui::Element BuildPreviewPart();
    ftxui::Element BuildThreadStatsPart();

    ftxui::Component BuildComponent() override;

private:
    std::shared_ptr<TLog> Log;
    TLogBackendWithCapture& LogBackend;
    std::shared_ptr<TRunDisplayData> DataToDisplay;
};

} // namespace NYdb::NTPCC
