#pragma once

#include "import_display_data.h"
#include "log.h"
#include "runner.h"
#include "tui_base.h"

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

namespace NYdb::NTPCC {

class TLogBackendWithCapture;

class TImportTui : public TuiBase {
public:
    TImportTui(std::shared_ptr<TLog>& log, const TRunConfig& runConfig, TLogBackendWithCapture& logBacked, const TImportDisplayData& data);

    void Update(const TImportDisplayData& data);

private:
    ftxui::Element BuildUpperPart(); // everything except bottom with logs
    ftxui::Component BuildComponent() override;

private:
    std::shared_ptr<TLog> Log;
    const TRunConfig Config;
    TLogBackendWithCapture& LogBackend;
    TImportDisplayData DataToDisplay;
};

} // namespace NYdb::NTPCC
