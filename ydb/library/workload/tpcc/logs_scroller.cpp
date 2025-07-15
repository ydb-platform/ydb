#include "logs_scroller.h"
#include "scroller.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

namespace NYdb::NTPCC {

ftxui::Component LogsScroller(TLogBackendWithCapture& logBackend) {
    return Scroller(ftxui::Renderer([&] {
        ftxui::Elements logElements;

        logBackend.GetLogLines([&](ELogPriority, const std::string& line) {
            logElements.push_back(ftxui::paragraph(line));
        });

        auto logsContent = ftxui::vbox(logElements) | ftxui::flex;
        return logsContent;
    }), "Logs");
}

} // namespace NYdb::NTPCC