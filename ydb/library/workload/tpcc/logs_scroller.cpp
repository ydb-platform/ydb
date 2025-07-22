#include "logs_scroller.h"

#include "log.h"
#include "scroller.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

using namespace ftxui;

namespace NYdb::NTPCC {

namespace {

inline std::optional<Color::Palette16> GetFtxuiLogColor(ELogPriority priority) {
    switch (priority) {
        case TLOG_EMERG:
            [[fallthrough]];
        case TLOG_ALERT:
            [[fallthrough]];
        case TLOG_CRIT:
            [[fallthrough]];
        case TLOG_ERR:
            return Color::Red;
        case TLOG_WARNING:
            return Color::Yellow;
        case TLOG_NOTICE:
            [[fallthrough]];
        case TLOG_INFO:
            [[fallthrough]];
        case TLOG_DEBUG:
            [[fallthrough]];
        case TLOG_RESOURCES:
            [[fallthrough]];
        default:
            return std::nullopt;
    }
}

} // anonymous

Component LogsScroller(TLogBackendWithCapture& logBackend) {
    return Scroller(Renderer([&] {
        Elements logElements;

        logBackend.GetLogLines([&](ELogPriority priority, const std::string& line) {
            // not very efficient, but it's OK.
            // TODO: currently it's impossible to have some words in paragraph colored
            size_t dtLen = GetLenOfFormatDate8601Part();
            size_t messageOffset = GetOffsetToLogMessage(priority);

            TString withoutColor = TStringBuilder() << line.substr(0, dtLen - 1) << " "
                << PriorityToString(priority) << line.substr(messageOffset, line.size() - 1);

            auto colorOpt = GetFtxuiLogColor(priority);
            logElements.push_back(paragraph(withoutColor)
                | (colorOpt ? color(*colorOpt) : color(Color::Default)));
        });

        auto logsContent = vbox(logElements) | flex;
        return logsContent;
    }), "Logs");
}

} // namespace NYdb::NTPCC