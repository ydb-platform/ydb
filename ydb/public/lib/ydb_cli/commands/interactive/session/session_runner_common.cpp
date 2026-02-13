#include "session_runner_common.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TSessionRunnerBase::TSessionRunnerBase(const TLineReaderSettings& settings)
    : Settings(settings)
{}

ILineReader::TPtr TSessionRunnerBase::Setup() {
    LineReader.reset();
    LineReader = CreateLineReader(Settings);
    return LineReader;
}

void TSessionRunnerBase::PrintCommonHotKeys(std::vector<ftxui::Element>& elements) {
    using namespace ftxui;

    elements.emplace_back(CreateListItem(hbox({
        CreateEntityName("Up and Down arrow keys"), text(": navigate through query history.")
    })));
    elements.emplace_back(CreateListItem(hbox({
        CreateEntityName("Ctrl+R"), text(": search for a query in history containing a specified substring.")
    })));
    elements.emplace_back(CreateListItem(hbox({
        CreateEntityName("Ctrl+J"), text(": insert new line.")
    })));
    elements.emplace_back(CreateListItem(hbox({
        CreateEntityName("Ctrl+D"), text(": exit interactive mode.")
    })));
}

void TSessionRunnerBase::PrintCommonInteractiveCommands(std::vector<ftxui::Element>& elements) {
    using namespace ftxui;

    elements.emplace_back(CreateListItem(hbox({
        CreateEntityName("/help"), text(": print this help message.")
    })));
}

ftxui::Element TSessionRunnerBase::CreateListItem(ftxui::Element element) {
    return ftxui::hbox({
        ftxui::text(L"  • "),
        element,
    });
}

ftxui::Element TSessionRunnerBase::CreateEntityName(const TString& name) {
    return ftxui::text(name) | ftxui::bold;
}

} // namespace NYdb::NConsoleClient
