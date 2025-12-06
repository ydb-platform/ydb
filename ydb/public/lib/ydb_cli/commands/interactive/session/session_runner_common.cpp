#include "session_runner_common.h"

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TSessionRunnerBase::TSessionRunnerBase(const TSessionSettings& settings, const TInteractiveLogger& log)
    : Log(log)
    , Settings(settings)
{}

bool TSessionRunnerBase::Setup(ILineReaderController::TPtr controller) {
    controller->Setup(Settings);
    Controller = std::move(controller);
    return true;
}

TString TSessionRunnerBase::PrintCommonHotKeys() {
    return TStringBuilder()
        << "  " << PrintBold("Up and Down arrow keys") << ": navigate through query history." << Endl
        << "  " << PrintBold("Ctrl+R") << ": search for a query in history containing a specified substring." << Endl
        << "  " << PrintBold("Ctrl+O") << ": open new line." << Endl
        << "  " << PrintBold("Ctrl+D") << ": exit interactive mode." << Endl
        << "  " << PrintBold("Ctrl+K") << ": print this help message." << Endl;
}

TString TSessionRunnerBase::PrintBold(const TString& text) {
    return TStringBuilder() << Colors.BoldColor() << text << Colors.OldColor();
}

TString TSessionRunnerBase::PrintGreen(const TString& text) {
    return TStringBuilder() << Colors.Green() << text << Colors.OldColor();
}

} // namespace NYdb::NConsoleClient
