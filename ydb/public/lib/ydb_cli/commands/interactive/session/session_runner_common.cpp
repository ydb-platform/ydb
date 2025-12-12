#include "session_runner_common.h"

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TSessionRunnerBase::TSessionRunnerBase(const TLineReaderSettings& settings, const TInteractiveLogger& log)
    : Log(log)
    , Settings(settings)
{}

ILineReader::TPtr TSessionRunnerBase::Setup() {
    LineReader = CreateLineReader(Settings, Log);
    return LineReader;
}

TString TSessionRunnerBase::PrintCommonHotKeys() {
    return TStringBuilder()
        << "  " << TInteractiveLogger::EntityName("Up and Down arrow keys") << ": navigate through query history." << Endl
        << "  " << TInteractiveLogger::EntityName("Ctrl+R") << ": search for a query in history containing a specified substring." << Endl
        << "  " << TInteractiveLogger::EntityName("Ctrl+J") << ": insert new line." << Endl
        << "  " << TInteractiveLogger::EntityName("Ctrl+D") << ": exit interactive mode." << Endl;
}

} // namespace NYdb::NConsoleClient
