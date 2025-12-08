#include "interactive_log.h"

namespace NYdb::NConsoleClient {

TInteractiveLogger::TEntry::TEntry(std::shared_ptr<TLog> log, ELogPriority priority)
    : Priority(std::move(priority))
    , Log(log)
{}

bool TInteractiveLogger::TEntry::LogEnabled() const {
    return Log && Log->IsOpen() && Log->FiltrationLevel() >= Priority;
}

TInteractiveLogger::TEntry::~TEntry() {
    if (LogEnabled()) {
        Log->Write(Priority, *this);
    }
}

TInteractiveLogger::TInteractiveLogger()
    : Log(std::make_shared<TLog>())
{}

void TInteractiveLogger::Setup(const TClientCommand::TConfig& config) {
    Log->ResetBackend(CreateLogBackend(
        "cerr",
        TClientCommand::TConfig::VerbosityLevelToELogPrioritySilent(config.VerbosityLevel)
    ));

    Log->SetFormatter([colors = NColorizer::AutoColors(Cerr)](ELogPriority priority, TStringBuf message) {
        TStringBuilder prefix;
        switch (priority) {
        case TLOG_EMERG:
        case TLOG_ALERT:
        case TLOG_CRIT:
            prefix << colors.Magenta() << "Critical error: " << colors.OldColor();
            break;
        case TLOG_ERR:
            prefix << colors.Red() << "Error: " << colors.OldColor();
            break;
        case TLOG_WARNING:
            prefix << colors.Yellow() << "Warning: " << colors.OldColor();
            break;
        case TLOG_NOTICE:
            prefix << colors.Blue() << "Notice: " << colors.OldColor();
            break;
        case TLOG_INFO:
            prefix << colors.Green() << "Info: " << colors.OldColor();
            break;
        default:
            prefix << colors.DarkGrayColor() << "Debug: " << colors.OldColor();
            break;
        }

        return TStringBuilder() << prefix << message << "\n";
    });
}

TInteractiveLogger::TEntry TInteractiveLogger::Critical() const {
    return TEntry(Log, TLOG_CRIT);
}

TInteractiveLogger::TEntry TInteractiveLogger::Error() const {
    return TEntry(Log, TLOG_ERR);
}

TInteractiveLogger::TEntry TInteractiveLogger::Warning() const {
    return TEntry(Log, TLOG_WARNING);
}

TInteractiveLogger::TEntry TInteractiveLogger::Notice() const {
    return TEntry(Log, TLOG_NOTICE);
}

TInteractiveLogger::TEntry TInteractiveLogger::Info() const {
    return TEntry(Log, TLOG_INFO);
}

TInteractiveLogger::TEntry TInteractiveLogger::Debug() const {
    return TEntry(Log, TLOG_DEBUG);
}

bool TInteractiveLogger::IsVerbose() const {
    return Log->FiltrationLevel() < TLOG_CRIT;
}

} // namespace NYdb::NConsoleClient
