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
    try {
        if (LogEnabled()) {
            Log->Write(Priority, *this);
        }
    } catch (...) {
        // ¯\_(ツ)_/¯
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

    Log->SetFormatter([colors = NConsoleClient::AutoColors(Cerr)](ELogPriority priority, TStringBuf message) {
        TStringBuilder prefix;
        switch (priority) {
        case TLOG_EMERG:
        case TLOG_ALERT:
        case TLOG_CRIT:
            prefix << colors.Magenta() << "Critical error: ";
            break;
        case TLOG_ERR:
            prefix << colors.Red() << "Error: ";
            break;
        case TLOG_WARNING:
            prefix << colors.Yellow() << "Warning: ";
            break;
        case TLOG_NOTICE:
            prefix << colors.Blue() << "Notice: ";
            break;
        case TLOG_INFO:
            prefix << colors.Green() << "Info: ";
            break;
        default:
            prefix << colors.DarkGrayColor() << "Debug: ";
            break;
        }

        return TStringBuilder() << prefix << message << colors.OldColor() << "\n";
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
    return Log->FiltrationLevel() >= TLOG_ERR;
}

TString TInteractiveLogger::EntityName(const TString& name) {
    return TStringBuilder() << Colors.BoldColor() << name << Colors.OldColor();
}

TString TInteractiveLogger::EntityNameQuoted(const TString& name) {
    return TStringBuilder() << '"' << Colors.BoldColor() << name << Colors.OldColor() << '"';
}

} // namespace NYdb::NConsoleClient
