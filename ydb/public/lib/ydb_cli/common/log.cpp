#include "log.h"

namespace NYdb::NConsoleClient {

ELogPriority VerbosityLevelToELogPriority(ui32 verbosityLevel) {
    switch (verbosityLevel) {
        case 0:
            return ELogPriority::TLOG_WARNING;
        case 1:
            return ELogPriority::TLOG_NOTICE;
        case 2:
            return ELogPriority::TLOG_INFO;
        case 3:
        default:
            return ELogPriority::TLOG_DEBUG;
    }
}

ELogPriority VerbosityLevelToELogPriorityChatty(ui32 verbosityLevel) {
    switch (verbosityLevel) {
        case 0:
            return ELogPriority::TLOG_INFO;
        default:
            return ELogPriority::TLOG_DEBUG;
    }
}

TLogger::TEntry::TEntry(std::shared_ptr<TLog> log, ELogPriority priority)
    : Priority(std::move(priority))
    , Log(log)
{}

bool TLogger::TEntry::LogEnabled() const {
    return Log && Log->IsOpen() && Log->FiltrationLevel() >= Priority;
}

TLogger::TEntry::~TEntry() {
    try {
        if (LogEnabled()) {
            Log->Write(Priority, *this);
        }
    } catch (...) {
        // ¯\_(ツ)_/¯
    }
}

TLogger::TLogger()
    : Log(std::make_shared<TLog>())
{}

void TLogger::Setup(ui32 verbosityLevel) {
    Log->ResetBackend(CreateLogBackend(
        "cerr",
        VerbosityLevelToELogPriority(verbosityLevel)
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

TLogger::TEntry TLogger::Critical() const {
    return TEntry(Log, TLOG_CRIT);
}

TLogger::TEntry TLogger::Error() const {
    return TEntry(Log, TLOG_ERR);
}

TLogger::TEntry TLogger::Warning() const {
    return TEntry(Log, TLOG_WARNING);
}

TLogger::TEntry TLogger::Notice() const {
    return TEntry(Log, TLOG_NOTICE);
}

TLogger::TEntry TLogger::Info() const {
    return TEntry(Log, TLOG_INFO);
}

TLogger::TEntry TLogger::Debug() const {
    return TEntry(Log, TLOG_DEBUG);
}

bool TLogger::IsVerbose() const {
    return Log->FiltrationLevel() >= TLOG_NOTICE;
}

TString TLogger::EntityName(const TString& name) {
    return TStringBuilder() << Colors.BoldColor() << name << Colors.OldColor();
}

TString TLogger::EntityNameQuoted(const TString& name) {
    return TStringBuilder() << '"' << Colors.BoldColor() << name << Colors.OldColor() << '"';
}

// Global logger instance
static TLogger GlobalLogger;

TLogger& GetGlobalLogger() {
    return GlobalLogger;
}

void SetupGlobalLogger(ui32 verbosityLevel) {
    GlobalLogger.Setup(verbosityLevel);
}

} // namespace NYdb::NConsoleClient
