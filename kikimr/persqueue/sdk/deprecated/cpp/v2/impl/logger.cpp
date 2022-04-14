#include <kikimr/persqueue/sdk/deprecated/cpp/v2/logger.h>
#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/stream/str.h>

namespace NPersQueue {

static const TStringBuf LogLevelsStrings[] = {
    "EMERG",
    "ALERT",
    "CRITICAL_INFO",
    "ERROR",
    "WARNING",
    "NOTICE",
    "INFO",
    "DEBUG",
};

TStringBuf TCerrLogger::LevelToString(int level) {
    return LogLevelsStrings[ClampVal(level, 0, int(Y_ARRAY_SIZE(LogLevelsStrings) - 1))];
}

void TCerrLogger::Log(const TString& msg, const TString& sourceId, const TString& sessionId, int level) {
    if (level > Level) {
        return;
    }

    TStringBuilder message;
    message << TInstant::Now() << " :" << LevelToString(level) << ":";
    if (sourceId) {
        message << " SourceId [" << sourceId << "]:";
    }
    if (sessionId) {
        message << " SessionId [" << sessionId << "]:";
    }
    message << " " << msg << "\n";
    Cerr << message;
}

}
