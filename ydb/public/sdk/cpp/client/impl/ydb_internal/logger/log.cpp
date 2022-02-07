#define INCLUDE_YDB_INTERNAL_H
#include "log.h"

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NYdb {

static const TStringBuf LogPrioritiesStrings[] = {
    " :EMERG: ",
    " :ALERT: ",
    " :CRIT: ",
    " :ERROR: ",
    " :WARNING: ",
    " :NOTICE: ",
    " :INFO: ",
    " :DEBUG: ",
    " :TRACE: ",
};

TStringBuf LogPriorityToString(ELogPriority priority) {
    constexpr int maxPriority = static_cast<int>(std::size(LogPrioritiesStrings) - 1);
    const int index = static_cast<int>(priority);
    return LogPrioritiesStrings[ClampVal(index, 0, maxPriority)];
}

TLogFormatter GetPrefixLogFormatter(const TString& prefix) {
    return [prefix](ELogPriority priority, TStringBuf message) -> TString {
        constexpr size_t timeLen = 27; // 2020-10-08T20:31:11.202588Z
        constexpr size_t endlLen = 1;

        const TStringBuf priorityString = LogPriorityToString(priority);
        TStringBuilder result;
        const size_t toReserve = prefix.size() + message.Size() + timeLen + endlLen + priorityString.Size();
        result.reserve(toReserve);

        result << TInstant::Now() << priorityString << prefix << message << Endl;
        Y_ASSERT(result.size() == toReserve);
        return std::move(result);
    };
}

TStringType GetDatabaseLogPrefix(const TStringType& database) {
    return "[" + database + "] ";
}

} // namespace NYdb
