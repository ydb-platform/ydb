#define INCLUDE_YDB_INTERNAL_H
#include "log.h"

#include <util/string/builder.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NYdb::inline Dev {

static const std::string_view LogPrioritiesStrings[] = {
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

std::string_view LogPriorityToString(ELogPriority priority) {
    constexpr int maxPriority = static_cast<int>(std::size(LogPrioritiesStrings) - 1);
    const int index = static_cast<int>(priority);
    return LogPrioritiesStrings[ClampVal(index, 0, maxPriority)];
}

TLogFormatter GetPrefixLogFormatter(const std::string& prefix) {
    return [prefix](ELogPriority priority, std::string_view message) -> std::string {
        constexpr size_t timeLen = 27; // 2020-10-08T20:31:11.202588Z
        constexpr size_t endlLen = 1;

        const std::string_view priorityString = LogPriorityToString(priority);
        TStringBuilder result;
        const size_t toReserve = prefix.size() + message.size() + timeLen + endlLen + priorityString.size();
        result.reserve(toReserve);

        result << TInstant::Now() << priorityString << prefix << message << Endl;
        Y_ASSERT(result.size() == toReserve);
        return std::move(result);
    };
}

std::string GetDatabaseLogPrefix(const std::string& database) {
    return "[" + database + "] ";
}

} // namespace NYdb
