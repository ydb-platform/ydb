#pragma once

#include <library/cpp/logger/priority.h>

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>


namespace NYql {
namespace NLog {

enum class ELevel {
    FATAL = TLOG_EMERG,
    ERROR = TLOG_ERR,
    WARN = TLOG_WARNING,
    NOTICE = TLOG_NOTICE,
    INFO = TLOG_INFO,
    DEBUG = TLOG_DEBUG,
    TRACE = TLOG_RESOURCES,
};

struct ELevelHelpers {
    static constexpr bool Lte(ELevel l1, ELevel l2) {
        return ToInt(l1) <= ToInt(l2);
    }

    static constexpr ELogPriority ToLogPriority(ELevel level) {
        return static_cast<ELogPriority>(ToInt(level));
    }

    static ELevel FromLogPriority(ELogPriority priority) {
        return FromInt(static_cast<int>(priority));
    }

    static constexpr int ToInt(ELevel level) {
        return static_cast<int>(level);
    }

    static ELevel FromInt(int level) {
        switch (level) {
        case TLOG_EMERG:
        case TLOG_ALERT:
        case TLOG_CRIT: return ELevel::FATAL;

        case TLOG_ERR: return ELevel::ERROR;
        case TLOG_WARNING: return ELevel::WARN;

        case TLOG_NOTICE:
        case TLOG_INFO: return ELevel::INFO;

        case TLOG_DEBUG: return ELevel::DEBUG;
        case TLOG_RESOURCES: return ELevel::TRACE;

        default:
            return ELevel::INFO;
        }
    }

    static TStringBuf ToString(ELevel level) {
        // aligned 5-letters string
        switch (level) {
        case ELevel::FATAL: return TStringBuf("FATAL");
        case ELevel::ERROR: return TStringBuf("ERROR");
        case ELevel::WARN:  return TStringBuf("WARN ");
        case ELevel::NOTICE:return TStringBuf("NOTE ");
        case ELevel::INFO:  return TStringBuf("INFO ");
        case ELevel::DEBUG: return TStringBuf("DEBUG");
        case ELevel::TRACE: return TStringBuf("TRACE");
        }
        ythrow yexception() << "unknown log level: " << ToInt(level);
    }

    static ELevel FromString(TStringBuf str) {
        // aligned 5-letters string
        if (str == TStringBuf("FATAL")) return ELevel::FATAL;
        if (str == TStringBuf("ERROR")) return ELevel::ERROR;
        if (str == TStringBuf("WARN ")) return ELevel::WARN;
        if (str == TStringBuf("NOTE ")) return ELevel::NOTICE;
        if (str == TStringBuf("INFO ")) return ELevel::INFO;
        if (str == TStringBuf("DEBUG")) return ELevel::DEBUG;
        if (str == TStringBuf("TRACE")) return ELevel::TRACE;
        ythrow yexception() << "unknown log level: " << str;
    }

    template <typename TFunctor>
    static void ForEach(TFunctor&& f) {
        static const int minValue = ToInt(ELevel::FATAL);
        static const int maxValue = ToInt(ELevel::TRACE);

        for (int l = minValue; l <= maxValue; l++) {
            f(FromInt(l));
        }
    }
};


} // namspace NLog
} // namspace NYql
