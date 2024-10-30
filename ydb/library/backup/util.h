#pragma once

#include <library/cpp/logger/log.h>

#include <util/string/builder.h>

#define LOG_IMPL(log, level, message) \
    if (log->FiltrationLevel() >= level) { \
        log->Write(level, TStringBuilder() << message); \
    } \
    Y_SEMICOLON_GUARD

#define LOG_D(message) LOG_IMPL(NYdb::NBackup::GetLog(), ELogPriority::TLOG_DEBUG, message)
#define LOG_I(message) LOG_IMPL(NYdb::NBackup::GetLog(), ELogPriority::TLOG_INFO, message)
#define LOG_W(message) LOG_IMPL(NYdb::NBackup::GetLog(), ELogPriority::TLOG_WARNING, message)
#define LOG_E(message) LOG_IMPL(NYdb::NBackup::GetLog(), ELogPriority::TLOG_ERR, message)

namespace NYdb {

namespace NBackup {
    void SetLog(const std::shared_ptr<::TLog>& log);
    const std::shared_ptr<::TLog>& GetLog();
}

// Retrive path relative to database root from absolute
TString RelPathFromAbsolute(TString db, TString path);

// Parses strings from human readable format to ui64
// Suppores decimal prefixes such as K(1000), M, G, T
// Suppores binary prefixes such as Ki(1024), Mi, Gi, Ti
// Example: "2Ki" -> 2048
ui64 SizeFromString(TStringBuf s);

}
