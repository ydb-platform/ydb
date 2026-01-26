#pragma once

#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/priority.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

// Converts verbosity level (number of -v flags) to log priority
// Normal: 0->WARNING, 1->NOTICE, 2->INFO, 3+->DEBUG
ELogPriority VerbosityLevelToELogPriority(ui32 verbosityLevel);
// Chatty: 0->INFO, 1+->DEBUG
ELogPriority VerbosityLevelToELogPriorityChatty(ui32 verbosityLevel);

class TLogger {
    inline static const NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

    class TEntry : public TStringBuilder {
    public:
        TEntry(std::shared_ptr<TLog> log, ELogPriority priority);

        bool LogEnabled() const;

        ~TEntry();

    private:
        const ELogPriority Priority;
        const std::shared_ptr<TLog> Log;
    };

public:
    TLogger();

    void Setup(ui32 verbosityLevel);

    TEntry Critical() const;
    TEntry Error() const;
    TEntry Warning() const;
    TEntry Notice() const;
    TEntry Info() const;
    TEntry Debug() const;

    bool IsVerbose() const;

    static TString EntityName(const TString& name);
    static TString EntityNameQuoted(const TString& name);

private:
    std::shared_ptr<TLog> Log;
};

// Global logger instance
TLogger& GetGlobalLogger();
void SetupGlobalLogger(ui32 verbosityLevel);

} // namespace NYdb::NConsoleClient

// Logging macro for convenience
#define YDB_CLI_LOG(level, stream)                                                       \
    if (auto entry = NYdb::NConsoleClient::GetGlobalLogger().level(); entry.LogEnabled()) { \
        entry << stream;                                                                  \
    }
