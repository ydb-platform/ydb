#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NConsoleClient {

class TInteractiveLogger {
    class TEntry : public TStringBuilder {
    public:
        TEntry(std::shared_ptr<TLog> log, ELogPriority priority);

        ~TEntry();

    private:
        const ELogPriority Priority;
        const std::shared_ptr<TLog> Log;
    };

public:
    TInteractiveLogger();

    void Setup(const TClientCommand::TConfig& config);

    TEntry Critical() const;
    TEntry Error() const;
    TEntry Warning() const;
    TEntry Notice() const;
    TEntry Info() const;
    TEntry Debug() const;

    bool IsVerbose() const;

private:
    std::shared_ptr<TLog> Log;
};

} // namespace NYdb::NConsoleClient
