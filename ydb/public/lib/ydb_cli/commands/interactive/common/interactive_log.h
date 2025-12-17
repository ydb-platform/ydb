#pragma once

#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NConsoleClient {

class TInteractiveLogger {
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
    TInteractiveLogger();

    void Setup(const TClientCommand::TConfig& config);

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

} // namespace NYdb::NConsoleClient
