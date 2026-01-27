#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NYdb::NConsoleClient {

class TCommandSql : public TYdbCommand, public TCommandWithOutput, public TCommandWithParameters, public TInterruptableCommand {
public:
    TCommandSql();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void SetCollectStatsMode(TString&& collectStatsMode);
    void SetScript(TString&& script);
    void SetSyntax(const TString& syntax);

private:
    int RunCommand(TConfig& config);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Progress;
    TExecuteGenericQuery::TSettings ExecSettings;
};

} // namespace NYdb::NConsoleClient
