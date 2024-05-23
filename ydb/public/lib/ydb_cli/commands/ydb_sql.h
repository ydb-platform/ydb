#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandSql : public TYdbCommand, public TCommandWithParameters,
    public TInterruptibleCommand
{
public:
    TCommandSql();
    TCommandSql(TString script, TString collectStatsMode);
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int RunCommand(TConfig& config);
    int PrintResponse(NQuery::TExecuteQueryIterator& result);
    void PrintResponse(const NQuery::TScriptExecutionOperation& result);
    int WaitForResultAndPrintResponse(TDriver& driver, NQuery::TQueryClient& client,
        const NQuery::TScriptExecutionOperation& result);
    int FetchResults(TDriver& driver, NQuery::TQueryClient& client);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Syntax;
    TString OperationIdToFetch;
    bool ExplainMode = false;
    bool ExplainAnalyzeMode = false;
    bool RunAsync = false;
    bool AsyncWait = false;
};

}
}
