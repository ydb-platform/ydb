#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandExecuteSqlBase : public TCommandWithFormat, public TInterruptibleCommand {
public:
    void DeclareScriptOptions(TClientCommand::TConfig& config);
    void DeclareCommonInputOptions(TClientCommand::TConfig& config);
    void DeclareCommonOutputOptions(TClientCommand::TConfig& config);
    // Common part for all subcommands that accepts query input: query text, params, e.t.c.
    void ParseCommonInputOptions();
    // Common part for all subcommands that prints query output
    void ParseCommonOutputOptions();

protected:
    int ExecuteScriptAsync(TDriver& driver, NQuery::TQueryClient& client);
    int PrintResponse(const NQuery::TScriptExecutionOperation& operation);
    int WaitForResultAndPrintResponse(NQuery::TQueryClient& queryClient, NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId,
        // First query status object. Received either form ExecuteQueryScriptResponse or from OperationService explicitly
        const NQuery::TScriptExecutionOperation& operation);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Syntax;
    TString ResultsTtl;
    bool AsyncWait = false;
};

class TCommandSql : public TYdbCommand, public TCommandExecuteSqlBase {
public:
    TCommandSql();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void SetSyntax(TString&& syntax);
    void SetCollectStatsMode(TString&& collectStatsMode);
    void SetScript(TString&& script);

private:
    int ExecuteQueryStream(NQuery::TQueryClient& client);
    int PrintResponse(NQuery::TExecuteQueryIterator& result);

    bool ExplainMode = false;
    bool ExplainAnalyzeMode = false;
    bool RunAsync = false;
};

class TCommandSqlAsync : public TClientCommandTree {
public:
    TCommandSqlAsync();
};

class TCommandSqlAsyncExecute : public TYdbCommand, public TCommandExecuteSqlBase {
public:
    TCommandSqlAsyncExecute();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandWithScriptExecutionOperationId : public TYdbCommand {
public:
    using TYdbCommand::TYdbCommand;
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;

protected:
    NQuery::TScriptExecutionOperation GetOperation(TDriver& driver);

    NKikimr::NOperationId::TOperationId OperationId;
};

class TCommandSqlAsyncFetch : public TCommandWithScriptExecutionOperationId, public TCommandExecuteSqlBase {
public:
    TCommandSqlAsyncFetch();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandSqlAsyncGet : public TCommandWithScriptExecutionOperationId, public TCommandWithFormat {
public:
    TCommandSqlAsyncGet();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int PrintResponse(const NQuery::TScriptExecutionOperation& operation);
};

}
}
