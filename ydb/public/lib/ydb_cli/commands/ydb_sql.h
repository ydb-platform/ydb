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
    int PrintOperationInfo();
    bool WaitForCompletion(NOperation::TOperationClient& operationClient, const TOperation::TOperationId& operationId);
    int PrintScriptResults(NQuery::TQueryClient& queryClient);
    int WaitAndPrintResults(NQuery::TQueryClient& queryClient, NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Syntax;
    TString ResultsTtl = "86400";
    bool AsyncWait = false;
    // Operation status object. May be received either form ExecuteQueryScriptResponse or from OperationService explicitly
    std::optional<NQuery::TScriptExecutionOperation> Operation = std::nullopt;
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
    int StreamExecuteQuery(NQuery::TQueryClient& client);
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
    virtual int Run(TConfig& config) override;
};

class TCommandSqlAsyncWait : public TCommandWithScriptExecutionOperationId, public TCommandExecuteSqlBase {
public:
    TCommandSqlAsyncWait();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlAsyncCancel : public TCommandWithScriptExecutionOperationId {
public:
    TCommandSqlAsyncCancel();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlAsyncForget : public TCommandWithScriptExecutionOperationId {
public:
    TCommandSqlAsyncForget();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlAsyncList : public TYdbCommand, public TCommandWithFormat {
public:
    TCommandSqlAsyncList();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 PageSize = 0;
    TString PageToken;
};

}
}
