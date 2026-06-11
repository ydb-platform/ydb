#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandExecuteSqlBase : public TCommandWithOutput, public TCommandWithParameters, public TInterruptableCommand {
public:
    void DeclareScriptOptions(TClientCommand::TConfig& config);
    void DeclareCommonInputOptions(TClientCommand::TConfig& config);
    void DeclareCommonOutputOptions(TClientCommand::TConfig& config);
    // Common part for all subcommands that accepts query input: query text, params, e.t.c.
    void ParseCommonInputOptions(TClientCommand::TConfig& config);
    // Common part for all subcommands that prints query output
    void ParseCommonOutputOptions();

protected:
    int ExecuteScriptAsync(TClientCommand::TConfig& config, TDriver& driver, NQuery::TQueryClient& client);
    int PrintOperationInfo(const TClientCommand::TConfig& config);
    bool WaitForCompletion(NOperation::TOperationClient& operationClient, const TOperation::TOperationId& operationId);
    int PrintScriptResults(NQuery::TQueryClient& queryClient);
    int WaitAndPrintResults(NQuery::TQueryClient& queryClient, NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Syntax;
    TDuration ResultsTtl = TDuration::Days(1);
    bool AsyncWait = false;
    // Operation status object. May be received either form ExecuteQueryScriptResponse or from OperationService explicitly
    std::optional<NQuery::TScriptExecutionOperation> Operation = std::nullopt;
};

class TCommandSqlExperimental : public TYdbCommand, public TCommandExecuteSqlBase {
public:
    TCommandSqlExperimental();
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

class TCommandSqlOperation : public TClientCommandTree {
public:
    TCommandSqlOperation();
};

class TCommandSqlOperationExecute : public TYdbCommand, public TCommandExecuteSqlBase {
public:
    TCommandSqlOperationExecute();
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

class TCommandSqlOperationFetch : public TCommandWithScriptExecutionOperationId, public TCommandExecuteSqlBase {
public:
    TCommandSqlOperationFetch();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandSqlOperationGet : public TCommandWithScriptExecutionOperationId, public TCommandWithOutput {
public:
    TCommandSqlOperationGet();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

class TCommandSqlOperationWait : public TCommandWithScriptExecutionOperationId, public TCommandExecuteSqlBase {
public:
    TCommandSqlOperationWait();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlOperationCancel : public TCommandWithScriptExecutionOperationId {
public:
    TCommandSqlOperationCancel();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlOperationForget : public TCommandWithScriptExecutionOperationId {
public:
    TCommandSqlOperationForget();
    virtual int Run(TConfig& config) override;
};

class TCommandSqlOperationList : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandSqlOperationList();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    ui64 PageSize = 0;
    TString PageToken;
};

}
}
