#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandScripting : public TClientCommandTree {
public:
    TCommandScripting();
};

class TCommandExecuteYqlScript : public TYdbOperationCommand,
    public TCommandWithResponseHeaders, TCommandWithParameters
{
public:
    TCommandExecuteYqlScript();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    void PrintResponse(NScripting::TExecuteYqlResult& result);
    void PrintExplainResult(NScripting::TExplainYqlResult& result);

private:
    TString CollectStatsMode;
    TMaybe<TString> FlameGraphPath;
    TString Script;
    TString ScriptFile;
    bool Explain = false;
};

}
}
