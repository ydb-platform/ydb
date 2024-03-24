#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandSql : public TYdbOperationCommand, public TCommandWithParameters,
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
    bool PrintResponse(NQuery::TExecuteQueryIterator& result);

    TString CollectStatsMode;
    TString Query;
    TString QueryFile;
    TString Syntax;
    bool ExplainMode = false;
};

}
}
