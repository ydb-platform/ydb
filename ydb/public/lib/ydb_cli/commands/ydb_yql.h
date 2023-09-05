#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptible.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandYql : public TYdbOperationCommand, public TCommandWithParameters,
    public TInterruptibleCommand
{
public:
    TCommandYql();
    TCommandYql(TString script, TString collectStatsMode);
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    int RunCommand(TConfig& config, const TString &script);
    bool PrintResponse(NScripting::TYqlResultPartIterator& result);

    TString CollectStatsMode;
    TMaybe<TString> FlameGraphPath;
    TString Script;
    TString ScriptFile;
};

}
}
