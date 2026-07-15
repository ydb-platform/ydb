#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandConfig : public TClientCommandTree {
public:
    TCommandConfig(TClientCommandTree* rootTree);
    virtual void Config(TConfig& config) override;
private:
    TClientCommandTree* RootTree;
};

class TCommandCompletion : public TClientCommand {
public:
    TCommandCompletion(TClientCommandTree* rootTree);
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
private:
    TClientCommandTree* RootTree;
    TString Shell;
};

}
}
