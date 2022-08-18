#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandUpdate : public TClientCommand {
public:
    TCommandUpdate();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    bool ForceUpdate = false;
};

}
}
