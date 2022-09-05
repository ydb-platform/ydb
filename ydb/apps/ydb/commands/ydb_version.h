#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandVersion : public TClientCommand {
public:
    TCommandVersion();
    virtual void Config(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    virtual void Parse(TConfig& config) override;

private:
    bool Semantic = false;
    bool DisableChecks = false;
    bool EnableChecks = false;
};

}
}
