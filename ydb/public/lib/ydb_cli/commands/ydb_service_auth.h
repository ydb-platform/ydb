#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

namespace NYdb {
namespace NConsoleClient {

class TCommandAuth : public TClientCommandTree {
public:
    TCommandAuth();
};

class TCommandGetToken : public TYdbSimpleCommand {
public:
    TCommandGetToken();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    bool ForceMode = false;
};

}
}
