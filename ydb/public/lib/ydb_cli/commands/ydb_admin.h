#pragma once

#include "ydb_command.h"

namespace NYdb {
namespace NConsoleClient {

class TCommandAdmin : public TClientCommandTree {
public:
    TCommandAdmin();
protected:
    virtual void Config(TConfig& config) override;
};

}
}
