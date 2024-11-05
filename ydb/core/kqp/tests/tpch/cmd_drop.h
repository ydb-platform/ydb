#pragma once

#include "commands.h"


namespace NYdb::NTpch {

class TCommandDrop : public TTpchCommandBase {
public:
    TCommandDrop();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

} // namespace NYdb::NTpch
