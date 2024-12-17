#pragma once

#include "commands.h"


namespace NYdb::NTpch {

class TCommandPrepare : public TTpchCommandBase {
public:
    TCommandPrepare();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString SrcDataPath;
    ui32 PartitionSize = 0;
    ui32 BatchSize = 1000;
};

} // namespace NYdb::NTpch
