#pragma once

#include "commands.h"


namespace NYdb::NTpch {

class TCommandPrepareScheme : public TTpchCommandBase {
public:
    TCommandPrepareScheme();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TMap<TString, ui32> TablePartitions;
};

} // namespace NYdb::NTpch
