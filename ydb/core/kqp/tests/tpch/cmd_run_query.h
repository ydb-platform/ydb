#pragma once

#include "commands.h"


namespace NYdb::NTpch {

class TCommandRunQuery : public TTpchCommandBase {
public:
    TCommandRunQuery();

    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Profile;
};

} // namespace NYdb::NTpch
