#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetBundleConfigCommand
    : public TTypedCommand<NApi::TGetBundleConfigOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetBundleConfigCommand);

    static void Register(TRegistrar registrar);

private:
    TString BundleName_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
