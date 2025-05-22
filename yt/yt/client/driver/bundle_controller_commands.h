#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetBundleConfigCommand
    : public TTypedCommand<NBundleControllerClient::TGetBundleConfigOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetBundleConfigCommand);

    static void Register(TRegistrar registrar);

private:
    TString BundleName_;

    void DoExecute(ICommandContextPtr context) override;
};

class TSetBundleConfigCommand
    : public TTypedCommand<NBundleControllerClient::TSetBundleConfigOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSetBundleConfigCommand);

    static void Register(TRegistrar registrar);

private:
    TString BundleName_;
    NBundleControllerClient::TBundleTargetConfigPtr BundleConfig_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
