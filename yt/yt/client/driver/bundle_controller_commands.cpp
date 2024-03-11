#include "bundle_controller_commands.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TGetBundleConfigCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName_);
}

void TGetBundleConfigCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetBundleConfig(
        BundleName_,
        Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(result));
}

void TSetBundleConfigCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName_);

    registrar.Parameter("bundle_config", &TThis::BundleConfig_)
        .DefaultNew();
}

void TSetBundleConfigCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SetBundleConfig(
        BundleName_,
        BundleConfig_,
        Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
