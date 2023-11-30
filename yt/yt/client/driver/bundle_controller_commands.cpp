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

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("bundle_name").Value(result.BundleName)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
