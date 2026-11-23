#include "core_validators.h"
#include "validator.h"
#include "validator_bootstrap.h"
#include "validator_composite_conveyor.h"
#include "validator_nameservice.h"

namespace NKikimr::NConsole {

void RegisterCoreValidators()
{
    RegisterValidator(new TBootstrapConfigValidator);
    RegisterValidator(new TCompositeConveyorConfigValidator);
    RegisterValidator(new TNameserviceConfigValidator);
}

} // namespace NKikimr::NConsole
