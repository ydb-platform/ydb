#include "core_validators.h"
#include "validator.h"
#include "validator_bootstrap.h"
#include "validator_nameservice.h"

namespace NKikimr {
namespace NConsole {

void RegisterCoreValidators()
{
    RegisterValidator(new TBootstrapConfigValidator);
    RegisterValidator(new TNameserviceConfigValidator);
}

} // namespace NConsole
} // namespace NKikimr
