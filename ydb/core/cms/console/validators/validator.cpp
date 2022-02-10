#include "validator.h" 
#include "registry.h" 
 
namespace NKikimr { 
namespace NConsole { 
 
void RegisterValidator(IConfigValidator::TPtr validator) 
{ 
    auto res = TValidatorsRegistry::Instance()->AddValidator(validator); 
    Y_VERIFY(res, "cannot register validator '%s' (locked=%" PRIu32 ")", 
             validator->GetName().data(), (ui32)TValidatorsRegistry::Instance()->IsLocked());
} 
 
} // namespace NConsole 
} // namespace NKikimr 
