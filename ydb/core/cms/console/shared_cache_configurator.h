#pragma once 
#include "defs.h" 
 
namespace NKikimr { 
namespace NConsole { 
 
/** 
 * Shared Cache Configurator tracks and applies changes to shared cache configuration. 
 */ 
IActor *CreateSharedCacheConfigurator(); 
 
} // namespace NConsole 
} // namespace NKikimr 
