#pragma once 
 
#include "defs.h" 
 
namespace NKikimr { 
namespace NCms { 
 
IActor* CreateInfoCollector(const TActorId& client, const TDuration& timeout);
 
} // NCms 
} // NKikimr 
