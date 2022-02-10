#pragma once 
 
#include <library/cpp/actors/core/actor.h> 
 
namespace NYql::NDq { 
    NActors::IActor* CreateHttpSenderActor(NActors::TActorId SenderId, NActors::TActorId HttpProxyId); 
} // NYql::NDq 
