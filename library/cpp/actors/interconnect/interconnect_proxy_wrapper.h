#pragma once 
 
#include "interconnect_common.h" 
 
#include <library/cpp/actors/core/actorsystem.h> 
 
namespace NActors { 
 
    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId, 
        class TInterconnectMock *mock = nullptr); 
 
} 
