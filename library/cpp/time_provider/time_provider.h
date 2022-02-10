#pragma once 
 
#include <util/datetime/base.h> 
 
class ITimeProvider: public TThrRefBase { 
public: 
    virtual TInstant Now() = 0; 
}; 
 
TIntrusivePtr<ITimeProvider> CreateDefaultTimeProvider(); 
TIntrusivePtr<ITimeProvider> CreateDeterministicTimeProvider(ui64 seed); 
