#pragma once 
 
#include <util/generic/hash.h> 
#include <util/generic/string.h> 
#include <util/system/mutex.h> 

#include <string_view>
 
namespace NYql { 
 
class TFailureInjector {
public:
    static void Activate();
 
    static void Set(std::string_view name, ui64 skip, ui64 countOfFails);
    static void Reach(std::string_view name, std::function<void()> action);
 
private:
    void ActivateImpl();
 
    void SetImpl(std::string_view name, ui64 skip, ui64 countOfFails);
    void ReachImpl(std::string_view name, std::function<void()> action);
 
    struct TFailureSpec {
        ui64 Skip;
        ui64 CountOfFails;
    };
 
    std::atomic<bool> Enabled_ = false;
    THashMap<TString, TFailureSpec> FailureSpecs;
    TMutex Lock;
};
 
} // NYql
