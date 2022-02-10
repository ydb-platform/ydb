#pragma once 
 
#include <util/string/builder.h> 
 
namespace NYdb { 
namespace NYq { 
 
class TScope { 
public: 
    static TString YandexCloudScopeSchema; 
 
    TScope() 
    { } 
 
    TScope(const TString& scope) 
        : Scope(scope) 
    { } 
 
    TScope(TString&& scope) 
        : Scope(std::move(scope)) 
    { } 
 
    const TString& ToString() const { 
        return Scope; 
    } 
 
    bool Empty() const { 
        return Scope.empty(); 
    } 
 
private: 
    TString Scope; 
}; 
 
} // namespace NYq 
} // namespace Ndb 
