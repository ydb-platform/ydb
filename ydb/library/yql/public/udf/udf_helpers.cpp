#include "udf_helpers.h" 
 
#include <library/cpp/resource/resource.h>
 
#include <util/generic/hash.h> 
#include <util/generic/singleton.h> 
 
namespace NYql {
namespace NUdf { 
 
struct TLoadedResources { 
    TString Get(TStringBuf resourceId) { 
        if (auto p = Resources.FindPtr(resourceId)) { 
            return *p; 
        } 
        return Resources.emplace(resourceId, NResource::Find(resourceId)).first->second; 
    } 
 
    THashMap<TString, TString> Resources; 
}; 
 
TString LoadResourceOnce(TStringBuf resourceId) { 
    return Singleton<TLoadedResources>()->Get(resourceId); 
} 
 
} 
} 
