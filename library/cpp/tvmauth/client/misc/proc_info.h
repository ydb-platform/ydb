#pragma once 
 
#include <util/generic/string.h> 
 
#include <optional> 
 
namespace NTvmAuth::NUtils { 
    struct TProcInfo { 
        TString Pid; 
        std::optional<TString> ProcessName; 
        TString VersionPrefix; 
 
        void AddToRequest(IOutputStream& out) const; 
 
        static TProcInfo Create(const TString& versionPrefix); 
        static std::optional<TString> GetProcessName(); 
    }; 
} 
