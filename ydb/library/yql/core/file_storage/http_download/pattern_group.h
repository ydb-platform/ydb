#pragma once 
 
#include <library/cpp/regex/pcre/regexp.h>
#include <util/generic/map.h> 
#include <util/generic/vector.h> 
 
namespace NYql { 
 
class TPatternGroup { 
public: 
    explicit TPatternGroup(const TVector<TString>& patterns = {}); 
    void Add(const TString& pattern); 
    bool IsEmpty() const; 
    bool Match(const TString& s) const; 
 
private: 
    TMap<TString, TRegExMatch> CompiledPatterns; 
}; 
 
} 
