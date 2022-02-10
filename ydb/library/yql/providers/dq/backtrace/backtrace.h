#pragma once 
 
#include <util/generic/hash.h> 
#include <util/generic/string.h> 
 
namespace NYql { 
 
namespace NBacktrace { 
 
void SetModulesMapping(const THashMap<TString, TString>& mapping); 
void PrintBacktraceToStderr(int signum); 
TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping); 
 
} /* namespace Backtrace */ 
 
} /* namespace NYql */ 
