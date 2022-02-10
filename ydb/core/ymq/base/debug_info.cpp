#include "debug_info.h" 
 
namespace NKikimr::NSQS { 
 
TDebugInfoHolder DebugInfo = {}; 
 
TDebugInfo::TDebugInfo() { 
    UnparsedHttpRequests.reserve(1000); 
} 
 
void TDebugInfo::MoveToParsedHttpRequests(const TString& requestId, class THttpRequest* request) { 
    ParsedHttpRequests.emplace(requestId, request); 
    UnparsedHttpRequests.erase(request); 
} 
 
void TDebugInfo::EraseHttpRequest(const TString& requestId, class THttpRequest* request) { 
    if (!ParsedHttpRequests.EraseKeyValue(requestId, request)) { 
        UnparsedHttpRequests.erase(request); 
    } 
} 
 
} // namespace NKikimr::NSQS 
