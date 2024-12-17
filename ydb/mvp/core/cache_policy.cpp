#include "cache_policy.h"

NHttp::TCachePolicy GetCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    // YDB
    if (url == "/viewer/json/tenantinfo") {
        policy.TimeToExpire = TDuration::Minutes(30);
        policy.TimeToRefresh = TDuration::Seconds(30);
        policy.KeepOnError = true;
        return policy;
    }
    if (url.EndsWith("/viewer/json/cluster") || url.EndsWith("/viewer/json/sysinfo")) {
        policy.TimeToExpire = TDuration::Hours(24);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
        return policy;
    }
    if (url.find("/databases") != TStringBuf::npos) {
        policy.TimeToExpire = TDuration::Hours(24);
        policy.TimeToRefresh = TDuration::Seconds(30);
        policy.KeepOnError = true;
        return policy;
    }
    return policy;
}

