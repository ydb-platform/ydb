#include "yql_yt_log_context.h"
#include <util/string/split.h>
#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/yql_panic.h>

using namespace NYql::NFmr;

std::pair<TString, TString> GetLogContext(const THttpInput& input) {
    auto headers = input.Headers();
    auto sessionIdHeader = headers.FindHeader("x_yql_session_id");
    auto logContextsHeader = headers.FindHeader("x_yql_log_context");
    std::pair<TString, TString> logContext{};
    YQL_ENSURE(sessionIdHeader);
    logContext.first = sessionIdHeader->Value();
    if (logContextsHeader) {
        logContext.second = logContextsHeader->Value();
    }
    return logContext;
}


TKeepAliveHttpClient::THeaders GetHeadersWithLogContext(const TKeepAliveHttpClient::THeaders& headers, bool addLogContext) {
    TKeepAliveHttpClient::THeaders curHeaders = headers;
    auto logContext = NYql::NLog::CurrentLogContextPath();
    curHeaders["x_yql_session_id"] = logContext.first;
    if (addLogContext) {
        curHeaders["x_yql_log_context"] = logContext.second;
    }
    return curHeaders;
}
