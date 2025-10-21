#include "helpers.h"

#include "context.h"
#include "requests.h"

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateHostNameWithPort(const TString& hostName, const TClientContext& context)
{
    static constexpr int HttpProxyPort = 80;
    static constexpr int HttpsProxyPort = 443;

    static constexpr int TvmOnlyHttpProxyPort = 9026;
    static constexpr int TvmOnlyHttpsProxyPort = 9443;

    if (hostName.find(':') == TString::npos) {
        int port;
        if (context.TvmOnly) {
            port = context.UseTLS
                ? TvmOnlyHttpsProxyPort
                : TvmOnlyHttpProxyPort;
        } else {
            port = context.UseTLS
                ? HttpsProxyPort
                : HttpProxyPort;
        }
        return Format("%v:%v", hostName, port);
    }
    return hostName;
}

TString GetFullUrl(const TString& hostName, const TClientContext& context, THttpHeader& header)
{
    Y_UNUSED(context);
    return Format("http://%v%v", hostName, header.GetUrl());
}

void UpdateHeaderForProxyIfNeed(const TString& hostName, const TClientContext& context, THttpHeader& header)
{
    if (context.ProxyAddress) {
        header.SetHostPort(Format("http://%v", hostName));
        header.SetProxyAddress(*context.ProxyAddress);
    }
}

TString GetFullUrlForProxy(const TString& hostName, const TClientContext& context, THttpHeader& header)
{
    if (context.ProxyAddress) {
        THttpHeader emptyHeader(header.GetMethod(), "", false);
        return GetFullUrl(*context.ProxyAddress, context, emptyHeader);
    }
    return GetFullUrl(hostName, context, header);
}

static TString GetParametersDebugString(const THttpHeader& header)
{
    const auto& parameters = header.GetParameters();
    if (parameters.Empty()) {
        return "<empty>";
    } else {
        return NodeToYsonString(parameters);
    }
}

TString TruncateForLogs(const TString& text, size_t maxSize)
{
    Y_ABORT_UNLESS(maxSize > 10);
    if (text.empty()) {
        static TString empty = "empty";
        return empty;
    } else if (text.size() > maxSize) {
        TStringStream out;
        out << text.substr(0, maxSize) + "... ("  << text.size() << " bytes total)";
        return out.Str();
    } else {
        return text;
    }
}

TString GetLoggedAttributes(const THttpHeader& header, const TString& url, bool includeParameters, size_t sizeLimit)
{
    const auto parametersDebugString = GetParametersDebugString(header);
    TStringStream out;
    out << "Method: " << url << "; "
        << "X-YT-Parameters (sent in " << (includeParameters ? "header" : "body") << "): " << TruncateForLogs(parametersDebugString, sizeLimit);
    return out.Str();
}

void LogRequest(const THttpHeader& header, const TString& url, bool includeParameters, const TString& requestId, const TString& hostName)
{
    YT_LOG_DEBUG("REQ %v - sending request (HostName: %v; %v)",
        requestId,
        hostName,
        GetLoggedAttributes(header, url, includeParameters, Max<size_t>()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
