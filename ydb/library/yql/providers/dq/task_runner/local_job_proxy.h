#pragma once

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NYql::NTaskRunnerProxy {

struct TLocalJobProxySettings {
    TString ProxyAddress;
    TString SocketPath;

    static constexpr TStringBuf PortoSocketPath = "/tmp/yt-job-proxy.sock";

    void ConfigureEnvironment(THashMap<TString, TString>& environment, bool porto) const {
        environment["YQL_YT_JOB_PROXY_ADDRESS"] = ProxyAddress;
        environment["YT_JOB_PROXY_SOCKET_PATH"] = porto ? TString(PortoSocketPath) : SocketPath;
    }

    TString GetPortoBind() const {
        TStringBuilder bind;
        for (char c : SocketPath) {
            if (c == '\\' || c == ' ' || c == ';') {
                bind << '\\';
            }
            bind << c;
        }
        bind << ' ' << PortoSocketPath << " ro;";
        return bind;
    }
};

inline TMaybe<TLocalJobProxySettings> ConfigureChildLocalJobProxy(
    THashMap<TString, TString>* environment,
    bool useLocalJobProxy,
    TStringBuf proxyAddress,
    TStringBuf socketPath)
{
    if (!useLocalJobProxy || proxyAddress.empty() || socketPath.empty()) {
        return Nothing();
    }

    TLocalJobProxySettings settings{TString(proxyAddress), TString(socketPath)};
    settings.ConfigureEnvironment(*environment, /*porto*/ false);
    return settings;
}

} // namespace NYql::NTaskRunnerProxy
