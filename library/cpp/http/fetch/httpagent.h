#pragma once

#include <cstdio>
#include <cstring>
#include <cstdlib>

#include <library/cpp/uri/http_url.h>
#include <util/datetime/base.h>
#include <util/network/hostip.h>
#include <util/network/ip.h>
#include <util/network/sock.h>
#include <util/generic/scope.h>
#include <util/generic/utility.h>
#include <util/string/cast.h>

#include "exthttpcodes.h"
#include "sockhandler.h"

class TIpResolver {
public:
    TAddrList Resolve(const char* host, TIpPort port) const {
        try {
            TAddrList result;
            TNetworkAddress na(host, port);
            for (auto i = na.Begin(); i != na.End(); ++i) {
                const struct addrinfo& ai = *i;
                switch (ai.ai_family) {
                    case AF_INET:
                        result.push_back(new NAddr::TIPv4Addr(*(sockaddr_in*)ai.ai_addr));
                        break;
                    case AF_INET6:
                        result.push_back(new NAddr::TIPv6Addr(*(sockaddr_in6*)ai.ai_addr));
                        break;
                }
            }
            return result;
        } catch (const TNetworkResolutionError&) {
        }
        return TAddrList();
    }
};

namespace NResolverHelpers {
    Y_HAS_MEMBER(Resolve);

    template <typename TResolver>
    std::enable_if_t<TClassHasResolve<TResolver>::value, TAddrList> Resolve(const TResolver& r, const char* host, TIpPort port) {
        return r.Resolve(host, port);
    }

    template <typename TResolver>
    std::enable_if_t<!TClassHasResolve<TResolver>::value, TAddrList> Resolve(const TResolver& r, const char* host, TIpPort port) {
        ui32 ip = 0;
        if (r.GetHostIP(host, &ip)) {
            // error
            return TAddrList();
        }
        if (!ip) {
            return TAddrList();
        }

        return TAddrList::MakeV4Addr(ip, port);
    }
}

template <typename TBase>
class TIpResolverWrapper {
private:
    TBase Base;

public:
    TIpResolverWrapper() = default;

    template <typename T>
    TIpResolverWrapper(T&& base)
        : Base(std::forward(base))
    {
    }

    TAddrList Resolve(const char* host, TIpPort port) const {
        return NResolverHelpers::Resolve(Base, host, port);
    }
};

template <class TSocketHandler = TSimpleSocketHandler, class TDnsClient = TIpResolver>
class THttpAgent {
public:
    THttpAgent()
        : Persistent(0)
        , Timeout(TDuration::MicroSeconds(150))
        , Hostheader(nullptr)
        , Footer(nullptr)
        , AltFooter(nullptr)
        , PostData(nullptr)
        , PostDataLen(0)
        , Method(nullptr)
        , MethodLen(0)
        , HostheaderLen(0)
    {
        SetIdentification("YandexSomething/1.0", "webadmin@yandex.ru");
    }

    ~THttpAgent() {
        Disconnect();
        free(Hostheader);
        free(Footer);
    }

    void SetIdentification(const char* user_agent, const char* http_from) {
        free(Footer);
        size_t len = user_agent ? strlen(user_agent) + 15 : 0;
        len += http_from ? strlen(http_from) + 9 : 0;
        len += 3;
        Footer = (char*)malloc(len);
        if (user_agent)
            strcat(strcat(strcpy(Footer, "User-Agent: "), user_agent), "\r\n");
        if (http_from)
            strcat(strcat(strcat(Footer, "From: "), http_from), "\r\n");
    }

    void SetUserAgentFooter(const char* altFooter) {
        AltFooter = altFooter;
    }

    void SetPostData(const char* postData, size_t postDataLen) {
        PostData = postData;
        PostDataLen = postDataLen;
    }

    void SetMethod(const char* method, size_t methodLen) {
        Method = method;
        MethodLen = methodLen;
    }

    // deprecated
    ui32 GetIp() const {
        return Addrs.GetV4Addr().first;
    }

    int GetScheme() const {
        return THttpURL::SchemeHTTP;
    }
    void SetTimeout(TDuration tim) {
        Timeout = tim;
    }

    void SetConnectTimeout(TDuration timeout) {
        ConnectTimeout = timeout;
    }

    int Disconnected() {
        return !Persistent || !Socket.Good();
    }

    int SetHost(const char* hostname, TIpPort port) {
        Disconnect();
        TAddrList addrs = DnsClient.Resolve(hostname, port);
        if (!addrs.size()) {
            return 1;
        }

        SetHost(hostname, port, addrs);
        return 0;
    }

    int SetHost(const char* hostname, TIpPort port, const TAddrList& addrs) {
        Disconnect();
        Addrs = addrs;
        size_t reqHostheaderLen = strlen(hostname) + 20;
        if (HostheaderLen < reqHostheaderLen) {
            free(Hostheader);
            Hostheader = (char*)malloc((HostheaderLen = reqHostheaderLen));
        }
        if (port == 80)
            snprintf(Hostheader, HostheaderLen, "Host: %s\r\n", hostname);
        else
            snprintf(Hostheader, HostheaderLen, "Host: %s:%u\r\n", hostname, port);
        pHostBeg = strchr(Hostheader, ' ') + 1;
        pHostEnd = strchr(pHostBeg, '\r');
        // convert hostname to lower case since some web server don't like
        // uppper case (Task ROBOT-562)
        for (char* p = pHostBeg; p < pHostEnd; p++)
            *p = tolower(*p);
        return 0;
    }

    // deprecated v4-only
    int SetHost(const char* hostname, TIpPort port, ui32 ip) {
        return SetHost(hostname, port, TAddrList::MakeV4Addr(ip, port));
    }

    void SetHostHeader(const char* host) {
        size_t reqHostheaderLen = strlen(host) + 20;
        if (HostheaderLen < reqHostheaderLen) {
            delete[] Hostheader;
            Hostheader = new char[(HostheaderLen = reqHostheaderLen)];
        }
        snprintf(Hostheader, HostheaderLen, "Host: %s\r\n", host);
    }

    void SetSocket(SOCKET fd) {
        Socket.SetSocket(fd);
    }

    SOCKET PickOutSocket() {
        return Socket.PickOutSocket();
    }

    void Disconnect() {
        Socket.Disconnect();
    }

    ssize_t read(void* buffer, size_t buflen) {
        return Socket.read(buffer, buflen);
    }

    int RequestGet(const char* url, const char* const* headers, int persistent = 1, bool head_request = false) {
        if (!Addrs.size())
            return HTTP_DNS_FAILURE;
        char message[MessageMax];
        ssize_t messlen = 0;
        if (Method) {
            strncpy(message, Method, MethodLen);
            message[MethodLen] = ' ';
            messlen = MethodLen + 1;
        } else if (PostData) {
            strcpy(message, "POST ");
            messlen = 5;
        } else if (head_request) {
            strcpy(message, "HEAD ");
            messlen = 5;
        } else {
            strcpy(message, "GET ");
            messlen = 4;
        }
#define _AppendMessage(mes) messlen += Min(MessageMax - messlen, \
                                           (ssize_t)strlcpy(message + messlen, (mes), MessageMax - messlen))
        _AppendMessage(url);
        _AppendMessage(" HTTP/1.1\r\n");
        if (*url == '/') //if not then Host is a proxy
            _AppendMessage(Hostheader);
        _AppendMessage("Connection: ");
        _AppendMessage(persistent ? "Keep-Alive\r\n" : "Close\r\n");
        while (headers && *headers)
            _AppendMessage(*headers++);
        if (AltFooter)
            _AppendMessage(AltFooter);
        else
            _AppendMessage(Footer);
        _AppendMessage("\r\n");
#undef _AppendMessage
        if (messlen >= MessageMax)
            return HTTP_HEADER_TOO_LARGE;

        if (!Persistent)
            Disconnect();
        Persistent = persistent;
        int connected = Socket.Good();
        for (int attempt = !connected; attempt < 2; attempt++) {
            const auto connectTimeout = ConnectTimeout ? ConnectTimeout : Timeout;
            if (!Socket.Good() && Socket.Connect(Addrs, connectTimeout))
                return HTTP_CONNECT_FAILED;

            int sendOk = Socket.send(message, messlen);
            if (sendOk && PostData && PostDataLen)
                sendOk = Socket.send(PostData, PostDataLen);
            if (!sendOk) {
                int err = errno;
                Disconnect();
                errno = err;
                continue;
            }

            if (!Socket.peek()) {
                int err = errno;
                Disconnect();
                if (err == EINTR) {
                    errno = err;
                    return HTTP_INTERRUPTED;
                }
            } else {
                if (!persistent)
                    Socket.shutdown();
                return 0;
            }
        }
        return connected ? HTTP_CONNECTION_LOST : HTTP_CONNECT_FAILED;
    }

protected:
    TSocketHandler Socket;
    TIpResolverWrapper<TDnsClient> DnsClient;
    TAddrList Addrs;
    int Persistent;
    TDuration Timeout;
    TDuration ConnectTimeout;
    char *Hostheader, *Footer, *pHostBeg, *pHostEnd;
    const char* AltFooter; // alternative footer can be set by the caller
    const char* PostData;
    size_t PostDataLen;
    const char* Method;
    size_t MethodLen;
    unsigned short HostheaderLen;
    static const ssize_t MessageMax = 65536;
};

struct TNoTimer {
    inline void OnBeforeSend() {
    }
    inline void OnAfterSend() {
    }
    inline void OnBeforeRecv() {
    }
    inline void OnAfterRecv() {
    }
};
