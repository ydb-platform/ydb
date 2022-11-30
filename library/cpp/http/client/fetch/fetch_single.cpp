#include "codes.h"
#include "fetch_single.h"
#include "parse.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <library/cpp/string_utils/url/url.h>

namespace NHttpFetcher {
    static sockaddr_in6 IPv6Loopback() {
        sockaddr_in6 sock = {};
        sock.sin6_family = AF_INET6;
        sock.sin6_addr = IN6ADDR_LOOPBACK_INIT;
        return sock;
    }

    class THeaders {
    public:
        inline void Build(const TRequestRef& request) {
           if (!!request->Password || !!request->Login) {
                TString pass;
                TString login;
                TString raw;
                TString encoded;

                if (!!request->Password) {
                    pass = *request->Password;
                }
                if (!!request->Login) {
                    login = *request->Login;
                }
                raw = TString::Join(login, ":", pass);
                Base64Encode(raw, encoded);
                BasicAuth_ = TString::Join("Authorization: Basic ", encoded, "\r\n");
                Headers_.push_back(BasicAuth_.c_str());
            }

            if (request->ExtraHeaders) {
                Headers_.reserve(Headers_.size() + request->ExtraHeaders.size());
                for (const TString& header : request->ExtraHeaders) {
                    if (AsciiHasSuffixIgnoreCase(header, "\r\n")) {
                        Headers_.push_back(header.c_str());
                    } else {
                        Fixed_.push_back(header + "\r\n");
                        Headers_.push_back(Fixed_.back().c_str());
                    }
                }
            }

            if (!!request->OAuthToken) {
                OAuth_ = TString::Join("Authorization: OAuth ", *request->OAuthToken, "\r\n");
                Headers_.push_back(OAuth_.c_str());
            }

            if (!!request->AcceptEncoding) {
                AcceptEncoding_ = TString::Join("Accept-Encoding: ", *request->AcceptEncoding, "\r\n");
                Headers_.push_back(AcceptEncoding_.c_str());
            }

            ContentType_ = "application/x-www-form-urlencoded";
            if (!!request->PostData) {
                if (!!request->ContentType) {
                    ContentType_ = *request->ContentType;
                }
                ContentLength_ = TString::Join("Content-Length: ", ToString(request->PostData->size()), "\r\n");
                ContentType_ = TString::Join("Content-Type: ", ContentType_, "\r\n");
                Headers_.push_back(ContentLength_.c_str());
                Headers_.push_back(ContentType_.c_str());
            }

            Headers_.push_back((const char*)nullptr);
        }

        inline const char* const* Data() {
            return Headers_.data();
        }

    private:
       TVector<const char*> Headers_;
       TVector<TString> Fixed_;
       TString BasicAuth_;
       TString OAuth_;
       TString AcceptEncoding_;
       TString ContentLength_;
       TString ContentType_;
    };

    TResultRef FetchSingleImpl(TRequestRef request, TSocketPool* pool) {
        Y_ASSERT(!!request->Url && "no url passed in fetch request");
        TResultRef result(new TResult(request->Url));
        try {
            TSimpleFetcherFetcher fetcher;
            THttpHeader header;

            THttpURL::EKind kind = THttpURL::SchemeHTTP;
            ui16 port = 80;
            TString host;
            ParseUrl(request->Url, kind, host, port);

            TString path = ToString(GetPathAndQuery(request->Url));

            bool defaultPort = (kind == THttpURL::SchemeHTTP && port == 80) ||
                               (kind == THttpURL::SchemeHTTPS && port == 443);

            if (request->UnixSocketPath && !request->UnixSocketPath->empty()) {
                TAddrList addrs;
                addrs.emplace_back(new NAddr::TUnixSocketAddr{*request->UnixSocketPath});
                fetcher.SetHost(host.data(), port, addrs, kind);
            } else if (host == "127.0.0.1") {
                // todo: correctly handle /etc/hosts records && ip-addresses

                // bypass normal DNS resolving for localhost
                TAddrList addrs({new NAddr::TIPv4Addr(TIpAddress(0x0100007F, port))});
                fetcher.SetHost(host.data(), port, addrs, kind);
            } else if (host == "localhost") {
                sockaddr_in6 ipv6Addr = IPv6Loopback();
                ipv6Addr.sin6_port = HostToInet(port);

                TAddrList addrs({new NAddr::TIPv6Addr(ipv6Addr), new NAddr::TIPv4Addr(TIpAddress(0x0100007F, port))});
                fetcher.SetHost(host.data(), port, addrs, kind);
            } else {
                Y_ASSERT(!!host && "no host detected in url passed");
                fetcher.SetHost(host.data(), port, kind);
            }
            header.Init();

            THeaders headers;
            headers.Build(request);

            TString hostHeader = (!!request->CustomHost ? *request->CustomHost : host) +
                                 (defaultPort ? "" : ":" + ToString(port));
            fetcher.SetHostHeader(hostHeader.data());

            if (!!request->UserAgent) {
                fetcher.SetIdentification((*request->UserAgent).data(), nullptr);
            } else {
                fetcher.SetIdentification("Mozilla/5.0 (compatible; YandexNews/3.0; +http://yandex.com/bots)", nullptr);
            }

            if (!!request->Method) {
                fetcher.SetMethod(request->Method->data(), request->Method->size());
            }

            if (!!request->PostData) {
                fetcher.SetPostData(request->PostData->data(), request->PostData->size());
            }

            fetcher.SetMaxBodySize(request->MaxBodySize);
            fetcher.SetMaxHeaderSize(request->MaxHeaderSize);

            if (request->ConnectTimeout) {
                fetcher.SetConnectTimeout(*request->ConnectTimeout);
            }

            {
                const TDuration rest = request->Deadline - Now();
                fetcher.SetTimeout(request->RdWrTimeout != TDuration::Zero()
                                       ? ::Min(request->RdWrTimeout, rest)
                                       : rest);
            }
            fetcher.SetNeedDataCallback(request->NeedDataCallback);
            bool persistent = !request->OnlyHeaders;
            void* handle = nullptr;

            if (pool) {
                while (auto socket = pool->GetSocket(host, port)) {
                    if (socket->Good()) {
                        handle = socket.Get();

                        fetcher.SetSocket(socket.Release());
                        fetcher.SetPersistent(true);
                        break;
                    }
                }
            }

            int fetchResult = fetcher.Fetch(&header, path.data(), headers.Data(), persistent, request->OnlyHeaders);

            if (!fetcher.Data.empty()) {
                TStringInput httpIn(fetcher.Data.Str());
                ParseHttpResponse(*result, httpIn, kind, host, port);
            }

            if (fetchResult < 0 || header.error != 0) {
                result->Code = header.error;
            }

            if (pool && persistent && !header.error && !header.connection_closed) {
                THolder<TSocketPool::TSocketHandle> socket(fetcher.PickOutSocket());

                if (!!socket && socket->Good()) {
                    if (handle == socket.Get()) {
                        result->ConnectionReused = true;
                    }
                    pool->ReturnSocket(host, port, std::move(socket));
                }
            }
        } catch (...) {
            result->Code = UNKNOWN_ERROR;
        }
        return result;
    }
}
