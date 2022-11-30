#pragma once

#include "cosocket.h"
#include "fetch_request.h"
#include "fetch_result.h"
#include "pool.h"

#include <library/cpp/coroutine/dns/helpers.h>
#include <library/cpp/http/client/ssl/sslsock.h>
#include <library/cpp/http/fetch_gpl/httpagent.h>
#include <library/cpp/http/fetch/httpfetcher.h>
#include <library/cpp/http/fetch/httpheader.h>

#include <util/generic/algorithm.h>

namespace NHttpFetcher {
    class TCoIpResolver {
    public:
        TAddrList Resolve(const char* host, TIpPort port) const {
            NAsyncDns::TAddrs addrs;
            try {
                NAsyncDns::ResolveAddr(*CoCtx()->Resolver, host, port, addrs, CoCtx()->DnsCache);
            } catch (...) {
                return TAddrList();
            }

            // prefer IPv6
            SortBy(addrs.begin(), addrs.end(), [](const auto& addr) {
                return addr->Addr()->sa_family == AF_INET6 ? 0 : 1;
            });

            return TAddrList(addrs.begin(), addrs.end());
        }
    };

    struct TStringSaver {
        int Write(const void* buf, size_t len) {
            Data.Write(buf, len);
            return 0;
        }
        TStringStream Data;
    };

    struct TSimpleCheck {
        inline bool Check(THttpHeader*) {
            return false;
        }
        void CheckDocPart(void* data, size_t size, THttpHeader*) {
            if (!!NeedDataCallback) {
                CheckData += TString(static_cast<const char*>(data), size);
                if (!NeedDataCallback(CheckData)) {
                    BodyMax = 0;
                }
            }
        }
        void CheckEndDoc(THttpHeader*) {
        }
        size_t GetMaxHeaderSize() {
            return HeaderMax;
        }
        size_t GetMaxBodySize(THttpHeader*) {
            return BodyMax;
        }
        void SetMaxHeaderSize(size_t headerMax) {
            HeaderMax = headerMax;
        }
        void SetMaxBodySize(size_t bodyMax) {
            BodyMax = bodyMax;
        }
        void SetNeedDataCallback(const TNeedDataCallback& callback) {
            NeedDataCallback = callback;
        }

    private:
        size_t HeaderMax;
        size_t BodyMax;
        TNeedDataCallback NeedDataCallback;
        TString CheckData;
    };

    using TSimpleHttpAgent = THttpsAgent<TCoSocketHandler, TCoIpResolver,
                                         TSslSocketBase::TFakeLogger, TNoTimer,
                                         NHttpFetcher::TSslSocketHandler>;
    using TSimpleFetcherFetcher = THttpFetcher<TFakeAlloc<>, TSimpleCheck, TStringSaver, TSimpleHttpAgent>;

    //! Private method of fetcher library. Don't use it in your code.
    TResultRef FetchSingleImpl(TRequestRef request, TSocketPool* pool = nullptr);
}
