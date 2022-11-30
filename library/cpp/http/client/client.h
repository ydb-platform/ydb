#pragma once

#include "query.h"
#include "request.h"

namespace NHttp {
    struct TClientOptions {
#define DECLARE_FIELD(name, type, default)                \
    type name{default};                                   \
    inline TClientOptions& Set##name(const type& value) { \
        name = value;                                     \
        return *this;                                     \
    }

        /// The size of stack of fetching coroutine.
        DECLARE_FIELD(ExecutorStackSize, size_t, 1 << 20);

        /// The number of fetching coroutines.
        DECLARE_FIELD(FetchCoroutines, size_t, 3);

        DECLARE_FIELD(Name, TString, "GlobalFetcher");

        /// The lifetime of entries in the DNS cache (if zero then cache is not used).
        DECLARE_FIELD(DnsCacheLifetime, TDuration, TDuration::Zero());

        /// Established connections will be keept for further usage.
        DECLARE_FIELD(KeepAlive, bool, false);

        /// How long established connections should be keept.
        DECLARE_FIELD(KeepAliveTimeout, TDuration, TDuration::Minutes(5));

#undef DECLARE_FIELD
    };

    /**
     * Statefull fetching client.
     * Can handle multiply fetching request simultaneously.  Also it's may apply
     * politeness policy to control load of each host.
     */
    class TFetchClient {
    public:
        explicit TFetchClient(const TClientOptions& options = TClientOptions());
        ~TFetchClient();

        /// Execute give fetch request in asynchronous fashion.
        TFetchState Fetch(const TFetchQuery& query, NHttpFetcher::TCallBack cb);

    private:
        class TImpl;
        THolder<TImpl> Impl_;
    };

    /// Execute give fetch request in synchronous fashion.
    NHttpFetcher::TResultRef Fetch(const TFetchQuery& query);

    /// Execute give fetch request in asynchronous fashion.
    TFetchState FetchAsync(const TFetchQuery& query, NHttpFetcher::TCallBack cb);

}
