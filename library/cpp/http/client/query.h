#pragma once

#include <library/cpp/http/client/fetch/fetch_result.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <functional>

namespace NHttp {
    /**
     * Various options for fetching a document.
     */
    struct TFetchOptions {
#define DECLARE_FIELD(name, type, default)               \
    type name{default};                                  \
    inline TFetchOptions& Set##name(const type& value) { \
        name = value;                                    \
        return *this;                                    \
    }

        /// Set a timeout for connection establishment
        DECLARE_FIELD(ConnectTimeout, TMaybe<TDuration>, Nothing());

        /// Total request timeout for each attempt
        DECLARE_FIELD(Timeout, TDuration, TDuration::Minutes(1));

        /// Count of additional attempts before return error code.
        DECLARE_FIELD(RetryCount, ui32, 0);

        /// Sleep delay before next retry
        DECLARE_FIELD(RetryDelay, TDuration, TDuration::Seconds(5));

        /// Parse cookie from server's response and attach its to further
        /// requests in case of redirects.
        DECLARE_FIELD(UseCookie, bool, true);

        /// Finite numbers of following redirects to prevent hang on infinitiy
        /// redirect sequence.
        DECLARE_FIELD(RedirectDepth, ui32, 15);

        /// If true - no content will be fetched (HEAD request).
        DECLARE_FIELD(OnlyHeaders, bool, false);

        /// Use custom host for "Host" header.
        DECLARE_FIELD(CustomHost, TMaybe<TString>, Nothing());

        /// Login for basic HTTP authorization.
        DECLARE_FIELD(Login, TMaybe<TString>, Nothing());

        /// Password for basic HTTP authorization.
        DECLARE_FIELD(Password, TMaybe<TString>, Nothing());

        /// OAuth token.
        DECLARE_FIELD(OAuthToken, TMaybe<TString>, Nothing());

        /// For http exotics like "PUT ", "PATCH ", "DELETE ".  If doesn't exist,
        /// GET or POST will be used.
        DECLARE_FIELD(Method, TMaybe<TString>, Nothing());

        /// If exists - send POST request.
        DECLARE_FIELD(PostData, TMaybe<TString>, Nothing());

        /// Custom content-type for POST requests.
        DECLARE_FIELD(ContentType, TMaybe<TString>, Nothing());

        /// Custom value for "UserAgent" header.
        DECLARE_FIELD(UserAgent, TString, "Python-urllib/2.6");

        /// Always establish new connection to the target host.
        DECLARE_FIELD(ForceReconnect, bool, false);

        /// Set max header size if needed
        DECLARE_FIELD(MaxHeaderSize, TMaybe<size_t>, Nothing());

        /// Set max body size if needed
        DECLARE_FIELD(MaxBodySize, TMaybe<size_t>, Nothing());

        /// If set, unix domain socket will be used as a backend
        DECLARE_FIELD(UnixSocketPath, TMaybe<TString>, Nothing());

#undef DECLARE_FIELD
    };

    using TFetchRequestRef = TIntrusivePtr<class TFetchRequest>;

    /// If handler will return true then try again else stop fetching.
    using TOnFail = std::function<bool(const NHttpFetcher::TResultRef& reply)>;

    /// If handler will return true then follow redirect else stop fetching.
    using TOnRedirect = std::function<bool(const TString& from, const TString& location)>;

    class TFetchQuery {
    public:
        TFetchQuery() = default;
        TFetchQuery(const TString& url,
                    const TFetchOptions& options = TFetchOptions());
        TFetchQuery(const TString& url,
                    const TVector<TString>& headers,
                    const TFetchOptions& options = TFetchOptions());
        ~TFetchQuery();

        /// Returns original request's url.
        TString GetUrl() const;

        /// Set handler for fail's handling.
        TFetchQuery& OnFail(TOnFail cb);

        /// Set handler for redirect's handling.
        TFetchQuery& OnRedirect(TOnRedirect cb);

        /// Set handler which will be called periodically while reading data from peer
        /// with ALL the data accumulated so far. One is able to cancel further data exchange
        /// by returning false from the callback.
        TFetchQuery& OnPartialRead(NHttpFetcher::TNeedDataCallback cb);

        TFetchRequestRef ConstructRequest() const;

    private:
        friend class TFetchRequest;

        TString Url_;
        TVector<TString> Headers_;
        TFetchOptions Options_;
        TMaybe<TOnFail> OnFailCb_;
        TMaybe<TOnRedirect> OnRedirectCb_;
        TMaybe<NHttpFetcher::TNeedDataCallback> OnPartialReadCb_;
    };

    class TFetchState {
    public:
        TFetchState();
        TFetchState(const TFetchRequestRef& req);
        TFetchState(const TFetchState&) = default;
        TFetchState(TFetchState&&) = default;

        ~TFetchState() = default;

        TFetchState& operator=(TFetchState&&) = default;
        TFetchState& operator=(const TFetchState&) = default;

        /// Cancel the request.
        void Cancel() const;

        /// Wait for completion of the request and return result.
        NHttpFetcher::TResultRef Get() const;

        /// Waits for completion of the request.
        void WaitI() const;

        /// Waits for completion of the request no more than given timeout.
        ///
        /// \return true  if fetching is finished.
        /// \return false if timed out.
        bool WaitT(TDuration timeout) const;

    private:
        TFetchRequestRef Request_;
    };

}
