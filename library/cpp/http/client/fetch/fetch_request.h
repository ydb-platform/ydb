#pragma once

#include "fetch_result.h"

#include <kernel/langregion/langregion.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NHttpFetcher {
    const TDuration DEFAULT_REQUEST_TIMEOUT = TDuration::Minutes(1);
    const TDuration DEFAULT_REQUEST_FRESHNESS = TDuration::Seconds(10000);

    class TRequest;
    using TRequestRef = TIntrusivePtr<TRequest>;

    class TRequest: public TAtomicRefCount<TRequest> {
    private:
        TRequest(const TRequest&) = default;
        TRequest& operator=(const TRequest&) = default;
        void GenerateSequence();

    public:
        TRequest(const TString& url = "", TCallBack onFetch = TCallBack());
        TRequest(const TString& url, bool ignoreRobotsTxt, TDuration timeout,
                 TDuration freshness, TCallBack onFetch = TCallBack());
        TRequest(const TString& url, TDuration timeout, TDuration freshness, bool ignoreRobots,
                 size_t priority, const TMaybe<TString>& login = TMaybe<TString>(),
                 const TMaybe<TString>& password = TMaybe<TString>(),
                 ELangRegion langRegion = ELR_RU, TCallBack onFetch = TCallBack());
        void Dump(IOutputStream& out);
        TRequestRef Clone();

    public:
        TString Url;
        TMaybe<TString> UnixSocketPath;

        TInstant Deadline; // [default = 1 min]
        TDuration RdWrTimeout;
        TMaybe<TDuration> ConnectTimeout;
        TDuration Freshness; // [default = 1000 sec]
        size_t Priority;     // lower is more important; range [0, 100], default 40

        TMaybe<TString> Login;
        TMaybe<TString> Password;
        TMaybe<TString> OAuthToken;
        bool IgnoreRobotsTxt;               // [default = false]
        ELangRegion LangRegion;             // [default = ELR_RU]
        TCallBack OnFetch;                  // for async requests
        ui64 Sequence;                      // unique id
        TMaybe<TString> CustomHost;         // Use custom host for "Host" header
        TMaybe<TString> UserAgent;          // custom user agen, [default = YandexNews]
        TMaybe<TString> AcceptEncoding;     // custom accept encoding, [default = "gzip, deflate"]
        bool OnlyHeaders;                   // [default = false], if true - no content will be fetched (HEAD request)
        size_t MaxHeaderSize;               // returns 1002 error if exceeded
        size_t MaxBodySize;                 // returns 1002 error if exceeded
        TNeedDataCallback NeedDataCallback; // set this callback if you need to check data while fetching
                                            // true - coninue fetching, false - stop
        TMaybe<TString> Method;             // for http exotics like "PUT ", "PATCH ", "DELETE ". if doesn't exist, GET or POST will br used
        TMaybe<TString> PostData;           // if exists - send post request
        TMaybe<TString> ContentType;        // custom content-type for post requests
                                            // [default = "application/x-www-form-urlencoded"]
        TVector<TString> ExtraHeaders;      // needed for some servers (to auth, for ex.); don't forget to add "\r\n"!
    };
}
