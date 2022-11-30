#include "fetch_request.h"

#include <library/cpp/deprecated/atomic/atomic.h>

// TRequest
namespace NHttpFetcher {
    const TString DEFAULT_ACCEPT_ENCODING = "gzip, deflate";
    const size_t DEFAULT_MAX_HEADER_SIZE = 100 << 10;
    const size_t DEFAULT_MAX_BODY_SIZE = 1 << 29;

    TRequest::TRequest(const TString& url, TCallBack onFetch)
        : Url(url)
        , Deadline(TInstant::Now() + DEFAULT_REQUEST_TIMEOUT)
        , Freshness(DEFAULT_REQUEST_FRESHNESS)
        , Priority(40)
        , IgnoreRobotsTxt(false)
        , LangRegion(ELR_RU)
        , OnFetch(onFetch)
        , AcceptEncoding(DEFAULT_ACCEPT_ENCODING)
        , OnlyHeaders(false)
        , MaxHeaderSize(DEFAULT_MAX_HEADER_SIZE)
        , MaxBodySize(DEFAULT_MAX_BODY_SIZE)
    {
        GenerateSequence();
    }

    TRequest::TRequest(const TString& url, bool ignoreRobotsTxt, TDuration timeout, TDuration freshness, TCallBack onFetch)
        : Url(url)
        , Deadline(Now() + timeout)
        , Freshness(freshness)
        , Priority(40)
        , IgnoreRobotsTxt(ignoreRobotsTxt)
        , LangRegion(ELR_RU)
        , OnFetch(onFetch)
        , AcceptEncoding(DEFAULT_ACCEPT_ENCODING)
        , OnlyHeaders(false)
        , MaxHeaderSize(DEFAULT_MAX_HEADER_SIZE)
        , MaxBodySize(DEFAULT_MAX_BODY_SIZE)
    {
        GenerateSequence();
    }

    TRequest::TRequest(const TString& url, TDuration timeout, TDuration freshness, bool ignoreRobots,
                       size_t priority, const TMaybe<TString>& login, const TMaybe<TString>& password,
                       ELangRegion langRegion, TCallBack onFetch)
        : Url(url)
        , Deadline(Now() + timeout)
        , Freshness(freshness)
        , Priority(priority)
        , Login(login)
        , Password(password)
        , IgnoreRobotsTxt(ignoreRobots)
        , LangRegion(langRegion)
        , OnFetch(onFetch)
        , AcceptEncoding(DEFAULT_ACCEPT_ENCODING)
        , OnlyHeaders(false)
        , MaxHeaderSize(DEFAULT_MAX_HEADER_SIZE)
        , MaxBodySize(DEFAULT_MAX_BODY_SIZE)
    {
        GenerateSequence();
    }

    void TRequest::GenerateSequence() {
        static TAtomic nextSeq = 0;
        Sequence = AtomicIncrement(nextSeq);
    }

    TRequestRef TRequest::Clone() {
        THolder<TRequest> request = THolder<TRequest>(new TRequest(*this));
        request->GenerateSequence();
        return request.Release();
    }

    void TRequest::Dump(IOutputStream& out) {
        out << "url:            " << Url << "\n";
        out << "timeout:        " << (Deadline - Now()).MilliSeconds() << " ms\n";
        out << "freshness:      " << Freshness.Seconds() << "\n";
        out << "priority:       " << Priority << "\n";
        if (!!Login) {
            out << "login:          " << *Login << "\n";
        }
        if (!!Password) {
            out << "password:       " << *Password << "\n";
        }
        if (!!OAuthToken) {
            out << "oauth token:    " << *OAuthToken << "\n";
        }
        if (IgnoreRobotsTxt) {
            out << "ignore robots:  " << IgnoreRobotsTxt << "\n";
        }
        out << "lang reg:       " << LangRegion2Str(LangRegion) << "\n";
        if (!!CustomHost) {
            out << "custom host:    " << *CustomHost << "\n";
        }
        if (!!UserAgent) {
            out << "user agent:     " << *UserAgent << "\n";
        }
        if (!!AcceptEncoding) {
            out << "accept enc:     " << *AcceptEncoding << "\n";
        }
        if (OnlyHeaders) {
            out << "only headers:   " << OnlyHeaders << "\n";
        }
        out << "max header sz:  " << MaxHeaderSize << "\n";
        out << "max body sz:    " << MaxBodySize << "\n";
        if (!!PostData) {
            out << "post data:      " << *PostData << "\n";
        }
        if (!!ContentType) {
            out << "content type:   " << *ContentType << "\n";
        }
    }

}
