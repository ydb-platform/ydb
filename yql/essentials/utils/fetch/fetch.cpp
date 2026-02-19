#include "fetch.h"

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/openssl/io/stream.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/charset/ci_string.h>

#include <util/network/socket.h>
#include <util/string/cast.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

namespace NYql {

namespace {

THttpURL ParseURL(const TStringBuf addr, NUri::TParseFlags features) {
    THttpURL url;
    THttpURL::TParsedState parsedState = url.Parse(addr, features, nullptr, 65536);
    if (THttpURL::ParsedOK != parsedState) {
        ythrow yexception() << "Bad URL: \"" << addr << "\", " << HttpURLParsedStateToString(parsedState);
    }
    return url;
}

const THashSet<TStringBuf> ValidMethods = {
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "HEAD",
    "OPTIONS",
};

class TFetchResultImpl: public IFetchResult {
public:
    TFetchResultImpl(const THttpURL& url, TStringBuf method, TStringBuf body, const THttpHeaders& additionalHeaders, TDuration timeout) {
        Y_ENSURE(ValidMethods.contains(method), "Unsupported method: " << method);
        TString host = url.Get(THttpURL::FieldHost);
        TString path = url.PrintS(THttpURL::FlagPath | THttpURL::FlagQuery);
        const char* p = url.Get(THttpURL::FieldPort);
        ui16 port = 80;
        bool https = false;

        if (url.Get(THttpURL::FieldScheme) == TStringBuf("https")) {
            port = 443;
            https = true;
        }

        if (p) {
            port = FromString<ui16>(p);
        }

        TString req;
        {
            TStringOutput rqs(req);
            TStringBuf userAgent = "User-Agent: Mozilla/5.0 (compatible; YQL/1.0)";

            auto request = std::to_array<IOutputStream::TPart>({
                IOutputStream::TPart(method),
                IOutputStream::TPart(" ", 1),
                IOutputStream::TPart(path.data(), path.size()),
                IOutputStream::TPart(" HTTP/1.1", 9),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart("Host: ", 6),
                IOutputStream::TPart(host.data(), host.size()),
                IOutputStream::TPart::CrLf(),
                IOutputStream::TPart(userAgent.data(), userAgent.size()),
                IOutputStream::TPart::CrLf(),
            });

            rqs.Write(request.data(), request.size());
            if (!additionalHeaders.Empty()) {
                additionalHeaders.OutTo(&rqs);
            }
            if (body) {
                THttpHeaders bodyHeaders;
                bodyHeaders.AddHeader("Content-Length", ToString(body.size()));
                bodyHeaders.OutTo(&rqs);
                rqs << "\r\n"
                    << body;
            } else {
                rqs << "\r\n";
            }
        }

        Socket_.Reset(new TSocket(TNetworkAddress(host, port), timeout));
        SocketInput_.Reset(new TSocketInput(*Socket_));
        SocketOutput_.Reset(new TSocketOutput(*Socket_));

        Socket_->SetSocketTimeout(timeout.Seconds(), timeout.MilliSeconds() % 1000);

        if (https) {
            Ssl_.Reset(new TOpenSslClientIO(SocketInput_.Get(), SocketOutput_.Get()));
        }

        {
            THttpOutput ho(Ssl_ ? (IOutputStream*)Ssl_.Get() : (IOutputStream*)SocketOutput_.Get());
            (ho << req).Finish();
        }
        HttpInput_.Reset(new THttpInput(Ssl_ ? (IInputStream*)Ssl_.Get() : (IInputStream*)SocketInput_.Get()));
    }

    THttpInput& GetStream() override {
        return *HttpInput_;
    }

    unsigned GetRetCode() override {
        return ParseHttpRetCode(HttpInput_->FirstLine());
    }

    THttpURL GetRedirectURL(const THttpURL& baseUrl) override {
        for (auto i = HttpInput_->Headers().Begin(); i != HttpInput_->Headers().End(); ++i) {
            if (0 == TCiString::compare(i->Name(), TStringBuf("location"))) {
                THttpURL target = ParseURL(i->Value(), THttpURL::FeaturesAll | NUri::TFeature::FeatureConvertHostIDN);
                if (!target.IsValidAbs()) {
                    target.Merge(baseUrl);
                }
                return target;
            }
        }
        ythrow yexception() << "Unknown redirect location from " << baseUrl.PrintS();
    }

    static TFetchResultPtr Fetch(const THttpURL& url, TStringBuf method, TStringBuf body, const THttpHeaders& additionalHeaders, const TDuration& timeout) {
        return new TFetchResultImpl(url, method, body, additionalHeaders, timeout);
    }

private:
    THolder<TSocket> Socket_;
    THolder<TSocketInput> SocketInput_;
    THolder<TSocketOutput> SocketOutput_;
    THolder<TOpenSslClientIO> Ssl_;
    THolder<THttpInput> HttpInput_;
};

inline bool IsRedirectCode(unsigned code) {
    switch (code) {
        case HTTP_MOVED_PERMANENTLY:
        case HTTP_FOUND:
        case HTTP_SEE_OTHER:
        case HTTP_TEMPORARY_REDIRECT:
            return true;
        default:
            return false;
    }
}

} // namespace

ERetryErrorClass DefaultClassifyHttpCode(unsigned code) {
    switch (code) {
        case HTTP_REQUEST_TIME_OUT:       // 408
        case HTTP_AUTHENTICATION_TIMEOUT: // 419
            return ERetryErrorClass::ShortRetry;
        case HTTP_TOO_MANY_REQUESTS:   // 429
        case HTTP_SERVICE_UNAVAILABLE: // 503
            return ERetryErrorClass::LongRetry;
        default:
            return IsServerError(code)
                       ? ERetryErrorClass::ShortRetry // 5xx
                       : ERetryErrorClass::NoRetry;
    }
}

IRetryPolicy<unsigned>::TPtr GetDefaultPolicy() {
    static const auto Policy = IRetryPolicy<unsigned>::GetExponentialBackoffPolicy(
        /*retryClassFunction=*/DefaultClassifyHttpCode,
        /*minDelay=*/TDuration::Seconds(1),
        /*minLongRetryDelay:*/ TDuration::Seconds(5),
        /*maxDelay=*/TDuration::Minutes(1),
        /*maxRetries=*/3,
        /*maxTime=*/TDuration::Minutes(3),
        /*scaleFactor=*/2);
    return Policy;
}

THttpURL ParseURL(const TStringBuf addr) {
    return ParseURL(addr, THttpURL::FeaturesAll | NUri::TFeature::FeatureConvertHostIDN | NUri::TFeature::FeatureNoRelPath);
}

TFetchResultPtr Fetch(const THttpURL& url, const THttpHeaders& additionalHeaders, const TDuration& timeout, size_t redirects, const IRetryPolicy<unsigned>::TPtr& policy) {
    return FetchEx(url, "GET"_sb, TStringBuf{}, additionalHeaders, timeout, redirects, policy);
}

TFetchResultPtr FetchEx(const THttpURL& url, TStringBuf method, TStringBuf body, const THttpHeaders& additionalHeaders, const TDuration& timeout, size_t redirects, const IRetryPolicy<unsigned>::TPtr& policy) {
    const auto& actualPolicy = policy ? policy : GetDefaultPolicy();
    THttpURL currentUrl = url;
    for (size_t fetchNum = 0; fetchNum < redirects; ++fetchNum) {
        IRetryPolicy<unsigned>::IRetryState::TPtr state = actualPolicy->CreateRetryState();
        unsigned responseCode = 0;
        TFetchResultPtr fr;
        while (true) {
            std::exception_ptr eptr;
            try {
                fr = TFetchResultImpl::Fetch(currentUrl, method, body, additionalHeaders, timeout);
                responseCode = fr->GetRetCode();
            } catch (const TSystemError& ex) {
                if (ex.Status() != ETIMEDOUT) {
                    throw;
                }

                responseCode = HTTP_REQUEST_TIME_OUT;
                eptr = std::current_exception();
            }

            if (auto delay = state->GetNextRetryDelay(responseCode)) {
                YQL_LOG(DEBUG) << "Connection failed. Retry delay: " << *delay;
                Sleep(*delay);
            } else if (eptr != nullptr) {
                std::rethrow_exception(eptr);
            } else {
                break;
            }
        }

        if (responseCode >= 200 && responseCode < 300) {
            return fr;
        }

        if (responseCode == HTTP_NOT_MODIFIED) {
            return fr;
        }

        if (IsRedirectCode(responseCode)) {
            currentUrl = fr->GetRedirectURL(currentUrl);
            YQL_LOG(INFO) << "Got redirect to " << currentUrl.PrintS();
            continue;
        }

        TString errorBody;
        try {
            errorBody = fr->GetStream().ReadAll();
        } catch (...) {
        }

        ythrow yexception() << "Failed to fetch url '" << currentUrl.PrintS() << "' with code " << responseCode << ", body: " << errorBody;
    }
    ythrow yexception() << "Failed to fetch url '" << currentUrl.PrintS() << "': too many redirects";
}

THttpURL AppendUrlPath(const THttpURL& url, const TString& part) {
    THttpURL resultUrl;
    if (resultUrl.Parse(part, THttpURL::FeaturesDefault | THttpURL::FeatureSchemeKnown) != THttpURL::ParsedOK) {
        throw yexception() << "url.Parse finished with not ParsedOK. Tried to parse `" << part << "`";
    }
    auto path = TString(url.Get(NUri::TField::FieldPath));
    if (!path.EndsWith('/')) {
        auto copy = url;
        copy.Set(NUri::TField::FieldPath, path + "/");
        resultUrl.Merge(copy);
        return resultUrl;
    }
    resultUrl.Merge(url);
    return resultUrl;
}

} // namespace NYql
