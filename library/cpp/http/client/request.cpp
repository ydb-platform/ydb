#include "request.h"

#include <library/cpp/http/client/fetch/codes.h>
#include <library/cpp/uri/location.h>

#include <util/string/ascii.h>

namespace NHttp {
    static const ui64 URI_PARSE_FLAGS =
        (NUri::TFeature::FeaturesRecommended | NUri::TFeature::FeatureConvertHostIDN | NUri::TFeature::FeatureEncodeExtendedDelim | NUri::TFeature::FeatureEncodePercent) & ~NUri::TFeature::FeatureHashBangToEscapedFragment;

    /// Generates sequence of unique identifiers of requests.
    static TAtomic RequestCounter = 0;

    TFetchRequest::TRedirects::TRedirects(bool parseCookie) {
        if (parseCookie) {
            CookieStore.Reset(new NHttp::TCookieStore);
        }
    }

    size_t TFetchRequest::TRedirects::Level() const {
        return this->size();
    }

    void TFetchRequest::TRedirects::ParseCookies(const TString& url,
                                                 const THttpHeaders& headers) {
        if (CookieStore) {
            NUri::TUri uri;
            if (uri.Parse(url, URI_PARSE_FLAGS) != NUri::TUri::ParsedOK) {
                return;
            }
            if (!uri.IsValidGlobal()) {
                return;
            }

            for (THttpHeaders::TConstIterator it = headers.Begin(); it != headers.End(); it++) {
                if (AsciiEqualsIgnoreCase(it->Name(), TStringBuf("Set-Cookie"))) {
                    CookieStore->SetCookie(uri, it->Value());
                }
            }
        }
    }

    TFetchRequest::TFetchRequest(const TString& url, const TFetchOptions& options)
        : Url_(url)
        , Options_(options)
        , Id_(AtomicIncrement(RequestCounter))
        , RetryAttempts_(options.RetryCount)
        , RetryDelay_(options.RetryDelay)
        , Cancel_(false)
        , CurrentUrl_(url)
    {
    }

    TFetchRequest::TFetchRequest(const TString& url, TVector<TString> headers, const TFetchOptions& options)
        : TFetchRequest(url, options)
    {
        Headers_ = std::move(headers);
    }

    void TFetchRequest::Cancel() {
        AtomicSet(Cancel_, 1);
    }

    NHttpFetcher::TRequestRef TFetchRequest::GetRequestImpl() const {
        NHttpFetcher::TRequestRef req(new NHttpFetcher::TRequest(CurrentUrl_));

        req->UnixSocketPath = Options_.UnixSocketPath;
        req->Login = Options_.Login;
        req->Password = Options_.Password;
        req->OAuthToken = Options_.OAuthToken;
        req->OnlyHeaders = Options_.OnlyHeaders;
        req->CustomHost = Options_.CustomHost;
        req->Method = Options_.Method;
        req->OAuthToken = Options_.OAuthToken;
        req->ContentType = Options_.ContentType;
        req->PostData = Options_.PostData;
        req->UserAgent = Options_.UserAgent;
        req->ExtraHeaders.assign(Headers_.begin(), Headers_.end());
        req->NeedDataCallback = OnPartialRead_;
        req->Deadline = TInstant::Now() + Options_.Timeout;

        if (Options_.ConnectTimeout) {
            req->ConnectTimeout = Options_.ConnectTimeout;
        }

        if (Options_.MaxHeaderSize) {
            req->MaxHeaderSize = Options_.MaxHeaderSize.GetRef();
        }
        if (Options_.MaxBodySize) {
            req->MaxBodySize = Options_.MaxBodySize.GetRef();
        }

        if (Redirects_ && Redirects_->CookieStore) {
            NUri::TUri uri;
            if (uri.Parse(CurrentUrl_, URI_PARSE_FLAGS) == NUri::TUri::ParsedOK) {
                if (TString cookies = Redirects_->CookieStore->GetCookieString(uri)) {
                    req->ExtraHeaders.push_back("Cookie: " + cookies + "\r\n");
                }
            }
        }

        return req;
    }

    bool TFetchRequest::IsValid() const {
        auto g(Guard(Lock_));
        return IsValidNoLock();
    }

    bool TFetchRequest::IsCancelled() const {
        return AtomicGet(Cancel_);
    }

    bool TFetchRequest::GetForceReconnect() const {
        return Options_.ForceReconnect;
    }

    NHttpFetcher::TResultRef TFetchRequest::MakeResult() const {
        if (Exception_) {
            std::rethrow_exception(Exception_);
        }

        return Result_;
    }

    void TFetchRequest::SetException(std::exception_ptr ptr) {
        Exception_ = ptr;
    }

    void TFetchRequest::SetCallback(NHttpFetcher::TCallBack cb) {
        Cb_ = cb;
    }

    void TFetchRequest::SetOnFail(TOnFail cb) {
        OnFail_ = cb;
    }

    void TFetchRequest::SetOnRedirect(TOnRedirect cb) {
        OnRedirect_ = cb;
    }

    void TFetchRequest::SetOnPartialRead(NHttpFetcher::TNeedDataCallback cb) {
        OnPartialRead_ = cb;
    }

    bool TFetchRequest::WaitT(TDuration timeout) {
        TCondVar c;

        {
            auto g(Guard(Lock_));

            if (IsValidNoLock()) {
                THolder<TWaitState> state(new TWaitState(&c));
                Awaitings_.PushBack(state.Get());
                if (!c.WaitT(Lock_, timeout)) {
                    AtomicSet(Cancel_, 1);
                    // Удаляем элемент из очереди ожидания в случае, если
                    // истёк установленный период времени.
                    state->Unlink();
                    return false;
                }
            }
        }

        return true;
    }

    bool TFetchRequest::IsValidNoLock() const {
        return !Result_ && !Exception_ && !AtomicGet(Cancel_);
    }

    void TFetchRequest::Reply(NHttpFetcher::TResultRef result) {
        NHttpFetcher::TCallBack cb;

        {
            auto g(Guard(Lock_));

            cb.swap(Cb_);
            Result_.Swap(result);

            if (Redirects_) {
                Result_->Redirects.assign(Redirects_->begin(), Redirects_->end());
            }
        }

        if (cb) {
            try {
                cb(Result_);
            } catch (...) {
                SetException(std::current_exception());
            }
        }

        {
            auto g(Guard(Lock_));
            while (!Awaitings_.Empty()) {
                Awaitings_.PopFront()->Signal();
            }
        }
    }

    TDuration TFetchRequest::OnResponse(NHttpFetcher::TResultRef result) {
        if (AtomicGet(Cancel_)) {
            goto finish;
        }

        if (NHttpFetcher::IsRedirectCode(result->Code)) {
            auto location = NUri::ResolveRedirectLocation(CurrentUrl_, result->Location);

            if (!Redirects_) {
                Redirects_.Reset(new TRedirects(true));
            }
            result->ResolvedUrl = location;
            Redirects_->push_back(result);

            if (Options_.UseCookie) {
                Redirects_->ParseCookies(CurrentUrl_, result->Headers);
            }

            if (Redirects_->Level() < Options_.RedirectDepth) {
                if (OnRedirect_) {
                    if (!OnRedirect_(CurrentUrl_, location)) {
                        goto finish;
                    }
                }

                CurrentUrl_ = location;
                return TDuration::Zero();
            }
        } else if (!NHttpFetcher::IsSuccessCode(result->Code)) {
            bool again = RetryAttempts_ > 0;

            if (OnFail_ && !OnFail_(result)) {
                again = false;
            }
            if (again) {
                RetryAttempts_--;
                return RetryDelay_;
            }
        }

    finish:
        Reply(result);

        return TDuration::Zero();
    }

}
