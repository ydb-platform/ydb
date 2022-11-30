#include "query.h"
#include "request.h"

namespace NHttp {
    TFetchQuery::TFetchQuery(const TString& url,
                             const TFetchOptions& options)
        : Url_(url)
        , Options_(options)
    {
    }

    TFetchQuery::TFetchQuery(const TString& url,
                             const TVector<TString>& headers,
                             const TFetchOptions& options)
        : Url_(url)
        , Headers_(headers)
        , Options_(options)
    {
    }

    TFetchQuery::~TFetchQuery() = default;

    TString TFetchQuery::GetUrl() const {
        return Url_;
    }

    TFetchQuery& TFetchQuery::OnFail(TOnFail cb) {
        OnFailCb_ = cb;
        return *this;
    }

    TFetchQuery& TFetchQuery::OnRedirect(TOnRedirect cb) {
        OnRedirectCb_ = cb;
        return *this;
    }

    TFetchQuery& TFetchQuery::OnPartialRead(NHttpFetcher::TNeedDataCallback cb) {
        OnPartialReadCb_ = cb;
        return *this;
    }

    TFetchRequestRef TFetchQuery::ConstructRequest() const {
        TFetchRequestRef request = new TFetchRequest(Url_, Headers_, Options_);
        if (OnFailCb_) {
            request->SetOnFail(*OnFailCb_);
        }

        if (OnRedirectCb_) {
            request->SetOnRedirect(*OnRedirectCb_);
        }

        if (OnPartialReadCb_) {
            request->SetOnPartialRead(*OnPartialReadCb_);
        }

        return request;
    }

    TFetchState::TFetchState() {
    }

    TFetchState::TFetchState(const TFetchRequestRef& req)
        : Request_(req)
    {
    }

    void TFetchState::Cancel() const {
        if (Request_) {
            Request_->Cancel();
        }
    }

    NHttpFetcher::TResultRef TFetchState::Get() const {
        if (Request_) {
            WaitI();
            return Request_->MakeResult();
        }
        return NHttpFetcher::TResultRef();
    }

    void TFetchState::WaitI() const {
        WaitT(TDuration::Max());
    }

    bool TFetchState::WaitT(TDuration timeout) const {
        if (Request_) {
            return Request_->WaitT(timeout);
        }
        return false;
    }

}
