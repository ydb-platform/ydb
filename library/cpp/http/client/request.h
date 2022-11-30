#pragma once

#include "query.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <library/cpp/http/client/fetch/fetch_request.h>
#include <library/cpp/http/client/fetch/fetch_result.h>

#include <library/cpp/http/client/cookies/cookiestore.h>

#include <util/generic/intrlist.h>
#include <util/system/condvar.h>
#include <util/system/spinlock.h>

namespace NHttp {
    class TFetchRequest: public TAtomicRefCount<TFetchRequest> {
    public:
        TFetchRequest(const TString& url, const TFetchOptions& options);
        TFetchRequest(const TString& url, TVector<TString> headers, const TFetchOptions& options);

        /// Returns reference to request object.
        static TFetchRequestRef FromQuery(const TFetchQuery& query) {
            return query.ConstructRequest();
        }

        /// Cancel the request.
        void Cancel();

        /// Is the current request is still valid?
        bool IsValid() const;

        /// Is the current request cancelled?
        bool IsCancelled() const;

        /// Makes request in the format of underlining fetch subsystem.
        NHttpFetcher::TRequestRef GetRequestImpl() const;

        /// Whether new connection should been established.
        bool GetForceReconnect() const;

        /// Unique identifier of the request.
        ui64 GetId() const {
            return Id_;
        }

        /// Returns url of original request.
        TString GetUrl() const {
            return Url_;
        }

        /// Makes final result.
        NHttpFetcher::TResultRef MakeResult() const;

        void SetException(std::exception_ptr ptr);

        void SetCallback(NHttpFetcher::TCallBack cb);

        void SetOnFail(TOnFail cb);

        void SetOnRedirect(TOnRedirect cb);

        void SetOnPartialRead(NHttpFetcher::TNeedDataCallback cb);

        /// Waits for completion of the request no more than given timeout.
        ///
        /// \return true  if fetching is finished.
        /// \return false if timed out.
        bool WaitT(TDuration timeout);

        /// Response has been gotten.
        TDuration OnResponse(NHttpFetcher::TResultRef result);

    private:
        bool IsValidNoLock() const;

        void Reply(NHttpFetcher::TResultRef result);

    private:
        /// State of redirects processing.
        struct TRedirects: public std::vector<NHttpFetcher::TResultRef> {
            THolder<NHttp::TCookieStore> CookieStore;

            explicit TRedirects(bool parseCookie);

            size_t Level() const;

            void ParseCookies(const TString& url, const THttpHeaders& headers);
        };

        struct TWaitState : TIntrusiveListItem<TWaitState> {
            inline TWaitState(TCondVar* c)
                : C_(c)
            {
            }

            inline void Signal() {
                C_->Signal();
            }

            TCondVar* const C_;
        };

        const TString Url_;
        const TFetchOptions Options_;
        TVector<TString> Headers_;

        TAtomic Id_;
        ui32 RetryAttempts_;
        TDuration RetryDelay_;

        NHttpFetcher::TCallBack
            Cb_;
        TOnFail OnFail_;
        TOnRedirect OnRedirect_;
        NHttpFetcher::TNeedDataCallback OnPartialRead_;

        std::exception_ptr Exception_;
        NHttpFetcher::TResultRef
            Result_;

        TMutex Lock_;
        TIntrusiveList<TWaitState>
            Awaitings_;
        TAtomic Cancel_;

        /// During following through redirects the url is changing.
        /// So, this is actual url for the current step.
        TString CurrentUrl_;
        THolder<TRedirects> Redirects_;
    };

}
