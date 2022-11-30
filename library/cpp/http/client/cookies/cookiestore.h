#pragma once

#include "cookie.h"

#include <library/cpp/uri/uri.h>

#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NHttp {
    /**
     * Cookie storage for values obtained from a server via Set-Cookie header.
     *
     * Later client may use GetCookieString to build a cookie for sending
     * back to the server via Cookie header.
     */
    class TCookieStore {
    public:
        TCookieStore();
        ~TCookieStore();

        /// Removes all cookies from store.
        void Clear();

        /// Builds Cookie header from the given url.
        TString GetCookieString(const NUri::TUri& requestUri) const;

        /// Parses cookie from the Set-Cookie header and stores it.
        bool SetCookie(const NUri::TUri& requestUri, const TString& cookieHeader);

    private:
        bool DomainMatch(const TStringBuf& requestDomain, const TStringBuf& cookieDomain) const;
        bool PathMatch(const TStringBuf& requestPath, const TStringBuf& cookiePath) const;
        static TInstant GetExpireTime(const TCookie& cookie);

    private:
        struct TStoredCookie {
            TCookie Cookie;
            TInstant CreateTime;
            TInstant ExpireTime;
            bool IsHostOnly = true;

            /// Compares only Domain, Path and name.
            bool IsEquivalent(const TStoredCookie& rhs) const;
        };

        using TCookieVector = TVector<TStoredCookie>;


        TMutex Lock_;
        TCookieVector Cookies_;
    };

}
