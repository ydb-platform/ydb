#include "cookiestore.h"

#include <library/cpp/deprecated/split/split_iterator.h>

#include <util/generic/algorithm.h>
#include <util/string/ascii.h>

#include <time.h>

namespace NHttp {
    bool TCookieStore::TStoredCookie::IsEquivalent(const TStoredCookie& rhs) const {
        return (IsHostOnly == rhs.IsHostOnly) && (Cookie.Domain == rhs.Cookie.Domain) &&
                (Cookie.Path == rhs.Cookie.Path) && (Cookie.Name == rhs.Cookie.Name);
    }

    TCookieStore::TCookieStore() {
    }

    TCookieStore::~TCookieStore() {
    }

    bool TCookieStore::SetCookie(const NUri::TUri& requestUri, const TString& cookieHeader) {
        // https://tools.ietf.org/html/rfc6265#section-5.3
        TStoredCookie stored;
        stored.CreateTime = Now();
        try {
            stored.Cookie = TCookie::Parse(cookieHeader);
        } catch (const yexception&) {
            // Parse failed, ignore cookie
            return false;
        }
        if (stored.Cookie.Domain) {
            if (!DomainMatch(requestUri.GetHost(), stored.Cookie.Domain)) {
                // Cookie for other domain
                return false;
            }
            stored.IsHostOnly = false;
        } else {
            stored.Cookie.Domain = requestUri.GetHost();
            stored.IsHostOnly = true;
        }
        if (!stored.Cookie.Path) {
            stored.Cookie.Path = requestUri.GetField(NUri::TField::FieldPath);
        }
        stored.ExpireTime = GetExpireTime(stored.Cookie);

        auto g(Guard(Lock_));
        for (auto it = Cookies_.begin(); it != Cookies_.end(); ++it) {
            if (it->IsEquivalent(stored)) {
                *it = stored;
                return true;
            }
        }
        Cookies_.push_back(stored);
        return true;
    }

    TString TCookieStore::GetCookieString(const NUri::TUri& requestUri) const {
        // https://tools.ietf.org/html/rfc6265#section-5.4
        const TInstant now = Now();
        auto g(Guard(Lock_));

        TVector<TCookieVector::const_iterator> validCookies;
        validCookies.reserve(Cookies_.size());

        // Filter cookies
        for (auto it = Cookies_.begin(); it != Cookies_.end(); ++it) {
            if (it->IsHostOnly) {
                if (!AsciiEqualsIgnoreCase(it->Cookie.Domain, requestUri.GetHost())) {
                    continue;
                }
            } else {
                if (!DomainMatch(requestUri.GetHost(), it->Cookie.Domain)) {
                    continue;
                }
            }
            if (!PathMatch(requestUri.GetField(NUri::TField::FieldPath), it->Cookie.Path)) {
                continue;
            }
            if (it->Cookie.IsSecure && requestUri.GetScheme() != NUri::TScheme::SchemeHTTPS) {
                continue;
            }
            if (now >= it->ExpireTime) {
                continue;
            }
            validCookies.push_back(it);
        }
        // Sort cookies
        Sort(validCookies.begin(), validCookies.end(), [](const TCookieVector::const_iterator& a, const TCookieVector::const_iterator& b) {
            // Cookies with longer paths are listed before cookies with shorter paths.
            auto pa = a->Cookie.Path.length();
            auto pb = b->Cookie.Path.length();
            if (pa != pb) {
                return pa > pb;
            }
            // cookies with earlier creation-times are listed before cookies with later creation-times.
            if (a->CreateTime != b->CreateTime) {
                return a->CreateTime < b->CreateTime;
            }
            return &*a < &*b; //Any order
        });
        TStringStream os;
        for (auto it = validCookies.begin(); it != validCookies.end(); ++it) {
            if (!os.Empty()) {
                os << "; ";
            }
            const TStoredCookie& stored = **it;
            os << stored.Cookie.Name << "=" << stored.Cookie.Value;
        }
        return os.Str();
    }

    void TCookieStore::Clear() {
        auto g(Guard(Lock_));
        Cookies_.clear();
    }

    bool TCookieStore::DomainMatch(const TStringBuf& requestDomain, const TStringBuf& cookieDomain) const {
        // https://tools.ietf.org/html/rfc6265#section-5.1.3
        if (AsciiEqualsIgnoreCase(requestDomain, cookieDomain)) {
            return true;
        }
        if (requestDomain.length() > cookieDomain.length() &&
            AsciiHasSuffixIgnoreCase(requestDomain, cookieDomain) &&
            requestDomain[requestDomain.length() - cookieDomain.length() - 1] == '.') {
            return true;
        }
        return false;
    }

    bool TCookieStore::PathMatch(const TStringBuf& requestPath, const TStringBuf& cookiePath) const {
        // https://tools.ietf.org/html/rfc6265#section-5.1.4
        if (cookiePath == requestPath) {
            return true;
        }
        if (requestPath.StartsWith(cookiePath)) {
            if (!cookiePath.empty() && cookiePath.back() == '/') {
                return true;
            }
            if (requestPath.length() > cookiePath.length() && requestPath[cookiePath.length()] == '/') {
                return true;
            }
        }
        return false;
    }

    TInstant TCookieStore::GetExpireTime(const TCookie& cookie) {
        // Алгоритм скопирован из blink: net/cookies/canonical_cookie.cc:CanonExpiration
        // First, try the Max-Age attribute.
        if (cookie.MaxAge >= 0) {
            return TDuration::Seconds(cookie.MaxAge).ToDeadLine();
        }
        // Try the Expires attribute.
        if (cookie.Expires) {
            static const TStringBuf kMonths[] = {
                TStringBuf("jan"), TStringBuf("feb"), TStringBuf("mar"),
                TStringBuf("apr"), TStringBuf("may"), TStringBuf("jun"),
                TStringBuf("jul"), TStringBuf("aug"), TStringBuf("sep"),
                TStringBuf("oct"), TStringBuf("nov"), TStringBuf("dec")};
            static const int kMonthsLen = Y_ARRAY_SIZE(kMonths);
            // We want to be pretty liberal, and support most non-ascii and non-digit
            // characters as a delimiter.  We can't treat : as a delimiter, because it
            // is the delimiter for hh:mm:ss, and we want to keep this field together.
            // We make sure to include - and +, since they could prefix numbers.
            // If the cookie attribute came in in quotes (ex expires="XXX"), the quotes
            // will be preserved, and we will get them here.  So we make sure to include
            // quote characters, and also \ for anything that was internally escaped.
            static const TSplitDelimiters kDelimiters("\t !\"#$%&'()*+,-./;<=>?@[\\]^_`{|}~");

            struct tm exploded;
            Zero(exploded);

            TDelimitersSplit tokenizer(cookie.Expires.data(), cookie.Expires.size(), kDelimiters);
            TDelimitersSplit::TIterator tokenizerIt = tokenizer.Iterator();

            bool found_day_of_month = false;
            bool found_month = false;
            bool found_time = false;
            bool found_year = false;

            while (true) {
                const TStringBuf token = tokenizerIt.NextTok();
                if (!token.IsInited()) {
                    break;
                }
                if (token.empty()) {
                    continue;
                }

                bool numerical = IsAsciiDigit(token[0]);

                // String field
                if (!numerical) {
                    if (!found_month) {
                        for (int i = 0; i < kMonthsLen; ++i) {
                            // Match prefix, so we could match January, etc
                            if (AsciiHasPrefixIgnoreCase(token, kMonths[i])) {
                                exploded.tm_mon = i;
                                found_month = true;
                                break;
                            }
                        }
                    } else {
                        // If we've gotten here, it means we've already found and parsed our
                        // month, and we have another string, which we would expect to be the
                        // the time zone name.  According to the RFC and my experiments with
                        // how sites format their expirations, we don't have much of a reason
                        // to support timezones.  We don't want to ever barf on user input,
                        // but this DCHECK should pass for well-formed data.
                        // DCHECK(token == "GMT");
                    }
                    // Numeric field w/ a colon
                } else if (token.Contains(':')) {
                    if (!found_time &&
                        sscanf(
                            token.data(), "%2u:%2u:%2u", &exploded.tm_hour,
                            &exploded.tm_min, &exploded.tm_sec) == 3) {
                        found_time = true;
                    } else {
                        // We should only ever encounter one time-like thing.  If we're here,
                        // it means we've found a second, which shouldn't happen.  We keep
                        // the first.  This check should be ok for well-formed input:
                        // NOTREACHED();
                    }
                    // Numeric field
                } else {
                    // Overflow with atoi() is unspecified, so we enforce a max length.
                    if (!found_day_of_month && token.size() <= 2) {
                        exploded.tm_mday = atoi(token.data());
                        found_day_of_month = true;
                    } else if (!found_year && token.size() <= 5) {
                        exploded.tm_year = atoi(token.data());
                        found_year = true;
                    } else {
                        // If we're here, it means we've either found an extra numeric field,
                        // or a numeric field which was too long.  For well-formed input, the
                        // following check would be reasonable:
                        // NOTREACHED();
                    }
                }
            }

            if (!found_day_of_month || !found_month || !found_time || !found_year) {
                // We didn't find all of the fields we need.  For well-formed input, the
                // following check would be reasonable:
                // NOTREACHED() << "Cookie parse expiration failed: " << time_string;
                return TInstant::Max();
            }

            // Normalize the year to expand abbreviated years to the full year.
            if (exploded.tm_year >= 69 && exploded.tm_year <= 99) {
                exploded.tm_year += 1900;
            }
            if (exploded.tm_year >= 0 && exploded.tm_year <= 68) {
                exploded.tm_year += 2000;
            }

            // If our values are within their correct ranges, we got our time.
            if (exploded.tm_mday >= 1 && exploded.tm_mday <= 31 &&
                exploded.tm_mon >= 0 && exploded.tm_mon <= 11 &&
                exploded.tm_year >= 1601 && exploded.tm_year <= 30827 &&
                exploded.tm_hour <= 23 && exploded.tm_min <= 59 && exploded.tm_sec <= 59)
            {
                exploded.tm_year -= 1900; // Adopt to tm struct
                // Convert to TInstant
                time_t tt = TimeGM(&exploded);
                if (tt != -1) {
                    return TInstant::Seconds(tt);
                }
            }

            // One of our values was out of expected range.  For well-formed input,
            // the following check would be reasonable:
            // NOTREACHED() << "Cookie exploded expiration failed: " << time_string;
        }
        // Invalid or no expiration, persistent cookie.
        return TInstant::Max();
    }

}
