#pragma once

#include <library/cpp/charset/doccodes.h>
#include <library/cpp/http/io/headers.h>
#include <library/cpp/langs/langs.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NHttpFetcher {
    // Result
    using TResultRef = TIntrusivePtr<struct TResult>;
    struct TResult: public TAtomicRefCount<TResult> {
        TResult(const TString& url, int code = 0);
        TString DecodeData(bool* decoded = nullptr) const;
        bool Success() const;

    public:
        TString RequestUrl;
        TString ResolvedUrl;
        TString Location;
        int Code;
        bool ConnectionReused;
        TString StatusStr;
        TString MimeType;
        THttpHeaders Headers;
        TString Data;
        TMaybe<ECharset> Encoding;
        TMaybe<ELanguage> Language;
        TVector<TResultRef> Redirects;
        TString HttpVersion;
    };

    using TCallBack = std::function<void(TResultRef)>;
    using TNeedDataCallback = std::function<bool(const TString&)>;
}
