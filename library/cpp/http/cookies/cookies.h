#pragma once

#include "lctable.h"

class THttpCookies: public TLowerCaseTable<TStringBuf> {
public:
    inline THttpCookies(const TStringBuf& cookieString) {
        Scan(cookieString);
    }

    inline THttpCookies() noexcept {
    }

    void Scan(const TStringBuf& cookieString);

    TString ToString() const;
};
