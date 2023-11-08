#include "cookies.h"

#include <library/cpp/string_utils/scan/scan.h>
#include <util/string/strip.h>
#include <util/string/builder.h>

namespace {
    struct TCookiesScanner {
        THttpCookies* C;

        inline void operator()(const TStringBuf& key, const TStringBuf& val) {
            C->Add(StripString(key), StripString(val));
        }
    };
}

void THttpCookies::Scan(const TStringBuf& s) {
    Clear();
    TCookiesScanner scan = {this};
    ScanKeyValue<true, ';', '='>(s, scan);
}

/*** https://datatracker.ietf.org/doc/html/rfc6265#section-5.4 ***/
TString THttpCookies::ToString() const {
    TStringBuilder result;
    for (const auto& [key, value] : *this) {
        if (!result.empty()) {
            result << "; ";
        }
        result << key << "=" << value;
    }
    return result;
}
