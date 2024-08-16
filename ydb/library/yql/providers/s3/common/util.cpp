#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>


namespace NYql::NS3Util {

namespace {

inline char d2x(unsigned x) {
    return (char)((x < 10) ? ('0' + x) : ('A' + x - 10));
}

char* UrlEscape(char* to, const char* from) {
    while (*from) {
        if (*from == '%' || *from == '#' || *from == '?' || (unsigned char)*from <= ' ' || (unsigned char)*from > '~') {
            *to++ = '%';
            *to++ = d2x((unsigned char)*from >> 4);
            *to++ = d2x((unsigned char)*from & 0xF);
        } else {
            *to++ = *from;
        }
        ++from;
    }

    *to = 0;

    return to;
}

}

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues) {
    if (!issues) {
        return TIssues{};
    }
    TIssue result(prefix);
    for (auto& issue: issues) {
        result.AddSubIssue(MakeIntrusive<TIssue>(issue));
    }
    return TIssues{result};
}

TString UrlEscapeRet(const TStringBuf from) {
    TString to;
    to.ReserveAndResize(CgiEscapeBufLen(from.size()));
    to.resize(UrlEscape(to.begin(), from.begin()) - to.data());
    return to;
}

}
