#include "url_builder.h"
#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/yexception.h>

namespace NYql {

TUrlBuilder::TUrlBuilder(const TString& uri)
    : MainUri(uri)
{
}

TUrlBuilder& TUrlBuilder::AddUrlParam(const TString& name, const TString& value) {
    Params.emplace_back(TParam {name, value});
    return *this;
}

TUrlBuilder& TUrlBuilder::AddPathComponent(const TString& value) {
    if (!value) {
        throw yexception() << "Empty path component is not allowed";
    }
    TStringBuilder res;
    res << MainUri;
    if (!MainUri.EndsWith('/')) {
        res << '/';
    }
    res << UrlEscapeRet(value, true);

    MainUri = std::move(res);
    return *this;
}

TString TUrlBuilder::Build() const {
    if (Params.empty()) {
        return MainUri;
    }

    TStringBuilder res;
    res << MainUri << "?";
    TStringBuf separator = ""sv;
    for (const auto& p : Params) {
        res << separator << p.Name;
        if (p.Value) {
            res << "=" << CGIEscapeRet(p.Value);
        }
        separator = "&"sv;
    }
    return std::move(res);
}

} // NYql
