#include "url_builder.h"
#include <library/cpp/string_utils/quote/quote.h>

namespace NYql {

TUrlBuilder::TUrlBuilder(const TString& uri)
    : MainUri(uri)
{
}

TUrlBuilder& TUrlBuilder::AddUrlParam(const TString& name, const TString& value) {
    Params.emplace_back(TParam {name, value});
    return *this;
}

TString TUrlBuilder::Build() const {
    TStringBuilder res;
    res << MainUri << "?";
    TStringBuf separator = ""sv;
    for (const auto& p : Params) {
        res << separator << p.Name  << "=" << CGIEscapeRet(p.Value);
        separator = "&"sv;
    }
    return std::move(res);
}

} // NYql
