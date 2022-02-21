#include "url_builder.h"
#include <library/cpp/string_utils/quote/quote.h>

namespace NYql {

TUrlBuilder::TUrlBuilder(
    const TString& uri)
    : MainUri(uri)
    {}

void TUrlBuilder::AddUrlParam(const TString& name, const TString& value) {
    Params.emplace_back(TParam {name, value});
}

TString TUrlBuilder::Build() const {
    TString res = MainUri;
    for (size_t i = 0; i < Params.size(); ++i) {
        res += TStringBuilder() << (i > 0 ? "&" : "") << Params[i].Name  << "=" << CGIEscapeRet(Params[i].Value);
    }
    return res;
}

} // NYql
