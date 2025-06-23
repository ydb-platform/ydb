#include "url_builder.h"
#include <library/cpp/string_utils/quote/quote.h>
#include <util/generic/yexception.h>

namespace NYql {

TUrlBuilder::TUrlBuilder(const TString& uri)
    : MainUri_(uri)
{
}

TUrlBuilder& TUrlBuilder::AddUrlParam(const TString& name, const TString& value) {
    Params_.emplace_back(TParam {name, value});
    return *this;
}

TUrlBuilder& TUrlBuilder::AddPathComponent(const TString& value) {
    if (!value) {
        throw yexception() << "Empty path component is not allowed";
    }
    TStringBuilder res;
    res << MainUri_;
    if (!MainUri_.EndsWith('/')) {
        res << '/';
    }
    res << UrlEscapeRet(value, true);

    MainUri_ = std::move(res);
    return *this;
}

TString TUrlBuilder::Build() const {
    if (Params_.empty()) {
        return MainUri_;
    }

    TStringBuilder res;
    res << MainUri_ << "?";
    TStringBuf separator = ""sv;
    for (const auto& p : Params_) {
        res << separator << p.Name;
        if (p.Value) {
            res << "=" << CGIEscapeRet(p.Value);
        }
        separator = "&"sv;
    }
    return std::move(res);
}

} // NYql
