#pragma once

#include <library/cpp/regex/pcre/regexp.h>
#include <util/generic/vector.h>

namespace NYql {

class TUrlMapper {
public:
    void AddMapping(const TString& pattern, const TString& targetUrl);
    bool MapUrl(const TString& url, TString& mappedUrl) const;

private:
    struct TCustomScheme {
        TCustomScheme(const TString& pattern, const TString& url)
            : Pattern(pattern)
            , TargetUrlHolder(url)
            , TargetUrlSubst(pattern.data()) {
            if (0 == TargetUrlSubst.ParseReplacement(TargetUrlHolder.data())) {
                ythrow yexception() << "Bad url replacement: " << TargetUrlHolder;
            }
        }
        TRegExMatch Pattern;
        TString TargetUrlHolder;
        TRegExSubst TargetUrlSubst;
    };

private:
    TVector<TCustomScheme> CustomSchemes;
};

}
