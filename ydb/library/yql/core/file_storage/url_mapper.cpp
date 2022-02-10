#include "url_mapper.h"

namespace NYql {

void TUrlMapper::AddMapping(const TString& pattern, const TString& targetUrl) {
    CustomSchemes.push_back(TCustomScheme(pattern, targetUrl));
}

bool TUrlMapper::MapUrl(const TString& url, TString& mappedUrl) const {
    for (const auto& sc : CustomSchemes) {
        if (sc.Pattern.Match(url.data())) {
            mappedUrl = TRegExSubst(sc.TargetUrlSubst).Replace(url.data());
            return true;
        }
    }
    return false;
}

}
