#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <utility>


namespace NYql {

struct IUrlPreprocessing: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IUrlPreprocessing>;
    // Returns pair of <new url>, <url alias>
    virtual std::pair<TString, TString> Preprocess(const TString& url) = 0;
};

}
