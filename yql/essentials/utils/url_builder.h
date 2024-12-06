#pragma once

#include <vector>
#include <util/string/builder.h>

namespace NYql {

class TUrlBuilder {
    struct TParam {
        TString Name;
        TString Value;
    };
public:
    explicit TUrlBuilder(const TString& uri);

    // Assuming name is already escaped, do not use strings from user input
    TUrlBuilder& AddUrlParam(const TString& name, const TString& value = "");
    TUrlBuilder& AddPathComponent(const TString& value);

    TString Build() const;
private:
    std::vector<TParam> Params;
    TString MainUri;
};

} // NYql
