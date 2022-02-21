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

    void AddUrlParam(const TString& name, const TString& value); // Assuming name is already escaped, do not use strings from user input

    TString Build() const;
private:
    std::vector<TParam> Params;
    TString MainUri;
};

} // NYql
