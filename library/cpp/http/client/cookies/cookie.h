#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NHttp {
    struct TCookie {
        TString Name;
        TString Value;
        TString Domain;
        TString Path;
        TString Expires;
        int MaxAge = -1;
        bool IsSecure = false;
        bool IsHttpOnly = false;

        static TCookie Parse(const TString& header);
    };

}
