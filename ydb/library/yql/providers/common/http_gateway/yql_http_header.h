#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/string.h>

namespace NYql {

struct THttpHeader {
    struct TOptions {
        TString UserPwd;
        TString AwsSigV4;
        bool CurlSignature = false;
    };

    TSmallVec<TString> Fields;
    TOptions Options;
};

}
