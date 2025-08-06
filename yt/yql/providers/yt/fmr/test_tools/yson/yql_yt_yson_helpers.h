#pragma once

#include <library/cpp/yson/writer.h>

namespace NYql::NFmr {
    // helper functions for yson reformatting

    TString GetBinaryYson(const TString& textYsonContent);

    TString GetTextYson(const TString& binaryYsonContent);

}  // namespace NYql::NFmr
