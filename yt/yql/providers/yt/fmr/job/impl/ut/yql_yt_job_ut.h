#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYql::NFmr {
    // helper functions for yson reformatting

    TString GetBinaryYson(const TString& textYsonContent);

    TString GetTextYson(const TString& binaryYsonContent);

}  // namespace NYql::NFmr
