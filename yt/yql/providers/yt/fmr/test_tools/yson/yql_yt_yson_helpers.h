#pragma once

#include <library/cpp/yson/writer.h>

namespace NYql::NFmr {
    // helper functions for yson reformatting

    TString GetBinaryYson(const TString& textYsonContent, NYson::EYsonType nodeType = NYson::EYsonType::ListFragment);

    TString GetTextYson(const TString& binaryYsonContent);

}  // namespace NYql::NFmr
