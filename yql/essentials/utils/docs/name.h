#pragma once

#include <contrib/libs/re2/re2/re2.h>

#include <util/string/builder.h>

namespace NYql::NDocs {

    extern const RE2 NormalizedNameRegex;

    bool IsNormalizedName(TStringBuf name);

    TMaybe<TString> NormalizedName(TString name);

    bool IsUDF(TStringBuf name);

    TMaybe<std::pair<TString, TString>> SplitUDF(TString name);

} // namespace NYql::NDocs
