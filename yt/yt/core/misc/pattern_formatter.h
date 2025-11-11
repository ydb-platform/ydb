#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatter
    : private TNonCopyable
{
public:
    void AddProperty(const TString& name, const TString& value);
    TString Format(const TString& pattern);

private:
    using TPropertyMap = THashMap<TString, TString>;
    TPropertyMap PropertyMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
