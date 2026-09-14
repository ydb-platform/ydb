#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatter
    : private TNonCopyable
{
public:
    TPatternFormatter& SetProperty(std::string name, std::string value);
    std::string Format(TStringBuf pattern);

private:
    std::unordered_map<std::string, std::string> PropertyMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
