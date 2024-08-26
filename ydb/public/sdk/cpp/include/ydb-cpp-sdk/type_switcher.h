#pragma once

#ifdef YDB_SDK_USE_STD_STRING

#include <string>
namespace NYdb {
using TStringType = std::string;
}

#else

#include <util/generic/string.h>
namespace NYdb {
using TStringType = TString;
}

#endif
