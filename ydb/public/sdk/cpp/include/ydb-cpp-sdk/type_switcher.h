#pragma once

#ifdef YDB_SDK_USE_TSTRING
#include <util/generic/string.h>
namespace NYdb {
using TStringType = TString;
}
#else
#include <string>
namespace NYdb {
using TStringType = std::string;
}
#endif
