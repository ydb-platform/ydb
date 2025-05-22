#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

TString BuildTablePath(TStringBuf prefixPath, TStringBuf path);

}
