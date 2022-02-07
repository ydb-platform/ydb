#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

TString EscapeChString(TStringBuf in, bool ident = false);

} // namespace NYql
