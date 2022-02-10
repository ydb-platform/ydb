#pragma once
#include "defs.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NSQS {

// SHA-256 encoded in hex
TString CalcSHA256(TStringBuf);

} // namespace NKikimr::NSQS
