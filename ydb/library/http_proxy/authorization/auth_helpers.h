#pragma once
#include <util/generic/is_in.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/map.h>

namespace NKikimr::NSQS {

TMap<TString, TString> ParseAuthorizationParams(TStringBuf value); // throws on error

} // namespace NKikimr::NSQS
