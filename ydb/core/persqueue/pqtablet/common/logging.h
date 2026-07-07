#pragma once

#include <ydb/library/actors/core/log.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

inline TString LogPrefix() { return {}; }


#define Y_ABORT_UNLESS_S(expr, msg) Y_ABORT_UNLESS(expr, "%s", (TStringBuilder() << msg).data())

} // namespace NKikimr::NPQ
