#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NKqp {

// One successfully committed dynamic rule application.  Ordinals are
// optimizer-wide, one-based, and contiguous within a diagnostic capture run.
struct TRBORuleApplicationV1 {
    ui64 Ordinal = 0;
    TString StageName;
    TString RuleName;
};

} // namespace NKikimr::NKqp
