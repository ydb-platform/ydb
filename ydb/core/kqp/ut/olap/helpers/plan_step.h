#pragma once
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

// Advances the database plan step (coordinator/mediator time) by committing one benign, non-critical schema
// change on an existing column table/store found under `root`.
//
// Internal columnshard modifications (compaction/actualization results) are committed one plan step in the
// future (GetOutdatedStep()+1) so that no in-flight read can observe them. So, only scans from the future
// can observe them. So, we need to advance the plan step to the future.
void AdvancePlanStep(TKikimrRunner& kikimr, const TString& root = "/Root");

}   // namespace NKikimr::NKqp
