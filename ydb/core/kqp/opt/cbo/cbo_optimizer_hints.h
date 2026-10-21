#pragma once

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

// Table Rows/Bytes hints are accepted by query alias, full table path or short table name.
THashSet<TString> BuildTableHintCandidates(const TString& alias, const TString& path);

void ApplySingleLabelHint(TCardinalityHints& hints, const THashSet<TString>& candidates, double& target);

} // namespace NKikimr::NKqp
