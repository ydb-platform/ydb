#pragma once

#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

TCardinalityHints::TCardinalityHint* FindCardHint(TVector<TString>& labels, TCardinalityHints& hints);
TCardinalityHints::TCardinalityHint* FindBytesHint(TVector<TString>& labels, TCardinalityHints& hints);
std::shared_ptr<TOptimizerStatistics> ApplyBytesHints(std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    TCardinalityHints hints);
std::shared_ptr<TOptimizerStatistics> ApplyRowsHints(
    std::shared_ptr<TOptimizerStatistics>& inputStats,
    TVector<TString>& labels,
    TCardinalityHints hints);

} // namespace NKikimr::NKqp
